/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.vm;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the EVM interpreter cost of transaction execution during block building, comparing the
 * two tracer configurations the block builder has used:
 *
 * <ul>
 *   <li>{@code SELECTION_TRACER} — what main's {@code BlockTransactionSelector} passes: a
 *       delegating tracer that checks the thread interrupt status in every hook. Its {@code
 *       isEnabled()} is true, so the EVM takes the traced path.
 *   <li>{@code NO_TRACING} — what the kill-switch branch passes when no plugin tracer is installed:
 *       the canonical no-op tracer, so the EVM takes the fast untraced path (which on the branch
 *       contains the disarmed kill-switch polls).
 * </ul>
 *
 * <p>{@code SELECTION_TRACER} has to be reproduced here rather than used directly: the real {@code
 * InterruptibleOperationTracer} lives in {@code :ethereum:blockcreation}, which this module's jmh
 * source set does not depend on, and the kill-switch branch deletes the class altogether, so a
 * benchmark that referenced it could not compile on both sides of the comparison. What is
 * reproduced is the whole per-operation path a default node runs, both layers of it, so the cost
 * measured is the real one:
 *
 * <ol>
 *   <li>{@code InterruptibleOperationTracer}: check the interrupt status, then call the delegate.
 *       Reporting {@code isEnabled() == true} is what puts the EVM on its traced path.
 *   <li>{@code TransactionSelectionServiceImpl.TracerAggregator}: {@code forEach} over the plugin
 *       tracers, with a lambda capturing the frame. The list is empty on a node with no
 *       transaction-selector plugin, which is the case measured here.
 * </ol>
 *
 * Dropping either layer would understate what block building pays today.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 4, time = 2)
public class BlockBuildingEvmLoopBenchmark {

  /** Mirrors TracerAggregator: the delegate the selection tracer wraps. */
  static final class TracerAggregator implements OperationTracer {
    private final List<OperationTracer> tracers;

    TracerAggregator(final List<OperationTracer> tracers) {
      this.tracers = tracers;
    }

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      tracers.forEach(tracer -> tracer.tracePreExecution(frame));
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final Operation.OperationResult operationResult) {
      tracers.forEach(tracer -> tracer.tracePostExecution(frame, operationResult));
    }
  }

  /** Mirrors InterruptibleOperationTracer: interrupt check, then delegation, in every hook. */
  static final class SelectionTracer implements OperationTracer {
    private final OperationTracer delegate;

    SelectionTracer(final OperationTracer delegate) {
      this.delegate = delegate;
    }

    private static void checkInterrupt() {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException("Transaction execution interrupted"));
      }
    }

    @Override
    public void tracePreExecution(final MessageFrame frame) {
      checkInterrupt();
      delegate.tracePreExecution(frame);
    }

    @Override
    public void tracePostExecution(
        final MessageFrame frame, final Operation.OperationResult operationResult) {
      checkInterrupt();
      delegate.tracePostExecution(frame, operationResult);
    }

    @Override
    public boolean isExtendedTracing() {
      checkInterrupt();
      return delegate.isExtendedTracing();
    }
  }

  /** EVM workloads representative of in-block contract execution. */
  public enum Workload {
    /**
     * A counted loop of stack/arithmetic/jump operations, the jump-heavy shape the interpreter runs
     * most: PUSH3 100000, then per iteration JUMPDEST(pc 4) PUSH1 1 SWAP1 SUB DUP1 PUSH1 4 JUMPI,
     * then POP STOP.
     */
    LOOP("0x620186a05b60019003806004575000"),
    /**
     * The same counted loop (25000 iterations) with a KECCAK256 of one word and an MSTORE of its
     * result in the body; the JUMPDEST and the JUMPI destination are pc 3 here.
     */
    HASH_LOOP("0x6161a85b602060002060005260019003806003575000"),
    /**
     * Jump-free compute filling the whole EIP-170 code size limit: PUSH1 1, PUSH1 2, ADD, POP
     * repeated 4096 times (24576 bytes, 16384 operations, stack-neutral), halting by running off
     * the end of the code. Every operation hits the fast cases of both interpreter loops, so on the
     * kill-switch branch the body contains no poll at all (polls sit on JUMPDEST and the generic
     * dispatch); a transaction of this shape is the poll-density opposite of the loop workloads.
     */
    STRAIGHT_LINE_COMPUTE(straightLineCompute());

    final Bytes code;

    Workload(final String hex) {
      this(Bytes.fromHexString(hex));
    }

    Workload(final Bytes code) {
      this.code = code;
    }

    private static Bytes straightLineCompute() {
      final byte[] unit = {0x60, 0x01, 0x60, 0x02, 0x01, 0x50}; // PUSH1 1, PUSH1 2, ADD, POP
      final byte[] code = new byte[24576]; // the EIP-170 limit, a multiple of the unit
      for (int i = 0; i < code.length; i += unit.length) {
        System.arraycopy(unit, 0, code, i, unit.length);
      }
      return Bytes.wrap(code);
    }
  }

  public enum TracerMode {
    SELECTION_TRACER,
    NO_TRACING
  }

  @Param public Workload workload;

  @Param public TracerMode tracerMode;

  @Param({"false", "true"})
  public boolean evmV2;

  private EVM evm;
  private MessageFrame frame;
  private OperationTracer tracer;

  @Setup
  public void setUp() {
    evm =
        MainnetEVMs.london(
            BigInteger.ONE,
            new EvmConfiguration(
                EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
                EvmConfiguration.DEFAULT.worldUpdaterMode(),
                EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
                evmV2));
    // empty plugin list: the default node, with no transaction-selector plugin
    tracer =
        tracerMode == TracerMode.SELECTION_TRACER
            ? new SelectionTracer(new TracerAggregator(List.of()))
            : null;
    frame =
        MessageFrame.builder()
            .enableEvmV2(evmV2)
            .worldUpdater(mock(WorldUpdater.class))
            .originator(Address.ZERO)
            .gasPrice(Wei.ONE)
            .blobGasPrice(Wei.ONE)
            .blockValues(mock(BlockValues.class))
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.ZERO)
            .type(MessageFrame.Type.MESSAGE_CALL)
            .initialGas(Long.MAX_VALUE)
            .address(Address.ZERO)
            .contract(Address.ZERO)
            .inputData(Bytes32.ZERO)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(new Code(workload.code))
            .completer(__ -> {})
            .build();
  }

  @Benchmark
  public MessageFrame runToHalt() {
    frame.setPC(0);
    frame.setGasRemaining(Long.MAX_VALUE);
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    evm.runToHalt(frame, tracer == null ? OperationTracer.NO_TRACING : tracer);
    return frame;
  }
}
