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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.vm.operations.GasCostBenchmark;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.EVMExecutor;
import org.hyperledger.besu.evm.fluent.EvmSpec;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Nested calls without value on EVM v2. A driver calls a contract in a loop until its gas runs out,
 * and the contract calls itself with the call operation under test, passing on all its gas. One
 * operation is a transaction of {@value #GAS} gas, so {@code -PgasProfiler=true} also reports the
 * score in MGas/s.
 *
 * <p>With {@code depth=16} the driver gives each call just the gas to go 16 frames deep. With
 * {@code depth=max} it passes on all its gas, and the calls go several hundred frames deep.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class NestedCallBenchmarkV2 implements GasCostBenchmark {

  // the most gas a transaction may use since Osaka
  private static final long GAS = 16_777_216L;
  private static final Address DRIVER = Address.fromHexString("0x1000");
  private static final Address CONTRACT = Address.fromHexString("0x2000");
  private static final Bytes ALL_GAS = Bytes.of(0x5a); // GAS

  /** The call operation the contract calls itself with. */
  public enum CallOperation {
    CALL(0xf1, true),
    CALLCODE(0xf2, true),
    DELEGATECALL(0xf4, false),
    STATICCALL(0xfa, false);

    private final int opcode;
    private final boolean takesValue;

    CallOperation(final int opcode, final boolean takesValue) {
      this.opcode = opcode;
      this.takesValue = takesValue;
    }
  }

  @Param public CallOperation operation;

  @Param({"16", "max"})
  public String depth;

  private WorldStateArchive archive;
  private EVMExecutor executor;

  @Setup
  public void setUp() {
    final EvmConfiguration evmV2 =
        new EvmConfiguration(
            EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
            EvmConfiguration.DEFAULT.worldUpdaterMode(),
            EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
            true);
    archive = createBonsaiInMemoryWorldStateArchive(mock(Blockchain.class), evmV2, null);
    final MutableWorldState worldState = archive.getWorldState();
    final WorldUpdater setup = worldState.updater();
    setup.createAccount(CONTRACT, 1, Wei.ZERO).setCode(contractCode(operation));
    setup.commit();
    worldState.persist(null);

    executor =
        new EVMExecutor(EvmSpec.evmSpec(EvmSpecVersion.OSAKA, BigInteger.ONE, evmV2))
            .receiver(DRIVER)
            .contract(DRIVER)
            .worldUpdater(worldState.updater());

    final Bytes callGas;
    final int expectedDepth;
    if (depth.equals("max")) {
      callGas = ALL_GAS;
      expectedDepth = -1;
    } else {
      expectedDepth = Integer.parseInt(depth);
      callGas = push4(callGasToReach(expectedDepth));
    }

    final FrameTracer tracer = new FrameTracer();
    executor.code(driverCode(callGas)).gas(GAS).tracer(tracer).execute();
    // the score covers all the gas only if the transaction spends it
    if (tracer.rootHaltReason.filter(ExceptionalHaltReason.INSUFFICIENT_GAS::equals).isEmpty()) {
      throw new IllegalStateException("The driver did not run out of gas");
    }
    if (expectedDepth >= 0 && tracer.maxDepth != expectedDepth) {
      throw new IllegalStateException(
          "Expected calls " + expectedDepth + " deep, got " + tracer.maxDepth);
    }
    executor.tracer(OperationTracer.NO_TRACING);
  }

  @TearDown
  public void tearDown() throws Exception {
    archive.close();
  }

  @Benchmark
  @Override
  public void executeOperation(final Blackhole blackhole) {
    blackhole.consume(executor.execute());
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return GAS;
  }

  // searched, since the depth a call's gas reaches follows from the gas rules of every operation
  private long callGasToReach(final int targetDepth) {
    long low = 0;
    long high = GAS;
    while (low < high) {
      final long middle = (low + high) >>> 1;
      if (depthReached(middle) >= targetDepth) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    return low;
  }

  private int depthReached(final long callGas) {
    final FrameTracer tracer = new FrameTracer();
    // enough gas that the first call gets all of callGas
    executor.code(driverCode(push4(callGas))).gas(2 * callGas + 100_000).tracer(tracer).execute();
    return tracer.maxDepth;
  }

  private static Bytes contractCode(final CallOperation operation) {
    final byte[] push0s = new byte[operation.takesValue ? 5 : 4];
    Arrays.fill(push0s, (byte) 0x5f);
    return Bytes.concatenate(
        // PUSH0 for the return and argument ranges and, if the call takes one, the value
        Bytes.wrap(push0s),
        // ADDRESS, GAS, the call, POP
        Bytes.of(0x30, 0x5a, operation.opcode, 0x50));
  }

  private static Bytes driverCode(final Bytes pushCallGas) {
    return Bytes.concatenate(
        // JUMPDEST, PUSH0 for the return and argument ranges and the value, PUSH20
        Bytes.of(0x5b, 0x5f, 0x5f, 0x5f, 0x5f, 0x5f, 0x73),
        CONTRACT.getBytes(),
        pushCallGas,
        // CALL, POP, PUSH0, JUMP
        Bytes.of(0xf1, 0x50, 0x5f, 0x56));
  }

  private static Bytes push4(final long value) {
    return Bytes.concatenate(Bytes.of(0x63), Bytes.ofUnsignedInt(value));
  }

  private static final class FrameTracer implements OperationTracer {
    private int maxDepth;
    private Optional<ExceptionalHaltReason> rootHaltReason = Optional.empty();

    @Override
    public void traceContextEnter(final MessageFrame frame) {
      maxDepth = Math.max(maxDepth, frame.getDepth());
    }

    @Override
    public void traceContextExit(final MessageFrame frame) {
      if (frame.getDepth() == 0) {
        rootHaltReason = frame.getExceptionalHaltReason();
      }
    }

    @Override
    public boolean tracesOperations() {
      return false;
    }
  }
}
