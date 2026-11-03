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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Runs one memory operation {@value #REPETITIONS} times through the EVM v2 loop without a tracer,
 * as block import does. Unlike calling the operation directly, this lets the loop tell the frame
 * that no tracer reads its memory updates. Run with {@code -prof gc} for the allocation per
 * operation.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class MemoryOperationBenchmarkV2 {

  private static final int REPETITIONS = 1_000;
  private static final byte[] RAW = pattern(16 + 260);
  // A selector and eight words. A transaction's call data is a slice of its RLP, so not at offset 0
  private static final Bytes CALL_DATA = Bytes.wrap(RAW, 16, 260);
  // A child's output is a whole array, as memory hands it out
  private static final Bytes RETURN_DATA = Bytes.wrap(Arrays.copyOf(RAW, 128));

  /** One step per operation: its arguments, the operation, and a POP for what it pushes. */
  public enum Program {
    // PUSH1 0 MLOAD POP
    MLOAD("60005150"),
    // PUSH1 0x2a PUSH1 0 MSTORE
    MSTORE("602a600052"),
    // PUSH1 0x2a PUSH1 0 MSTORE8
    MSTORE8("602a600053"),
    // PUSH1 96 PUSH1 0 PUSH1 0 CALLDATACOPY
    CALLDATACOPY("606060006000" + "37"),
    // CALLDATASIZE PUSH1 0 PUSH1 0 CALLDATACOPY, all of it, as a proxy forwards it
    CALLDATACOPY_ALL("3660006000" + "37"),
    // PUSH1 96 CALLDATASIZE PUSH1 0 CALLDATACOPY, past its end, to zero memory
    CALLDATACOPY_ZERO("6060" + "36" + "6000" + "37"),
    // PUSH1 96 PUSH1 0 PUSH1 0 CODECOPY
    CODECOPY("606060006000" + "39"),
    // PUSH2 1024 PUSH1 0 PUSH1 0 CODECOPY
    CODECOPY_1K("610400" + "6000" + "6000" + "39"),
    // PUSH1 96 PUSH1 0 PUSH1 0 RETURNDATACOPY
    RETURNDATACOPY("606060006000" + "3e"),
    // RETURNDATASIZE PUSH1 0 PUSH1 0 RETURNDATACOPY, all of it, as a proxy returns it
    RETURNDATACOPY_ALL("3d60006000" + "3e"),
    // PUSH1 96 PUSH1 0 PUSH1 0x80 MCOPY, between ranges that don't overlap
    MCOPY("606060006080" + "5e"),
    // PUSH2 1024 PUSH1 0 PUSH2 1024 MCOPY, between ranges that don't overlap
    MCOPY_1K("610400" + "6000" + "610400" + "5e");

    private final Bytes code;

    Program(final String step) {
      final Bytes once = Bytes.fromHexString(step);
      final Bytes[] steps = new Bytes[REPETITIONS + 1];
      for (int i = 0; i < REPETITIONS; i++) {
        steps[i] = once;
      }
      steps[REPETITIONS] = Bytes.of(0x00); // STOP
      this.code = Bytes.concatenate(steps);
    }
  }

  @Param public Program program;

  private EVM evm;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    evm =
        MainnetEVMs.osaka(
            BigInteger.ONE,
            new EvmConfiguration(
                EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
                EvmConfiguration.DEFAULT.worldUpdaterMode(),
                EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
                true));
    frame =
        BenchmarkHelperV2.messageCallFrameBuilder()
            .code(new Code(program.code))
            .inputData(CALL_DATA)
            .build();
    frame.setReturnData(RETURN_DATA);
  }

  private static byte[] pattern(final int size) {
    final byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i * 31 + 7);
    }
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(REPETITIONS)
  public MessageFrame runProgram() {
    frame.setPC(0);
    frame.setGasRemaining(Long.MAX_VALUE);
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    evm.runToHalt(frame, OperationTracer.NO_TRACING);
    return frame;
  }
}
