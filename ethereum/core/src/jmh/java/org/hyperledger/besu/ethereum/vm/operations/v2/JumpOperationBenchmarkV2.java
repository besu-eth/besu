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
 * Runs {@value #REPETITIONS} jumps through the EVM v2 loop without a tracer, as block import does.
 * Each one jumps to the JUMPDEST right after it, so a step is the jump's arguments, the jump and
 * the JUMPDEST it lands on. The JUMPI that is not taken runs the same operations without a jump.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class JumpOperationBenchmarkV2 {

  private static final int REPETITIONS = 1_000;

  /** One step per jump, with the offset of the JUMPDEST after it as a PUSH2. */
  public enum Program {
    // PUSH2 dest JUMP JUMPDEST
    JUMP("", "56"),
    // PUSH1 1 PUSH2 dest JUMPI JUMPDEST
    JUMPI_TAKEN("6001", "57"),
    // PUSH1 0 PUSH2 dest JUMPI JUMPDEST
    JUMPI_NOT_TAKEN("6000", "57");

    private final Bytes code;

    Program(final String condition, final String jump) {
      final int stepSize = condition.length() / 2 + 5;
      final StringBuilder code = new StringBuilder();
      for (int i = 0; i < REPETITIONS; i++) {
        final int jumpDest = (i + 1) * stepSize - 1;
        code.append(condition).append(String.format("61%04x", jumpDest)).append(jump).append("5b");
      }
      code.append("00"); // STOP
      this.code = Bytes.fromHexString(code.toString());
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
    frame = BenchmarkHelperV2.messageCallFrameBuilder().code(new Code(program.code)).build();
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
