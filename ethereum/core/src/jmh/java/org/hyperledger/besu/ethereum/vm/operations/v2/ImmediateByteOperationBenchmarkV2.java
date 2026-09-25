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

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Base benchmark class for EVM V2 operations that use immediate byte operands.
 *
 * <p>This class provides common setup for operations like DUPN, SWAPN, and EXCHANGE (EIP-8024)
 * which read an immediate byte following the opcode to determine their behaviour.
 */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public abstract class ImmediateByteOperationBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;
  protected static final int STACK_DEPTH = 50;

  protected UInt256[] valuePool;
  protected int index;
  protected MessageFrame frame;
  protected byte[] code;

  @Setup
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();
    valuePool = new UInt256[SAMPLE_SIZE];
    BenchmarkHelperV2.fillUInt256Pool(valuePool);
    index = 0;

    code = new byte[] {(byte) getOpcode(), getImmediate()};

    for (int i = 0; i < STACK_DEPTH; i++) {
      BenchmarkHelperV2.pushUInt256(frame, valuePool[i]);
    }
  }

  /**
   * Returns the opcode for this operation.
   *
   * @return the opcode value
   */
  protected abstract int getOpcode();

  /**
   * Returns the immediate byte operand for this operation.
   *
   * @return the immediate byte value
   */
  protected abstract byte getImmediate();

  /**
   * Invokes the operation.
   *
   * @param frame the message frame
   * @param code the bytecode array containing opcode and immediate
   * @param pc the program counter (typically 0 for benchmarks)
   * @return the operation result
   */
  protected abstract Operation.OperationResult invoke(MessageFrame frame, byte[] code, int pc);

  /**
   * Returns the stack size change after operation execution. Positive means items were added,
   * negative means items were removed.
   *
   * @return the stack delta
   */
  protected abstract int getStackDelta();

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    blackhole.consume(invoke(frame, code, 0));

    int delta = getStackDelta();
    if (delta > 0) {
      frame.setTopV2(frame.stackTopV2() - delta);
    } else if (delta < 0) {
      for (int i = 0; i < -delta; i++) {
        BenchmarkHelperV2.pushUInt256(frame, valuePool[index]);
        index = (index + 1) % SAMPLE_SIZE;
      }
    }
  }
}
