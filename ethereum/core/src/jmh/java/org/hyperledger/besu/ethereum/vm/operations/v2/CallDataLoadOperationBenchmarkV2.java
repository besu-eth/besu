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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.vm.operations.GasCostBenchmark;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.operation.CallDataLoadOperationV2;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class CallDataLoadOperationBenchmarkV2 implements GasCostBenchmark {

  // a call with a selector and eight static ABI arguments
  private static final int CALL_DATA_SIZE = 4 + 8 * 32;

  private CallDataLoadOperationV2 operation;
  private MessageFrame frame;
  private long[] offsetPool;
  private int index;

  @Setup
  public void setup() {
    operation = new CallDataLoadOperationV2(new BerlinGasCalculator());
    final byte[] callData = new byte[CALL_DATA_SIZE];
    new Random(BenchmarkHelperV2.SEED).nextBytes(callData);
    frame = BenchmarkHelperV2.messageCallFrameBuilder().inputData(Bytes.wrap(callData)).build();
    offsetPool = offsetPool();
    index = 0;
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getVeryLowTierGasCost();
  }

  @Override
  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    // CALLDATALOAD overwrites its offset, so push it again on every iteration
    final long[] stack = frame.stackDataV2();
    stack[0] = 0L;
    stack[1] = 0L;
    stack[2] = 0L;
    stack[3] = offsetPool[index];
    frame.setTopV2(1);
    blackhole.consume(operation.execute(frame, null));
    frame.setTopV2(0);
    index = (index + 1) % BenchmarkHelperV2.SAMPLE_SIZE;
  }

  /**
   * Creates the seeded pool of offsets: mostly the selector and the arguments, which lie wholly in
   * the call data, and now and then a word that runs past its end.
   *
   * @return the pool
   */
  static long[] offsetPool() {
    final Random random = new Random(BenchmarkHelperV2.SEED);
    final long[] pool = new long[BenchmarkHelperV2.SAMPLE_SIZE];
    for (int i = 0; i < pool.length; i++) {
      final int slot = random.nextInt(10);
      pool[i] =
          switch (slot) {
            case 0 -> 0L; // selector
            case 9 -> CALL_DATA_SIZE - 16; // a word that runs past the end
            default -> 4L + 32L * (slot - 1); // arguments at 4, 36, ..., 228
          };
    }
    return pool;
  }
}
