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

import org.hyperledger.besu.ethereum.vm.operations.GasCostBenchmark;
import org.hyperledger.besu.evm.fluent.SimpleBlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.operation.NumberOperationV2;

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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class NumberOperationBenchmarkV2 implements GasCostBenchmark {

  private NumberOperationV2 operation;
  private MessageFrame frame;
  private SimpleBlockValues blockValues;
  private long[] numberPool;
  private int index;

  @Setup
  public void setup() {
    operation = new NumberOperationV2(new BerlinGasCalculator());
    blockValues = new SimpleBlockValues();
    frame = BenchmarkHelperV2.messageCallFrameBuilder().blockValues(blockValues).build();
    numberPool = numberPool();
    index = 0;
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator gasCalculator) {
    return gasCalculator.getBaseTierGasCost();
  }

  @Override
  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    blockValues.setNumber(numberPool[index]);
    blackhole.consume(operation.execute(frame, null));
    frame.setTopV2(0);
    index = (index + 1) % BenchmarkHelperV2.SAMPLE_SIZE;
  }

  /**
   * Creates the seeded pool of block numbers. Shared with the V1 benchmark so both see the same
   * inputs.
   *
   * @return the pool
   */
  static long[] numberPool() {
    return BenchmarkHelperV2.randomLongPool(0, 1L << 32);
  }
}
