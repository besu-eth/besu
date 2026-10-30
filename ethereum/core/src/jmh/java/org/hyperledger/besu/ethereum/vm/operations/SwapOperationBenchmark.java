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
package org.hyperledger.besu.ethereum.vm.operations;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SwapOperation;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

/** JMH benchmark for the SWAP1-16 operation, which exchanges the top with the (n+1)-th item. */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class SwapOperationBenchmark implements GasCostBenchmark {

  private static final int STACK_DEPTH = 32;

  @Param({"1", "8", "16"})
  private int index;

  private MessageFrame frame;

  @Setup
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();
    final Bytes[] values = new Bytes[STACK_DEPTH];
    BenchmarkHelper.fillPool(values);
    for (final Bytes value : values) {
      frame.pushStackItem(value);
    }
  }

  @Override
  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    blackhole.consume(SwapOperation.staticOperation(frame, index));
  }

  @Override
  public long getGasCost(final BenchmarkParams params, final GasCalculator calc) {
    return new SwapOperation(index, calc).getGasCost();
  }
}
