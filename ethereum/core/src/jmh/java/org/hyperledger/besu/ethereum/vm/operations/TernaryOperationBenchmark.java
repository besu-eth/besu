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
import org.hyperledger.besu.evm.operation.Operation;

import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
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

@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public abstract class TernaryOperationBenchmark {

  protected static final int SAMPLE_SIZE = 30_000;

  protected Bytes[] aPool;
  protected Bytes[] bPool;
  protected Bytes[] cPool;
  protected int index;
  protected MessageFrame frame;

  @Setup()
  public void setUp() {
    frame = BenchmarkHelper.createMessageCallFrame();
    aPool = new Bytes[SAMPLE_SIZE];
    bPool = new Bytes[SAMPLE_SIZE];
    cPool = new Bytes[SAMPLE_SIZE];
    BenchmarkHelper.fillPool(aPool);
    BenchmarkHelper.fillPool(bPool);
    BenchmarkHelper.fillPool(cPool);
    index = 0;
  }

  @Benchmark
  public void executeOperation(final Blackhole blackhole) {
    frame.pushStackItem(cPool[index]);
    frame.pushStackItem(bPool[index]);
    frame.pushStackItem(aPool[index]);

    blackhole.consume(invoke(frame));

    frame.popStackItem();

    index = (index + 1) % SAMPLE_SIZE;
  }

  protected abstract Operation.OperationResult invoke(MessageFrame frame);
}
