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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.v2.operation.Keccak256OperationV2;

import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.infra.Blackhole;

/** Benchmark for KECCAK256 EVM v2 operation. */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class Keccak256OperationBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;

  @Param({"32", "64", "128", "256", "512"})
  public int dataSize;

  private CancunGasCalculator gasCalculator;
  private long[] offsetPool;
  private long[] sizePool;
  private int index;
  private MessageFrame frame;

  @Setup
  public void setUp() {
    gasCalculator = new CancunGasCalculator();
    frame = BenchmarkHelperV2.createMessageCallFrame();
    offsetPool = new long[SAMPLE_SIZE];
    sizePool = new long[SAMPLE_SIZE];

    final Random random = new Random(42);
    // Use offsets that keep data within a bounded window to avoid memory expansion cost noise
    final long maxOffset = 4096L;
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      sizePool[i] = dataSize;
      offsetPool[i] = (random.nextInt((int) (maxOffset / 32))) * 32L;
    }
    index = 0;

    // Pre-warm memory so expansion costs don't skew results
    final byte[] data = new byte[dataSize];
    random.nextBytes(data);
    for (long off = 0; off < maxOffset + dataSize; off += 32) {
      frame.writeMemory(off, 32, org.apache.tuweni.bytes.Bytes.EMPTY);
    }
    // Write benchmark data into the region we'll be hashing
    frame.writeMemory(0, dataSize, org.apache.tuweni.bytes.Bytes.wrap(data));
  }

  @Benchmark
  public void keccak256(final Blackhole blackhole) {
    // Push size then offset; KECCAK256 pops both and pushes hash
    pushLong(sizePool[index]);
    pushLong(offsetPool[index]);
    blackhole.consume(
        Keccak256OperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator));
    // Pop the resulting hash
    frame.setTopV2(frame.stackTopV2() - 1);
    index = (index + 1) % SAMPLE_SIZE;
  }

  private void pushLong(final long value) {
    final long[] s = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final int dst = top << 2;
    s[dst] = 0L;
    s[dst + 1] = 0L;
    s[dst + 2] = 0L;
    s[dst + 3] = value;
    frame.setTopV2(top + 1);
  }
}
