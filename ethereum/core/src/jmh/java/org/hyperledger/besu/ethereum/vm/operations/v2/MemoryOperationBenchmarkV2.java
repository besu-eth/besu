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
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.v2.operation.MloadOperationV2;
import org.hyperledger.besu.evm.v2.operation.MstoreOperationV2;

import java.util.Random;
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

/** Benchmark for MSTORE and MLOAD V2 operations. */
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(value = TimeUnit.NANOSECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class MemoryOperationBenchmarkV2 {

  protected static final int SAMPLE_SIZE = 30_000;
  // Use offsets within a small window to avoid unbounded memory expansion cost
  private static final long MAX_OFFSET = 1024;

  private CancunGasCalculator gasCalculator;
  protected UInt256[] valuePool;
  protected long[] offsetPool;
  protected int index;
  protected MessageFrame frame;

  @Setup
  public void setUp() {
    gasCalculator = new CancunGasCalculator();
    frame = BenchmarkHelperV2.createMessageCallFrame();
    valuePool = new UInt256[SAMPLE_SIZE];
    offsetPool = new long[SAMPLE_SIZE];

    BenchmarkHelperV2.fillUInt256Pool(valuePool);
    final Random random = new Random(42);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      // Align offsets to 32-byte boundaries within the first 1KiB
      offsetPool[i] = (random.nextInt((int) (MAX_OFFSET / 32))) * 32L;
    }
    index = 0;

    // Pre-warm memory to avoid expansion costs during benchmark
    for (long off = 0; off < MAX_OFFSET + 32; off += 32) {
      frame.writeMemory(off, 32, org.apache.tuweni.bytes.Bytes.EMPTY);
    }
  }

  @Benchmark
  public void mstore(final Blackhole blackhole) {
    // Push offset then value; MSTORE pops both
    pushOffset(offsetPool[index]);
    BenchmarkHelperV2.pushUInt256(frame, valuePool[index]);
    blackhole.consume(MstoreOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator));
    index = (index + 1) % SAMPLE_SIZE;
  }

  @Benchmark
  public void mload(final Blackhole blackhole) {
    // Push offset; MLOAD replaces it with the 32 bytes read from memory
    pushOffset(offsetPool[index]);
    blackhole.consume(MloadOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator));
    // MLOAD leaves one item on the stack; pop it
    frame.setTopV2(frame.stackTopV2() - 1);
    index = (index + 1) % SAMPLE_SIZE;
  }

  private void pushOffset(final long offset) {
    final long[] s = frame.stackDataV2();
    final int t = frame.stackTopV2();
    final int dst = t << 2;
    s[dst] = 0;
    s[dst + 1] = 0;
    s[dst + 2] = 0;
    s[dst + 3] = offset;
    frame.setTopV2(t + 1);
  }
}
