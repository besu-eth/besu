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

import org.hyperledger.besu.evm.UInt256;

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

/**
 * Benchmarks the shared UInt256 MUL implementation with unequal operand widths in both orders.
 * Operand generation is done outside the measured method to isolate multiplication cost.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class UInt256MulBenchmark {
  private static final int SAMPLE_SIZE = 1024;
  private static final int SAMPLE_MASK = SAMPLE_SIZE - 1;

  @Param({"WIDE_NARROW", "NARROW_WIDE", "WIDE_WIDE", "NARROW_NARROW"})
  public String operandOrder;

  private UInt256[] left;
  private UInt256[] right;
  private int index;

  @Setup
  public void setup() {
    final Random random = new Random(11717);
    left = new UInt256[SAMPLE_SIZE];
    right = new UInt256[SAMPLE_SIZE];
    for (int i = 0; i < SAMPLE_SIZE; i++) {
      final UInt256 narrow = newUInt256(random, 8);
      final UInt256 wide = newUInt256(random, 32);
      switch (operandOrder) {
        case "WIDE_NARROW" -> {
          left[i] = wide;
          right[i] = narrow;
        }
        case "NARROW_WIDE" -> {
          left[i] = narrow;
          right[i] = wide;
        }
        case "WIDE_WIDE" -> {
          left[i] = wide;
          right[i] = newUInt256(random, 32);
        }
        case "NARROW_NARROW" -> {
          left[i] = narrow;
          right[i] = newUInt256(random, 8);
        }
        default -> throw new IllegalArgumentException(operandOrder);
      }
    }
  }

  private static UInt256 newUInt256(final Random random, final int byteLength) {
    final byte[] bytes = new byte[byteLength];
    random.nextBytes(bytes);
    bytes[0] |= (byte) 0x80; // Ensure an exact unsigned 64-bit or 256-bit width.
    return UInt256.fromBytesBE(bytes);
  }

  @Benchmark
  public UInt256 multiply() {
    final int current = index;
    index = (index + 1) & SAMPLE_MASK;
    return left[current].mul(right[current]);
  }
}
