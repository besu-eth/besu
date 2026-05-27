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

import static org.hyperledger.besu.ethereum.vm.operations.v2.BenchmarkHelperV2.bytesToUInt256;
import static org.hyperledger.besu.ethereum.vm.operations.v2.BenchmarkHelperV2.randomUInt256Value;

import org.hyperledger.besu.evm.UInt256;

import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

/**
 * Abstract base class for shift operation benchmarks (SHL, SHR, SAR).
 *
 * <p>Provides shared test case definitions and setup logic.
 */
public abstract class AbstractShiftOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {

  /** Test cases covering different execution paths for shift operations. */
  public enum Case {
    /** Shift by 0 - no shift needed. */
    SHIFT_0,
    /** Small shift by 1 bit. */
    SHIFT_1,
    /** Medium shift by 128 bits. */
    SHIFT_128,
    /** Large shift by 255 bits (max valid). */
    SHIFT_255,
    /** Overflow: shift of exactly 256. */
    OVERFLOW_SHIFT_256,
    /** Overflow: shift amount > 4 bytes. */
    OVERFLOW_LARGE_SHIFT,
    /** Random values (original behavior). */
    FULL_RANDOM
  }

  @Param protected Case scenario;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

    aPool = new UInt256[SAMPLE_SIZE]; // shift amount (pushed second, popped first)
    bPool = new UInt256[SAMPLE_SIZE]; // value (pushed first, popped second)

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      switch (scenario) {
        case SHIFT_0:
          aPool[i] = UInt256.ZERO;
          bPool[i] = randomUInt256Value(random);
          break;

        case SHIFT_1:
          aPool[i] = UInt256.ONE;
          bPool[i] = randomUInt256Value(random);
          break;

        case SHIFT_128:
          aPool[i] = UInt256.fromInt(128);
          bPool[i] = randomUInt256Value(random);
          break;

        case SHIFT_255:
          aPool[i] = UInt256.fromInt(255);
          bPool[i] = randomUInt256Value(random);
          break;

        case OVERFLOW_SHIFT_256:
          aPool[i] = UInt256.fromInt(256); // 256
          bPool[i] = randomUInt256Value(random);
          break;

        case OVERFLOW_LARGE_SHIFT:
          aPool[i] = bytesToUInt256(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00, 0x00});
          bPool[i] = randomUInt256Value(random);
          break;

        case FULL_RANDOM:
        default:
          final byte[] shift = new byte[1 + random.nextInt(4)];
          final byte[] value = new byte[1 + random.nextInt(32)];
          random.nextBytes(shift);
          random.nextBytes(value);
          aPool[i] = bytesToUInt256(shift);
          bPool[i] = bytesToUInt256(value);
          break;
      }
    }
    index = 0;
  }
}
