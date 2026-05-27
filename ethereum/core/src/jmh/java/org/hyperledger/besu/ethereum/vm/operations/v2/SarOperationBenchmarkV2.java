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
import static org.hyperledger.besu.ethereum.vm.operations.v2.BenchmarkHelperV2.randomNegativeUInt256Value;
import static org.hyperledger.besu.ethereum.vm.operations.v2.BenchmarkHelperV2.randomPositiveUInt256Value;
import static org.hyperledger.besu.ethereum.vm.operations.v2.BenchmarkHelperV2.randomUInt256Value;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.operation.SarOperationV2;

import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

public class SarOperationBenchmarkV2 extends BinaryOperationBenchmarkV2 {

  /** Test cases covering different execution paths for SAR operations. */
  public enum Case {
    /** Shift by 0 - early return path. */
    SHIFT_0,
    /** Negative number with shift=1 - tests sign extension. */
    NEGATIVE_SHIFT_1,
    /** Value with all bits set with shift=1. */
    ALL_BITS_SHIFT_1,
    /** Positive number with shift=1 - no sign extension needed. */
    POSITIVE_SHIFT_1,
    /** Negative number with medium shift. */
    NEGATIVE_SHIFT_128,
    /** Negative number with max shift. */
    NEGATIVE_SHIFT_255,
    /** Positive number with medium shift. */
    POSITIVE_SHIFT_128,
    /** Positive number with max shift. */
    POSITIVE_SHIFT_255,
    /** Overflow: shift >= 256. */
    OVERFLOW_SHIFT_256,
    /** Overflow: shift amount > 4 bytes. */
    OVERFLOW_LARGE_SHIFT,
    /** Random values. */
    FULL_RANDOM
  }

  @Param protected Case scenario;

  @Setup(Level.Iteration)
  @Override
  public void setUp() {
    frame = BenchmarkHelperV2.createMessageCallFrame();

    aPool = new UInt256[SAMPLE_SIZE];
    bPool = new UInt256[SAMPLE_SIZE];

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = 0; i < SAMPLE_SIZE; i++) {
      switch (scenario) {
        case SHIFT_0:
          aPool[i] = UInt256.fromInt(0);
          bPool[i] = randomUInt256Value(random);
          break;

        case NEGATIVE_SHIFT_1:
          aPool[i] = UInt256.fromInt(1);
          bPool[i] = randomNegativeUInt256Value(random);
          break;

        case ALL_BITS_SHIFT_1:
          aPool[i] = UInt256.fromInt(1);
          bPool[i] = UInt256.MAX;
          break;

        case POSITIVE_SHIFT_1:
          aPool[i] = UInt256.fromInt(1);
          bPool[i] = randomPositiveUInt256Value(random);
          break;

        case NEGATIVE_SHIFT_128:
          aPool[i] = UInt256.fromInt(128);
          bPool[i] = randomNegativeUInt256Value(random);
          break;

        case NEGATIVE_SHIFT_255:
          aPool[i] = UInt256.fromInt(255);
          bPool[i] = randomNegativeUInt256Value(random);
          break;

        case POSITIVE_SHIFT_128:
          aPool[i] = UInt256.fromInt(128);
          bPool[i] = randomPositiveUInt256Value(random);
          break;
        case POSITIVE_SHIFT_255:
          aPool[i] = UInt256.fromInt(255);
          bPool[i] = randomPositiveUInt256Value(random);
          break;

        case OVERFLOW_SHIFT_256:
          aPool[i] = UInt256.fromInt(256);
          bPool[i] = randomUInt256Value(random);
          break;

        case OVERFLOW_LARGE_SHIFT:
          aPool[i] = bytesToUInt256(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00, 0x00});
          bPool[i] = randomUInt256Value(random);
          break;

        case FULL_RANDOM:
        default:
          final byte[] shift = new byte[1 + random.nextInt(2)];
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

  @Override
  protected Operation.OperationResult invoke(final MessageFrame frame) {
    return SarOperationV2.staticOperation(frame);
  }
}
