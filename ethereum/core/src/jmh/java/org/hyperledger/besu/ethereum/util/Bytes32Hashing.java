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
package org.hyperledger.besu.ethereum.util;

import org.apache.tuweni.bytes.Bytes32;

/**
 * This class provides utility functions for generating hashcodes when Bytes32 instances are used as
 * keys. It may not be used right now, but it's a useful utility class to reproduce problems with
 * performance benchmarks in places where such keys are used.
 */
public final class Bytes32Hashing {

  private Bytes32Hashing() {
    throw new UnsupportedOperationException("utility class");
  }

  private static void writeZeroSumPair(final byte[] bytes, final int offset, final int digit) {
    switch (digit) {
      case 0 -> {
        bytes[offset] = 0;
        bytes[offset + 1] = 0;
      }
      case 1 -> {
        bytes[offset] = 1;
        bytes[offset + 1] = (byte) -31;
      }
      default -> {
        bytes[offset] = (byte) -1;
        bytes[offset + 1] = 31;
      }
    }
  }

  public static Bytes32 collidingHash(final int index) {
    final byte[] bytes = new byte[32];
    long remaining = index;
    for (int pair = 0; pair < 16; pair++) {
      writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
      remaining /= 3;
    }
    return Bytes32.wrap(bytes);
  }

  public static Bytes32 distinctHash(final int index) {
    final byte[] bytes = new byte[32];
    int remaining = index;
    for (int i = 0; i < 32; i++) {
      bytes[i] = (byte) (remaining % 31);
      remaining /= 31;
    }
    return Bytes32.wrap(bytes);
  }
}
