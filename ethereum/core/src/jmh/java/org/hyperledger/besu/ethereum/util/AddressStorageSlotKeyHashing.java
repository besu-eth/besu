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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.tuweni.bytes.Bytes32;

public final class AddressStorageSlotKeyHashing {
  private AddressStorageSlotKeyHashing() {
    throw new UnsupportedOperationException("utility class");
  }

  private static long[] readSeeds() throws Exception {
    final long[] seeds = new long[7];
    for (int i = 0; i < 7; i++) {
      final Field f = AddressStorageSlotKey.class.getDeclaredField("SEED_" + i);
      f.setAccessible(true);
      seeds[i] = f.getLong(null);
    }
    return seeds;
  }

  /** Inverse mod 2^64 by Newton iteration; exists because the seeded multipliers are forced odd. */
  private static long inv(final long x) {
    long y = x;
    for (int i = 0; i < 6; i++) {
      y *= 2 - x * y;
    }
    return y;
  }

  /**
   * Algorithm: hash = s0*A0 + s1*A1 + s2*A2 + s3*A3 + a0*A4 + a1·A5 + a2*A6
   *
   * <p>hashCode = (int)(H >>> 32)
   *
   * <p>sN - slot limbs
   *
   * <p>aN - address limbs
   *
   * <p>AN - seeds
   *
   * <p>if hash collides then its integer shifted version (hashCode) will also collide
   */
  public static Bytes32 collidingHash(final Address address, final int index) throws Exception {
    final ByteBuffer addrBytes =
        ByteBuffer.wrap(address.getBytes().toArrayUnsafe()).order(ByteOrder.LITTLE_ENDIAN);
    final long[] seeds = readSeeds();
    final long k =
        addrBytes.getLong(0) * seeds[4]
            + addrBytes.getLong(8) * seeds[5]
            + addrBytes.getLong(12) * seeds[6];
    final long invA1 = inv(seeds[1]);

    final ByteBuffer slotBytes = ByteBuffer.wrap(new byte[32]).order(ByteOrder.LITTLE_ENDIAN);
    // s0 is free choice
    slotBytes.putLong(0, index);
    // solves for s1; s2 == s3 == hash = 0
    slotBytes.putLong(8, -invA1 * (k + index * seeds[0]));
    return Bytes32.wrap(slotBytes.array());
  }

  /**
   * Algorithm:
   *
   * <p>hash = s0*A0 + s1*A1 + s2*A2 + s3*A3 + a0*A4 + a1·A5 + a2*A6
   *
   * <p>hashCode = (int)(H >>> 32)
   *
   * <p>sN - slot limbs
   *
   * <p>aN - address limbs
   *
   * <p>AN - seeds
   *
   * <p>Computes `s1` as all other limbs are made zero. `index` controls high order limbs of `hash`
   * so there are no collisions for the whole size of the int.
   */
  public static Bytes32 distinctHash(final Address address, final int index) throws Exception {
    final ByteBuffer addrBytes =
        ByteBuffer.wrap(address.getBytes().toArrayUnsafe()).order(ByteOrder.LITTLE_ENDIAN);
    final long[] seeds = readSeeds();
    final long k =
        addrBytes.getLong(0) * seeds[4]
            + addrBytes.getLong(8) * seeds[5]
            + addrBytes.getLong(12) * seeds[6];
    final long invA1 = inv(seeds[1]);

    final ByteBuffer slotBytes = ByteBuffer.wrap(new byte[32]).order(ByteOrder.LITTLE_ENDIAN);
    // s0 = s2 = s3 = 0
    slotBytes.putLong(8, invA1 * ((((long) index) << 32) - k));
    return Bytes32.wrap(slotBytes.array());
  }
}
