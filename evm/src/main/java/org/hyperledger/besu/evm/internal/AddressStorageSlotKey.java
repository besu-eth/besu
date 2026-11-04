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
package org.hyperledger.besu.evm.internal;

import org.hyperledger.besu.crypto.SecureRandomProvider;
import org.hyperledger.besu.datatypes.Address;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Key to use for lookups in Maps/Sets with an address and storage slot combined as a key.
 *
 * <p>The slot is fully attacker-controlled - e.g. it can be whatever a contract pushes to {@code
 * TSTORE} - so {@link #hashCode()} is a <em>seeded</em> hash, mixing in per-process random values
 * drawn once at class initialisation. Seed randomization is very important as it makes the hash
 * non-deterministic off chain so collisions can only be found out while executing EVM payloads
 * which costs gas.
 *
 * <p>{@link #hashCode()} is the multilinear family over {@code Z_2^64}: {@code h = LANES +
 * SUM(lane_i * seed_i)} accumulated modulo {@code 2^64}, returning the top 32 bits. Each {@code
 * lane_i} is a 32-bit chunk of the key and each {@code seed_i} a uniformly random 64-bit seed. The
 * family is <em>exactly strongly universal</em> on its 32-bit output: for any two distinct keys and
 * any two output values, {@code Pr[hashCode(x) == a and hashCode(y) == b] == 2^-64}. Two distinct
 * keys therefore collide with probability exactly {@code 2^-32}, and share a bucket of a {@code
 * 2^m} table with probability exactly {@code 2^-m} - the same as an ideal random function, whatever
 * keys are chosen. Choosing them cleverly gains an attacker nothing over choosing them at random.
 *
 * <p><strong>The lanes must stay narrow, and this is exact rather than a rule of thumb.</strong>
 * Let {@code t} be the number of trailing zero bits of the difference between two keys in some
 * lane. Conditioned on the other seeds the accumulator difference is uniform on the multiples of
 * {@code 2^t}, and the per-bucket overlap is a triangular kernel of half-width {@code 2^32}. A
 * triangular kernel is a linear B-spline, so it sums to a constant over any lattice whose spacing
 * divides its half-width. Hence the collision probability is exactly {@code 2^-32} while {@code t
 * <= 32}, and degrades to exactly {@code 2^(t-64)} beyond it. A {@code w}-bit lane admits {@code t
 * <= w-1}, so the guarantee holds while {@code w <= 33}; this code uses 32, with a bit to spare.
 */
public class AddressStorageSlotKey implements Comparable<AddressStorageSlotKey> {

  private final byte[] address;
  private final byte[] slot;

  /**
   * The AddressStorageSlotKey constructor.
   *
   * @param address the bytes from the address part of the key
   * @param slot the bytes from slot part of the key
   */
  public AddressStorageSlotKey(final Address address, final Bytes32 slot) {
    this.address = address.getBytes().toArrayUnsafe();
    this.slot = slot.toArrayUnsafe();
  }

  private static final VarHandle INT_VIEW =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

  /** Size of multiplier lanes */
  private static final int LANES = 13;

  /** randomly generated seeds. Last one is the base seed */
  private static final long[] SEEDS = new long[LANES + 1];

  static {
    final SecureRandom random = SecureRandomProvider.createSecureRandom();
    for (int i = 0; i < SEEDS.length; i++) {
      SEEDS[i] = random.nextLong();
    }
  }

  private static long lane(final byte[] bytes, final int offset) {
    return ((int) INT_VIEW.get(bytes, offset)) & 0xFFFF_FFFFL;
  }

  /**
   * Sizes are fixed - the address is 20 bytes and the slot is 32 - both divide evenly into 32-bit
   * lanes. The reads are little-endian over big-endian data, which is irrelevant for the hash.
   */
  @Override
  public int hashCode() {
    final long hash =
        SEEDS[LANES]
            + lane(slot, 0) * SEEDS[0]
            + lane(slot, 4) * SEEDS[1]
            + lane(slot, 8) * SEEDS[2]
            + lane(slot, 12) * SEEDS[3]
            + lane(slot, 16) * SEEDS[4]
            + lane(slot, 20) * SEEDS[5]
            + lane(slot, 24) * SEEDS[6]
            + lane(slot, 28) * SEEDS[7]
            + lane(address, 0) * SEEDS[8]
            + lane(address, 4) * SEEDS[9]
            + lane(address, 8) * SEEDS[10]
            + lane(address, 12) * SEEDS[11]
            + lane(address, 16) * SEEDS[12];
    return (int) (hash >>> 32);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof AddressStorageSlotKey other) {
      return Arrays.equals(address, other.address) && Arrays.equals(slot, other.slot);
    }
    return false;
  }

  @Override
  public int compareTo(final AddressStorageSlotKey other) {
    if (this == other) {
      return 0;
    }
    if (other == null) {
      return 1;
    }
    int compare = Arrays.compare(address, other.address);
    return compare != 0 ? compare : Arrays.compare(slot, other.slot);
  }
}
