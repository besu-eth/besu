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
 * <p>Hashcode is based on multiply-shift universal hashing (Dietzfelbinger) which is efficient
 * enough and has good collision probability for hash tables ({@code <= 2^-32} probability of
 * collision). Even in the case of collisions Java HashMaps treeifies buckets, iff this class
 * implements {@link Comparable} directly, and search goes O(log n) instead of O(n) which limits
 * effects of collisions.
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

  private static final VarHandle LONG_VIEW =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static final long SEED_0;
  private static final long SEED_1;
  private static final long SEED_2;
  private static final long SEED_3;
  private static final long SEED_4;
  private static final long SEED_5;
  private static final long SEED_6;

  static {
    final SecureRandom random = SecureRandomProvider.createSecureRandom();
    SEED_0 = random.nextLong() | 1L;
    SEED_1 = random.nextLong() | 1L;
    SEED_2 = random.nextLong() | 1L;
    SEED_3 = random.nextLong() | 1L;
    SEED_4 = random.nextLong() | 1L;
    SEED_5 = random.nextLong() | 1L;
    SEED_6 = random.nextLong() | 1L;
  }

  private static long word(final byte[] bytes, final int offset) {
    return (long) LONG_VIEW.get(bytes, offset);
  }

  /**
   * Dietzfelbinger based hash uses aligned byte reads as sizes are deterministic: Address is 20
   * bytes and Slot is 32 bytes.
   */
  @Override
  public int hashCode() {
    final long hash =
        word(slot, 0) * SEED_0
            + word(slot, 8) * SEED_1
            + word(slot, 16) * SEED_2
            + word(slot, 24) * SEED_3
            + word(address, 0) * SEED_4
            + word(address, 8) * SEED_5
            + word(address, 12) * SEED_6;
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
