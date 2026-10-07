/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.datatypes;

import java.util.Optional;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * StorageSlotKey represents a key used for storage slots in Ethereum. It contains the hash of the
 * slot key and an optional representation of the key itself.
 *
 * <p>The class provides methods for accessing the hash and key, as well as methods for equality,
 * hashcode, and comparison.
 *
 * <p>StorageSlotKey is used to uniquely identify storage slots within the Ethereum state.
 */
public class StorageSlotKey implements Comparable<StorageSlotKey> {

  private final Hash slotHash;
  private final Optional<UInt256> slotKey;

  /**
   * Creates a StorageSlotKey.
   *
   * @param slotHash Hashed storage slot key.
   * @param slotKey Optional UInt256 storage slot key.
   */
  public StorageSlotKey(final Hash slotHash, final Optional<UInt256> slotKey) {
    this.slotHash = slotHash;
    this.slotKey = slotKey;
  }

  /**
   * Creates a StorageSlotKey, hashing the slotKey.
   *
   * @param slotKey the UInt256 storage slot key.
   */
  public StorageSlotKey(final UInt256 slotKey) {
    this(Hash.hash(slotKey), Optional.of(slotKey));
  }

  /**
   * Gets the hash representation of the storage slot key.
   *
   * @return the hash of the storage slot key.
   */
  public Hash getSlotHash() {
    return slotHash;
  }

  /**
   * Gets the optional UInt256 representation of the storage slot key.
   *
   * @return an Optional containing the UInt256 storage slot key if present, otherwise an empty
   *     Optional.
   */
  public Optional<UInt256> getSlotKey() {
    return slotKey;
  }

  /**
   * Indicates whether some other object is "equal to" this one. Two StorageSlotKey objects are
   * considered equal if their slot hash values are equal.
   *
   * @param o the reference object with which to compare.
   * @return true if this object is the same as the obj argument; false otherwise.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageSlotKey that = (StorageSlotKey) o;
    // Hash compares the arrays; Bytes.equals reads both values byte by byte through an interface
    return slotHash.equals(that.slotHash);
  }

  @Override
  public int hashCode() {
    return slotHash.getBytes().hashCode();
  }

  @Override
  public String toString() {
    return String.format(
        "StorageSlotKey{slotHash=%s, slotKey=%s}",
        slotHash, slotKey.map(UInt256::toString).orElse("null"));
  }

  @Override
  public int compareTo(@NotNull final StorageSlotKey other) {
    return this.slotHash.getBytes().compareTo(other.slotHash.getBytes());
  }
}
