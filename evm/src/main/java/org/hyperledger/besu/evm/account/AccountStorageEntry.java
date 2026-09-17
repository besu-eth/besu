/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm.account;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** The Account storage entry. */
public class AccountStorageEntry {

  private final UInt256 value;
  private final Optional<UInt256> key;
  private final Bytes32 keyHash;

  private AccountStorageEntry(
      final UInt256 value, final Bytes32 keyHash, final Optional<UInt256> key) {
    this.key = key;
    this.keyHash = keyHash;
    this.value = value;
  }

  /**
   * Create account storage entry.
   *
   * @param value the value
   * @param keyHash the key hash
   * @param key the key
   * @return the account storage entry
   */
  public static AccountStorageEntry create(
      final UInt256 value, final Bytes32 keyHash, final UInt256 key) {
    return create(value, keyHash, Optional.ofNullable(key));
  }

  /**
   * Create account storage entry.
   *
   * @param value the value
   * @param keyHash the key hash
   * @param key the key
   * @return the account storage entry
   */
  public static AccountStorageEntry create(
      final UInt256 value, final Bytes32 keyHash, final Optional<UInt256> key) {
    return new AccountStorageEntry(value, keyHash, key);
  }

  /**
   * Create account storage entry for key and value.
   *
   * @param key the key
   * @param value the value
   * @return the account storage entry
   */
  public static AccountStorageEntry forKeyAndValue(final UInt256 key, final UInt256 value) {
    return create(value, Bytes32.wrap(Hash.hash(key).getBytes()), key);
  }

  /**
   * The original key used for this storage entry. The hash of this key is used to store the value
   * in the storage trie, so a preimage of the trie key must be available in order to provide the
   * original key value.
   *
   * @return If available, returns the original key corresponding to this storage entry.
   */
  public Optional<UInt256> getKey() {
    return key;
  }

  /**
   * The hash of the storage key. When inserting storage entries the hash of the original key is
   * used as the key. When iterating over values in the storage trie, only the hashedKey and value
   * are available.
   *
   * @return The hash of the storage key.
   */
  public Bytes32 getKeyHash() {
    return keyHash;
  }

  /**
   * Gets value.
   *
   * @return the value
   */
  public UInt256 getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof AccountStorageEntry that)) {
      return false;
    }
    return Objects.equals(value, that.value)
        && Objects.equals(key, that.key)
        && Objects.equals(keyHash, that.keyHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, key, keyHash);
  }

  @Override
  public String toString() {
    return "AccountStorageEntry{"
        + "key="
        + key
        + ", keyHash="
        + keyHash
        + ", value="
        + value
        + '}';
  }
}
