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
package org.hyperledger.besu.plugin.services.storage;

import org.hyperledger.besu.datatypes.Address;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** Stores and retrieves preimage mappings for the Ethereum world-state trie. */
public interface WorldStatePreimageStorage {

  /**
   * Returns the storage slot index preimage for the given storage trie key.
   *
   * @param trieKey whose associated with storage index being retrieved
   * @return an {@link Optional} containing the {@link UInt256} slot index, or empty if not found
   */
  Optional<UInt256> getStorageTrieKeyPreimage(Bytes32 trieKey);

  /**
   * Returns the account address preimage for the given account trie key
   *
   * @param trieKey whose associated with the {@link Address} is being retrieved
   * @return an {@link Optional} containing the {@link Address}, or empty if not found
   */
  Optional<Address> getAccountTrieKeyPreimage(Bytes32 trieKey);

  /**
   * Creates and returns a new {@link Updater} for batching preimage write operations.
   *
   * @return a new {@link Updater} scoped to a single write transaction
   */
  Updater updater();

  /** A write transaction for batching preimage insertions into world state preimage storage. */
  interface Updater {

    /**
     * Stages a mapping from a storage trie key to its original slot index preimage.
     *
     * @param trieKey the hash of the slot index
     * @param preimage the original {@link UInt256} storage slot index
     * @return this updater, to allow method chaining
     */
    Updater putStorageTrieKeyPreimage(Bytes32 trieKey, UInt256 preimage);

    /**
     * Stages a mapping from an account trie key to its original address preimage.
     *
     * @param trieKey the hash of the address
     * @param preimage the original {@link Address}
     * @return this updater, to allow method chaining
     */
    Updater putAccountTrieKeyPreimage(Bytes32 trieKey, Address preimage);

    /** Atomically persists all staged preimage mappings to the underlying storage. */
    void commit();

    /** Discards all staged preimage mappings without writing to the underlying storage. */
    void rollback();
  }
}
