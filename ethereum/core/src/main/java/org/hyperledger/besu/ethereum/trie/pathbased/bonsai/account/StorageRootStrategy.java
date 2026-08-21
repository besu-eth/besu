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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.PatriciaAccountValue;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;

/**
 * Owns the storage-root state and behavior of a {@link BonsaiAccount}.
 *
 * <p>The strategy holds the root value and exposes the RLP field encoding/decoding as the 4-field
 * {@code [nonce, balance, storageRoot, codeHash]} account list. {@link BonsaiAccount} holds only a
 * {@code StorageRootStrategy} reference — no root data field of its own — so the root state lives
 * entirely in the strategy.
 *
 * <p>The strategy is injected by the caller (world state / accumulator / codec, which know the
 * active {@code DataStorageFormat}); {@code BonsaiAccount} never probes the format itself.
 */
public interface StorageRootStrategy {

  /**
   * The held storage root.
   *
   * @return the held root.
   */
  Hash getStorageRoot();

  /**
   * Updates the held storage root. Used by the MPT state-root committers.
   *
   * @param root the new storage root.
   */
  void setStorageRoot(Hash root);

  /**
   * Whether the held storage root is the empty-trie hash.
   *
   * @return {@code true} if the held root equals {@link Hash#EMPTY_TRIE_HASH}.
   */
  boolean isStorageEmpty();

  /**
   * Encodes the storage-root field into the RLP account list.
   *
   * @param out the RLP output, positioned between {@code balance} and {@code codeHash}.
   */
  void writeStorageRoot(RLPOutput out);

  /**
   * Decodes the storage-root field from the RLP account list, storing it in this strategy.
   *
   * @param in the RLP input, positioned between {@code balance} and {@code codeHash}.
   * @param listSize the element count of the enclosing account RLP list.
   * @return the decoded storage root.
   */
  Optional<Hash> readStorageRoot(RLPInput in, int listSize);

  /**
   * Returns an independent copy of this strategy.
   *
   * @return a copy of this strategy.
   */
  StorageRootStrategy copy();

  /**
   * Asserts this strategy's storage root matches the other account's storage root, throwing an
   * {@link IllegalStateException} if they differ. Used by account diffing.
   *
   * <p>Compares this strategy's held root against the other account's storage root (read via {@link
   * PatriciaAccountValue#getStorageRoot()} or {@link BonsaiAccount#getStorageRoot()}).
   *
   * @param other the account value to compare against.
   * @param context a description added to the thrown exception message.
   * @throws IllegalStateException if the two storage roots differ.
   */
  void assertStorageRootMatches(AccountValue other, String context);

  /**
   * Selects the strategy for the given storage format.
   *
   * @param dataStorageFormat the active {@code DataStorageFormat}.
   * @return a fresh {@link PatriciaStorageRootStrategy} holding {@link Hash#EMPTY_TRIE_HASH}.
   */
  static StorageRootStrategy forFormat(final DataStorageFormat dataStorageFormat) {
    return new PatriciaStorageRootStrategy(Hash.EMPTY_TRIE_HASH);
  }
}
