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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

/**
 * Owns the storage root of an account and its field in the account RLP, which depend on the trie
 * format.
 */
public interface StorageRootStrategy {

  /**
   * The held storage root.
   *
   * @return the storage root.
   */
  Hash getStorageRoot();

  /**
   * Updates the held storage root. Used by the state root committers.
   *
   * @param root the new storage root.
   */
  void setStorageRoot(Hash root);

  /**
   * Writes the storage root field of the account RLP list.
   *
   * @param out the RLP output, positioned between {@code balance} and {@code codeHash}.
   */
  void writeStorageRoot(RLPOutput out);

  /**
   * Reads the storage root field of the account RLP list and holds it.
   *
   * @param in the RLP input, positioned between {@code balance} and {@code codeHash}.
   */
  void readStorageRoot(RLPInput in);

  /**
   * Returns an independent copy, so that two accounts never share a mutable storage root.
   *
   * @return a copy of this strategy.
   */
  StorageRootStrategy copy();

  /**
   * Checks the held storage root against the one of another account. Used by account diffing.
   *
   * @param other the account value to compare against.
   * @param context a description added to the exception message.
   * @throws IllegalStateException if the storage roots differ.
   */
  void assertStorageRootMatches(AccountValue other, String context);
}
