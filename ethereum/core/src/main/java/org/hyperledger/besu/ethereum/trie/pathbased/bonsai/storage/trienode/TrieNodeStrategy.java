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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Defines the strategy for storing and retrieving trie nodes in a flat key-value storage.
 * Implementations of this interface can define different strategies for how trie nodes are stored
 * and retrieved, such as using different key formats or storage segments.
 *
 * <p>Reads are keyed purely by {@code location}: callers are responsible for prefixing the account
 * hash into {@code location} when reading a storage-trie node (see {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage#getTrieNode}).
 *
 * <p>Writes take the account hash as an {@link Optional} ({@link Optional#empty()} for an
 * account-trie node, present for a storage-trie node) rather than a pre-concatenated location,
 * because implementations (e.g. the archive strategy) may need the raw account hash to index the
 * node separately from the live key.
 */
public interface TrieNodeStrategy {

  Optional<Bytes> getTrieNode(Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  void putTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Optional<Hash> accountHash,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);
}
