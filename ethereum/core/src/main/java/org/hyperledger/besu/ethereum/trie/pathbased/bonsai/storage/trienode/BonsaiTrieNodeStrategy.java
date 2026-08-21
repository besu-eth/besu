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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The Bonsai strategy for storing and retrieving trie nodes in a flat key-value storage. This
 * implementation uses a single segment for all trie nodes, with account trie nodes keyed by their
 * location and storage trie nodes keyed by a combination of the account hash and their location.
 */
public class BonsaiTrieNodeStrategy implements TrieNodeStrategy {

  private final SegmentIdentifier trieSegment;

  public BonsaiTrieNodeStrategy() {
    this(TRIE_BRANCH_STORAGE);
  }

  public BonsaiTrieNodeStrategy(final SegmentIdentifier trieSegment) {
    this.trieSegment = trieSegment;
  }

  @Override
  public Optional<Bytes> getTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return storage.get(trieSegment, location.toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public void putTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Optional<Hash> accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    final Bytes key =
        accountHash.map(hash -> Bytes.concatenate(hash.getBytes(), location)).orElse(location);
    transaction.put(trieSegment, key.toArrayUnsafe(), node.toArrayUnsafe());
  }
}
