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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.TrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A read-only {@link TrieNodeStrategy} that resolves trie-node RLP from the archive history store
 * for a fixed target block. Used by {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveWorldStateStorageCoordinator}
 * to serve historical proofs without bypassing the standard {@code getAccountStateTrieNode} /
 * {@code getAccountStorageTrieNode} path.
 *
 * <p>All write methods throw {@link UnsupportedOperationException}: this strategy is instantiated
 * only for proof reads, never for block imports.
 */
public final class ArchiveReadTrieNodeStrategy implements TrieNodeStrategy {

  private final long blockNumber;
  private final ArchiveHistoryReader historyReader;

  public ArchiveReadTrieNodeStrategy(
      final long blockNumber, final ArchiveHistoryReader historyReader) {
    this.blockNumber = blockNumber;
    this.historyReader = historyReader;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return historyReader.nodeAt(ArchiveNodeKey.account(location), blockNumber);
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return historyReader.nodeAt(
        ArchiveNodeKey.storage(accountHash.getBytes(), location), blockNumber);
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    throw new UnsupportedOperationException("read-only archive strategy");
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    throw new UnsupportedOperationException("read-only archive strategy");
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    throw new UnsupportedOperationException("read-only archive strategy");
  }
}
