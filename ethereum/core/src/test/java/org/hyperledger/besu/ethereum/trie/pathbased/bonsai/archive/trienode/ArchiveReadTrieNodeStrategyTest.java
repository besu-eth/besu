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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveReadTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveHistoryReader historyReader;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    historyReader = new ArchiveHistoryReader(historyStore);
  }

  private static Bytes32 keccak(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void putArchive(final Bytes location, final long block, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.put(tx, ArchiveNodeKey.account(location), block, node);
    tx.commit();
  }

  private void putStorageArchive(
      final Hash accountHash, final Bytes location, final long block, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    historyStore.put(tx, ArchiveNodeKey.storage(accountHash.getBytes(), location), block, node);
    tx.commit();
  }

  @Test
  void nodeFoundAtTargetBlock() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");
    putArchive(location, 5L, node);

    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(5L, historyReader);
    assertThat(strategy.getFlatAccountTrieNode(location, keccak(node), storage)).contains(node);
  }

  @Test
  void unknownNodeReturnsEmpty() {
    final Bytes location = Bytes.of(0x0e);

    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(5L, historyReader);
    assertThat(strategy.getFlatAccountTrieNode(location, keccak(Bytes.of(0x99)), storage))
        .isEmpty();
  }

  @Test
  void storageNodeUsesStorageNamespacedKey() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes location = Bytes.of(0x01);
    final Bytes node = Bytes.fromHexString("0xffee");
    putStorageArchive(accountHash, location, 3L, node);

    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(3L, historyReader);
    assertThat(strategy.getFlatStorageTrieNode(accountHash, location, keccak(node), storage))
        .contains(node);
    // Account-trie key must not match the storage-trie entry
    assertThat(strategy.getFlatAccountTrieNode(location, keccak(node), storage)).isEmpty();
  }

  @Test
  void putFlatAccountTrieNodeThrows() {
    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(0L, historyReader);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    assertThatThrownBy(
            () ->
                strategy.putFlatAccountTrieNode(
                    storage, tx, Bytes.of(0x01), Bytes32.ZERO, Bytes.of(0x00)))
        .isInstanceOf(UnsupportedOperationException.class);
    tx.rollback();
  }

  @Test
  void putFlatStorageTrieNodeThrows() {
    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(0L, historyReader);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    assertThatThrownBy(
            () ->
                strategy.putFlatStorageTrieNode(
                    storage, tx, Hash.EMPTY, Bytes.of(0x01), Bytes32.ZERO, Bytes.of(0x00)))
        .isInstanceOf(UnsupportedOperationException.class);
    tx.rollback();
  }

  @Test
  void removeFlatAccountStateTrieNodeThrows() {
    final ArchiveReadTrieNodeStrategy strategy = new ArchiveReadTrieNodeStrategy(0L, historyReader);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    assertThatThrownBy(() -> strategy.removeFlatAccountStateTrieNode(storage, tx, Bytes.of(0x01)))
        .isInstanceOf(UnsupportedOperationException.class);
    tx.rollback();
  }
}
