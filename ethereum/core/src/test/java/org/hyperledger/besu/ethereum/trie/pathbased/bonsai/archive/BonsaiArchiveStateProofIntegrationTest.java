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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveReadTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiArchiveStateProofIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveHistoryReader historyReader;
  private ArchiveTrieNodeStrategy archiveStrategy;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveCoverageTracker coverageTracker = new ArchiveCoverageTracker(storage);
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(storage);
    final BonsaiTrieNodeStrategy baseStrategy = new BonsaiTrieNodeStrategy();
    historyReader = new ArchiveHistoryReader(historyStore);
    // Gate always open: acts as initial-sync mode
    archiveStrategy =
        new ArchiveTrieNodeStrategy(baseStrategy, historyStore, coverageTracker, () -> true);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private Optional<Bytes> readAccountNode(
      final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getFlatAccountTrieNode(location, nodeHash, storage);
  }

  private Optional<Bytes> readStorageNode(
      final Hash accountHash, final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Test
  void archivedNodeIsRetrievableViaReadStrategy() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef01");

    // Block 0 (no WORLD_BLOCK_NUMBER_KEY in storage → block is 0)
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    assertThat(readAccountNode(0L, location, hash(node))).contains(node);
  }

  @Test
  void archivePathServesHistoricalNodeWhenLiveStateAdvanced() {
    final Bytes location = Bytes.of(0x0a);
    final Bytes nodeAtBlock0 = Bytes.fromHexString("0xaabb");
    final Bytes nodeAtBlock1 = Bytes.fromHexString("0xccdd");

    // --- Block 0 ---
    final SegmentedKeyValueStorageTransaction tx0 = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(
        storage, tx0, location, hash(nodeAtBlock0), nodeAtBlock0);
    tx0.commit();

    // Advance the stored block number to 0 (simulates what the block commit also writes)
    final SegmentedKeyValueStorageTransaction advance = storage.startTransaction();
    advance.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(0L).toArrayUnsafe());
    advance.commit();

    // --- Block 1 ---
    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(
        storage, tx1, location, hash(nodeAtBlock1), nodeAtBlock1);
    tx1.commit();

    assertThat(readAccountNode(0L, location, hash(nodeAtBlock0))).contains(nodeAtBlock0);
    assertThat(readAccountNode(1L, location, hash(nodeAtBlock1))).contains(nodeAtBlock1);
  }

  @Test
  void progressCoversBlockAfterArchive() {
    final Bytes location = Bytes.of(0x00);
    final Bytes node = Bytes.fromHexString("0x01");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    final ArchiveCoverageTracker loaded = new ArchiveCoverageTracker(storage);
    assertThat(loaded.hasArchiveBlock(0L)).isTrue();
    assertThat(loaded.hasArchiveBlock(1L)).isFalse(); // only block 0 was archived
  }

  @Test
  void readStrategyReturnsEmptyForUnarchivedBlock() {
    // Nothing written to storage
    final Bytes location = Bytes.of(0x0f);
    final Bytes phantomNode = Bytes.fromHexString("0x9999");
    assertThat(readAccountNode(5L, location, hash(phantomNode))).isEmpty();
  }

  @Test
  void archivesAndRetrievesStorageTrieNode() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes location = Bytes.of(0x01);
    final Bytes node = Bytes.fromHexString("0xffee");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatStorageTrieNode(storage, tx, accountHash, location, hash(node), node);
    tx.commit();

    assertThat(readStorageNode(accountHash, 0L, location, hash(node))).contains(node);
  }

  @Test
  void accountTrieReaderIgnoresStorageTrieEntries() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1234567812345678123456781234567812345678123456781234567812345678"));
    final Bytes storageLocation = Bytes.of(0x02);
    final Bytes storageNode = Bytes.fromHexString("0xabcd");

    // Write only to storage-trie archive
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatStorageTrieNode(
        storage, tx, accountHash, storageLocation, hash(storageNode), storageNode);
    tx.commit();

    // Account-trie reader must not return anything for the same location
    assertThat(readAccountNode(0L, storageLocation, hash(storageNode))).isEmpty();
  }
}
