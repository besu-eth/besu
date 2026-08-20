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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
  }

  private ArchiveTrieNodeStrategy strategyWithGate(final boolean gateOpen) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), historyStore, coverageTracker, () -> gateOpen);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private void put(final ArchiveTrieNodeStrategy strategy, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();
  }

  private void setStoredBlockNumber(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    tx.commit();
  }

  @Test
  void archivesFullNodeWhenGateOpen() {
    // Gate wide open (initial sync): block 0 (no prior stored block) must be archived.
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xdeadbeef");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).contains(node);
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void gateLatchesClosedOnFirstFalse() {
    final AtomicBoolean gate = new AtomicBoolean(true);
    final ArchiveTrieNodeStrategy strategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), historyStore, coverageTracker, gate::get);

    final Bytes locationA = Bytes.of(0x0a);
    final Bytes locationB = Bytes.of(0x0b);
    final Bytes locationC = Bytes.of(0x0c);

    // Block 1: gate open — should archive.
    setStoredBlockNumber(0L);
    put(strategy, locationA, Bytes.fromHexString("0xaaaa"));

    // Block 2: gate closes — latch triggers, nothing archived.
    gate.set(false);
    setStoredBlockNumber(1L);
    put(strategy, locationB, Bytes.fromHexString("0xbbbb"));

    // Block 3: gate re-opens but latch is already set — nothing archived.
    gate.set(true);
    setStoredBlockNumber(2L);
    put(strategy, locationC, Bytes.fromHexString("0xcccc"));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(locationA), 1L))
        .as("block 1 archived before latch")
        .isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(locationB), 2L))
        .as("block 2 not archived — gate was false")
        .isEmpty();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(locationC), 3L))
        .as("block 3 not archived — gate was latched")
        .isEmpty();
  }

  @Test
  void genesisArchivedEvenWhenGateClosed() {
    // No WORLD_BLOCK_NUMBER_KEY set → block = 0 (genesis), which bypasses the gate entirely.
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(false);
    final Bytes location = Bytes.of(0x01);
    final Bytes node = Bytes.fromHexString("0xaabb");

    put(strategy, location, node);

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).contains(node);
  }

  @Test
  void doesNotArchiveWhenGateClosedAndNotGenesis() {
    // Gate closed (at-head sync): node writes go live but must NOT be archived.
    // Store block 5 as the last committed block, making the current block 6.
    setStoredBlockNumber(5L);

    final ArchiveTrieNodeStrategy strategy = strategyWithGate(false);
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 6L)).isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }

  // --- gap 1: putFlatStorageTrieNode ---

  @Test
  void archivesStorageTrieNodeWhenGateOpen() {
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Hash accountHash = Hash.hash(Bytes.of(0xAA));
    final Bytes location = Bytes.of(0x0f);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx, accountHash, location, hash(node), node);
    tx.commit();

    assertThat(
            historyStore.getLatestBefore(
                ArchiveNodeKey.storage(accountHash.getBytes(), location), 0L))
        .contains(node);
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void doesNotArchiveStorageTrieNodeWhenGateClosed() {
    setStoredBlockNumber(5L);
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(false);
    final Hash accountHash = Hash.hash(Bytes.of(0xAA));
    final Bytes location = Bytes.of(0x0f);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx, accountHash, location, hash(node), node);
    tx.commit();

    assertThat(
            historyStore.getLatestBefore(
                ArchiveNodeKey.storage(accountHash.getBytes(), location), 6L))
        .isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }

  @Test
  void multipleNodesInSameTransactionBothArchivedWithOneCoverageRecord() {
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Bytes location1 = Bytes.of(0x01);
    final Bytes location2 = Bytes.of(0x02);
    final Bytes node1 = Bytes.fromHexString("0x1111");
    final Bytes node2 = Bytes.fromHexString("0x2222");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location1, hash(node1), node1);
    strategy.putFlatAccountTrieNode(storage, tx, location2, hash(node2), node2);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location1), 0L)).contains(node1);
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location2), 0L)).contains(node2);
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void accountReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    put(strategy, location, node);

    assertThat(strategy.getFlatAccountTrieNode(location, hash(node), storage)).contains(node);
  }

  @Test
  void storageReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Hash accountHash = Hash.hash(Bytes.of(0xBB));
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(storage, tx, accountHash, location, hash(node), node);
    tx.commit();

    assertThat(strategy.getFlatStorageTrieNode(accountHash, location, hash(node), storage))
        .contains(node);
  }

  @Test
  void removeDoesNotWriteToArchive() {
    setStoredBlockNumber(5L);
    final ArchiveTrieNodeStrategy strategy = strategyWithGate(true);
    final Bytes location = Bytes.of(0x0e);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.commit();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 6L)).isEmpty();
    assertThat(coverageTracker.hasArchiveBlock(6L)).isFalse();
  }
}
