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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.LongPredicate;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveTrieNodeWriter capture;
  private List<BlockHeader> chain;

  @FunctionalInterface
  private interface Write {
    void apply(ArchiveTrieNodeStrategy strategy, SegmentedKeyValueStorageTransaction tx);
  }

  @BeforeEach
  void setUp() {
    storage = newStorage();
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    capture =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL,
            Optional.empty());
    chain = buildChain(BlockHeaderTestFixture::new, 6);
  }

  private static SegmentedKeyValueStorage newStorage() {
    return new SegmentedInMemoryKeyValueStorage(
        List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
  }

  private static List<BlockHeader> buildChain(
      final Supplier<BlockHeaderTestFixture> fixture, final int length) {
    final List<BlockHeader> headers = new ArrayList<>();
    Hash parentHash = Hash.EMPTY;
    for (int i = 0; i < length; i++) {
      final BlockHeader header = fixture.get().number(i).parentHash(parentHash).buildHeader();
      headers.add(header);
      parentHash = header.getHash();
    }
    return headers;
  }

  private ArchiveTrieNodeStrategy strategy() {
    return strategy(b -> true);
  }

  private ArchiveTrieNodeStrategy strategy(final LongPredicate isFinal) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), capture, coverageTracker, s -> s == storage, isFinal);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  private Write put(final Bytes key, final Bytes node) {
    return (strategy, tx) -> strategy.putTrieNode(storage, tx, key, hash(node), node);
  }

  private Write remove(final Bytes key) {
    return (strategy, tx) -> strategy.removeTrieNode(storage, tx, key);
  }

  private void setStoredBlockHash(final SegmentedKeyValueStorage target, final Hash blockHash) {
    final SegmentedKeyValueStorageTransaction tx = target.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.getBytes().toArrayUnsafe());
    tx.commit();
  }

  // Persists chain block {@code block} as a single step on canonical storage.
  private void persistBlock(
      final ArchiveTrieNodeStrategy strategy, final long block, final Write... writes) {
    if (block > 0) {
      setStoredBlockHash(storage, chain.get((int) block - 1).getHash());
    }
    persist(strategy, chain.get((int) block), writes);
  }

  private void persist(
      final ArchiveTrieNodeStrategy strategy, final BlockHeader header, final Write... writes) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBlockPersist(storage, tx, header);
    for (final Write write : writes) {
      write.apply(strategy, tx);
    }
    strategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  private Optional<Coverage> coverage() {
    return coverageTracker.getCoverage();
  }

  private Coverage coverageThrough(final int last) {
    return new Coverage(0, last, chain.get(last).getHash());
  }

  @Test
  void capturesContiguousFinalSingleStepOnCanonicalStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);

    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(coverage()).contains(coverageThrough(1));
    final var latest = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L);
    assertThat(latest).isPresent();
    assertThat(latest.get().block()).isEqualTo(1L);
  }

  @Test
  void emptyBlockExtendsCoverage() {
    final ArchiveTrieNodeStrategy strategy = strategy();

    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1);

    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void multiBlockPersistIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    setStoredBlockHash(storage, chain.get(0).getHash());
    persist(strategy, chain.get(2), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void siblingForkBlockIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1);
    final List<BlockHeader> forkB =
        buildChain(() -> new BlockHeaderTestFixture().extraData(Bytes.of(0x0b)), 3);

    // Stored state is block 1 on fork A; header 2 on fork B has a different parent.
    setStoredBlockHash(storage, chain.get(1).getHash());
    persist(strategy, forkB.get(2), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void blockOnTopOfReorgedOutCoverageIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1);
    final List<BlockHeader> forkB =
        buildChain(() -> new BlockHeaderTestFixture().extraData(Bytes.of(0x0b)), 3);

    // Coverage ends at A1, but the persisted state was reorged to B1.
    setStoredBlockHash(storage, forkB.get(1).getHash());
    persist(strategy, forkB.get(2), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void nonFinalBlockIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy(b -> b <= 0);
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void nonCanonicalStorageIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    final SegmentedKeyValueStorage otherStorage = newStorage();
    setStoredBlockHash(otherStorage, chain.get(0).getHash());
    final SegmentedKeyValueStorageTransaction tx = otherStorage.startTransaction();
    strategy.onBlockPersist(otherStorage, tx, chain.get(1));
    final Bytes node = Bytes.fromHexString("0xbbbb");
    strategy.putTrieNode(otherStorage, tx, location, hash(node), node);
    strategy.onBeforeCommit(otherStorage, tx);
    tx.commit();

    assertThat(otherStorage.stream(TRIE_BRANCH_STORAGE_ARCHIVE)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void gapIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    persistBlock(strategy, 5, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 5L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void rollbackDiscardsCapture() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xbbbb");
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    setStoredBlockHash(storage, chain.get(0).getHash());
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBlockPersist(storage, tx, chain.get(1));
    strategy.putTrieNode(storage, tx, location, hash(node), node);
    strategy.onRollback(tx);
    tx.rollback();

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));

    persistBlock(strategy, 1, put(location, node));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isPresent();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void writesOutsideBlockPersistAreNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xcafe");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, location, hash(node), node);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 0L)).isEmpty();
    assertThat(coverage()).isEmpty();
  }

  @Test
  void multipleNodesInSameTransactionBothArchivedWithOneCoverageRecord() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location1 = Bytes.of(0x01);
    final Bytes location2 = Bytes.of(0x02);

    persistBlock(
        strategy,
        0,
        put(location1, Bytes.fromHexString("0x1111")),
        put(location2, Bytes.fromHexString("0x2222")));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location1), 0L)).isPresent();
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location2), 0L)).isPresent();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void skipsArchivingNoOpRewriteOfUnchangedNode() {
    // A node re-written with its already-committed bytes (the no-op storage write Bonsai still
    // re-commits along a touched path) must not add a redundant archive entry: getLatestBefore
    // resolves to the last real change, whose value is identical.
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x07);
    final Bytes node = Bytes.fromHexString("0xfeed");

    // Block 0: real write — archived.
    persistBlock(strategy, 0, put(location, node));

    // Block 1: identical bytes re-written — no-op, must be skipped (no new entry).
    persistBlock(strategy, 1, put(location, node));

    final var latest = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L);
    assertThat(latest).isPresent();
    assertThat(latest.get().block())
        .as("no entry written at the no-op block; resolves to the block-0 entry")
        .isEqualTo(0L);
  }

  @Test
  void accountReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    persistBlock(strategy, 0, put(location, node));

    assertThat(strategy.getTrieNode(storage, location, hash(node))).contains(node);
  }

  @Test
  void storageReadsDelegateToLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0xBB));
    final Bytes location = Bytes.of(0x0d);
    final Bytes node = Bytes.fromHexString("0xabcd");

    persistBlock(strategy, 0, put(TrieNodeKey.of(accountHash, location), node));

    assertThat(strategy.getTrieNode(storage, TrieNodeKey.of(accountHash, location), hash(node)))
        .contains(node);
  }

  @Test
  void removingExistingNodeWritesDeletionTombstone() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x05);

    // Block 0: create the node so it exists in committed storage.
    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xdeadbeef")));

    // Block 1: remove the node — prior lookup finds it, tombstone must be captured.
    persistBlock(strategy, 1, remove(location));

    final var tombstone = historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L);
    assertThat(tombstone).as("deletion tombstone written at block 1").isPresent();
    assertThat(tombstone.get().codecEntry().isDeletion())
        .as("entry at block 1 must be a deletion tombstone")
        .isTrue();
    assertThat(tombstone.get().block()).isEqualTo(1L);
    assertThat(coverageTracker.hasArchiveBlock(1L)).isTrue();
  }

  @Test
  void removingExistingStorageTrieNodeWritesDeletionTombstoneUnderStorageKey() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Hash accountHash = Hash.hash(Bytes.of(0xAA));
    final Bytes location = Bytes.of(0x05);

    // Block 0: create the node so it exists in committed storage.
    persistBlock(
        strategy, 0, put(TrieNodeKey.of(accountHash, location), Bytes.fromHexString("0xdeadbeef")));

    // Block 1: remove the node — prior lookup finds it, tombstone must be captured.
    persistBlock(strategy, 1, remove(TrieNodeKey.of(accountHash, location)));

    final var tombstone =
        historyStore.getLatestBefore(ArchiveNodeKey.storage(accountHash.getBytes(), location), 1L);
    assertThat(tombstone).as("deletion tombstone written at block 1").isPresent();
    assertThat(tombstone.get().codecEntry().isDeletion()).isTrue();
    assertThat(tombstone.get().block()).isEqualTo(1L);
    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
  }

  @Test
  void removingMissingNodeDoesNotWriteToArchive() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    persistBlock(strategy, 1, remove(location));

    assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L)).isEmpty();
  }
}
