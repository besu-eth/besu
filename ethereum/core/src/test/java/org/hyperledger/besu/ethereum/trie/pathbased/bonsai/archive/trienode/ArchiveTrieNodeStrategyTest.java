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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore.ArchivedBlock;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
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
    capture =
        new ArchiveTrieNodeWriter(
            historyStore,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
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
      final BlockHeader header =
          fixture
              .get()
              .number(i)
              .parentHash(parentHash)
              .stateRoot(
                  Hash.hash(Bytes.concatenate(Bytes.ofUnsignedInt(i), parentHash.getBytes())))
              .buildHeader();
      headers.add(header);
      parentHash = header.getHash();
    }
    return headers;
  }

  private ArchiveTrieNodeStrategy strategy() {
    return strategy(() -> false);
  }

  private ArchiveTrieNodeStrategy strategy(final BooleanSupplier hasChainEstimate) {
    return new ArchiveTrieNodeStrategy(
        new BonsaiTrieNodeStrategy(), capture, historyStore, storage, hasChainEstimate);
  }

  private ArchiveTrieNodeStrategy strategyOver(final BonsaiTrieNodeStrategy base) {
    return new ArchiveTrieNodeStrategy(base, capture, historyStore, storage, () -> false);
  }

  // A state-root trie rooted at {@code stateRoot} loading {@code node} at {@code location}.
  private static void load(
      final ArchiveTrieNodeStrategy strategy,
      final Hash stateRoot,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    strategy
        .wrapNodeLoader(stateRoot, (l, h) -> Optional.of(node), Function.identity())
        .getNode(location, nodeHash);
  }

  private Hash rootOf(final int block) {
    return chain.get(block).getStateRoot();
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

  private void setStoredHead(final SegmentedKeyValueStorage target, final BlockHeader head) {
    final SegmentedKeyValueStorageTransaction tx = target.startTransaction();
    putHead(tx, head);
    tx.commit();
  }

  private static void putHead(
      final SegmentedKeyValueStorageTransaction tx, final BlockHeader head) {
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, head.getHash().getBytes().toArrayUnsafe());
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, head.getStateRoot().getBytes().toArrayUnsafe());
  }

  // Persists chain block {@code block} as a single step on canonical storage.
  private void persistBlock(
      final ArchiveTrieNodeStrategy strategy, final long block, final Write... writes) {
    if (block > 0) {
      setStoredHead(storage, chain.get((int) block - 1));
    }
    persist(strategy, chain.get((int) block), writes);
  }

  // Writes the block's nodes and moves the persisted head to it, as a world-state persist does.
  private void persist(
      final ArchiveTrieNodeStrategy strategy, final BlockHeader header, final Write... writes) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBlockPersist(storage, tx, header);
    for (final Write write : writes) {
      write.apply(strategy, tx);
    }
    putHead(tx, header);
    strategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  private Optional<ArchivedBlock> coverage() {
    return historyStore.getArchivedBlock();
  }

  private ArchivedBlock coverageThrough(final int last) {
    return new ArchivedBlock(last, chain.get(last).getHash());
  }

  private Optional<ArchiveNodeHistoryStore.HistoryEntry> latest(
      final Bytes location, final long block) {
    return historyStore.getLatestBefore(ArchiveNodeKey.account(location), block);
  }

  private static List<BlockHeader> forkB(final int length) {
    return buildChain(() -> new BlockHeaderTestFixture().extraData(Bytes.of(0x0b)), length);
  }

  @Test
  void capturesContiguousSingleStepOnLiveStorage() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);

    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(coverage()).contains(coverageThrough(1));
    final var latest = latest(location, 1L);
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

    setStoredHead(storage, chain.get(0));
    persist(strategy, chain.get(2), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void siblingOfLastArchivedBlockIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1);
    final List<BlockHeader> fork = forkB(2);

    // B1 persists in one step at the archived A1's height.
    setStoredHead(storage, fork.get(0));
    persist(strategy, fork.get(1), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void blockOnTopOfReorgedOutCoverageIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    persistBlock(strategy, 1);
    final List<BlockHeader> fork = forkB(3);

    // Coverage ends at A1, but the persisted state was reorged to B1.
    setStoredHead(storage, fork.get(1));
    persist(strategy, fork.get(2), put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void genesisIsCapturedWhenGateClosed() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x01);

    strategy.setArchiving(false);
    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xaabb")));

    assertThat(latest(location, 0L)).isPresent();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void peerlessInSyncKeepsCapturing() {
    final ArchiveTrieNodeStrategy strategy = strategy(() -> false);
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    strategy.onInSyncStatusChange(true);
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 1L)).isPresent();
    assertThat(coverage()).contains(coverageThrough(1));
  }

  @Test
  void inSyncWithRemoteEstimateStopsCapturing() {
    final ArchiveTrieNodeStrategy strategy = strategy(() -> true);
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    strategy.onInSyncStatusChange(true);
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void reopenedGateDoesNotCaptureAcrossTheGap() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));
    strategy.setArchiving(false);
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbbbb")));

    assertThat(latest(location, 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));

    strategy.setArchiving(true);
    persistBlock(strategy, 2, put(location, Bytes.fromHexString("0xcccc")));

    assertThat(latest(location, 2L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void otherStorageIsNotCaptured() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    final SegmentedKeyValueStorage otherStorage = newStorage();
    setStoredHead(otherStorage, chain.get(0));
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

    assertThat(latest(location, 5L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void rollbackDiscardsCapture() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.fromHexString("0xbbbb");
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    setStoredHead(storage, chain.get(0));
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBlockPersist(storage, tx, chain.get(1));
    strategy.putTrieNode(storage, tx, location, hash(node), node);
    strategy.onRollback(tx);
    tx.rollback();

    assertThat(latest(location, 1L)).isEmpty();
    assertThat(coverage()).contains(coverageThrough(0));

    persistBlock(strategy, 1, put(location, node));

    assertThat(latest(location, 1L)).isPresent();
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
    assertThat(latest(location, 0L)).isEmpty();
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

    assertThat(latest(location1, 0L)).isPresent();
    assertThat(latest(location2, 0L)).isPresent();
    assertThat(coverage()).contains(coverageThrough(0));
  }

  @Test
  void skipsArchivingNoOpRewriteOfUnchangedNode() {
    final BonsaiTrieNodeStrategy base = spy(new BonsaiTrieNodeStrategy());
    final ArchiveTrieNodeStrategy strategy = strategyOver(base);
    final Bytes location = Bytes.of(0x07);
    final Bytes node = Bytes.fromHexString("0xfeed");
    persistBlock(strategy, 0, put(location, node));

    // Bonsai re-commits the unchanged node along a touched path.
    load(strategy, rootOf(0), location, hash(node), node);
    clearInvocations(base);
    persistBlock(strategy, 1, put(location, node));

    verify(base, never()).getTrieNode(any(), any(), any());
    assertThat(latest(location, 1L).orElseThrow().block()).isZero();
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
    final BonsaiTrieNodeStrategy base = spy(new BonsaiTrieNodeStrategy());
    final ArchiveTrieNodeStrategy strategy = strategyOver(base);
    final Bytes location = Bytes.of(0x05);
    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xdeadbeef")));

    clearInvocations(base);
    persistBlock(strategy, 1, remove(location));

    // The node wasn't loaded, so storage tells whether it existed.
    verify(base).getTrieNode(storage, location, Bytes32.ZERO);
    final var tombstone = latest(location, 1L).orElseThrow();
    assertThat(tombstone.codecEntry().isDeletion()).isTrue();
    assertThat(tombstone.block()).isEqualTo(1L);
    assertThat(coverage()).contains(coverageThrough(1));
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
    assertThat(latest(location, 1L)).isEmpty();
  }

  @Test
  void removingMissingNodeDoesNotWriteToArchive() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x0e);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    persistBlock(strategy, 1, remove(location));

    assertThat(latest(location, 1L)).isEmpty();
  }

  @Test
  void putOfNodeNotLoadedHasNoPriorAndIsWrittenFull() {
    // e.g. a location holding a node Bonsai left behind
    final BonsaiTrieNodeStrategy base = spy(new BonsaiTrieNodeStrategy());
    final ArchiveTrieNodeStrategy strategy = strategyOver(base);
    final Bytes location = Bytes.of(0x07);
    persistBlock(strategy, 0, put(location, Bytes.fromHexString("0xfeed")));

    clearInvocations(base);
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xbeef")));

    verify(base, never()).getTrieNode(any(), any(), any());
    assertThat(latest(location, 1L).orElseThrow().codecEntry().isFull()).isTrue();
  }

  @Test
  void loaderIsNotDecoratedWhileGateClosed() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final NodeLoader loader = (location, hash) -> Optional.empty();

    strategy.setArchiving(false);

    assertThat(strategy.wrapNodeLoader(rootOf(0), loader, Function.identity())).isSameAs(loader);
  }

  @Test
  void emptyTrieNodeIsNotAPrior() {
    final BonsaiTrieNodeStrategy base = spy(new BonsaiTrieNodeStrategy());
    final ArchiveTrieNodeStrategy strategy = strategyOver(base);
    final Bytes location = Bytes.of(0x07);
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    load(
        strategy, rootOf(0), location, MerkleTrie.EMPTY_TRIE_NODE_HASH, MerkleTrie.EMPTY_TRIE_NODE);
    clearInvocations(base);
    persistBlock(strategy, 1, put(location, Bytes.fromHexString("0xfeed")));

    verify(base, never()).getTrieNode(any(), any(), any());
    assertThat(latest(location, 1L).orElseThrow().codecEntry().isFull()).isTrue();
  }

  @Test
  void nodesLoadedBeforeTheHeadMovesAreNotPriors() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x07);
    final Bytes node = Bytes.fromHexString("0xfeed");
    persistBlock(strategy, 0, put(location, node));

    load(strategy, rootOf(0), location, hash(node), node);
    persistBlock(strategy, 1);
    persistBlock(strategy, 2, put(location, node));

    // block 2 replaces block 1's state, so the node loaded at block 0's isn't its prior
    assertThat(latest(location, 2L).orElseThrow().block()).isEqualTo(2L);
  }

  @Test
  void commitDropsLoadedNodesEvenWhenTheRootIsUnchanged() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final Bytes location = Bytes.of(0x07);
    final Bytes node = Bytes.fromHexString("0xfeed");
    persistBlock(strategy, 0, put(location, node));
    final BlockHeader emptyBlock =
        new BlockHeaderTestFixture()
            .number(1)
            .parentHash(chain.get(0).getHash())
            .stateRoot(rootOf(0))
            .buildHeader();
    final BlockHeader next =
        new BlockHeaderTestFixture().number(2).parentHash(emptyBlock.getHash()).buildHeader();

    load(strategy, rootOf(0), location, hash(node), node);
    persist(strategy, emptyBlock);
    persist(strategy, next, put(location, node));

    // without a prior the unchanged node isn't skipped as a re-commit
    assertThat(latest(location, 2L).orElseThrow().block()).isEqualTo(2L);
  }

  @Test
  void loadsPastTheCapAreNotPriors() {
    final ArchiveTrieNodeStrategy strategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), capture, historyStore, storage, () -> false, 1);
    final Bytes first = Bytes.of(0x07);
    final Bytes second = Bytes.of(0x08);
    final Bytes node = Bytes.fromHexString("0xfeed");
    persistBlock(strategy, 0, put(first, node), put(second, node));

    load(strategy, rootOf(0), first, hash(node), node);
    load(strategy, rootOf(0), second, hash(node), node);
    persistBlock(strategy, 1, put(first, node), put(second, node));

    assertThat(latest(first, 1L).orElseThrow().block()).isZero();
    final var uncapped = latest(second, 1L).orElseThrow();
    assertThat(uncapped.block()).isEqualTo(1L);
    assertThat(uncapped.codecEntry().isFull()).isTrue();
  }

  @Test
  void loaderRootedAtAnotherStateIsNotDecorated() {
    final ArchiveTrieNodeStrategy strategy = strategy();
    final NodeLoader loader = (location, hash) -> Optional.empty();
    persistBlock(strategy, 0, put(Bytes.of(0x01), Bytes.fromHexString("0xaaaa")));

    assertThat(strategy.wrapNodeLoader(rootOf(1), loader, Function.identity())).isSameAs(loader);
  }
}
