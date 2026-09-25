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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ArchiveTrieNodeFrontierRollerTest {

  private static final long AWAIT_SECONDS = 5L;
  private static final int SHALLOW_INTERVAL = 128;
  private static final int DEEP_INTERVAL = 1024;

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private BonsaiWorldStateProvider worldStateProvider;
  @Mock private TrieLogManager trieLogManager;

  // Used by real-captureRange tests only (retention-gap coverage).
  @Mock private BonsaiWorldState rollerWorldState;

  @SuppressWarnings("rawtypes")
  @Mock
  private PathBasedWorldStateUpdateAccumulator mockAccumulator;

  @Mock private TrieLog mockTrieLog;

  private SegmentedInMemoryKeyValueStorage archiveStorage;
  private MutableBlockchain blockchain;
  private BlockDataGenerator blockDataGenerator;
  private final List<ArchiveTrieNodeFrontierRoller> rollers = new ArrayList<>();

  @BeforeEach
  void setUp() {
    archiveStorage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    blockDataGenerator = new BlockDataGenerator();
    blockchain = createInMemoryBlockchain(blockDataGenerator.genesisBlock());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(archiveStorage);
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(1L);
  }

  @AfterEach
  void tearDown() {
    rollers.forEach(
        r -> {
          try {
            r.close();
          } catch (final Exception ignored) {
            // best-effort teardown
          }
        });
  }

  // --- loadPersistedCursor ---

  @Test
  void loadPersistedCursorReturnsEmptyWhenKeyAbsent() {
    final ControllableRoller roller = createControllableRoller();
    assertThat(roller.loadPersistedCursor()).isEqualTo(OptionalLong.empty());
  }

  @Test
  void loadPersistedCursorReturnsPresentWhenKeyExists() {
    final SegmentedKeyValueStorageTransaction tx = archiveStorage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ArchiveTrieNodeFrontierRoller.ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
        Bytes.ofUnsignedLong(7L).toArrayUnsafe());
    tx.commit();

    final ControllableRoller roller = createControllableRoller();
    assertThat(roller.loadPersistedCursor()).hasValue(7L);
  }

  // --- startOngoing cursor seeding ---

  @Test
  void startOngoingPrefersPersisteedCursorWhenHigher() {
    final SegmentedKeyValueStorageTransaction tx = archiveStorage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ArchiveTrieNodeFrontierRoller.ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
        Bytes.ofUnsignedLong(5L).toArrayUnsafe());
    tx.commit();

    appendBlocks(10); // head=10, frontier=9
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(3L); // initial=3, persisted=5 → cursor=max(3,5)=5
    assertThat(roller.cursor.get()).isEqualTo(5L);
  }

  @Test
  void startOngoingPrefersInitialCursorWhenHigher() {
    final SegmentedKeyValueStorageTransaction tx = archiveStorage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ArchiveTrieNodeFrontierRoller.ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
        Bytes.ofUnsignedLong(2L).toArrayUnsafe());
    tx.commit();

    appendBlocks(10); // head=10, frontier=9
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(5L); // initial=5, persisted=2 → cursor=max(5,2)=5
    assertThat(roller.cursor.get()).isEqualTo(5L);
  }

  // --- block observer registration ---

  @Test
  void startOngoingRegistersBlockObserver() {
    appendBlocks(3); // head=3
    final ControllableRoller roller = createControllableRoller();
    assertThat(roller.blockObserverId).isEqualTo(OptionalLong.empty());

    roller.startOngoing(0L);
    assertThat(roller.blockObserverId).isNotEqualTo(OptionalLong.empty());
  }

  @Test
  void closeRemovesBlockObserverAndClearsId() {
    appendBlocks(3);
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(0L);
    assertThat(roller.blockObserverId).isNotEqualTo(OptionalLong.empty());

    roller.close();
    assertThat(roller.blockObserverId).isEqualTo(OptionalLong.empty());
  }

  @Test
  void startOngoingAfterCloseIsNoOp() {
    final ControllableRoller roller = createControllableRoller();
    roller.close();

    roller.startOngoing(0L);
    assertThat(roller.blockObserverId).isEqualTo(OptionalLong.empty());
  }

  // --- single-flight coalescing ---

  @Test
  void singleFlightGuardPreventsConcurrentCatchUps() throws Exception {
    final ScheduledExecutorService spyExecutor = spy(Executors.newScheduledThreadPool(1));
    appendBlocks(2); // head=2, frontier=1
    final PausedCapture paused = new PausedCapture();
    final ControllableRoller roller = createControllableRoller(spyExecutor, paused);
    roller.startOngoing(0L);

    paused.awaitStart();
    assertThat(roller.catchUpRunning.get()).isTrue();

    // Burst of head events while catch-up is paused — each bumps ongoingTarget, CAS rejects
    // additional submissions
    appendBlocks(10);
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.ongoingTarget.get()).isEqualTo(11L));

    // Only one submission despite 10+ head events
    verify(spyExecutor, times(1)).submit(any(Runnable.class));

    // Release: cursor advances to first run's toInclusive, then re-arm catches remaining blocks
    paused.release();
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(11L));
  }

  // --- re-arm after frontier advance during run ---

  @Test
  void catchUpReArmsWhenFrontierAdvancedDuringRun() throws Exception {
    appendBlocks(3); // head=3, frontier=2
    final PausedCapture paused = new PausedCapture();
    final ControllableRoller roller =
        createControllableRoller(Executors.newScheduledThreadPool(1), paused);
    roller.startOngoing(0L);

    paused.awaitStart(); // first captureRange(0, 2) running

    // Advance frontier while paused
    appendBlocks(2); // head=5, frontier=4
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.ongoingTarget.get()).isEqualTo(4L));

    paused.release(); // first run sets cursor=2, then re-arms
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(4L));
  }

  // --- retention-gap advance in the real captureRange ---
  //
  // These tests exercise the actual captureRange / ensureRollerState path (not ControllableRoller).
  // They cover the off-by-one fix: when cursor < head - maxLayers the retention-gap branch must
  // advance the cursor AND fall through to position rollerState (previously it returned early,
  // leaving rollerState null and causing the roller to spin without archiving anything).

  @Test
  @SuppressWarnings("unchecked")
  void captureRangeAdvancesCursorAndPositionsStateWhenBeyondRetention() {
    // maxLayers=1 (setUp), 3 blocks → head=3, oldestReachable=2, target=2
    // captureRange(0, 2): cursor 0 < 2 → advance cursor to 2, position state at block 2.
    // Loop is empty (effectiveFrom == toInclusive=2), but rollerState is ready for next call.
    appendBlocks(3);
    final BlockHeader block3 = blockchain.getBlockHeader(3L).get();
    when(trieLogManager.getTrieLogLayer(block3.getHash())).thenReturn(Optional.of(mockTrieLog));
    when(worldStateProvider.newTrieEnabledWorldState(any())).thenReturn(rollerWorldState);
    when(rollerWorldState.getAccumulator()).thenReturn(mockAccumulator);

    final ArchiveTrieNodeFrontierRoller roller = createRealRoller();
    roller.cursor.set(0L);
    roller.captureRange(0L, 2L);

    assertThat(roller.cursor.get()).isEqualTo(2L);
    assertThat(roller.rollerStatePosition).isEqualTo(2L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void captureRangeArchivesNextBlockAfterRetentionGapAdvance() {
    // Same setup; second call reuses state and archives block 3 (cursor 2 → 3).
    appendBlocks(3);
    final BlockHeader block3 = blockchain.getBlockHeader(3L).get();
    when(trieLogManager.getTrieLogLayer(block3.getHash())).thenReturn(Optional.of(mockTrieLog));
    when(worldStateProvider.newTrieEnabledWorldState(any())).thenReturn(rollerWorldState);
    when(rollerWorldState.getAccumulator()).thenReturn(mockAccumulator);

    final ArchiveTrieNodeFrontierRoller roller = createRealRoller();
    roller.cursor.set(0L);
    roller.captureRange(0L, 2L); // retention-gap advance, state positioned at 2
    roller.captureRange(2L, 3L); // reuses state, rolls forward to 3

    assertThat(roller.cursor.get()).isEqualTo(3L);
  }

  // --- helpers ---

  private ControllableRoller createControllableRoller() {
    return createControllableRoller(Executors.newScheduledThreadPool(1), null);
  }

  /**
   * Creates a real (non-overriding) roller for tests that need to exercise {@code captureRange}.
   */
  private ArchiveTrieNodeFrontierRoller createRealRoller() {
    final ArchiveTrieNodeFrontierRoller roller =
        new ArchiveTrieNodeFrontierRoller(
            worldStateStorage,
            worldStateProvider,
            trieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            Executors.newFixedThreadPool(2),
            SHALLOW_INTERVAL,
            DEEP_INTERVAL);
    rollers.add(roller);
    return roller;
  }

  private ControllableRoller createControllableRoller(
      final ScheduledExecutorService executor, final PausedCapture paused) {
    final ControllableRoller roller =
        new ControllableRoller(
            worldStateStorage,
            worldStateProvider,
            trieLogManager,
            blockchain,
            executor,
            Executors.newFixedThreadPool(2),
            SHALLOW_INTERVAL,
            DEEP_INTERVAL,
            paused);
    rollers.add(roller);
    return roller;
  }

  private MutableBlockchain createInMemoryBlockchain(final Block genesisBlock) {
    return DefaultBlockchain.createMutable(
        genesisBlock,
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false),
        new NoOpMetricsSystem(),
        0);
  }

  private void appendBlocks(final int count) {
    final Block head = blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).get();
    final List<Block> blocks = blockDataGenerator.blockSequence(head, count);
    for (final Block block : blocks) {
      blockchain.appendBlock(block, blockDataGenerator.receipts(block));
    }
  }

  // --- test doubles ---

  /**
   * A roller subclass that overrides {@code captureRange} so tests focus on scheduling / cursor
   * behaviour without requiring a real world-state stack. Each call advances the cursor to {@code
   * toInclusive}, optionally pausing first (for concurrency tests).
   */
  static class ControllableRoller extends ArchiveTrieNodeFrontierRoller {

    final AtomicInteger captureRangeCalls = new AtomicInteger();
    private final PausedCapture paused;

    ControllableRoller(
        final BonsaiWorldStateKeyValueStorage worldStateStorage,
        final BonsaiWorldStateProvider worldStateProvider,
        final TrieLogManager trieLogManager,
        final org.hyperledger.besu.ethereum.chain.Blockchain blockchain,
        final ScheduledExecutorService executorService,
        final ExecutorService trieCapturePool,
        final int shallowCheckpointInterval,
        final int deepCheckpointInterval,
        final PausedCapture paused) {
      super(
          worldStateStorage,
          worldStateProvider,
          trieLogManager,
          blockchain,
          executorService,
          trieCapturePool,
          shallowCheckpointInterval,
          deepCheckpointInterval);
      this.paused = paused;
    }

    @Override
    void captureRange(final long fromExclusive, final long toInclusive) {
      captureRangeCalls.incrementAndGet();
      if (paused != null) {
        paused.started.countDown();
        paused.awaitRelease();
      }
      if (toInclusive > fromExclusive) {
        cursor.set(toInclusive);
      }
    }
  }

  static class PausedCapture {
    final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch proceed = new CountDownLatch(1);

    void awaitStart() throws InterruptedException {
      if (!started.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("captureRange did not start within " + AWAIT_SECONDS + "s");
      }
    }

    void release() {
      proceed.countDown();
    }

    void awaitRelease() {
      try {
        if (!proceed.await(AWAIT_SECONDS, TimeUnit.SECONDS)) {
          throw new AssertionError("release() not called within " + AWAIT_SECONDS + "s");
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("interrupted while waiting for release", e);
      }
    }
  }
}
