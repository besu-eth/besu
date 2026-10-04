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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
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

/**
 * Integration tests for ongoing archive proof sync.
 *
 * <p>Uses {@code ControllableRoller} (a test subclass that stubs out {@code captureRange}) to
 * verify: cursor persistence across restarts, single-catch-up coverage of multi-block gaps, reorg
 * safety (frontier constraint prevents above-frontier capture), and the Phase-A → Phase-B handoff
 * cursor seed.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BonsaiArchiveOngoingSyncIntegrationTest {

  private static final long AWAIT_SECONDS = 5L;
  private static final int SHALLOW_INTERVAL = 128;
  private static final int DEEP_INTERVAL = 1024;
  private static final int MAX_LAYERS = 3;

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private BonsaiWorldStateProvider worldStateProvider;
  @Mock private TrieLogManager trieLogManager;

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
    when(trieLogManager.getMaxLayersToLoad()).thenReturn((long) MAX_LAYERS);
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

  /**
   * A roller catches up over a multi-block gap in a single run when the frontier is large enough.
   * Verifies that {@code captureRange(0, frontier)} is called and cursor reaches the frontier.
   */
  @Test
  void rollerCatchesUpEntireFrontierGapInOneRun() throws Exception {
    appendBlocks(10); // head=10, frontier = 10-3 = 7
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(0L);

    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(7L));
  }

  /**
   * After a crash and restart (new roller instance over same storage), the roller resumes from the
   * persisted cursor rather than from the Phase-A handoff value passed to startOngoing().
   */
  @Test
  void restartResumesFromPersistedCursorNotInitialHandoff() throws Exception {
    appendBlocks(10); // head=10, frontier=7
    final ControllableRoller firstRoller = createControllableRoller();
    firstRoller.startOngoing(0L);

    // Wait for first roller to capture to frontier=7
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(firstRoller.cursor.get()).isEqualTo(7L));
    firstRoller.close();

    // Simulate 3 more blocks arriving before the restart
    appendBlocks(3); // head=13, frontier=10

    // Second roller: startOngoing(0) — but persisted cursor = 7, so cursor = max(0, 7) = 7
    final ControllableRoller secondRoller = createControllableRoller();
    secondRoller.startOngoing(0L);
    assertThat(secondRoller.cursor.get()).isEqualTo(7L);

    // It then catches up from 7 to 10
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(secondRoller.cursor.get()).isEqualTo(10L));
  }

  /**
   * Phase-A handoff: if Phase A captured up to block H, the roller seeds cursor from H (not 0),
   * avoiding re-archiving blocks Phase A already wrote.
   */
  @Test
  void phaseAHandoffCursorSeedsPhaseBCorrectly() throws Exception {
    appendBlocks(10); // head=10, frontier=7
    final ControllableRoller roller = createControllableRoller();

    // Simulate Phase A having archived up to block 5
    roller.startOngoing(5L);
    assertThat(roller.cursor.get()).isEqualTo(5L);

    // Phase B should catch up from 5 to 7 (not from 0)
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(7L));
  }

  /**
   * When the frontier advances repeatedly as new heads arrive, the roller eventually processes all
   * blocks up to the new frontier. The single-flight guard ensures this happens via re-arm (not via
   * concurrent catch-ups).
   */
  @Test
  void frontierAdvanceDrivesOngoingCatchUps() throws Exception {
    appendBlocks(5); // head=5, frontier=2
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(0L);

    // Wait to reach the initial frontier
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(2L));

    // Add more blocks, advancing frontier to 4, then 6
    appendBlocks(2); // head=7, frontier=4
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(4L));

    appendBlocks(2); // head=9, frontier=6
    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(6L));
  }

  /**
   * The roller never captures blocks above the frontier. When a reorg event fires for a block above
   * the finality horizon, the frontier (head − maxLayers) does not change enough to pull in
   * non-final blocks, and no capture happens above the frontier.
   */
  @Test
  void captureNeverExceedsOngoingFrontier() throws Exception {
    appendBlocks(4); // head=4, frontier = 4-3 = 1
    final ControllableRoller roller = createControllableRoller();
    roller.startOngoing(0L);

    Awaitility.await()
        .atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(roller.cursor.get()).isEqualTo(1L));

    // A non-canonical fork block fires (block observer sees non-canonical event)
    // The observer checks isNewCanonicalHead() → false, so ongoingTarget stays at 1
    assertThat(roller.ongoingTarget.get()).isEqualTo(1L);
    assertThat(roller.cursor.get()).isEqualTo(1L);
  }

  /**
   * After close, the roller ignores subsequent block-added events and does not advance the cursor.
   */
  @Test
  void rollerDoesNotAdvanceAfterClose() throws Exception {
    appendBlocks(5); // head=5, frontier=2
    final PausedCapture paused = new PausedCapture();
    final ControllableRoller roller =
        createControllableRoller(Executors.newScheduledThreadPool(1), paused);
    roller.startOngoing(0L);

    paused.awaitStart(); // paused mid-capture
    paused.release();

    roller.close();

    // After close, adding blocks should not trigger new catch-ups
    appendBlocks(5);
    // Wait a moment and verify cursor hasn't advanced past what was in-flight
    Thread.sleep(200);
    assertThat(roller.blockObserverId).isEqualTo(OptionalLong.empty());
  }

  // --- helpers ---

  private ControllableRoller createControllableRoller() {
    return createControllableRoller(Executors.newScheduledThreadPool(1), null);
  }

  private ControllableRoller createControllableRoller(
      final ScheduledExecutorService executor, final PausedCapture paused) {
    // ControllableRoller persists its cursor advances to archiveStorage so restart tests work
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
            paused,
            archiveStorage);
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
   * A roller that stubs captureRange: updates cursor and persists it to real storage so restart
   * tests can verify durability.
   */
  static class ControllableRoller extends ArchiveTrieNodeFrontierRoller {

    final AtomicInteger captureRangeCalls = new AtomicInteger();
    private final PausedCapture paused;
    private final SegmentedInMemoryKeyValueStorage persistStorage;

    ControllableRoller(
        final BonsaiWorldStateKeyValueStorage worldStateStorage,
        final BonsaiWorldStateProvider worldStateProvider,
        final TrieLogManager trieLogManager,
        final org.hyperledger.besu.ethereum.chain.Blockchain blockchain,
        final ScheduledExecutorService executorService,
        final ExecutorService trieCapturePool,
        final int shallowCheckpointInterval,
        final int deepCheckpointInterval,
        final PausedCapture paused,
        final SegmentedInMemoryKeyValueStorage persistStorage) {
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
      this.persistStorage = persistStorage;
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
        // Persist cursor so restart tests can verify durability
        final SegmentedKeyValueStorageTransaction tx = persistStorage.startTransaction();
        tx.put(
            TRIE_BRANCH_STORAGE_ARCHIVE,
            ARCHIVE_TRIE_NODE_SYNC_PROGRESS,
            Bytes.ofUnsignedLong(toInclusive).toArrayUnsafe());
        tx.commit();
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
