/*
 * Copyright contributors to Hyperledger Besu.
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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMinSuffix;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TrailingFlatStateArchiverTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");
  private static final long BOUNDARY_DISABLED = 0L;
  private static final long ARCHIVE_TIMEOUT_SECONDS = 10L;
  private static final long AWAIT_TIMEOUT_SECONDS = 5L;

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private TrieLogManager trieLogManager;
  private MutableBlockchain blockchain;
  private SegmentedKeyValueStorage storage;
  private BlockDataGenerator blockDataGenerator;
  private final List<TrailingFlatStateArchiver> archivers = new ArrayList<>();

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
    blockDataGenerator = new BlockDataGenerator();
    blockchain = createInMemoryBlockchain(blockDataGenerator.genesisBlock());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(storage);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.ONE)));
  }

  @AfterEach
  public void tearDown() {
    archivers.forEach(
        m -> {
          try {
            m.close();
          } catch (final Exception ignored) {
            // Ignore exceptions during close
          }
        });
  }

  @Test
  public void archivesAccountChangesFromTrieLogs() {
    appendBlocks(2);

    final TrailingFlatStateArchiver archiver = createArchiver();
    archiver.start();

    awaitProgress(archiver, 2L);
    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
  }

  @Test
  public void archivesStorageChangesFromTrieLogs() {
    appendBlocks(1);
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));
    when(trieLogManager.getTrieLogLayer(hashAt(1L))).thenReturn(Optional.of(trieLog));

    final TrailingFlatStateArchiver archiver = createArchiver();
    archiver.start();

    awaitProgress(archiver, 1L);
    assertThat(getArchivedStorageKey(1L, slotKey)).isPresent();
  }

  @Test
  public void savesProgressToStorage() {
    appendBlocks(5);

    final TrailingFlatStateArchiver archiver = createArchiver();
    assertThat(archiver.getSavedProgress()).isEmpty();

    archiver.start();
    awaitProgress(archiver, 5L);
  }

  @Test
  public void resumesFromNextBlockAfterSavedProgress() {
    appendBlocks(3);

    // Run first archiving over blocks 0-3
    final TrailingFlatStateArchiver firstArchiver = createArchiver();
    firstArchiver.start();
    awaitProgress(firstArchiver, 3L);
    firstArchiver.close(); // simulate node restart — deregisters the observer

    // Append a new block and run a second archiver (simulating a restart)
    appendBlocks(1);

    final TrailingFlatStateArchiver secondArchiver = createArchiver();
    secondArchiver.start();
    awaitProgress(secondArchiver, 4L);

    // Blocks 1-3 must not be re-processed — each queried exactly once across both archivings
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(1L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(2L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(3L));
    // Block 4 must be processed by the second archiving
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(4L));
  }

  @Test
  public void usesLowPriorityTransactionsForArchiving() {
    appendBlocks(1);
    final SegmentedInMemoryKeyValueStorage spyStorage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(spyStorage);

    final TrailingFlatStateArchiver archiver = createArchiver();
    archiver.start();

    awaitProgress(archiver, 1L);
    verify(spyStorage, atLeastOnce()).startLowPriorityTransaction();
  }

  @Test
  public void archivingStopsAtHeadMinusBoundaryDistance() {
    // head=5, boundaryDistance=3 → target = 5-3 = 2; blocks 1 and 2 archived, 3 not
    appendBlocks(5);
    final TrailingFlatStateArchiver archiver = createArchiver(3);
    archiver.start();

    awaitProgress(archiver, 2L);
    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isEmpty();
  }

  @Test
  public void startObserverIgnoresForkEvents() throws Exception {
    appendBlocks(5); // canonical head = 5
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final PausedArchiver paused = pauseAtAnyTrieLogLookup();

    final TrailingFlatStateArchiver archiver =
        createArchiver(spyBlockchain, /*boundaryDistance*/ 2);
    archiver.start();

    final ArgumentCaptor<BlockAddedObserver> captor =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(spyBlockchain, atLeastOnce()).observeBlockAdded(captor.capture());
    final BlockAddedObserver observer = captor.getAllValues().get(0);

    paused.awaitStart();

    // Fire a FORK event at block height 1
    final Block forkBlock =
        blockDataGenerator.block(BlockDataGenerator.BlockOptions.create().setBlockNumber(1L));
    observer.onBlockAdded(BlockAddedEvent.createForFork(forkBlock));

    paused.release();

    // head=5, boundaryDistance=2 → canonical target = 3; block 3 must still be archived.
    awaitProgress(archiver, 3L);
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void blockObserverPersistsAndArchivesBlockAtBoundary() {
    // head=3, boundaryDistance=3 → initial target=0, only genesis is replayed
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(3);
    archiver.start();
    awaitProgress(archiver, 0L);
    assertThat(getArchivedAccountKey(1L)).isEmpty();

    // block 4 arrives → observer submits archiving of block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void startRegistersObserverAndArchivesBlocks() {
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(3);
    assertThat(archiver.blockObserverId).isEmpty();

    archiver.start();
    assertThat(archiver.blockObserverId).isPresent();

    // block 4 arrives → observer archives block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void ongoingArchivingCatchesUpMultipleBlocksBehind() {
    // head=3, boundaryDistance=1 → initial target=2; archive blocks 1 and 2
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(1);
    archiver.start();
    awaitProgress(archiver, 2L);

    // blocks 4, 5 and 6 arrive; catch-up archives up to 6-1=5
    appendBlocks(3);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(4L)).isPresent());
  }

  @Test
  public void ongoingArchivingCoalescesBurstOfHeadEventsIntoOneSubmit() throws Exception {
    // boundaryDistance=1 → target = head - 1
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(1L);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final ScheduledExecutorService spyExecutor = spy(Executors.newScheduledThreadPool(1));
    final TrailingFlatStateArchiver archiver =
        new TrailingFlatStateArchiver(
            worldStateStorage,
            trieLogManager,
            blockchain,
            spyExecutor,
            metricsSystem,
            new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy()));
    archivers.add(archiver);
    archiver.start();
    // The fresh start replays genesis and head=1 (target 0) schedules a no-op catch-up; let both
    // finish before counting submits.
    awaitProgress(archiver, 0L);
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(archiver.catchUpRunning.get()).isFalse());
    clearInvocations(spyExecutor);

    final PausedArchiver pause = pauseAtAnyTrieLogLookup();
    // head=2 → target=1: triggers a single drain submission, paused at block 1's trie-log lookup.
    appendBlocks(1);
    pause.awaitStart();

    // Burst of head events while drain is paused. Each bumps targetBlockNumber; the single-flight
    // CAS prevents additional submissions.
    appendBlocks(20);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(archiver.targetBlockNumber.get()).isEqualTo(21L));
    assertThat(archiver.catchUpRunning.get()).isTrue();
    verify(spyExecutor, times(1)).submit(any(Runnable.class));

    // Release: the same in-flight drain reads the live target and walks all the way to block 21.
    pause.release();
    Awaitility.await()
        .atMost(ARCHIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(archiver.archivedBlockNumber.get()).isEqualTo(21L));
    // No additional submissions after the burst — the moving target absorbed everything.
    verify(spyExecutor, times(1)).submit(any(Runnable.class));
  }

  @Test
  public void ongoingArchivingUpdatesMetric() {
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(1);
    archiver.start();
    awaitProgress(archiver, 2L);
    final long metricAfterCatchUp = archiver.archivedBlockNumber.get();

    // block 4 arrives → observer archives block 3; metric should increment
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(archiver.archivedBlockNumber.get()).isGreaterThan(metricAfterCatchUp));
  }

  @Test
  public void startAfterCloseIsNoOp() {
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(3);
    archiver.close();

    archiver.start();
    assertThat(archiver.blockObserverId).isEmpty();
  }

  @Test
  public void startIsIdempotent() {
    appendBlocks(3);
    final TrailingFlatStateArchiver archiver = createArchiver(3);

    archiver.start();
    final OptionalLong firstId = archiver.blockObserverId;
    assertThat(firstId).isPresent();

    archiver.start(); // second call — should no-op
    assertThat(archiver.blockObserverId).isEqualTo(firstId);
  }

  @Test
  public void startInitializesMetricFromSavedProgress() {
    appendBlocks(5);
    final TrailingFlatStateArchiver firstArchiver = createArchiver(BOUNDARY_DISABLED);
    firstArchiver.start();
    awaitProgress(firstArchiver, 5L);
    firstArchiver.close();

    // Second archiver simulates a restart — metric should be initialized from saved progress
    final TrailingFlatStateArchiver secondArchiver = createArchiver(BOUNDARY_DISABLED);
    assertThat(secondArchiver.archivedBlockNumber.get()).isEqualTo(-1L);
    secondArchiver.start();
    assertThat(secondArchiver.archivedBlockNumber.get()).isEqualTo(5L);
    secondArchiver.close();
  }

  @Test
  public void startAfterFailureIsNoOp() throws Exception {
    appendBlocks(3);
    when(trieLogManager.getTrieLogLayer(hashAt(2L))).thenReturn(Optional.empty());
    final TrailingFlatStateArchiver archiver = createArchiver(BOUNDARY_DISABLED);
    archiver.start();
    awaitProgress(archiver, 1L);
    awaitFailed(archiver);
    final OptionalLong observerId = archiver.blockObserverId;

    archiver.start();

    assertThat(archiver.blockObserverId).isEqualTo(observerId);
  }

  @Test
  public void closeStopsCatchUpAfterTheBlockInFlight() throws Exception {
    appendBlocks(5);
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(BOUNDARY_DISABLED);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    final TrailingFlatStateArchiver archiver =
        new TrailingFlatStateArchiver(
            worldStateStorage,
            trieLogManager,
            blockchain,
            executor,
            metricsSystem,
            new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy()));
    archivers.add(archiver);
    // ignores the interrupt from close(), so only the loop's own check can stop the catch-up
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenAnswer(
            invocation -> {
              started.countDown();
              Uninterruptibles.awaitUninterruptibly(proceed);
              return Optional.of(createAccountTrieLog(Wei.ONE));
            });
    archiver.start();
    assertThat(started.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

    final Thread closer = new Thread(archiver::close);
    closer.start();
    Awaitility.await().atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS).until(executor::isShutdown);
    proceed.countDown();
    closer.join(ARCHIVE_TIMEOUT_SECONDS * 1000);

    assertThat(executor.isTerminated()).isTrue();
    // the in-flight block (genesis) completes, nothing after it is read
    assertThat(archiver.getArchivedBlockNumber()).isEqualTo(0L);
    verify(trieLogManager, times(1)).getTrieLogLayer(any());
  }

  @Test
  public void startCatchesUpToCurrentHeadWithoutNewBlocks() {
    appendBlocks(5);
    final TrailingFlatStateArchiver archiver = createArchiver(2);

    archiver.start();

    awaitProgress(archiver, 3L);
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void freshStartReportsNothingArchivedAndReplaysGenesisTrieLog() {
    final TrailingFlatStateArchiver archiver = createArchiver(BOUNDARY_DISABLED);
    assertThat(archiver.getArchivedBlockNumber()).isEqualTo(-1L);
    when(trieLogManager.getTrieLogLayer(hashAt(0L)))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.ONE)));

    archiver.start();

    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(0L)).isPresent());
    awaitProgress(archiver, 0L);
  }

  @Test
  public void freshStartWithoutGenesisTrieLogStopsAtGenesis() {
    when(trieLogManager.getTrieLogLayer(hashAt(0L))).thenReturn(Optional.empty());
    final TrailingFlatStateArchiver archiver = createArchiver(BOUNDARY_DISABLED);

    archiver.start();

    awaitFailed(archiver);
    assertThat(archiver.getSavedProgress()).isEmpty();
    assertThat(archiver.getArchivedBlockNumber()).isEqualTo(-1L);
  }

  @Test
  public void catchUpStopsAfterMissingTrieLog() throws Exception {
    appendBlocks(3);
    when(trieLogManager.getTrieLogLayer(hashAt(2L))).thenReturn(Optional.empty());
    final TrailingFlatStateArchiver archiver = createArchiver(BOUNDARY_DISABLED);

    archiver.start();

    awaitProgress(archiver, 1L);
    awaitFailed(archiver);

    appendBlocks(2);
    Thread.sleep(500);

    assertThat(archiver.getSavedProgress()).hasValue(1L);
    verify(trieLogManager, atMost(2)).getTrieLogLayer(hashAt(2L));
  }

  @Test
  public void archivesDuringSyncWithoutInSyncSignal() {
    final TrailingFlatStateArchiver archiver = createArchiver(2);
    archiver.start();

    for (int i = 0; i < 10; i++) {
      appendBlocks(1);
    }

    awaitProgress(archiver, 8L);
  }

  // --- test helpers ---

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

  private void awaitProgress(final TrailingFlatStateArchiver archiver, final long expectedBlock) {
    Awaitility.await()
        .atMost(ARCHIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(archiver.getSavedProgress()).hasValue(expectedBlock));
  }

  private void awaitFailed(final TrailingFlatStateArchiver archiver) {
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(archiver.failed).isTrue());
  }

  private void appendBlocks(final int count) {
    final Block head = blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber()).get();
    final List<Block> blocks = blockDataGenerator.blockSequence(head, count);
    for (Block block : blocks) {
      blockchain.appendBlock(block, blockDataGenerator.receipts(block));
    }
  }

  private Hash hashAt(final long blockNumber) {
    return blockchain.getBlockHeader(blockNumber).orElseThrow().getHash();
  }

  private TrailingFlatStateArchiver createArchiver() {
    return createArchiver(BOUNDARY_DISABLED);
  }

  private TrailingFlatStateArchiver createArchiver(final long boundaryDistance) {
    return createArchiver(this.blockchain, boundaryDistance);
  }

  private TrailingFlatStateArchiver createArchiver(
      final MutableBlockchain blockchain, final long boundaryDistance) {
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(boundaryDistance);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    final TrailingFlatStateArchiver archiver =
        new TrailingFlatStateArchiver(
            worldStateStorage,
            trieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            metricsSystem,
            archiveStrategy);
    archivers.add(archiver);
    return archiver;
  }

  private TrieLogLayer createAccountTrieLog(final Wei balance) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    final PatriciaTrieAccountValue value =
        new PatriciaTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    trieLog.addAccountChange(TEST_ADDRESS, null, value);
    return trieLog;
  }

  private Optional<byte[]> getArchivedAccountKey(final long blockNumber) {
    final byte[] key =
        calculateArchiveKeyWithMinSuffix(
            new BonsaiArchiveContext(blockNumber),
            TEST_ADDRESS.addressHash().getBytes().toArrayUnsafe());
    return storage.get(ACCOUNT_INFO_STATE_ARCHIVE, key);
  }

  private Optional<byte[]> getArchivedStorageKey(
      final long blockNumber, final StorageSlotKey slotKey) {
    final byte[] naturalKey =
        calculateNaturalSlotKey(TEST_ADDRESS.addressHash(), slotKey.getSlotHash());
    final byte[] key =
        calculateArchiveKeyWithMinSuffix(new BonsaiArchiveContext(blockNumber), naturalKey);
    return storage.get(ACCOUNT_STORAGE_ARCHIVE, key);
  }

  private PausedArchiver pauseAtAnyTrieLogLookup() {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenAnswer(invocation -> waitThenReturnTrieLog(started, proceed));
    return new PausedArchiver(started, proceed);
  }

  private Optional<TrieLogLayer> waitThenReturnTrieLog(
      final CountDownLatch started, final CountDownLatch proceed) {
    started.countDown();
    try {
      if (!proceed.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("release() was not called within " + AWAIT_TIMEOUT_SECONDS + "s");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("archiving interrupted", e);
    }
    return Optional.of(createAccountTrieLog(Wei.ONE));
  }

  private record PausedArchiver(CountDownLatch started, CountDownLatch proceed) {

    void awaitStart() throws InterruptedException {
      if (!started.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("Archiving did not reach the paused trie-log lookup in time");
      }
    }

    void release() {
      proceed.countDown();
    }
  }
}
