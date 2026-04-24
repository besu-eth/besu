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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsaiarchive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMinSuffix;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
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
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
public class BonsaiFlatDbToArchiveMigratorTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");
  private static final long BOUNDARY_DISABLED = 0L;
  private static final long MIGRATION_TIMEOUT_SECONDS = 10L;
  private static final long AWAIT_TIMEOUT_SECONDS = 5L;

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private TrieLogManager trieLogManager;
  private MutableBlockchain blockchain;
  private SegmentedKeyValueStorage storage;
  private BlockDataGenerator blockDataGenerator;
  private final List<BonsaiFlatDbToArchiveMigrator> migrators = new ArrayList<>();

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
    migrators.forEach(
        m -> {
          try {
            m.close();
          } catch (final Exception ignored) {
            // Ignore exceptions during close
          }
        });
  }

  @Test
  public void migratesAccountChangesFromTrieLogs() throws Exception {
    appendBlocks(2);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
  }

  @Test
  public void migratesStorageChangesFromTrieLogs() throws Exception {
    appendBlocks(1);
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer trieLog = new TrieLogLayer();
    trieLog.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));
    when(trieLogManager.getTrieLogLayer(hashAt(1L))).thenReturn(Optional.of(trieLog));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedStorageKey(1L, slotKey)).isPresent();
  }

  @Test
  public void futureCompletesExceptionallyOnFailure() {
    appendBlocks(1);

    when(trieLogManager.getTrieLogLayer(any())).thenThrow(new RuntimeException("Test failure"));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate())
        .failsWithin(1, TimeUnit.SECONDS)
        .withThrowableThat()
        .havingRootCause()
        .withMessage("Test failure");
  }

  @Test
  public void rejectsConcurrentMigrations() throws Exception {
    appendBlocks(1);
    final PausedMigration paused = pauseAtTrieLogLookup(hashAt(1L));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    final CompletableFuture<Void> first = migrator.migrate();

    paused.awaitStart();
    assertThat(migrator.migrationRunning).isTrue();

    // Second migration should return immediately without running
    final CompletableFuture<Void> second = migrator.migrate();
    second.get(1, TimeUnit.SECONDS);

    // Second migration must not have interacted with the database
    verify(trieLogManager).getTrieLogLayer(hashAt(1L));

    paused.release();
    first.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void tracksRunningState() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.migrationRunning).isFalse();

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void savesProgressToStorage() throws Exception {
    appendBlocks(5);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.getMigrationProgress()).isEmpty();

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(5L);
  }

  @Test
  public void failsMigrationWhenTrieLogIsMissing() {
    appendBlocks(1);
    when(trieLogManager.getTrieLogLayer(hashAt(1L))).thenReturn(Optional.empty());

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate())
        .failsWithin(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .withThrowableThat()
        .havingRootCause()
        .withMessage("No trie log found for block 1");
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void migratesNewBlocksAddedDuringMigration() throws Exception {
    appendBlocks(2);
    final PausedMigration paused = pauseAtTrieLogLookup(hashAt(1L));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    final CompletableFuture<Void> future = migrator.migrate();

    paused.awaitStart();

    // Append block 3 while migration is paused — target should update to cover it
    appendBlocks(1);

    paused.release();
    future.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void switchesToArchiveModeOnCompletion() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    verify(worldStateStorage).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void resumesFromNextBlockAfterSavedProgress() throws Exception {
    appendBlocks(3);

    // Run first migration over blocks 1-3
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator();
    firstMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(firstMigrator.getMigrationProgress()).hasValue(3L);
    firstMigrator.close(); // simulate node restart — deregisters ongoing observer

    // Append a new block and run a second migrator (simulating a restart)
    appendBlocks(1);

    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator();
    secondMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // Blocks 1-3 must not be re-processed — each queried exactly once across both migrations
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(1L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(2L));
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(3L));
    // Block 4 must be processed by the second migration
    verify(trieLogManager, times(1)).getTrieLogLayer(hashAt(4L));
  }

  @Test
  public void usesLowPriorityTransactionsForMigration() throws Exception {
    appendBlocks(1);
    final SegmentedInMemoryKeyValueStorage spyStorage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(spyStorage);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    verify(spyStorage, atLeastOnce()).startLowPriorityTransaction();
  }

  @Test
  public void doesNotSwitchToArchiveModeOnFailure() {
    appendBlocks(1);
    when(trieLogManager.getTrieLogLayer(any())).thenThrow(new RuntimeException("Test failure"));
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate()).failsWithin(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void migrationStopsAtHeadMinusBoundaryDistance() throws Exception {
    // head=5, boundaryDistance=3 → target = 5-3 = 2; blocks 1 and 2 migrated, 3 not
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isEmpty();
  }

  @Test
  public void migrateHandsOffObserversFromInitialToOngoingWithoutGap() throws Exception {
    appendBlocks(3);
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigrator(spyBlockchain, /*boundaryDistance*/ 1);

    final AtomicBoolean injected = new AtomicBoolean(false);
    doAnswer(
            invocation -> {
              final Object result = invocation.callRealMethod();
              if (injected.compareAndSet(false, true)) {
                // Now the old observer has just been removed. Fire a canonical head event.
                appendBlocks(1);
              }
              return result;
            })
        .when(spyBlockchain)
        .removeObserver(anyLong());

    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // head after the injected block = 4; boundaryDistance=1 → archiveTarget = 3.
    // Block 3 must be migrated via catch-up triggered by the injected block 4 event.
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
  }

  @Test
  public void migrateObserverIgnoresForkEvents() throws Exception {
    appendBlocks(5); // canonical head = 5
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final PausedMigration paused = pauseAtAnyTrieLogLookup();

    final BonsaiFlatDbToArchiveMigrator migrator =
        createMigrator(spyBlockchain, /*boundaryDistance*/ 2);
    final CompletableFuture<Void> future = migrator.migrate();

    final ArgumentCaptor<BlockAddedObserver> captor =
        ArgumentCaptor.forClass(BlockAddedObserver.class);
    verify(spyBlockchain, atLeastOnce()).observeBlockAdded(captor.capture());
    final BlockAddedObserver migrateObserver = captor.getAllValues().get(0);

    paused.awaitStart();

    // Fire a FORK event at block height 1
    final Block forkBlock =
        blockDataGenerator.block(BlockDataGenerator.BlockOptions.create().setBlockNumber(1L));
    migrateObserver.onBlockAdded(BlockAddedEvent.createForFork(forkBlock));

    paused.release();
    future.get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

    // head=5, boundaryDistance=2 → canonical target = 3; block 3 must still be migrated.
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void closeDuringMigrationInterruptsAndSkipsArchiveUpgrade() throws Exception {
    appendBlocks(3);
    final PausedMigration paused = pauseAtAnyTrieLogLookup();

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(BOUNDARY_DISABLED);
    final CompletableFuture<Void> future = migrator.migrate();

    paused.awaitStart();
    assertThat(migrator.blockObserverId).isPresent();

    migrator.close();

    assertThatThrownBy(() -> future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class);
    assertThat(migrator.blockObserverId).isEmpty();
    assertThat(migrator.migrationRunning.get()).isFalse();
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void blockObserverPersistsAndMigratesBlockAtBoundary() throws Exception {
    // head=3, boundaryDistance=3 → initial target=0, nothing migrated initially
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(getArchivedAccountKey(1L)).isEmpty();

    // block 4 arrives → observer submits migration of block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void startOngoingMigrationRegistersObserverAndMigratesBlocks() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    assertThat(migrator.blockObserverId).isEmpty();

    migrator.startOngoingMigration();
    assertThat(migrator.blockObserverId).isPresent();

    // block 4 arrives → observer migrates block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void ongoingMigrationCatchesUpMultipleBlocksBehind() throws Exception {
    // head=3, boundaryDistance=1 → initial target=2; migrate blocks 1 and 2
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(2L);

    // blocks 4 and 5 arrive while executor is idle; block 6 triggers catch-up of 4-1=3 up to 5-1=4
    appendBlocks(3);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(4L)).isPresent());
  }

  @Test
  public void ongoingMigrationUpdatesMetric() throws Exception {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    final long metricAfterMigration = migrator.migratedBlockNumber.get();

    // block 4 arrives → observer migrates block 3; metric should increment
    appendBlocks(1);
    Awaitility.await()
        .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(migrator.migratedBlockNumber.get()).isGreaterThan(metricAfterMigration));
  }

  @Test
  public void startOngoingMigrationAfterCloseIsNoOp() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.close();

    migrator.startOngoingMigration();
    assertThat(migrator.blockObserverId).isEmpty();
  }

  @Test
  public void migrateAfterCloseIsNoOp() throws Exception {
    appendBlocks(3);
    final MutableBlockchain spyBlockchain = spy(blockchain);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(spyBlockchain, BOUNDARY_DISABLED);
    migrator.close();

    final CompletableFuture<Void> future = migrator.migrate();

    assertThat(future).isCompletedWithValue(null);
    assertThat(migrator.blockObserverId).isEmpty();
    assertThat(migrator.migrationRunning.get()).isFalse();
    verify(spyBlockchain, never()).observeBlockAdded(any());
  }

  @Test
  public void startOngoingMigrationIsIdempotent() {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);

    migrator.startOngoingMigration();
    final OptionalLong firstId = migrator.blockObserverId;
    assertThat(firstId).isPresent();

    migrator.startOngoingMigration(); // second call — should no-op
    assertThat(migrator.blockObserverId).isEqualTo(firstId);
  }

  @Test
  public void startOngoingMigrationInitializesMetricFromSavedProgress() throws Exception {
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator(BOUNDARY_DISABLED);
    firstMigrator.migrate().get(MIGRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    firstMigrator.close();

    // Second migrator simulates a restart — metric should be initialized from saved progress
    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator(BOUNDARY_DISABLED);
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(0L);
    secondMigrator.startOngoingMigration();
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(5L);
    secondMigrator.close();
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

  private BonsaiFlatDbToArchiveMigrator createMigrator() {
    return createMigrator(BOUNDARY_DISABLED);
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator(final long boundaryDistance) {
    return createMigrator(this.blockchain, boundaryDistance);
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator(
      final MutableBlockchain blockchain, final long boundaryDistance) {
    when(trieLogManager.getMaxLayersToLoad()).thenReturn(boundaryDistance);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    final BonsaiFlatDbToArchiveMigrator migrator =
        new BonsaiFlatDbToArchiveMigrator(
            worldStateStorage,
            trieLogManager,
            blockchain,
            Executors.newScheduledThreadPool(1),
            metricsSystem,
            archiveStrategy);
    migrators.add(migrator);
    return migrator;
  }

  private TrieLogLayer createAccountTrieLog(final Wei balance) {
    final TrieLogLayer trieLog = new TrieLogLayer();
    final PmtStateTrieAccountValue value =
        new PmtStateTrieAccountValue(1, balance, Hash.EMPTY, Hash.EMPTY);
    trieLog.addAccountChange(TEST_ADDRESS, null, value);
    return trieLog;
  }

  private Optional<byte[]> getArchivedAccountKey(final long blockNumber) {
    final byte[] key =
        calculateArchiveKeyWithMinSuffix(
            new BonsaiContext(blockNumber), TEST_ADDRESS.addressHash().getBytes().toArrayUnsafe());
    return storage.get(ACCOUNT_INFO_STATE_ARCHIVE, key);
  }

  private Optional<byte[]> getArchivedStorageKey(
      final long blockNumber, final StorageSlotKey slotKey) {
    final byte[] naturalKey =
        calculateNaturalSlotKey(TEST_ADDRESS.addressHash(), slotKey.getSlotHash());
    final byte[] key = calculateArchiveKeyWithMinSuffix(new BonsaiContext(blockNumber), naturalKey);
    return storage.get(ACCOUNT_STORAGE_ARCHIVE, key);
  }

  private PausedMigration pauseAtTrieLogLookup(final Hash blockHash) {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(blockHash))
        .thenAnswer(invocation -> waitThenReturnTrieLog(started, proceed));
    return new PausedMigration(started, proceed);
  }

  private PausedMigration pauseAtAnyTrieLogLookup() {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenAnswer(invocation -> waitThenReturnTrieLog(started, proceed));
    return new PausedMigration(started, proceed);
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
      throw new RuntimeException("migration interrupted", e);
    }
    return Optional.of(createAccountTrieLog(Wei.ONE));
  }

  private record PausedMigration(CountDownLatch started, CountDownLatch proceed) {

    void awaitStart() throws InterruptedException {
      if (!started.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new AssertionError("Migration did not reach the paused trie-log lookup in time");
      }
    }

    void release() {
      proceed.countDown();
    }
  }
}
