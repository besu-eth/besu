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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMinSuffix;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BonsaiFlatDbToArchiveMigratorTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x95cD8499051f7FE6a2F53749eC1e9F4a81cafa13");

  @Mock private BonsaiWorldStateKeyValueStorage worldStateStorage;
  @Mock private TrieLogManager trieLogManager;
  private MutableBlockchain blockchain;
  private SegmentedKeyValueStorage storage;
  private BlockDataGenerator blockDataGenerator;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
    blockDataGenerator = new BlockDataGenerator();
    blockchain = createInMemoryBlockchain(blockDataGenerator.genesisBlock());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(storage);
    when(trieLogManager.getTrieLogLayer(any()))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.ONE)));
  }

  @Test
  public void migratesAccountChangesFromTrieLogs() throws Exception {
    appendBlocks(2);
    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();
    final Hash hash2 = blockchain.getBlockHeader(2L).get().getHash();
    final TrieLogLayer trieLog1 = createAccountTrieLog(Wei.fromHexString("0x100"));
    final TrieLogLayer trieLog2 = createAccountTrieLog(Wei.fromHexString("0x200"));

    when(trieLogManager.getTrieLogLayer(hash1)).thenReturn(Optional.of(trieLog1));
    when(trieLogManager.getTrieLogLayer(hash2)).thenReturn(Optional.of(trieLog2));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(10, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
  }

  @Test
  public void migratesStorageChangesFromTrieLogs() throws Exception {
    appendBlocks(1);
    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final TrieLogLayer trieLog1 = new TrieLogLayer();
    trieLog1.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));

    when(trieLogManager.getTrieLogLayer(hash1)).thenReturn(Optional.of(trieLog1));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(10, TimeUnit.SECONDS);

    final byte[] naturalKey =
        calculateNaturalSlotKey(TEST_ADDRESS.addressHash(), slotKey.getSlotHash());
    final byte[] key = calculateArchiveKeyWithMinSuffix(new BonsaiContext(1L), naturalKey);
    assertThat(storage.get(ACCOUNT_STORAGE_ARCHIVE, key)).isPresent();
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
    final CountDownLatch migrationStartedLatch = new CountDownLatch(1);
    final CountDownLatch allowMigrationToFinishLatch = new CountDownLatch(1);
    appendBlocks(1);
    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();

    // Block the trie log manager to pause migration
    when(trieLogManager.getTrieLogLayer(hash1))
        .thenAnswer(
            invocation -> {
              migrationStartedLatch.countDown();
              allowMigrationToFinishLatch.await(10, TimeUnit.SECONDS);
              return Optional.of(createAccountTrieLog(Wei.ONE));
            });

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    // Start first migration
    final CompletableFuture<Void> first = migrator.migrate();

    // Wait for migration to start
    assertThat(migrationStartedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(migrator.migrationRunning).isTrue();

    // Second migration should return immediately without running
    final CompletableFuture<Void> second = migrator.migrate();
    second.get(1, TimeUnit.SECONDS);

    // Second migration must not have interacted with the database
    verify(trieLogManager).getTrieLogLayer(hash1);

    // Allow first migration to complete
    allowMigrationToFinishLatch.countDown();
    first.get(10, TimeUnit.SECONDS);

    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void tracksRunningState() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.migrationRunning).isFalse();

    migrator.migrate().get(10, TimeUnit.SECONDS);
    assertThat(migrator.migrationRunning).isFalse();
  }

  @Test
  public void savesProgressToStorage() throws Exception {
    appendBlocks(5);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    assertThat(migrator.getMigrationProgress()).isEmpty();

    migrator.migrate().get(10, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(5L);
  }

  @Test
  public void failsMigrationWhenTrieLogIsMissing() {
    appendBlocks(1);
    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();
    when(trieLogManager.getTrieLogLayer(hash1)).thenReturn(Optional.empty());

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate())
        .failsWithin(10, TimeUnit.SECONDS)
        .withThrowableThat()
        .havingRootCause()
        .withMessage("No trie log found for block 1");
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void migratesNewBlocksAddedDuringMigration() throws Exception {
    appendBlocks(2);

    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();
    final Hash hash2 = blockchain.getBlockHeader(2L).get().getHash();

    final CountDownLatch block1ProcessingLatch = new CountDownLatch(1);
    final CountDownLatch allowBlock1ToFinishLatch = new CountDownLatch(1);

    // Pause migration after block 1 starts processing
    when(trieLogManager.getTrieLogLayer(hash1))
        .thenAnswer(
            invocation -> {
              block1ProcessingLatch.countDown();
              allowBlock1ToFinishLatch.await(10, TimeUnit.SECONDS);
              return Optional.of(createAccountTrieLog(Wei.fromHexString("0x100")));
            });
    when(trieLogManager.getTrieLogLayer(hash2))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.fromHexString("0x200"))));

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    final CompletableFuture<Void> future = migrator.migrate();

    // Wait for migration to reach block 1
    assertThat(block1ProcessingLatch.await(5, TimeUnit.SECONDS)).isTrue();

    // Append block 3 while migration is paused, target should update
    appendBlocks(1);
    final Hash hash3 = blockchain.getBlockHeader(3L).get().getHash();
    when(trieLogManager.getTrieLogLayer(hash3))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.fromHexString("0x300"))));

    // Let migration continue
    allowBlock1ToFinishLatch.countDown();
    future.get(10, TimeUnit.SECONDS);

    // All three blocks should have been migrated
    assertThat(getArchivedAccountKey(1L)).isPresent();
    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isPresent();
  }

  @Test
  public void switchesToArchiveModeOnCompletion() throws Exception {
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(10, TimeUnit.SECONDS);

    verify(worldStateStorage).upgradeToArchiveFlatDbMode();
  }

  @Test
  public void resumesFromNextBlockAfterSavedProgress() throws Exception {
    appendBlocks(3);
    final Hash hash1 = blockchain.getBlockHeader(1L).get().getHash();
    final Hash hash2 = blockchain.getBlockHeader(2L).get().getHash();
    final Hash hash3 = blockchain.getBlockHeader(3L).get().getHash();

    // Run first migration over blocks 1-3
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator();
    firstMigrator.migrate().get(10, TimeUnit.SECONDS);
    assertThat(firstMigrator.getMigrationProgress()).hasValue(3L);
    firstMigrator.close(); // simulate node restart — deregisters ongoing observer

    // Append a new block and run a second migrator (simulating a restart)
    appendBlocks(1);
    final Hash hash4 = blockchain.getBlockHeader(4L).get().getHash();
    when(trieLogManager.getTrieLogLayer(hash4))
        .thenReturn(Optional.of(createAccountTrieLog(Wei.fromHexString("0x400"))));

    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator();
    secondMigrator.migrate().get(10, TimeUnit.SECONDS);

    // Blocks 1-3 must not be re-processed — each queried exactly once across both migrations
    verify(trieLogManager, times(1)).getTrieLogLayer(hash1);
    verify(trieLogManager, times(1)).getTrieLogLayer(hash2);
    verify(trieLogManager, times(1)).getTrieLogLayer(hash3);
    // Block 4 must be processed by the second migration
    verify(trieLogManager, times(1)).getTrieLogLayer(hash4);
  }

  @Test
  public void usesLowPriorityTransactionsForMigration() throws Exception {
    appendBlocks(1);
    final SegmentedInMemoryKeyValueStorage spyStorage = spy(new SegmentedInMemoryKeyValueStorage());
    when(worldStateStorage.getComposedWorldStateStorage()).thenReturn(spyStorage);

    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();
    migrator.migrate().get(10, TimeUnit.SECONDS);

    verify(spyStorage, atLeastOnce()).startLowPriorityTransaction();
  }

  @Test
  public void doesNotSwitchToArchiveModeOnFailure() {
    appendBlocks(1);
    when(trieLogManager.getTrieLogLayer(any())).thenThrow(new RuntimeException("Test failure"));
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator();

    assertThat(migrator.migrate()).failsWithin(10, TimeUnit.SECONDS);
    verify(worldStateStorage, never()).upgradeToArchiveFlatDbMode();
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
    for (Block block : blocks) {
      blockchain.appendBlock(block, blockDataGenerator.receipts(block));
    }
  }

  @Test
  public void migrationStopsAtHeadMinusBoundaryDistance() throws Exception {
    // head=5, boundaryDistance=3 → target = 5-3 = 2; blocks 1 and 2 migrated, 3 not
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(10, TimeUnit.SECONDS);

    assertThat(getArchivedAccountKey(2L)).isPresent();
    assertThat(getArchivedAccountKey(3L)).isEmpty();
  }

  @Test
  public void blockObserverPersistsAndMigratesBlockAtBoundary() throws Exception {
    // head=3, boundaryDistance=3 → initial target=0, nothing migrated initially
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(3);
    migrator.migrate().get(10, TimeUnit.SECONDS);
    assertThat(getArchivedAccountKey(1L)).isEmpty();

    // block 4 arrives → observer submits migration of block 4-3=1
    appendBlocks(1);
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
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
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(1L)).isPresent());
  }

  @Test
  public void ongoingMigrationCatchesUpMultipleBlocksBehind() throws Exception {
    // head=3, boundaryDistance=1 → initial target=2; migrate blocks 1 and 2
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(10, TimeUnit.SECONDS);
    assertThat(migrator.getMigrationProgress()).hasValue(2L);

    // blocks 4 and 5 arrive while executor is idle; block 6 triggers catch-up of 4-1=3 up to 5-1=4
    appendBlocks(3);
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(3L)).isPresent());
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getArchivedAccountKey(4L)).isPresent());
  }

  @Test
  public void ongoingMigrationUpdatesMetric() throws Exception {
    appendBlocks(3);
    final BonsaiFlatDbToArchiveMigrator migrator = createMigrator(1);
    migrator.migrate().get(10, TimeUnit.SECONDS);
    final long metricAfterMigration = migrator.migratedBlockNumber.get();

    // block 4 arrives → observer migrates block 3; metric should increment
    appendBlocks(1);
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(migrator.migratedBlockNumber.get()).isGreaterThan(metricAfterMigration));
  }

  @Test
  public void startOngoingMigrationInitializesMetricFromSavedProgress() throws Exception {
    appendBlocks(5);
    final BonsaiFlatDbToArchiveMigrator firstMigrator = createMigrator(0);
    firstMigrator.migrate().get(10, TimeUnit.SECONDS);
    firstMigrator.close();

    // Second migrator simulates a restart — metric should be initialized from saved progress
    final BonsaiFlatDbToArchiveMigrator secondMigrator = createMigrator(0);
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(0L);
    secondMigrator.startOngoingMigration();
    assertThat(secondMigrator.migratedBlockNumber.get()).isEqualTo(5L);
    secondMigrator.close();
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator() {
    return createMigrator(0);
  }

  private BonsaiFlatDbToArchiveMigrator createMigrator(final long boundaryDistance) {
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
    final BonsaiArchiveFlatDbStrategy archiveStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy());
    return new BonsaiFlatDbToArchiveMigrator(
        worldStateStorage,
        trieLogManager,
        blockchain,
        Executors.newScheduledThreadPool(1),
        metricsSystem,
        archiveStrategy,
        boundaryDistance);
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
}
