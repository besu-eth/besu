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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.NoOpBonsaiCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.CacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveMigrationTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;
import org.hyperledger.besu.util.log.LogUtil;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migrates a Bonsai flat DB node to Bonsai archive format without requiring a full resync.
 *
 * <p>Migration replays trie logs sequentially from block 0 (or the last saved checkpoint) to the
 * current chain head, writing archive-keyed entries into the archive column families. Progress is
 * persisted atomically with each block's data so the migration can safely resume after a restart.
 *
 * <p>The chain head target is updated in real time as new blocks arrive, so the migrator chases the
 * head until it converges. Once all blocks are processed, the flat DB mode is atomically switched
 * to {@link org.hyperledger.besu.ethereum.worldstate.FlatDbMode#ARCHIVE}.
 */
public class BonsaiFlatDbToArchiveMigrator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiFlatDbToArchiveMigrator.class);
  private static final int LOG_INTERVAL_SECONDS = 60;
  private static final long CATCHUP_LOG_THRESHOLD = 32;
  private static final Executor PREFETCH_POOL = Executors.newVirtualThreadPerTaskExecutor();

  private static final byte[] MIGRATION_PROGRESS_KEY =
      "ARCHIVE_MIGRATION_PROGRESS".getBytes(StandardCharsets.UTF_8);

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final TrieLogManager trieLogManager;
  private final Blockchain blockchain;
  private final ScheduledExecutorService executorService;
  private final BonsaiArchiveFlatDbStrategy archiveStrategy;
  private final AtomicBoolean shouldLogProgress = new AtomicBoolean(true);
  protected final AtomicLong migratedBlockNumber = new AtomicLong(0);
  protected final AtomicBoolean migrationRunning = new AtomicBoolean(false);
  protected final AtomicLong ongoingTarget = new AtomicLong(0);
  protected final AtomicBoolean catchUpRunning = new AtomicBoolean(false);
  private volatile boolean catchUpFailed = false;
  protected volatile OptionalLong blockObserverId = OptionalLong.empty();
  private boolean closed = false;

  private BonsaiWorldState migrationWorldState;
  private MigrationTrieStorage migrationTrieStorage;
  private BonsaiCachedMerkleTrieLoader migrationTrieLoader;
  private BonsaiWorldStateKeyValueStorage migrationKvStorage;

  /**
   * Creates a new BonsaiFlatDbToArchiveMigrator.
   *
   * @param worldStateStorage the Bonsai world state storage
   * @param trieLogManager the trie log manager for reading trie logs
   * @param blockchain the blockchain for reading block headers
   * @param executorService the executor service for running migration on a separate thread
   * @param metricsSystem the metrics system for tracking migration progress
   * @param archiveStrategy the archive flat DB strategy for writing archive keys
   */
  public BonsaiFlatDbToArchiveMigrator(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final TrieLogManager trieLogManager,
      final Blockchain blockchain,
      final ScheduledExecutorService executorService,
      final MetricsSystem metricsSystem,
      final BonsaiArchiveFlatDbStrategy archiveStrategy) {
    this.worldStateStorage = worldStateStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.executorService = executorService;
    this.archiveStrategy = archiveStrategy;
    metricsSystem.createLongGauge(
        BesuMetricCategory.BLOCKCHAIN,
        "bonsai_archive_migration_block",
        "The current block the Bonsai archive migration has reached",
        migratedBlockNumber::get);
    if (archiveStrategy.getTrieNodeCheckpointInterval() != null) {
      initMigrationWorldState(metricsSystem);
      recoverTrieState();
    }
  }

  /**
   * Migrates Bonsai flat DB to Bonsai archive format.
   *
   * @return a CompletableFuture that completes when migration finishes
   */
  public synchronized CompletableFuture<Void> migrate() {
    if (closed) {
      LOG.debug("migrate called after close; skipping");
      return CompletableFuture.completedFuture(null);
    }
    if (!migrationRunning.compareAndSet(false, true)) {
      LOG.warn("Bonsai migration already in progress, ignoring");
      return CompletableFuture.completedFuture(null);
    }

    final Instant migrationStartTime = Instant.now();
    final long lastProcessedBlock = getMigrationProgress().orElse(-1L);
    final long startBlock = lastProcessedBlock + 1;
    migratedBlockNumber.set(Math.max(0, lastProcessedBlock));

    final AtomicLong target = new AtomicLong(archiveTarget(blockchain.getChainHeadBlockNumber()));
    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (event.isNewCanonicalHead()) {
                    final long newTarget = archiveTarget(event.getHeader().getNumber());
                    target.updateAndGet(current -> Math.max(current, newTarget));
                  }
                }));

    LOG.info("Starting Bonsai Archive migration from block {}", startBlock);
    try {
      return CompletableFuture.runAsync(
          () -> {
            try {
              migrateBlocks(startBlock, target, true);
              worldStateStorage.upgradeToArchiveFlatDbMode();
              logCompletion(startBlock, target.get(), migrationStartTime);
              // Hand off observers without a gap: register the ongoing observer first, then
              // remove the bulk observer. A block arriving mid-handoff still reaches the ongoing
              // observer; removing the bulk one first would drop any event landing in between.
              final OptionalLong bulkObserverId = blockObserverId;
              blockObserverId = OptionalLong.empty();
              startOngoingMigration();
              bulkObserverId.ifPresent(blockchain::removeObserver);
            } catch (final RuntimeException ex) {
              blockObserverId.ifPresent(blockchain::removeObserver);
              blockObserverId = OptionalLong.empty();
              LOG.error("Bonsai to Bonsai archive migration failed", ex);
              throw ex;
            } finally {
              migrationRunning.set(false);
            }
          },
          executorService);
    } catch (final RejectedExecutionException e) {
      blockObserverId.ifPresent(blockchain::removeObserver);
      blockObserverId = OptionalLong.empty();
      migrationRunning.set(false);
      LOG.warn("Bonsai migration executor rejected scheduling", e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private void migrateBlocks(
      final long startBlock, final AtomicLong target, final boolean shouldLog) {
    CompletableFuture<Optional<TrieLog>> prefetched = prefetchTrieLog(startBlock);
    for (long blockNumber = startBlock; blockNumber <= target.get(); blockNumber++) {
      final Optional<TrieLog> maybeTrieLog = prefetched.join();
      prefetched = prefetchTrieLog(blockNumber + 1);
      if (maybeTrieLog.isPresent()) {
        final SegmentedKeyValueStorageTransaction tx =
            worldStateStorage.getComposedWorldStateStorage().startLowPriorityTransaction();
        if (migrationWorldState != null) {
          migrateTrieBlock(maybeTrieLog.get(), blockNumber);
        }
        processBlock(maybeTrieLog.get(), blockNumber, tx);
        saveProgress(blockNumber, tx);
        tx.commit();
        migratedBlockNumber.set(blockNumber);
        if (shouldLog) {
          logProgress(blockNumber, target.get());
        }
      } else if (blockNumber > 0) {
        throw new IllegalStateException("No trie log found for block " + blockNumber);
      }
    }
  }

  private CompletableFuture<Optional<TrieLog>> prefetchTrieLog(final long blockNumber) {
    return CompletableFuture.supplyAsync(
        () ->
            blockchain
                .getBlockHeader(blockNumber)
                .flatMap(header -> trieLogManager.getTrieLogLayer(header.getHash())),
        PREFETCH_POOL);
  }

  /**
   * Starts the ongoing migration of blocks to bonsai archive. This should only be called after the
   * initial migration has been completed.
   */
  public synchronized void startOngoingMigration() {
    if (closed) {
      LOG.debug("startOngoingMigration called after close; skipping");
      return;
    }
    if (blockObserverId.isPresent()) {
      LOG.debug("startOngoingMigration called while an observer is already registered; skipping");
      return;
    }
    migratedBlockNumber.set(getMigrationProgress().orElse(0L));
    blockObserverId =
        OptionalLong.of(
            blockchain.observeBlockAdded(
                event -> {
                  if (!event.isNewCanonicalHead()) {
                    return;
                  }
                  final long newTarget = archiveTarget(event.getHeader().getNumber());
                  if (newTarget <= 0) {
                    return;
                  }
                  ongoingTarget.accumulateAndGet(newTarget, Math::max);
                  scheduleCatchUpIfNeeded();
                }));
  }

  private void scheduleCatchUpIfNeeded() {
    if (catchUpFailed) {
      return;
    }
    if (!catchUpRunning.compareAndSet(false, true)) {
      return;
    }
    try {
      executorService.submit(this::catchUp);
    } catch (final RejectedExecutionException e) {
      catchUpRunning.set(false);
      LOG.debug(
          "Bonsai migrator executor shut down; skipping migration up to block {}",
          ongoingTarget.get());
    }
  }

  private void catchUp() {
    boolean failed = false;
    try {
      final long startBlock = migratedBlockNumber.get() + 1;
      final long initialTarget = ongoingTarget.get();
      if (startBlock > initialTarget) {
        return;
      }
      final long blocksToMigrate = initialTarget - startBlock + 1;
      final boolean shouldLog = blocksToMigrate >= CATCHUP_LOG_THRESHOLD;
      final Instant catchUpStart = shouldLog ? Instant.now() : null;
      LOG.debug(
          "Bonsai archive catch-up starting: {} blocks from {} to {}",
          blocksToMigrate,
          startBlock,
          initialTarget);
      migrateBlocks(startBlock, ongoingTarget, shouldLog);
      if (shouldLog) {
        final Duration duration = Duration.between(catchUpStart, Instant.now());
        LOG.info(
            "Bonsai archive catch-up complete: {} blocks in {}",
            (migratedBlockNumber.get() - startBlock + 1),
            DurationFormatUtils.formatDurationWords(duration.toMillis(), true, true));
      }
    } catch (final RuntimeException ex) {
      failed = true;
      catchUpFailed = true;
      LOG.error(
          "Bonsai archive catch-up failed at block {} — archive proofs will be unavailable until restart: {}",
          migratedBlockNumber.get() + 1,
          ex.getMessage(),
          ex);
    } finally {
      catchUpRunning.set(false);
      // Do not reschedule on failure — a persistent error would otherwise create a
      // tight log-spam loop. The migration will resume on the next node restart.
      if (!failed && migratedBlockNumber.get() < ongoingTarget.get()) {
        scheduleCatchUpIfNeeded();
      }
    }
  }

  private long archiveTarget(final long blockNumber) {
    return Math.max(0, blockNumber - trieLogManager.getMaxLayersToLoad());
  }

  /**
   * Returns the current migrated block.
   *
   * @return the highest block number that bonsai archive has migrated to
   */
  public long getMigratedBlockNumber() {
    return migratedBlockNumber.get();
  }

  @Override
  public synchronized void close() {
    closed = true;
    blockObserverId.ifPresent(blockchain::removeObserver);
    blockObserverId = OptionalLong.empty();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Migration executor did not terminate within 10 seconds");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void logProgress(final long blockNumber, final long endBlock) {
    LogUtil.throttledLog(
        () -> {
          long progressPercent = endBlock > 0 ? (blockNumber * 100) / endBlock : 0;
          LOG.info(
              "Bonsai Archive migration progress: {}% (block {}/{})",
              progressPercent, blockNumber, endBlock);
        },
        shouldLogProgress,
        LOG_INTERVAL_SECONDS);
  }

  private void logCompletion(
      final long startBlock, final long endBlock, final Instant migrationStartTime) {
    final Duration migrationDuration = Duration.between(migrationStartTime, Instant.now());
    final String formattedDuration =
        DurationFormatUtils.formatDurationWords(migrationDuration.toMillis(), true, true);
    LOG.info(
        "Bonsai Archive migration completed. Processed {} blocks in {}.",
        endBlock - startBlock + 1,
        formattedDuration);
  }

  private void processBlock(
      final TrieLog trieLog, final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    final BonsaiContext context = new BonsaiContext(blockNumber);
    processAccountChanges(trieLog, context, tx);
    processStorageChanges(trieLog, context, tx);
  }

  private void processAccountChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
    trieLog
        .getAccountChanges()
        .forEach(
            (address, accountChange) -> {
              if (accountChange.getUpdated() != null) {
                final BytesValueRLPOutput out = new BytesValueRLPOutput();
                accountChange.getUpdated().writeTo(out);
                archiveStrategy.putFlatAccount(context, tx, address.addressHash(), out.encoded());
              } else {
                archiveStrategy.removeFlatAccount(context, tx, address.addressHash());
              }
            });
  }

  private void processStorageChanges(
      final TrieLog trieLog,
      final BonsaiContext context,
      final SegmentedKeyValueStorageTransaction tx) {
    trieLog
        .getStorageChanges()
        .forEach(
            (address, storageMap) ->
                storageMap.forEach(
                    (slotKey, storageChange) -> {
                      if (storageChange.getUpdated() != null) {
                        archiveStrategy.putFlatAccountStorageValueByStorageSlotHash(
                            context,
                            tx,
                            address.addressHash(),
                            slotKey.getSlotHash(),
                            storageChange.getUpdated().toBytes());
                      } else {
                        archiveStrategy.removeFlatAccountStorageValueByStorageSlotHash(
                            context, tx, address.addressHash(), slotKey.getSlotHash());
                      }
                    }));
  }

  @VisibleForTesting
  protected Optional<Long> getMigrationProgress() {
    return worldStateStorage
        .getComposedWorldStateStorage()
        .get(ACCOUNT_INFO_STATE_ARCHIVE, MIGRATION_PROGRESS_KEY)
        .map(Bytes::wrap)
        .map(Bytes::toLong);
  }

  private void saveProgress(final long blockNumber, final SegmentedKeyValueStorageTransaction tx) {
    tx.put(
        ACCOUNT_INFO_STATE_ARCHIVE,
        MIGRATION_PROGRESS_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
  }

  private void initMigrationWorldState(final MetricsSystem metricsSystem) {
    final long interval = archiveStrategy.getTrieNodeCheckpointInterval();
    final BonsaiArchiveFlatDbStrategy readStrategy =
        new BonsaiArchiveFlatDbStrategy(metricsSystem, new CodeHashCodeStorageStrategy(), interval);
    migrationTrieStorage =
        new MigrationTrieStorage(worldStateStorage.getComposedWorldStateStorage());
    final StaticArchiveFlatDbStrategyProvider provider =
        new StaticArchiveFlatDbStrategyProvider(metricsSystem, readStrategy);
    provider.loadFlatDbStrategy(migrationTrieStorage);
    migrationTrieLoader = new NoopBonsaiCachedMerkleTrieLoader();
    migrationKvStorage =
        new BonsaiWorldStateKeyValueStorage(
            provider,
            migrationTrieStorage,
            new InMemoryKeyValueStorage(),
            CacheManager.NO_OP_CACHE,
            0L,
            new BonsaiArchiveMigrationTrieNodeStrategy(interval, migrationTrieLoader));
    final CodeCache codeCache = new CodeCache();
    migrationWorldState =
        new BonsaiWorldState(
            migrationKvStorage,
            migrationTrieLoader,
            new NoOpBonsaiCachedWorldStorageManager(
                migrationKvStorage, EvmConfiguration.DEFAULT, codeCache),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            WorldStateConfig.newBuilder(WorldStateConfig.createStatefulConfigWithTrie())
                .parallelStateRootComputationEnabled(false)
                .build(),
            codeCache);
  }

  private void recoverTrieState() {
    final long progress = getMigrationProgress().orElse(-1L);
    if (progress < 0) {
      // Fresh start: leave the migration world state at the default empty-trie root
      // (WORLD_ROOT_HASH_KEY unset → Hash.EMPTY_TRIE_HASH). migrateBlocks() will roll
      // forward from block 0, including the genesis trie log, so all accounts end up
      // in the accumulator. persist() at the first checkpoint then builds the trie
      // from scratch without needing historical nodes from TRIE_BRANCH_STORAGE.
      return;
    }
    final long interval = archiveStrategy.getTrieNodeCheckpointInterval();
    // Derive the last trie checkpoint: largest block B ≤ progress where (B+1) % interval == 0
    final long lastCheckpoint = ((progress + 1) / interval) * interval - 1;
    if (lastCheckpoint >= 0) {
      // A real checkpoint was persisted. Seed the metadata layer and reset the in-memory
      // worldStateRootHash field so persist() at the next checkpoint starts from this
      // block's state root, not Hash.EMPTY_TRIE_HASH (which is what initMigrationWorldState
      // sets on a fresh JVM start before any persist() has been called).
      blockchain
          .getBlockHeader(lastCheckpoint)
          .ifPresent(
              header -> {
                migrationTrieStorage.seedCheckpoint(header);
                // resetWorldStateTo updates the in-memory worldStateRootHash / worldStateBlockHash
                // fields that persist() reads as the base trie root.  seedCheckpoint only writes
                // the key-value layer; without this call the field stays EMPTY_TRIE_HASH and the
                // next checkpoint's persist() produces the wrong state root.
                migrationWorldState.resetWorldStateTo(header);
              });
      final long reRollStart = lastCheckpoint + 1;
      if (reRollStart <= progress) {
        reRollTrieFrom(reRollStart, progress);
      }
    } else {
      // No trie checkpoint reached yet. Re-roll from block 0 (the genesis trie log creates all
      // genesis accounts) so the accumulator has the full account set. persist() at the first
      // checkpoint builds from the empty-trie root without reading from TRIE_BRANCH_STORAGE,
      // avoiding stale HEAD nodes that have overwritten the genesis/historical trie.
      reRollTrieFrom(0, progress);
    }
  }

  private void reRollTrieFrom(final long startBlock, final long endBlock) {
    for (long b = startBlock; b <= endBlock; b++) {
      final long blockNum = b;
      blockchain
          .getBlockHeader(blockNum)
          .flatMap(h -> trieLogManager.getTrieLogLayer(h.getHash()))
          .ifPresent(
              tl ->
                  ((PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater())
                      .rollForward(tl));
    }
  }

  private void migrateTrieBlock(final TrieLog trieLog, final long blockNumber) {
    ((PathBasedWorldStateUpdateAccumulator<?>) migrationWorldState.updater()).rollForward(trieLog);
    if (isTrieCheckpointBlock(blockNumber)) {
      final long interval = archiveStrategy.getTrieNodeCheckpointInterval();
      final long suffix = (blockNumber / interval) * interval;
      LOG.info(
          "Archive trie checkpoint: persisting block {} (window suffix {})", blockNumber, suffix);
      blockchain
          .getBlockHeader(blockNumber)
          .ifPresent(
              header -> {
                migrationWorldState.persist(header);
                // The migration CF is persistent and overwrite-in-place, so trie-node reads in
                // the next window find this window's nodes immediately — no clearInMemory() wipe
                // of a read substrate. Only the in-memory metadata keys are refreshed.
                migrationTrieStorage.seedCheckpoint(header);
                LOG.info(
                    "Archive trie checkpoint complete: block {} suffix {} stateRoot {}",
                    blockNumber,
                    suffix,
                    header.getStateRoot());
              });
    }
  }

  private boolean isTrieCheckpointBlock(final long blockNumber) {
    return blockNumber > 0
        && (blockNumber + 1) % archiveStrategy.getTrieNodeCheckpointInterval() == 0;
  }

  private static final class MigrationTrieStorage extends LayeredKeyValueStorage {
    private final SegmentedKeyValueStorage real;

    MigrationTrieStorage(final SegmentedKeyValueStorage real) {
      super(real);
      this.real = real;
    }

    @Override
    public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Metadata keys must stay in-memory (live HEAD values would corrupt migration context).
        // ARCHIVE_PROOF_BLOCK_NUMBER_KEY is also intercepted: the proof-serving code writes this
        // key to TRIE_BRANCH_STORAGE with a live HEAD-adjacent block number, and
        // BonsaiArchiveTrieNodeStrategy.getStateTrieArchiveContextForWrite reads it first. If it
        // leaked from live storage into the migration context, archive trie-node writes would
        // receive a large suffix (near HEAD) instead of the correct window-based suffix, making
        // them invisible to subsequent window reads (which search suffix ≤ checkpoint block).
        if (java.util.Arrays.equals(key, WORLD_BLOCK_NUMBER_KEY)
            || java.util.Arrays.equals(key, WORLD_BLOCK_HASH_KEY)
            || java.util.Arrays.equals(key, WORLD_ROOT_HASH_KEY)
            || java.util.Arrays.equals(key, ARCHIVE_PROOF_BLOCK_NUMBER_KEY)) {
          return getFromLayerOnly(segmentId, key);
        }
        // Trie node data: check in-memory first, then live storage.
        // Unchanged nodes are identical at any historical state and at HEAD.
        final Optional<byte[]> inMemory = getFromLayerOnly(segmentId, key);
        return inMemory.isPresent() ? inMemory : real.get(segmentId, key);
      }
      return real.get(segmentId, key);
    }

    @Override
    public SegmentedKeyValueStorageTransaction startTransaction() {
      return new MigrationTransaction(super.startTransaction(), real.startLowPriorityTransaction());
    }

    void seedCheckpoint(final BlockHeader header) {
      // Write metadata keys to in-memory layer so trie archive context reads are correct
      final SegmentedKeyValueStorageTransaction tx = super.startTransaction();
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_ROOT_HASH_KEY,
          header.getStateRoot().getBytes().toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_HASH_KEY,
          header.getBlockHash().getBytes().toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_NUMBER_KEY,
          Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
      tx.commit();
    }
  }

  private static final class MigrationTransaction implements SegmentedKeyValueStorageTransaction {
    private final SegmentedKeyValueStorageTransaction inMemoryTx;
    private final SegmentedKeyValueStorageTransaction realTx;

    MigrationTransaction(
        final SegmentedKeyValueStorageTransaction inMemoryTx,
        final SegmentedKeyValueStorageTransaction realTx) {
      this.inMemoryTx = inMemoryTx;
      this.realTx = realTx;
    }

    @Override
    public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        // Metadata keys only (WORLD_*, ARCHIVE_PROOF_BLOCK_NUMBER_KEY) — kept in-memory so they
        // never touch live HEAD's TRIE_BRANCH_STORAGE.
        inMemoryTx.put(segmentId, key, value);
      } else if (segmentId == TRIE_BRANCH_STORAGE_ARCHIVE) {
        realTx.put(segmentId, key, value);
      }
      // flat account/storage writes dropped — processBlock() handles those separately
    }

    @Override
    public void remove(final SegmentIdentifier segmentId, final byte[] key) {
      if (segmentId == TRIE_BRANCH_STORAGE) {
        inMemoryTx.remove(segmentId, key);
      }
      // archive removes dropped
    }

    @Override
    public void commit() {
      inMemoryTx.commit();
      realTx.commit();
    }

    @Override
    public void rollback() {
      inMemoryTx.rollback();
      realTx.rollback();
    }

    @Override
    public void close() {
      inMemoryTx.close();
      realTx.close();
    }
  }

  private static final class StaticArchiveFlatDbStrategyProvider
      extends BonsaiFlatDbStrategyProvider {
    private final BonsaiArchiveFlatDbStrategy strategy;

    StaticArchiveFlatDbStrategyProvider(
        final MetricsSystem metricsSystem, final BonsaiArchiveFlatDbStrategy strategy) {
      super(metricsSystem, DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);
      this.strategy = strategy;
    }

    @Override
    protected FlatDbStrategy createFlatDbStrategy(
        final FlatDbMode flatDbMode,
        final MetricsSystem metricsSystem,
        final CodeStorageStrategy codeStorageStrategy) {
      return strategy;
    }
  }
}
