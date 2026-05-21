/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Two-stage fast sync chain downloader that orchestrates:
 *
 * <p>Stage 1: Backward header download from pivot block to stop block
 *
 * <p>Stage 2: Forward bodies/receipts download from start block to pivot block
 *
 * <p>Supports incremental continuation when the world state downloader updates the pivot block,
 * avoiding re-downloading already synced data.
 */
public class SnapSyncChainDownloader
    implements ChainDownloader, PivotUpdateListener, WorldStateHealFinishedListener {
  private static final Logger LOG = LoggerFactory.getLogger(SnapSyncChainDownloader.class);
  public static final int SMALL_DELAY_MILLISECONDS = 100;
  static final int NO_PEER_RETRY_DELAY_MILLISECONDS = 5_000;
  private static final int MAX_SAME_STATE_RETRIES = 20;

  private final SnapSyncChainDownloadPipelineFactory pipelineFactory;
  private final SynchronizerConfiguration syncConfig;
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final MutableBlockchain blockchain;
  private final EthContext ethContext;
  private final SyncState syncState;
  private final SyncDurationMetrics syncDurationMetrics;
  private final ChainSyncStateStorage chainSyncStateStorage;
  private final BlockHeader initialPivotHeader;
  private final SingleBlockHeaderDownloader headerDownloader;

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicReference<ChainSyncState> chainSyncState = new AtomicReference<>(null);
  private final AtomicReference<BlockHeader> pendingPivotUpdate = new AtomicReference<>(null);
  private CompletableFuture<Void> pivotUpdateFuture = new CompletableFuture<>();
  private final CompletableFuture<Void> worldStateHealFinishedFuture = new CompletableFuture<>();
  private volatile SnapWorldDownloadState worldDownloadState;

  private final AtomicBoolean downloadInProgress = new AtomicBoolean(false);
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private final AtomicInteger sameStateRetryCount = new AtomicInteger(0);
  private volatile ChainSyncState lastRetriedState = null;

  private volatile Pipeline<?> currentPipeline;
  private volatile BackwardHeaderDriver currentDriver;
  private Instant overallStartTime;

  /**
   * Creates a new TwoStageFastSyncChainDownloader. The first stage is to download all headers from
   * a safe pivot down to the genesis. Due to the chain of parent hashes in the headers, as well as
   * the trusted pivot and the known genesis, we can trust the downloaded headers.
   *
   * <p>The second stage is to download the bodies and receipts. Bodies and receipts are validated
   * by checking the transactions root and the receipts root against the values contained in the
   * trusted headers from the first stage. Bodies and receipts might not be downloaded from genesis,
   * but from a checkpoint block, e.g. for mainnet from the merge block.
   *
   * <p>Once the second stage is completed we start downloading headers, bodies, and receipts, when
   * the pivot block is updated. In this case we download the headers from the new pivot down to the
   * new pivot block, followed by the bodies and receipts from the old pivot block to the new pivot
   * block.
   *
   * @param pipelineFactory the pipeline factory for creating download pipelines
   * @param syncConfig the synchronizer configuration
   * @param protocolSchedule the protocol schedule for deserializing headers
   * @param protocolContext the protocol context providing access to the blockchain
   * @param ethContext the ethContext for running pipelines
   * @param syncState the sync state tracker
   * @param syncDurationMetrics the sync duration metrics tracker
   * @param initialPivotHeader the initial pivot block header
   * @param chainStateStorage the storage for chain sync state
   * @param headerDownloader the downloader for fetching checkpoint headers
   */
  public SnapSyncChainDownloader(
      final SnapSyncChainDownloadPipelineFactory pipelineFactory,
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final SyncDurationMetrics syncDurationMetrics,
      final BlockHeader initialPivotHeader,
      final ChainSyncStateStorage chainStateStorage,
      final SingleBlockHeaderDownloader headerDownloader) {
    this.pipelineFactory = pipelineFactory;
    this.syncConfig = syncConfig;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.blockchain = protocolContext.getBlockchain();
    this.ethContext = ethContext;
    this.syncState = syncState;
    this.syncDurationMetrics = syncDurationMetrics;
    this.initialPivotHeader = initialPivotHeader;
    this.chainSyncStateStorage = chainStateStorage;
    this.headerDownloader = headerDownloader;
  }

  public static ChainDownloader create(
      final SynchronizerConfiguration config,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final MetricsSystem metricsSystem,
      final PivotSyncState fastSyncState,
      final SyncDurationMetrics syncDurationMetrics,
      final Path fastSyncDataDirectory) {

    final SnapSyncChainDownloadPipelineFactory pipelineFactory =
        new SnapSyncChainDownloadPipelineFactory(
            config, protocolSchedule, protocolContext, ethContext, fastSyncState, metricsSystem);

    final BlockHeader pivotBlockHeader =
        fastSyncState
            .getPivotBlockHeader()
            .orElseThrow(() -> new RuntimeException("pivot block header not available"));
    final Hash pivotBlockHash = pivotBlockHeader.getHash();
    LOG.debug(
        "Using two-stage fast sync with pivotHash={}, pivotBlockNumber={}, ",
        pivotBlockHash,
        pivotBlockHeader.getNumber());

    final ChainSyncStateStorage chainSyncStateStorage =
        new ChainSyncStateStorage(fastSyncDataDirectory);

    final SingleBlockHeaderDownloader headerDownloader =
        new SingleBlockHeaderDownloader(ethContext, protocolSchedule);

    return new SnapSyncChainDownloader(
        pipelineFactory,
        config,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        syncDurationMetrics,
        pivotBlockHeader,
        chainSyncStateStorage,
        headerDownloader);
  }

  @Override
  public void onPivotUpdated(final BlockHeader newPivotBlockHeader) {
    synchronized (this) {
      pendingPivotUpdate.getAndSet(newPivotBlockHeader);
      pivotUpdateFuture.complete(null);
    }
    LOG.info("Received pivot update to block no {}", newPivotBlockHeader.getNumber());
  }

  @Override
  public void onWorldStateHealFinished() {
    LOG.info("World state download is stable, no more pivot updates expected");
    worldStateHealFinishedFuture.complete(null);
  }

  /**
   * Sets the world download state reference so the chain downloader can trigger completion checks.
   *
   * @param worldDownloadState the world download state
   */
  public void setWorldDownloadState(final SnapWorldDownloadState worldDownloadState) {
    this.worldDownloadState = worldDownloadState;
  }

  @Override
  public CompletableFuture<Void> start() {
    if (!initialized.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("SnapSyncChainDownloader already started"));
    }

    overallStartTime = Instant.now();
    syncDurationMetrics.startTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);

    // Initialize chain sync state asynchronously before starting download
    return initializeChainSyncState()
        .thenCompose(
            state -> {
              final BlockHeader pivotBlockHeader = state.pivotBlockHeader();
              final BlockHeader checkpointBlockHeader = state.blockDownloadAnchor();
              LOG.info(
                  "Starting two-stage fast sync chain download from pivot block {}, {}, and checkpoint block {}.",
                  pivotBlockHeader.getHash(),
                  pivotBlockHeader.getNumber(),
                  checkpointBlockHeader.getNumber());

              return downloadAccordingToChainState();
            })
        .handle(
            (ignored, throwable) -> {
              if (throwable != null) {
                if (throwable instanceof CancellationException) {
                  LOG.info("Two-stage fast sync chain download cancelled");
                } else {
                  LOG.error("Two-stage fast sync chain download failed", throwable);
                }
                // Stop metrics on failure
                syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                return CompletableFuture.<Void>failedFuture(throwable);
              } else {
                final Duration totalDuration = Duration.between(overallStartTime, Instant.now());
                LOG.info(
                    "Two-stage fast sync chain download finished in {} seconds (including pivot updates)",
                    totalDuration.toSeconds());
                // Stop metrics on success
                syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                chainSyncStateStorage.deleteState();
                return CompletableFuture.<Void>completedFuture(null);
              }
            })
        .thenCompose(f -> f);
  }

  /**
   * Initializes the chain sync state by loading from storage or creating new state. This method
   * performs async operations including network calls to download checkpoint headers if needed.
   *
   * @return CompletableFuture that completes with the initialized ChainSyncState
   */
  private CompletableFuture<ChainSyncState> initializeChainSyncState() {
    // Try to load existing state from storage
    final ChainSyncState loadedState =
        chainSyncStateStorage.loadState(
            rlpInput ->
                BlockHeader.readFrom(
                    rlpInput, ScheduleBasedBlockHeaderFunctions.create(protocolSchedule)));

    if (loadedState != null) {
      final ChainSyncState stateToUse;

      // Stage 1 only re-writes canonical index entries from the new pivot down to the anchor.
      // Entries above the new pivot that were written in a previous session are not overwritten and
      // would give false positives from isOnOurChain(). Strip them now so the index is accurate.
      final long newPivotNumber = initialPivotHeader.getNumber();
      final long oldPivotNumber = loadedState.pivotBlockHeader().getNumber();
      if (newPivotNumber < oldPivotNumber) {
        protocolContext
            .getBlockchain()
            .unsafeStripCanonicalIndexRange(newPivotNumber, oldPivotNumber);
      }

      if (isOnOurChain(initialPivotHeader)) {
        // Case A: the new pivot is already on our canonical chain, so the headers up to it are
        // stored. Preserve the existing headersDownloadComplete flag: if the prior download
        // finished we can skip Stage 1; if it did not, Stage 1 still needs to complete.
        stateToUse =
            loadedState.withPivot(initialPivotHeader, loadedState.headersDownloadComplete());
        chainSyncStateStorage.storeState(stateToUse);

      } else if (initialPivotHeader.getNumber() - loadedState.pivotBlockHeader().getNumber()
          < syncConfig.getChainSyncContinuationThresholdBlocks()) {
        // Case B: the new pivot is within the continuation threshold of the persisted pivot
        // (or even below it). The header work we had in flight is cheap to redo — restart
        // Stage 1 from the new pivot down to the existing floor, with the completion flag
        // explicitly reset.
        stateToUse = loadedState.withPivot(initialPivotHeader, false);
        chainSyncStateStorage.storeState(stateToUse);
        LOG.info(
            "Initial pivot {} is within continuation threshold of persisted pivot {}; restarting header download from new pivot",
            initialPivotHeader.getNumber(),
            loadedState.pivotBlockHeader().getNumber());

      } else {
        // Case C: the new pivot is further than the threshold ahead. Don't throw away the
        // headers already downloaded — finish the current cycle against the persisted pivot,
        // then let the existing pivot-update plumbing transition to the new pivot via
        // continueToNewPivot.
        stateToUse = loadedState;
        LOG.info(
            "Initial pivot {} is more than continuation threshold ahead of persisted pivot {}; finishing current cycle then continuing to new pivot",
            initialPivotHeader.getNumber(),
            loadedState.pivotBlockHeader().getNumber());
        onPivotUpdated(initialPivotHeader);
      }

      chainSyncState.set(stateToUse);
      return CompletableFuture.completedFuture(stateToUse);
    }

    // No existing state - create initial state
    LOG.debug("No existing chain sync state found, creating initial state");

    final Optional<Checkpoint> maybeCheckpoint = syncState.getCheckpoint();

    if (maybeCheckpoint.isEmpty()) {
      // No checkpoint - use current chain head as lower trust anchor
      final BlockHeader checkpointBlockHeader = blockchain.getChainHeadHeader();
      final Hash checkpointHash = checkpointBlockHeader.getHash();
      LOG.debug(
          "No checkpoint found, using current chain head as lower trust anchor: {}, {}",
          checkpointBlockHeader.getNumber(),
          checkpointHash);

      final BlockHeader genesisBlockHeader = blockchain.getGenesisBlockHeader();
      final ChainSyncState newState =
          ChainSyncState.initialSync(initialPivotHeader, checkpointBlockHeader, genesisBlockHeader);

      LOG.info("Created initial chain sync state: {}", newState);
      chainSyncState.set(newState);
      chainSyncStateStorage.storeState(newState);
      return CompletableFuture.completedFuture(newState);
    }

    // Checkpoint exists - download checkpoint header asynchronously
    final Checkpoint checkpoint = maybeCheckpoint.get();
    final Hash checkpointHash = checkpoint.blockHash();
    final Difficulty checkpointDifficulty = checkpoint.totalDifficulty();

    LOG.debug(
        "Downloading checkpoint block header {}, {} (difficulty: {})",
        checkpoint.blockNumber(),
        checkpointHash,
        checkpointDifficulty);

    return headerDownloader
        .downloadBlockHeader(checkpointHash)
        .thenApply(
            checkpointBlockHeader -> {
              // Store checkpoint header in blockchain
              blockchain.unsafeSetChainHead(checkpointBlockHeader, checkpointDifficulty);
              blockchain.unsafeStoreHeader(checkpointBlockHeader, checkpointDifficulty);

              LOG.debug(
                  "Using block number {} as lower trust anchor: {}, {}",
                  checkpoint.blockNumber(),
                  checkpoint.blockHash(),
                  checkpoint.totalDifficulty());

              final BlockHeader genesisBlockHeader = blockchain.getGenesisBlockHeader();
              final ChainSyncState newState =
                  ChainSyncState.initialSync(
                      initialPivotHeader, checkpointBlockHeader, genesisBlockHeader);

              LOG.info("Created initial chain sync state: {}", newState);
              chainSyncState.set(newState);
              chainSyncStateStorage.storeState(newState);
              return newState;
            });
  }

  private boolean isOnOurChain(final BlockHeader header) {
    return blockchain.blockIsOnCanonicalChain(header.getHash());
  }

  // Returns true if a body is stored under the canonical hash for blockNumber.
  private boolean hasBodyForCanonicalBlock(final long blockNumber) {
    return protocolContext
        .getBlockchain()
        .getBlockHashByNumber(blockNumber)
        .flatMap(blockchain::getBlockBody)
        .isPresent();
  }

  /**
   * Binary-searches {@code (anchorNumber, upperBound]} for the highest block that has a body stored
   * under the current canonical chain hash, with 128-block precision. Returns empty when no block
   * above the anchor has a body.
   */
  private Optional<BlockHeader> findBodyDownloadFrontier(
      final long anchorNumber, final long upperBound) {
    if (hasBodyForCanonicalBlock(upperBound)) {
      return blockchain.getBlockHeader(upperBound);
    }

    long low = anchorNumber;
    long high = upperBound - 1;

    while (high - low > 128) {
      final long mid = low + (high - low) / 2;
      if (hasBodyForCanonicalBlock(mid)) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }

    if (low <= anchorNumber) {
      return Optional.empty();
    }
    return blockchain.getBlockHeader(low);
  }

  /**
   * Advances {@code blockDownloadAnchor} to the highest already-imported block on the canonical
   * chain so Stage 2 skips bodies that were imported in a prior session. Called between Stage 1 and
   * Stage 2 in {@link #performSingleDownloadCycle()}.
   */
  private void advanceBodyDownloadAnchor() {
    final ChainSyncState state = chainSyncState.get();
    final long anchorNumber = state.blockDownloadAnchor().getNumber();
    final long pivotNumber = state.pivotBlockHeader().getNumber();
    final long chainHeadNumber = blockchain.getChainHeadBlockNumber();
    final long upperBound = Math.min(chainHeadNumber, pivotNumber);

    if (upperBound <= anchorNumber) {
      return;
    }

    findBodyDownloadFrontier(anchorNumber, upperBound)
        .ifPresent(
            frontier -> {
              if (frontier.getNumber() > anchorNumber) {
                LOG.info(
                    "Advancing body download anchor from {} to {} (previously imported blocks)",
                    anchorNumber,
                    frontier.getNumber());
                chainSyncState.updateAndGet(s -> s.withAdvancedBodyAnchor(frontier));
                chainSyncStateStorage.storeState(chainSyncState.get());
              }
            });
  }

  /**
   * Decides whether the download should be retried after an error.
   *
   * @param error the error that caused the download to fail
   * @return empty if the download should be retried, or the throwable to fail with otherwise
   */
  private Optional<Throwable> shouldRetry(final Throwable error) {
    final Throwable cause = error instanceof CompletionException ? error.getCause() : error;
    if (cause instanceof CancellationException || cause instanceof WrongChainException) {
      return Optional.of(error);
    }

    // If the chain sync state has not changed since the previous retry, no real progress was made.
    // This can happen when a pivot block has been reorged: peers can no longer serve those headers,
    // so retrying is futile. After MAX_SAME_STATE_RETRIES such attempts, escalate with a
    // StalledDownloadException so PivotSyncDownloader selects a fresh pivot.
    final ChainSyncState currentState = chainSyncState.get();
    if (currentState.equals(lastRetriedState)) {
      if (sameStateRetryCount.incrementAndGet() > MAX_SAME_STATE_RETRIES) {
        LOG.warn(
            "Chain download stalled after {} retries with no progress — escalating to re-pivot",
            sameStateRetryCount.get());
        return Optional.of(
            new StalledDownloadException(
                "Chain download stalled after "
                    + sameStateRetryCount.get()
                    + " retries with no progress"));
      }
    } else {
      sameStateRetryCount.set(0);
      lastRetriedState = currentState;
    }

    return Optional.empty();
  }

  /**
   * Determines whether Stage 1 (backward header download) needs to run based on the current chain
   * sync state.
   *
   * @param state the chain sync state to use for this stage
   * @return CompletableFuture that completes when Stage 1 is done
   */
  private CompletableFuture<Void> runStage1Download(final ChainSyncState state) {
    if (state.headersDownloadComplete()) {
      LOG.debug(
          "Backward header download already complete for pivot {}. Skipping Stage 1.",
          state.pivotBlockHeader().getNumber());
      return CompletableFuture.completedFuture(null);
    } else {
      return runStage1BackwardHeaderDownload(state);
    }
  }

  private CompletableFuture<Void> runStage1BackwardHeaderDownload(final ChainSyncState state) {
    LOG.debug(
        "Stage 1: Starting backward header download from pivot {} down to stop block {}, progress={}",
        state.pivotBlockHeader().getNumber(),
        state.headerDownloadAnchor() != null
            ? state.headerDownloadAnchor().getNumber()
            : state.blockDownloadAnchor().getNumber(),
        state.headerDownloadProgress() != null
            ? state.headerDownloadProgress().getNumber()
            : "none");

    final Instant stage1StartTime = Instant.now();

    final SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult pipelineResult =
        pipelineFactory.createBackwardHeaderDownloadPipeline(state);
    currentPipeline = pipelineResult.pipeline();
    currentDriver = pipelineResult.driver();

    return ethContext
        .getScheduler()
        .startPipeline(pipelineResult.pipeline())
        .thenApply(
            ignore -> {
              final Duration stage1Duration = Duration.between(stage1StartTime, Instant.now());
              LOG.debug(
                  "Stage 1 complete: Backward header download finished in {} seconds",
                  stage1Duration.toSeconds());

              // If Stage 1 recovery walked below the original anchor and matched a stored canonical
              // ancestor, replace blockDownloadAnchor + headerDownloadAnchor with the matched
              // ancestor so Stage 2 starts there instead of from the (side-chain) chain head.
              final Optional<BlockHeader> matched = pipelineResult.driver().getMatchedAncestor();
              if (matched.isPresent()) {
                final ChainSyncState before = chainSyncState.get();
                LOG.info(
                    "Stage 1 recovery extended anchor: previous anchor at #{}, matched ancestor at #{} ({})",
                    before.blockDownloadAnchor().getNumber(),
                    matched.get().getNumber(),
                    matched.get().getHash());
                chainSyncState.updateAndGet(s -> s.withRecoveryMatch(matched.get()));
                chainSyncStateStorage.storeState(chainSyncState.get());
              }

              // Mark headers download as complete and persist
              chainSyncState.updateAndGet(ChainSyncState::withHeadersDownloadComplete);
              chainSyncStateStorage.storeState(chainSyncState.get());
              currentDriver = null;
              LOG.debug("Persisted backward header download completion state");

              return null;
            });
  }

  private CompletableFuture<Void> runStage2ForwardBodiesAndReceipts(final ChainSyncState state) {
    // Start from the block-download anchor in ChainSyncState. After Stage 1 recovery, this may be
    // the matched ancestor (which is below the original anchor / blockchain head).
    final long stage2StartBlock = state.blockDownloadAnchor().getNumber();
    final BlockHeader pivotBlockHeader = state.pivotBlockHeader();
    final long pivotBlockNumber = pivotBlockHeader.getNumber();

    // Check if already at or past pivot
    if (stage2StartBlock >= pivotBlockNumber) {
      LOG.debug(
          "Stage 2: Block download anchor ({}) already at or past pivot ({}). Skipping bodies/receipts download.",
          stage2StartBlock,
          pivotBlockNumber);
      return CompletableFuture.completedFuture(null);
    }

    LOG.debug(
        "Stage 2: Starting forward bodies and receipts download from {} to pivot {}",
        stage2StartBlock,
        pivotBlockNumber);

    final Instant stage2StartTime = Instant.now();

    final Pipeline<List<BlockHeader>> bodiesAndReceiptsPipeline =
        pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(
            stage2StartBlock, pivotBlockHeader, syncState);
    currentPipeline = bodiesAndReceiptsPipeline;

    return ethContext
        .getScheduler()
        .startPipeline(bodiesAndReceiptsPipeline)
        .thenApply(
            ignore -> {
              final Duration stage2Duration = Duration.between(stage2StartTime, Instant.now());
              LOG.info(
                  "Stage 2 complete: Forward bodies/receipts download finished in {} seconds",
                  stage2Duration.toSeconds());
              return null;
            });
  }

  /**
   * Starts the chain download process. Only one download can be in progress at a time.
   *
   * @return CompletableFuture that completes when all download cycles are done
   */
  private CompletableFuture<Void> downloadAccordingToChainState() {
    // Guard against concurrent executions
    if (!downloadInProgress.compareAndSet(false, true)) {
      LOG.warn("Download already in progress, ignoring concurrent call");
      return CompletableFuture.failedFuture(
          new IllegalStateException("Download already in progress"));
    }

    final CompletableFuture<Void> result = new CompletableFuture<>();

    // Ensure we always reset the guard when complete
    result.whenComplete((r, error) -> downloadInProgress.set(false));

    // Start the download attempt loop
    attemptDownload(result);

    return result;
  }

  /**
   * Attempts a single download cycle, with retry logic. Non-recursive - schedules next attempt via
   * executor.
   *
   * @param overallResult the future to complete when all attempts are done
   */
  private void attemptDownload(final CompletableFuture<Void> overallResult) {
    if (cancelled.get()) {
      overallResult.completeExceptionally(new CancellationException());
      return;
    }

    // Already completed from another path
    if (overallResult.isDone()) {
      return;
    }

    // Guard against starting an expensive 160-concurrent-future pipeline with no peers.
    // With no peers every future immediately hits NO_PEER_AVAILABLE, spins for 60 s, and
    // the pipeline restarts every 60 s accumulating scheduler/thread overhead indefinitely.
    if (ethContext.getEthPeers().peerCount() == 0) {
      LOG.debug(
          "No peers available, deferring chain sync pipeline start for {} ms",
          NO_PEER_RETRY_DELAY_MILLISECONDS);
      ethContext
          .getScheduler()
          .scheduleFutureTask(
              () -> attemptDownload(overallResult),
              Duration.ofMillis(NO_PEER_RETRY_DELAY_MILLISECONDS));
      return;
    }

    performSingleDownloadCycle()
        .whenComplete(
            (downloadResult, error) -> {
              if (error != null) {
                handleDownloadError(error, overallResult);
              } else {
                // we are stopping the time after the initial chain download has finished. From now
                // on we are only waiting for new pivots or the world state download to finish.
                syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                handlePivotUpdateLoop(overallResult);
              }
            });
  }

  /**
   * Performs a single download cycle: Stage 1 (headers) → Stage 2 (bodies/receipts). Returns when
   * both stages complete (successfully or with error).
   *
   * @return CompletableFuture that completes when the cycle is done
   */
  private CompletableFuture<Void> performSingleDownloadCycle() {
    // Stage 1 reads its own snapshot; Stage 2 must re-read because Stage 1's recovery handoff
    // (BackwardHeaderDriver.getMatchedAncestor) may have rewritten blockDownloadAnchor via
    // ChainSyncState.withRecoveryMatch before Stage 2 starts. advanceBodyDownloadAnchor may
    // further advance the anchor based on previously imported blocks.
    return runStage1Download(chainSyncState.get())
        .thenCompose(
            ignore -> {
              if (cancelled.get()) {
                return CompletableFuture.failedFuture(new CancellationException());
              }
              advanceBodyDownloadAnchor();
              return runStage2ForwardBodiesAndReceipts(chainSyncState.get());
            });
  }

  /**
   * Saves header download progress from the current BackwardHeaderDriver into ChainSyncState. On
   * pipeline restart, the backward header download will resume from this point instead of starting
   * over.
   */
  private void saveHeaderProgress() {
    final BackwardHeaderDriver driver = currentDriver;
    if (driver == null) {
      return;
    }

    // Don't save header progress if headers are already complete
    if (chainSyncState.get().headersDownloadComplete()) {
      return;
    }

    final BlockHeader lowestImported = driver.getLowestImportedHeader();
    final long pivotNumber = chainSyncState.get().pivotBlockHeader().getNumber();

    // Only save if progress was actually made (lowest imported is below pivot)
    if (lowestImported.getNumber() < pivotNumber) {
      LOG.debug(
          "Saving header download progress: lowest imported header {}", lowestImported.getNumber());
      chainSyncState.updateAndGet(state -> state.withHeaderProgress(lowestImported));
      chainSyncStateStorage.storeState(chainSyncState.get());
    }
  }

  /**
   * Handles error from a download cycle. Updates state and decides whether to retry.
   *
   * @param error the error that occurred
   * @param overallResult the future to complete/continue
   */
  private void handleDownloadError(
      final Throwable error, final CompletableFuture<Void> overallResult) {

    // Save header download progress if any was made
    saveHeaderProgress();

    // Update chain state to current blockchain head
    chainSyncState.updateAndGet(state -> state.fromHead(blockchain.getChainHeadHeader()));
    chainSyncStateStorage.storeState(chainSyncState.get());

    final Optional<Throwable> failWith = shouldRetry(error);
    if (failWith.isPresent()) {
      // Non-retryable error - fail (metrics will be stopped by outer handler)
      overallResult.completeExceptionally(failWith.get());
    } else {
      LOG.debug("Chain sync encountered error, will retry from saved state", error);
      // Schedule next attempt without recursion; small delay to avoid tight retry loops
      ethContext
          .getScheduler()
          .scheduleFutureTask(
              () -> attemptDownload(overallResult), Duration.ofMillis(SMALL_DELAY_MILLISECONDS));
    }
  }

  /**
   * Handles the pivot update check loop without recursion. Uses explicit iteration via
   * CompletableFuture chaining.
   *
   * @param overallResult the future to complete when done
   */
  private void handlePivotUpdateLoop(final CompletableFuture<Void> overallResult) {
    if (overallResult.isDone()) {
      return;
    }

    isAnotherDownloadCycleNeeded()
        .whenComplete(
            (needsContinue, error) -> {
              if (error != null) {
                overallResult.completeExceptionally(error);
              } else if (needsContinue) {
                // Pivot updated, need another download cycle
                LOG.debug("Pivot updated, scheduling next download cycle");
                attemptDownload(overallResult);
              } else {
                // All done - world state heal finished, no more pivot updates
                // Metrics will be stopped by outer handler
                overallResult.complete(null);
              }
            });
  }

  /**
   * Checks for pivot updates and updates state if needed. Returns a future that completes with true
   * if a pivot update occurred (requiring another download cycle), or false if done.
   *
   * @return CompletableFuture<Boolean> - true if should continue, false if complete
   */
  private CompletableFuture<Boolean> isAnotherDownloadCycleNeeded() {

    final BlockHeader previousPivot;
    // Check if there's an immediate pivot update available
    if (pivotUpdateFuture.isDone()) {
      final BlockHeader updatedPivot;
      synchronized (this) {
        updatedPivot = pendingPivotUpdate.getAndSet(null);
        pivotUpdateFuture = new CompletableFuture<>();
      }
      previousPivot = chainSyncState.get().pivotBlockHeader();

      if (updatedPivot != null && updatedPivot.getNumber() > previousPivot.getNumber()) {
        LOG.debug(
            "Pivot block has been updated from {} to {}. Continuing sync to new pivot.",
            previousPivot.getNumber(),
            updatedPivot.getNumber());

        // Update chain state to new pivot
        chainSyncState.updateAndGet(state -> state.continueToNewPivot(updatedPivot, previousPivot));
        chainSyncStateStorage.storeState(chainSyncState.get());

        return CompletableFuture.completedFuture(true); // Need to continue
      } else {
        LOG.error(
            "The pivot block number has not increased, even though onPivotUpdated() has been called. previous pivot: {}, updated pivot: {}",
            previousPivot.getNumber(),
            updatedPivot != null ? updatedPivot.getNumber() : "null");
        throw new IllegalStateException("The pivot block number has not increased");
      }
    }

    // Proactively check if world state download is already complete before waiting.
    // This ensures that if all tasks are complete, the healing process gets triggered
    // immediately without waiting for the next pipeline cycle.
    if (worldDownloadState != null) {
      final BlockHeader currentPivot = chainSyncState.get().pivotBlockHeader();
      worldDownloadState.checkCompletion(currentPivot);
    }

    LOG.info(
        "No immediate pivot update detected. Waiting for world state heal to finish or pivot update ...");

    // Wait for either a pivot update or world state heal to complete
    return CompletableFuture.anyOf(pivotUpdateFuture, worldStateHealFinishedFuture)
        .thenApply(
            ignore -> {
              if (pivotUpdateFuture.isDone()) {
                // Recursive check - a pivot update arrived while waiting
                // We need to check it on the next iteration
                return true; // Need to continue with another cycle
              } else {
                // World state heal finished
                return false; // All done
              }
            });
  }

  @Override
  public void cancel() {
    LOG.info("Cancelling two-stage fast sync chain download");
    cancelled.set(true);

    final Pipeline<?> pipeline = currentPipeline;
    if (pipeline != null) {
      pipeline.abort();
    }
  }
}
