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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.hyperledger.besu.util.FutureUtils.exceptionallyCompose;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.Subscribers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackwardSyncContext {
  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncContext.class);
  public static final int BATCH_SIZE = 200;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final long MILLIS_DELAY_BETWEEN_PROGRESS_LOG = 10_000L;
  private static final long DEFAULT_MILLIS_BETWEEN_RETRIES = 5000;
  private static final int DEFAULT_MAX_CHAIN_EVENT_ENTRIES = BadBlockManager.MAX_BAD_BLOCKS_SIZE;

  protected final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  private final SynchronizerConfiguration synchronizerConfiguration;
  private final MetricsSystem metricsSystem;
  private final SyncState syncState;
  private final AtomicReference<Status> currentBackwardSyncStatus = new AtomicReference<>();
  private final BackwardChain backwardChain;
  private int batchSize = BATCH_SIZE;
  private final int maxRetries;
  private final int maxBadChainEventEntries;
  private final long millisBetweenRetries = DEFAULT_MILLIS_BETWEEN_RETRIES;
  private final Subscribers<BadChainListener> badChainListeners = Subscribers.create();

  public BackwardSyncContext(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration synchronizerConfiguration,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final SyncState syncState,
      final BackwardChain backwardChain) {
    this(
        protocolContext,
        protocolSchedule,
        synchronizerConfiguration,
        metricsSystem,
        ethContext,
        syncState,
        backwardChain,
        DEFAULT_MAX_RETRIES,
        DEFAULT_MAX_CHAIN_EVENT_ENTRIES);
  }

  public BackwardSyncContext(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration synchronizerConfiguration,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final SyncState syncState,
      final BackwardChain backwardChain,
      final int maxRetries,
      final int maxBadChainEventEntries) {

    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.synchronizerConfiguration = synchronizerConfiguration;
    this.metricsSystem = metricsSystem;
    this.syncState = syncState;
    this.backwardChain = backwardChain;
    this.maxRetries = maxRetries;
    this.maxBadChainEventEntries = maxBadChainEventEntries;
  }

  public synchronized boolean isSyncing() {
    return Optional.ofNullable(currentBackwardSyncStatus.get())
        .map(status -> status.currentFuture.isDone())
        .orElse(Boolean.FALSE);
  }

  public synchronized void maybeUpdateTargetHeight(final Hash headHash) {
    if (!Hash.ZERO.equals(headHash)) {
      Optional<Status> maybeCurrentStatus = Optional.ofNullable(currentBackwardSyncStatus.get());
      maybeCurrentStatus.ifPresent(
          status ->
              backwardChain
                  .getBlock(headHash)
                  .ifPresent(
                      block -> {
                        LOG.atTrace()
                            .setMessage("updateTargetHeight to {}")
                            .addArgument(block::toLogString)
                            .log();
                        status.updateTargetHeight(block.getHeader().getNumber());
                      }));
    }
  }

  public synchronized CompletableFuture<Void> syncBackwardsUntil(final Hash newBlockHash) {
    if (isReady()) {
      if (!isTrusted(newBlockHash)) {
        LOG.atDebug()
            .setMessage("Appending new head block hash {} to backward sync")
            .addArgument(() -> newBlockHash.getBytes().toHexString())
            .log();
        backwardChain.addNewHash(newBlockHash);
      }

      final Status status = getOrStartSyncSession();
      backwardChain
          .getBlock(newBlockHash)
          .ifPresent(
              newTargetBlock -> status.updateTargetHeight(newTargetBlock.getHeader().getNumber()));
      return status.currentFuture;
    } else {
      return CompletableFuture.failedFuture(new Throwable("Backward sync is not ready"));
    }
  }

  public synchronized CompletableFuture<Void> syncBackwardsUntil(final Block newPivot) {
    if (!isTrusted(newPivot.getHash())) {
      backwardChain.appendTrustedBlock(newPivot);
    }

    if (isReady()) {
      final Status status = getOrStartSyncSession();
      status.updateTargetHeight(newPivot.getHeader().getNumber());
      return status.currentFuture;
    } else {
      return CompletableFuture.failedFuture(new Throwable("Backward sync is not ready"));
    }
  }

  private Status getOrStartSyncSession() {
    Optional<Status> maybeCurrentStatus = Optional.ofNullable(this.currentBackwardSyncStatus.get());
    return maybeCurrentStatus.orElseGet(
        () -> {
          LOG.info("Starting a new backward sync session");
          Status newStatus = new Status(prepareBackwardSyncFutureWithRetry());
          this.currentBackwardSyncStatus.set(newStatus);
          return newStatus;
        });
  }

  private boolean isTrusted(final Hash hash) {
    if (backwardChain.isTrusted(hash)) {
      LOG.atDebug()
          .setMessage(
              "Not fetching or appending hash {} to backward sync since it is present in successors")
          .addArgument(() -> hash.getBytes().toHexString())
          .log();
      return true;
    }
    return false;
  }

  private CompletableFuture<Void> prepareBackwardSyncFutureWithRetry() {
    return prepareBackwardSyncFutureWithRetry(maxRetries)
        .handle(
            (unused, throwable) -> {
              this.currentBackwardSyncStatus.set(null);
              if (throwable != null) {
                LOG.info("Current backward sync session failed, it will be restarted");
                throw extractBackwardSyncException(throwable)
                    .orElse(new BackwardSyncException(throwable));
              }
              return null;
            });
  }

  private CompletableFuture<Void> prepareBackwardSyncFutureWithRetry(final int retries) {
    if (retries == 0) {
      return CompletableFuture.failedFuture(
          new BackwardSyncException("Max number of retries " + maxRetries + " reached"));
    }

    return exceptionallyCompose(
        prepareBackwardSyncFuture(),
        throwable -> {
          processException(throwable);
          return ethContext
              .getScheduler()
              .scheduleFutureTask(
                  () -> prepareBackwardSyncFutureWithRetry(retries - 1),
                  Duration.ofMillis(millisBetweenRetries));
        });
  }

  @VisibleForTesting
  protected void processException(final Throwable throwable) {
    extractBackwardSyncException(throwable)
        .ifPresentOrElse(
            backwardSyncException -> {
              if (backwardSyncException.shouldRestart()) {
                LOG.debug(
                    "Backward sync failed ({}). Current Peers: {}. Retrying in {} milliseconds",
                    throwable.getMessage(),
                    ethContext.getEthPeers().peerCount(),
                    millisBetweenRetries);
              } else {
                LOG.atDebug()
                    .setMessage("Not recoverable backward sync exception {}")
                    .addArgument(throwable::getMessage)
                    .log();
                throw backwardSyncException;
              }
            },
            () -> {
              LOG.debug(
                  "Backward sync failed ({}). Current Peers: {}. Retrying in {} milliseconds",
                  throwable.getMessage(),
                  ethContext.getEthPeers().peerCount(),
                  millisBetweenRetries);
              LOG.debug("Exception details:", throwable);
            });
  }

  private Optional<BackwardSyncException> extractBackwardSyncException(final Throwable throwable) {
    Throwable currentCause = throwable;

    while (currentCause != null) {
      if (currentCause instanceof BackwardSyncException) {
        return Optional.of((BackwardSyncException) currentCause);
      }
      currentCause = currentCause.getCause();
    }
    return Optional.empty();
  }

  @VisibleForTesting
  CompletableFuture<Void> prepareBackwardSyncFuture() {
    final MutableBlockchain blockchain = getProtocolContext().getBlockchain();
    return new BackwardSyncAlgorithm(
            this,
            FinalBlockConfirmation.confirmationChain(
                FinalBlockConfirmation.genesisConfirmation(blockchain),
                FinalBlockConfirmation.ancestorConfirmation(blockchain)))
        .executeBackwardsSync(null);
  }

  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  public EthContext getEthContext() {
    return ethContext;
  }

  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }

  public ProtocolContext getProtocolContext() {
    return protocolContext;
  }

  public BlockValidator getBlockValidator(final BlockHeader blockHeader) {
    return protocolSchedule.getByBlockHeader(blockHeader).getBlockValidator();
  }

  public BlockValidator getBlockValidatorForBlock(final Block block) {
    return getBlockValidator(block.getHeader());
  }

  public boolean isReady() {
    // we aren't ready if we have 0 peers
    int peerCount = getEthContext().getEthPeers().peerCount();
    LOG.debug(
        "checking if BWS is ready: ttd reached {}, initial sync done {}, peerCount {}",
        syncState.hasReachedTerminalDifficulty().orElse(Boolean.FALSE),
        syncState.isInitialSyncPhaseDone(),
        peerCount);
    return syncState.hasReachedTerminalDifficulty().orElse(Boolean.FALSE)
        && syncState.isInitialSyncPhaseDone()
        && peerCount > 0;
  }

  public void subscribeBadChainListener(final BadChainListener badChainListener) {
    badChainListeners.subscribe(badChainListener);
  }

  // In rare case when we request too many headers/blocks we get response that does not contain all
  // data, and we might want to retry with smaller batch size
  public int getBatchSize() {
    return batchSize;
  }

  public void halveBatchSize() {
    this.batchSize = batchSize / 2 + 1;
  }

  public void resetBatchSize() {
    this.batchSize = BATCH_SIZE;
  }

  protected Void saveBlock(final Block block) {
    LOG.atTrace().setMessage("Going to validate block {}").addArgument(block::toLogString).log();
    var optResult =
        this.getBlockValidatorForBlock(block)
            .validateAndProcessBlock(
                this.getProtocolContext(),
                block,
                HeaderValidationMode.FULL,
                HeaderValidationMode.NONE);
    if (optResult.isSuccessful()) {
      LOG.atTrace()
          .setMessage("Block {} was validated, going to move the head")
          .addArgument(block::toLogString)
          .log();
      this.getProtocolContext()
          .getBlockchain()
          .appendBlock(
              block,
              optResult.getYield().get().getReceipts(),
              optResult.getYield().get().getBlockAccessList());
      possiblyMoveHead(block);
      logBlockImportProgress(block.getHeader().getNumber());
    } else {
      emitBadChainEvent(block);
      throw new BackwardSyncException(
          "Cannot save block "
              + block.toLogString()
              + " because of "
              + optResult.errorMessage.orElseThrow());
    }

    return null;
  }

  @VisibleForTesting
  protected void possiblyMoveHead(final Block lastSavedBlock) {
    final MutableBlockchain blockchain = getProtocolContext().getBlockchain();

    if (blockchain.getChainHead().getHash().equals(lastSavedBlock.getHash())) {
      LOG.atDebug()
          .setMessage("Head is already properly set to {}")
          .addArgument(lastSavedBlock::toLogString)
          .log();
      return;
    }

    LOG.atDebug()
        .setMessage("Rewinding head to last saved block {}")
        .addArgument(lastSavedBlock::toLogString)
        .log();
    blockchain.rewindToBlock(lastSavedBlock.getHash());
  }

  public SyncState getSyncState() {
    return syncState;
  }

  public synchronized BackwardChain getBackwardChain() {
    return backwardChain;
  }

  public Status getStatus() {
    return currentBackwardSyncStatus.get();
  }

  private void emitBadChainEvent(final Block badBlock) {
    final List<Block> badBlockDescendants = new ArrayList<>();
    final List<BlockHeader> badBlockHeaderDescendants = new ArrayList<>();

    Optional<Hash> descendant = backwardChain.getDescendant(badBlock.getHash());

    while (descendant.isPresent()
        && badBlockDescendants.size() < maxBadChainEventEntries
        && badBlockHeaderDescendants.size() < maxBadChainEventEntries) {
      final Optional<Block> block = backwardChain.getBlock(descendant.get());
      if (block.isPresent()) {
        badBlockDescendants.add(block.get());
      } else {
        backwardChain.getHeader(descendant.get()).ifPresent(badBlockHeaderDescendants::add);
      }

      descendant = backwardChain.getDescendant(descendant.get());
    }

    badChainListeners.forEach(
        listener -> listener.onBadChain(badBlock, badBlockDescendants, badBlockHeaderDescendants));
  }

  private void logBlockImportProgress(final long currImportedHeight) {
    final Status currentStatus = getStatus();
    final long targetHeight = currentStatus.getTargetChainHeight();
    final long initialHeight = currentStatus.getInitialChainHeight();
    final long estimatedTotal = targetHeight - initialHeight;
    final long imported = currImportedHeight - initialHeight;

    final float completedPercentage = 100.0f * imported / estimatedTotal;

    if (completedPercentage < 100.0f) {
      if (currentStatus.progressLogDue() && targetHeight > 0) {
        LOG.info(
            String.format(
                "Backward sync phase 2 of 2, %.2f%% completed, imported %d blocks of at least %d (current head %d, target head %d). Peers: %d",
                completedPercentage,
                imported,
                estimatedTotal,
                currImportedHeight,
                currentStatus.getTargetChainHeight(),
                getEthContext().getEthPeers().peerCount()));
      }
    } else {
      LOG.info(
          String.format(
              "Backward sync phase 2 of 2 completed, imported a total of %d blocks. Peers: %d",
              imported, getEthContext().getEthPeers().peerCount()));
    }
  }

  public SynchronizerConfiguration getSynchronizerConfiguration() {
    return synchronizerConfiguration;
  }

  class Status {
    private final CompletableFuture<Void> currentFuture;
    private final long initialChainHeight;
    private long targetChainHeight;

    private long lastLogAt = 0;

    public Status(final CompletableFuture<Void> currentFuture) {
      this.currentFuture = currentFuture;
      this.initialChainHeight = protocolContext.getBlockchain().getChainHeadBlockNumber();
    }

    public void updateTargetHeight(final long newTargetHeight) {
      targetChainHeight = newTargetHeight;
    }

    public boolean progressLogDue() {
      final long now = System.currentTimeMillis();
      if (now - lastLogAt > MILLIS_DELAY_BETWEEN_PROGRESS_LOG) {
        lastLogAt = now;
        return true;
      }
      return false;
    }

    public long getTargetChainHeight() {
      return targetChainHeight;
    }

    public long getInitialChainHeight() {
      return initialChainHeight;
    }
  }
}
