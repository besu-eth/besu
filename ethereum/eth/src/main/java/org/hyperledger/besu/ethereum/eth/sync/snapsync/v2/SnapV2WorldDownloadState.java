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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.InMemoryTaskQueue;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.services.tasks.TaskCollection;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static-pivot snap/2 leaf download state. */
public class SnapV2WorldDownloadState extends WorldDownloadState<SnapDataRequest>
    implements SnapRequestContext {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2WorldDownloadState.class);

  protected final InMemoryTaskQueue<SnapDataRequest> pendingAccountRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingStorageRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingLargeStorageRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingCodeRequests =
      new InMemoryTaskQueue<>();

  private final SnapSyncStatePersistenceManager snapContext;
  private final SnapSyncProcessState snapSyncState;
  private final SnapSyncMetricsManager metricsManager;
  private final AtomicBoolean worldStateFinishedNotified = new AtomicBoolean(false);
  private final WorldStateHealFinishedListener worldStateHealFinishedListener;
  private final DownloadedAccountRangeTracker accountRangeTracker =
      new DownloadedAccountRangeTracker();
  private final DownloadedStorageRangeTracker storageRangeTracker =
      new DownloadedStorageRangeTracker();
  private final SnapV2PivotCatchupListener pivotCatchupListener;
  private final SnapV2BlockAccessListApplier blockAccessListApplier;
  private final Blockchain blockchain;
  // future that completes once every in-flight (already-dequeued) task has finished
  private CompletableFuture<Void> pivotCatchupFuture; // null unless pivot catchup is in progress

  public SnapV2WorldDownloadState(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncStatePersistenceManager snapContext,
      final SnapSyncProcessState snapSyncState,
      final InMemoryTasksPriorityQueues<SnapDataRequest> pendingRequests,
      final int maxRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final SnapSyncMetricsManager metricsManager,
      final Clock clock,
      final SyncDurationMetrics syncDurationMetrics,
      final WorldStateHealFinishedListener worldStateHealFinishedListener,
      final SnapV2PivotCatchupListener pivotCatchupListener,
      final SnapV2BlockAccessListApplier blockAccessListApplier,
      final Blockchain blockchain) {
    super(
        worldStateStorageCoordinator,
        pendingRequests,
        maxRequestsWithoutProgress,
        minMillisBeforeStalling,
        clock,
        syncDurationMetrics);
    this.snapContext = snapContext;
    this.snapSyncState = snapSyncState;
    this.metricsManager = metricsManager;
    this.worldStateHealFinishedListener = worldStateHealFinishedListener;
    this.pivotCatchupListener = pivotCatchupListener;
    this.blockAccessListApplier = blockAccessListApplier;
    this.blockchain = blockchain;

    accountRangeTracker.setOnRangeCompleted(
        (rangeStart, rangeEnd) ->
            storageRangeTracker.removeAccountHashesInRange(rangeStart, rangeEnd));

    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_account_requests_current",
            "Number of account pending requests for snap/2 world state download",
            pendingAccountRequests::size);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_storage_requests_current",
            "Number of storage pending requests for snap/2 world state download",
            pendingStorageRequests::size);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_big_storage_requests_current",
            "Number of big storage pending requests for snap/2 world state download",
            pendingLargeStorageRequests::size);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_code_requests_current",
            "Number of code pending requests for snap/2 world state download",
            pendingCodeRequests::size);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_completed_ranges_current",
            "Number of completed account ranges for snap/2 world state download",
            accountRangeTracker::completedRangeCount);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_ranges_current",
            "Number of pending account ranges for snap/2 world state download",
            accountRangeTracker::pendingRangeCount);
    syncDurationMetrics.startTimer(
        SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
  }

  @Override
  public synchronized boolean checkCompletion(final BlockHeader header) {
    if (!isStateDownloadFinished()
        && !isPivotCatchupInProgress()
        && pendingAccountRequests.allTasksCompleted()
        && pendingCodeRequests.allTasksCompleted()
        && pendingStorageRequests.allTasksCompleted()
        && pendingLargeStorageRequests.allTasksCompleted()
        && accountRangeTracker.pendingRangeCount() == 0) {
      persistWorldStateRoot(header);
      notifyWorldStateFinished();
      syncDurationMetrics.stopTimer(
          SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
      metricsManager.notifySnapSyncCompleted();
      snapContext.clear();
      internalFuture.complete(null);
      return true;
    }
    return false;
  }

  private void persistWorldStateRoot(final BlockHeader header) {
    final Bytes nodeData =
        rootNodeData != null
            ? rootNodeData
            : worldStateStorageCoordinator
                .getAccountStateTrieNode(
                    Bytes.EMPTY, Bytes32.wrap(header.getStateRoot().getBytes()))
                .orElseThrow(
                    () ->
                        new WorldStateDownloaderException(
                            "Unable to persist snap/2 world state root: root node was not downloaded"));

    final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
    applyForStrategy(
        updater,
        onBonsai ->
            onBonsai.saveWorldState(
                header.getHash().getBytes(),
                Bytes32.wrap(header.getStateRoot().getBytes()),
                nodeData),
        onForest ->
            onForest.saveWorldState(Bytes32.wrap(header.getStateRoot().getBytes()), nodeData));
    updater.commit();
  }

  private void notifyWorldStateFinished() {
    if (worldStateFinishedNotified.compareAndSet(false, true)) {
      if (worldStateHealFinishedListener != null) {
        LOG.info("Notifying that snap/2 world state download has finished");
        worldStateHealFinishedListener.onWorldStateHealFinished();
      }
    }
  }

  @Override
  public synchronized void cleanupQueues() {
    super.cleanupQueues();
    pendingAccountRequests.clear();
    pendingStorageRequests.clear();
    pendingLargeStorageRequests.clear();
    pendingCodeRequests.clear();
    accountRangeTracker.clear();
    storageRangeTracker.clear();
    pivotCatchupFuture = null;
  }

  @Override
  public synchronized void enqueueRequest(final SnapDataRequest request) {
    if (!isStateDownloadFinished()) {
      if (request instanceof SnapV2BytecodeRequest) {
        pendingCodeRequests.add(request);
      } else if (request instanceof SnapV2StorageRangeRequest storageRangeDataRequest) {
        if (!storageRangeDataRequest.getStartKeyHash().equals(RangeManager.MIN_RANGE)) {
          pendingLargeStorageRequests.add(request);
        } else {
          pendingStorageRequests.add(request);
        }
      } else if (request instanceof SnapV2AccountRangeRequest) {
        pendingAccountRequests.add(request);
      } else {
        throw new IllegalArgumentException(
            "Unsupported snap/2 world state request: " + request.getClass().getSimpleName());
      }
      notifyAll();
    }
  }

  @Override
  public synchronized void enqueueRequests(final Stream<SnapDataRequest> requests) {
    if (!isStateDownloadFinished()) {
      requests.forEach(this::enqueueRequest);
    }
  }

  private boolean isStateDownloadFinished() {
    return internalFuture.isDone();
  }

  private boolean isPivotCatchupInProgress() {
    return pivotCatchupFuture != null;
  }

  private boolean isWaitingForInFlightCompletion() {
    return isPivotCatchupInProgress() && !pivotCatchupFuture.isDone();
  }

  public synchronized Task<SnapDataRequest> dequeueRequestBlocking(
      final BooleanSupplier extraPauseCondition, final TaskCollection<SnapDataRequest> queue) {
    while (!isStateDownloadFinished()
        && (isPivotCatchupInProgress() || extraPauseCondition.getAsBoolean() || queue.isEmpty())) {
      try {
        wait();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    if (isStateDownloadFinished()) {
      return null;
    }
    return queue.remove();
  }

  public synchronized Task<SnapDataRequest> dequeueAccountRequestBlocking() {
    return dequeueRequestBlocking(this::shouldPauseAccountRequests, pendingAccountRequests);
  }

  public synchronized Task<SnapDataRequest> dequeueLargeStorageRequestBlocking() {
    return dequeueRequestBlocking(() -> false, pendingLargeStorageRequests);
  }

  public synchronized Task<SnapDataRequest> dequeueStorageRequestBlocking() {
    return dequeueRequestBlocking(() -> false, pendingStorageRequests);
  }

  public synchronized Task<SnapDataRequest> dequeueCodeRequestBlocking() {
    return dequeueRequestBlocking(() -> false, pendingCodeRequests);
  }

  /**
   * Returns true when no in-flight task remains across any queue. Queued tasks are intentionally
   * ignored.
   */
  public synchronized boolean areAllInflightTasksComplete() {
    return pendingAccountRequests.outstandingTaskCount() == 0
        && pendingStorageRequests.outstandingTaskCount() == 0
        && pendingLargeStorageRequests.outstandingTaskCount() == 0
        && pendingCodeRequests.outstandingTaskCount() == 0;
  }

  private synchronized void maybeCompleteInFlightTasks() {
    if (isWaitingForInFlightCompletion() && areAllInflightTasksComplete()) {
      pivotCatchupFuture.complete(null);
    }
  }

  @Override
  public synchronized void notifyTaskAvailable() {
    maybeCompleteInFlightTasks();
    super.notifyTaskAvailable();
  }

  public void startPivotCatchup(final BlockHeader newPivotBlockHeader) {
    final BlockHeader currentPivotBlockHeader;
    synchronized (this) {
      if (isStateDownloadFinished()) {
        return;
      }
      currentPivotBlockHeader = snapSyncState.getPivotBlockHeader().orElseThrow();
      if (newPivotBlockHeader.getNumber() <= currentPivotBlockHeader.getNumber()) {
        return;
      }
      if (isPivotCatchupInProgress()) {
        LOG.debug(
            "Snap/2 pivot catch-up to {} ignored because another catch-up is in progress",
            newPivotBlockHeader.getNumber());
        return;
      }

      if (pivotCatchupListener == null) {
        internalFuture.completeExceptionally(
            new WorldStateDownloaderException("Snap/2 pivot catch-up listener is not available"));
        return;
      }
      pivotCatchupFuture = new CompletableFuture<>(); // pause dequeueing
      maybeCompleteInFlightTasks(); // handle case of 0 in-flight tasks when catchup starts
    }

    final CompletableFuture<Void> chainCatchupFuture;
    try {
      chainCatchupFuture =
          pivotCatchupListener.preparePivotCatchup(currentPivotBlockHeader, newPivotBlockHeader);
    } catch (final RuntimeException e) {
      failPivotCatchup(e);
      return;
    }
    if (chainCatchupFuture == null) {
      failPivotCatchup(
          new WorldStateDownloaderException("Snap/2 pivot catch-up listener returned null"));
      return;
    }

    CompletableFuture.allOf(pivotCatchupFuture, chainCatchupFuture)
        .whenComplete(
            (unused, error) -> {
              if (error != null) {
                failPivotCatchup(error);
                return;
              }
              if (!blockchain.areBothBlocksOnCanonicalChain(
                  currentPivotBlockHeader.getHash(), newPivotBlockHeader.getHash())) {
                failPivotCatchup(
                    new WorldStateDownloaderException(
                        "Chain reorg detected during snap/2 pivot catch-up: current pivot "
                            + currentPivotBlockHeader.getNumber()
                            + " ("
                            + currentPivotBlockHeader.getHash()
                            + ") and new pivot "
                            + newPivotBlockHeader.getNumber()
                            + " ("
                            + newPivotBlockHeader.getHash()
                            + ") are not both on the canonical chain. Sync restart required."));
                return;
              }
              finishPivotCatchup(currentPivotBlockHeader, newPivotBlockHeader);
            });
  }

  private synchronized void failPivotCatchup(final Throwable error) {
    pivotCatchupFuture = null;
    internalFuture.completeExceptionally(error);
    notifyAll();
  }

  private void finishPivotCatchup(
      final BlockHeader currentPivotBlockHeader, final BlockHeader newPivotBlockHeader) {
    synchronized (this) {
      if (isStateDownloadFinished()) {
        return;
      }
      applyBlockAccessLists(currentPivotBlockHeader, newPivotBlockHeader);
      retargetQueuedRequests(newPivotBlockHeader);
      snapSyncState.setCurrentHeader(newPivotBlockHeader);
      pivotCatchupFuture = null;
      notifyAll();
    }
    checkCompletion(newPivotBlockHeader);
  }

  private void applyBlockAccessLists(
      final BlockHeader currentPivotBlockHeader, final BlockHeader newPivotBlockHeader) {
    LOG.info(
        "Snap/2 applying BALs: pivot {} -> {} (completed ranges: {}, pending ranges: {})",
        currentPivotBlockHeader.getNumber(),
        newPivotBlockHeader.getNumber(),
        accountRangeTracker.completedRangeCount(),
        accountRangeTracker.pendingRangeCount());
    blockAccessListApplier.applyBlockAccessLists(
        currentPivotBlockHeader, newPivotBlockHeader, accountRangeTracker, storageRangeTracker);
  }

  private void retargetQueuedRequests(final BlockHeader newPivotBlockHeader) {
    retargetQueuedAccountRequests(newPivotBlockHeader);
    retargetQueuedStorageRequests(pendingStorageRequests, newPivotBlockHeader);
    retargetQueuedStorageRequests(pendingLargeStorageRequests, newPivotBlockHeader);
    retargetQueuedCodeRequests(newPivotBlockHeader);
  }

  private void retargetQueuedAccountRequests(final BlockHeader newPivotBlockHeader) {
    final List<SnapDataRequest> queuedAccountRequests = pendingAccountRequests.asList();
    pendingAccountRequests.clearInternalQueue();
    for (final SnapDataRequest request : queuedAccountRequests) {
      if (request instanceof SnapV2AccountRangeRequest accountRangeRequest) {
        pendingAccountRequests.add(accountRangeRequest.retarget(newPivotBlockHeader));
      } else {
        throw new IllegalStateException(
            "Unexpected snap/2 account queue request: " + request.getClass().getSimpleName());
      }
    }
  }

  private void retargetQueuedCodeRequests(final BlockHeader newPivotBlockHeader) {
    final List<SnapDataRequest> queuedCodeRequests = pendingCodeRequests.asList();
    pendingCodeRequests.clearInternalQueue();
    for (final SnapDataRequest request : queuedCodeRequests) {
      if (request instanceof SnapV2BytecodeRequest codeRequest) {
        pendingCodeRequests.add(codeRequest.retarget(newPivotBlockHeader));
      } else {
        throw new IllegalStateException(
            "Unexpected snap/2 code queue request: " + request.getClass().getSimpleName());
      }
    }
  }

  private void retargetQueuedStorageRequests(
      final InMemoryTaskQueue<SnapDataRequest> queue, final BlockHeader newPivotBlockHeader) {
    final List<SnapDataRequest> queuedRequests = queue.asList();
    queue.clearInternalQueue();
    for (final SnapDataRequest request : queuedRequests) {
      if (request instanceof SnapV2StorageRangeRequest storageRequest) {
        final Bytes32 newRoot = readStorageRoot(storageRequest.getAccountHash());
        queue.add(storageRequest.retarget(newPivotBlockHeader, newRoot));
      } else {
        throw new IllegalStateException(
            "Unexpected snap/2 storage queue request: " + request.getClass().getSimpleName());
      }
    }
  }

  private Bytes32 readStorageRoot(final Hash accountHash) {
    return worldStateStorageCoordinator
        .applyForStrategy(
            bonsai -> bonsai.getAccount(accountHash), forest -> Optional.<Bytes>empty())
        .map(
            b ->
                Bytes32.wrap(
                    PmtStateTrieAccountValue.readFrom(RLP.input(b)).getStorageRoot().getBytes()))
        .orElseThrow(
            () ->
                new WorldStateDownloaderException(
                    "Storage root not found for account "
                        + accountHash
                        + " after BAL application during pivot catch-up"));
  }

  private boolean shouldPauseAccountRequests() {
    // TODO: Replace this drain-to-zero gate with bounded backpressure. Account ranges should pause
    // only while child queues are above a high-watermark, then resume below a low-watermark.
    return hasIncompleteTasks(pendingStorageRequests)
        || hasIncompleteTasks(pendingLargeStorageRequests)
        || hasIncompleteTasks(pendingCodeRequests);
  }

  private boolean hasIncompleteTasks(final TaskCollection<SnapDataRequest> queue) {
    return !queue.allTasksCompleted();
  }

  @Override
  public SnapSyncMetricsManager getMetricsManager() {
    return metricsManager;
  }

  @Override
  public void addAccountToHealingList(final Bytes account) {
    LOG.debug("Ignoring snap/2 healing marker for account {}", account);
  }

  public DownloadedAccountRangeTracker getAccountRangeTracker() {
    return accountRangeTracker;
  }

  public DownloadedStorageRangeTracker getStorageRangeTracker() {
    return storageRangeTracker;
  }
}
