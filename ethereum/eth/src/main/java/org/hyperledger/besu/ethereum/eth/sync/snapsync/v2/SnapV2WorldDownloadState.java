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

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
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
import org.hyperledger.besu.ethereum.trie.RangeManager;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static-pivot snap/2 leaf download state. Healing queues are intentionally absent. */
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
  private final DownloadedAccountRangeTracker rangeTracker = new DownloadedAccountRangeTracker();
  private final SnapV2PivotCatchupListener pivotCatchupListener;
  private boolean accountRangeRequestsPausedForPivotCatchup = false;
  private boolean pivotCatchupInProgress = false;
  private CompletableFuture<Void> pivotCatchupSafePointFuture;

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
      final SnapV2PivotCatchupListener pivotCatchupListener) {
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
            rangeTracker::completedRangeCount);
    metricsManager
        .getMetricsSystem()
        .createLongGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pending_ranges_current",
            "Number of pending account ranges for snap/2 world state download",
            rangeTracker::pendingRangeCount);
    syncDurationMetrics.startTimer(
        SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
  }

  @Override
  public synchronized boolean checkCompletion(final BlockHeader header) {
    if (!internalFuture.isDone()
        && !pivotCatchupInProgress
        && pendingAccountRequests.allTasksCompleted()
        && pendingCodeRequests.allTasksCompleted()
        && pendingStorageRequests.allTasksCompleted()
        && pendingLargeStorageRequests.allTasksCompleted()
        && rangeTracker.pendingRangeCount() == 0) {
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
    rangeTracker.clear();
    accountRangeRequestsPausedForPivotCatchup = false;
    pivotCatchupInProgress = false;
    pivotCatchupSafePointFuture = null;
  }

  @Override
  public synchronized void enqueueRequest(final SnapDataRequest request) {
    if (!internalFuture.isDone()) {
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
    if (!internalFuture.isDone()) {
      requests.forEach(this::enqueueRequest);
    }
  }

  public synchronized Task<SnapDataRequest> dequeueRequestBlocking(
      final BooleanSupplier areRequestsPaused, final TaskCollection<SnapDataRequest> queue) {
    while (!internalFuture.isDone()) {
      while (!internalFuture.isDone() && (areRequestsPaused.getAsBoolean() || queue.isEmpty())) {
        try {
          wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
      if (internalFuture.isDone()) {
        return null;
      }
      final Task<SnapDataRequest> task = queue.remove();
      if (task != null) {
        return task;
      }
    }
    return null;
  }

  public synchronized Task<SnapDataRequest> dequeueAccountRequestBlocking() {
    return dequeueRequestBlocking(this::shouldPauseAccountRequests, pendingAccountRequests);
  }

  /**
   * Pauses new account-range dequeues so already-persisted ranges can finish their storage/code
   * children before a snap/2 pivot catch-up applies BALs.
   *
   * <p>Storage and code queues intentionally keep draining while this pause is active. Queued but
   * not yet dequeued account ranges are not persisted yet, so a catch-up coordinator can retarget
   * them to the next pivot after the safe point is reached.
   */
  public synchronized CompletableFuture<Void> pauseAccountRangeRequestsAndWaitForSafePoint() {
    accountRangeRequestsPausedForPivotCatchup = true;
    pivotCatchupSafePointFuture = new CompletableFuture<>();
    maybeCompletePivotCatchupSafePoint();
    notifyAll();
    return pivotCatchupSafePointFuture;
  }

  public synchronized void resumeAccountRangeRequestsAfterPivotCatchup() {
    accountRangeRequestsPausedForPivotCatchup = false;
    pivotCatchupSafePointFuture = null;
    notifyAll();
  }

  public synchronized boolean isAccountRangeRequestsPausedForPivotCatchup() {
    return accountRangeRequestsPausedForPivotCatchup;
  }

  /**
   * Returns true when no partially persisted snap/2 range can still receive child data.
   *
   * <p>The account queue may still contain tasks: those ranges have not been dequeued or persisted
   * yet and can be safely retargeted by the caller. A dequeued account task is not safe because it
   * can still persist leaves and spawn storage/code children.
   */
  public synchronized boolean isPivotCatchupSafePoint() {
    return accountRangeRequestsPausedForPivotCatchup
        && pendingAccountRequests.outstandingTaskCount() == 0
        && pendingStorageRequests.allTasksCompleted()
        && pendingLargeStorageRequests.allTasksCompleted()
        && pendingCodeRequests.allTasksCompleted()
        && rangeTracker.pendingRangeCount() == 0;
  }

  private synchronized void maybeCompletePivotCatchupSafePoint() {
    if (pivotCatchupSafePointFuture != null
        && !pivotCatchupSafePointFuture.isDone()
        && isPivotCatchupSafePoint()) {
      pivotCatchupSafePointFuture.complete(null);
    }
  }

  @Override
  public synchronized void notifyTaskAvailable() {
    maybeCompletePivotCatchupSafePoint();
    super.notifyTaskAvailable();
  }

  public void startPivotCatchup(final BlockHeader newPivotBlockHeader) {
    final BlockHeader currentPivotBlockHeader;
    final CompletableFuture<Void> safePointFuture;
    synchronized (this) {
      if (internalFuture.isDone()) {
        return;
      }
      currentPivotBlockHeader = snapSyncState.getPivotBlockHeader().orElseThrow();
      if (newPivotBlockHeader.getNumber() <= currentPivotBlockHeader.getNumber()) {
        return;
      }
      if (pivotCatchupInProgress) {
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
      pivotCatchupInProgress = true;
      safePointFuture = pauseAccountRangeRequestsAndWaitForSafePoint();
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

    CompletableFuture.allOf(safePointFuture, chainCatchupFuture)
        .whenComplete(
            (unused, error) -> {
              if (error != null) {
                failPivotCatchup(error);
              } else {
                finishPivotCatchup(currentPivotBlockHeader, newPivotBlockHeader);
              }
            });
  }

  private synchronized void failPivotCatchup(final Throwable error) {
    pivotCatchupInProgress = false;
    accountRangeRequestsPausedForPivotCatchup = false;
    pivotCatchupSafePointFuture = null;
    internalFuture.completeExceptionally(error);
    notifyAll();
  }

  private void finishPivotCatchup(
      final BlockHeader currentPivotBlockHeader, final BlockHeader newPivotBlockHeader) {
    synchronized (this) {
      if (internalFuture.isDone()) {
        return;
      }
      applyBlockAccessListsNoop(currentPivotBlockHeader, newPivotBlockHeader);
      retargetQueuedAccountRangeRequests(newPivotBlockHeader);
      snapSyncState.setCurrentHeader(newPivotBlockHeader);
      pivotCatchupInProgress = false;
      resumeAccountRangeRequestsAfterPivotCatchup();
    }
    checkCompletion(newPivotBlockHeader);
  }

  private void applyBlockAccessListsNoop(
      final BlockHeader currentPivotBlockHeader, final BlockHeader newPivotBlockHeader) {
    LOG.info(
        "Snap/2 BAL application placeholder: pivot {} -> {}",
        currentPivotBlockHeader.getNumber(),
        newPivotBlockHeader.getNumber());
  }

  private void retargetQueuedAccountRangeRequests(final BlockHeader newPivotBlockHeader) {
    final List<SnapDataRequest> queuedAccountRequests = pendingAccountRequests.asList();
    pendingAccountRequests.clear();
    for (final SnapDataRequest request : queuedAccountRequests) {
      if (request instanceof SnapV2AccountRangeRequest accountRangeRequest) {
        pendingAccountRequests.add(accountRangeRequest.retarget(newPivotBlockHeader));
      } else {
        throw new IllegalStateException(
            "Unexpected snap/2 account queue request: " + request.getClass().getSimpleName());
      }
    }
  }

  public synchronized boolean isPivotCatchupInProgress() {
    return pivotCatchupInProgress;
  }

  public synchronized Task<SnapDataRequest> dequeueLargeStorageRequestBlocking() {
    return dequeueRequestBlocking(() -> false, pendingLargeStorageRequests);
  }

  public synchronized Task<SnapDataRequest> dequeueStorageRequestBlocking() {
    return dequeueRequestBlocking(() -> false, pendingStorageRequests);
  }

  public synchronized Task<SnapDataRequest> dequeueCodeRequestBlocking() {
    return dequeueRequestBlocking(this::shouldPauseCodeRequests, pendingCodeRequests);
  }

  private boolean shouldPauseAccountRequests() {
    // TODO: Replace this drain-to-zero gate with bounded backpressure. Account ranges should pause
    // only while child queues are above a high-watermark, then resume below a low-watermark.
    return accountRangeRequestsPausedForPivotCatchup
        || hasIncompleteTasks(pendingStorageRequests)
        || hasIncompleteTasks(pendingLargeStorageRequests)
        || hasIncompleteTasks(pendingCodeRequests);
  }

  private boolean shouldPauseCodeRequests() {
    // TODO: Revisit this v1-style storage-before-code priority for snap/2. Code downloads may be
    // able to drain independently once account backpressure is bounded.
    return hasIncompleteTasks(pendingStorageRequests);
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

  public DownloadedAccountRangeTracker getRangeTracker() {
    return rangeTracker;
  }
}
