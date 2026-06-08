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
  private final SnapSyncMetricsManager metricsManager;
  private final AtomicBoolean worldStateFinishedNotified = new AtomicBoolean(false);
  private final WorldStateHealFinishedListener worldStateHealFinishedListener;
  private final DownloadedAccountRangeTracker rangeTracker = new DownloadedAccountRangeTracker();

  public SnapV2WorldDownloadState(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncStatePersistenceManager snapContext,
      final InMemoryTasksPriorityQueues<SnapDataRequest> pendingRequests,
      final int maxRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final SnapSyncMetricsManager metricsManager,
      final Clock clock,
      final SyncDurationMetrics syncDurationMetrics,
      final WorldStateHealFinishedListener worldStateHealFinishedListener) {
    super(
        worldStateStorageCoordinator,
        pendingRequests,
        maxRequestsWithoutProgress,
        minMillisBeforeStalling,
        clock,
        syncDurationMetrics);
    this.snapContext = snapContext;
    this.metricsManager = metricsManager;
    this.worldStateHealFinishedListener = worldStateHealFinishedListener;

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
    return hasIncompleteTasks(pendingStorageRequests)
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

  @Override
  public void markAccountRangeComplete(final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    rangeTracker.registerPending(startKeyHash, endKeyHash, 0);
    LOG.atDebug()
        .setMessage("Marked account range complete: {} -> {}")
        .addArgument(startKeyHash)
        .addArgument(endKeyHash)
        .log();
  }
}
