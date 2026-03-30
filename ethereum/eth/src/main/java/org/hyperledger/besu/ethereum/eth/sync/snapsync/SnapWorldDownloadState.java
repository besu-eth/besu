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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest.createAccountFlatHealingRangeRequest;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest.createAccountTrieNodeDataRequest;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.InMemoryTaskQueue;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.services.tasks.TaskCollection;

import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapWorldDownloadState extends WorldDownloadState<SnapDataRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(SnapWorldDownloadState.class);

  protected final InMemoryTaskQueue<SnapDataRequest> pendingAccountRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingStorageRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingLargeStorageRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTaskQueue<SnapDataRequest> pendingCodeRequests =
      new InMemoryTaskQueue<>();
  protected final InMemoryTasksPriorityQueues<SnapDataRequest> pendingTrieNodeRequests =
      new InMemoryTasksPriorityQueues<>();

  protected final InMemoryTasksPriorityQueues<SnapDataRequest>
      pendingAccountFlatDatabaseHealingRequests = new InMemoryTasksPriorityQueues<>();

  protected final InMemoryTasksPriorityQueues<SnapDataRequest>
      pendingStorageFlatDatabaseHealingRequests = new InMemoryTasksPriorityQueues<>();
  private Set<Bytes> accountsHealingList = new HashSet<>();
  private DynamicPivotBlockSelector pivotBlockSelector;
  private WorldStateHealFinishedListener worldStateHealFinishedListener;

  private final SnapSyncStatePersistenceManager snapContext;
  private final SnapSyncProcessState snapSyncState;

  // blockchain
  private final Blockchain blockchain;
  private final Long blockObserverId;
  private final EthContext ethContext;

  // metrics around the snapsync
  private final SnapSyncMetricsManager metricsManager;

  private final AtomicBoolean trieHealStartedBefore = new AtomicBoolean(false);
  private final AtomicBoolean worldStateHealFinishedNotified = new AtomicBoolean(false);

  public SnapWorldDownloadState(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncStatePersistenceManager snapContext,
      final Blockchain blockchain,
      final SnapSyncProcessState snapSyncState,
      final InMemoryTasksPriorityQueues<SnapDataRequest> pendingRequests,
      final int maxRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final SnapSyncMetricsManager metricsManager,
      final Clock clock,
      final EthContext ethContext,
      final SyncDurationMetrics syncDurationMetrics) {
    super(
        worldStateStorageCoordinator,
        pendingRequests,
        maxRequestsWithoutProgress,
        minMillisBeforeStalling,
        clock,
        syncDurationMetrics);
    this.snapContext = snapContext;
    this.blockchain = blockchain;
    this.snapSyncState = snapSyncState;
    this.metricsManager = metricsManager;
    this.blockObserverId = blockchain.observeBlockAdded(createBlockchainObserver());
    this.ethContext = ethContext;

    final MetricsSystem metricsSystem = metricsManager.getMetricsSystem();
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_pending_account_requests_current",
        "Number of account pending requests for snap sync world state download",
        pendingAccountRequests::size);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_pending_storage_requests_current",
        "Number of storage pending requests for snap sync world state download",
        pendingStorageRequests::size);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_pending_big_storage_requests_current",
        "Number of storage pending requests for snap sync world state download",
        pendingLargeStorageRequests::size);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_pending_code_requests_current",
        "Number of code pending requests for snap sync world state download",
        pendingCodeRequests::size);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap_world_state_pending_trie_node_requests_current",
        "Number of trie node pending requests for snap sync world state download",
        pendingTrieNodeRequests::size);
    syncDurationMetrics.startTimer(
        SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
  }

  @Override
  protected synchronized void markAsStalled(final int maxNodeRequestRetries) {
    // TODO retry when mark as stalled
  }

  @Override
  public synchronized boolean checkCompletion(final BlockHeader header) {

    // Check if all snapsync tasks are completed
    if (!internalFuture.isDone()
        && pendingAccountRequests.allTasksCompleted()
        && pendingCodeRequests.allTasksCompleted()
        && pendingStorageRequests.allTasksCompleted()
        && pendingLargeStorageRequests.allTasksCompleted()
        && pendingTrieNodeRequests.allTasksCompleted()
        && pendingAccountFlatDatabaseHealingRequests.allTasksCompleted()
        && pendingStorageFlatDatabaseHealingRequests.allTasksCompleted()) {

      // if all snapsync tasks are completed and the healing process was not running
      if (!snapSyncState.isHealTrieInProgress()) {
        // Start the healing process
        startTrieHeal();
      }
      // if all snapsync tasks are completed and the healing was running and blockchain is behind
      // the pivot block
      else if (pivotBlockSelector.isBlockchainBehind()) {
        LOG.debug("Pausing world state download while waiting for chain sync to complete");
        // Set the snapsync to wait for the blockchain to catch up
        snapSyncState.setWaitingBlockchain(true);
      }
      // if all snapsync tasks are completed and the healing was running and the blockchain is not
      // behind the pivot block
      else {
        // Notify that world state heal has finished
        notifyWorldStateHealFinished();

        syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.SNAP_WORLD_STATE_HEALING_DURATION);

        // If the flat database healing process is not in progress and the flat database mode is
        // FULL
        if (!snapSyncState.isHealFlatDatabaseInProgress()
            && (worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)
                || worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.ARCHIVE))) {
          startFlatDatabaseHeal(header);
        }
        // If the flat database healing process is in progress or the flat database mode is not FULL
        else {
          final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
          applyForStrategy(
              updater,
              onBonsai -> {
                onBonsai.saveWorldState(
                    header.getHash().getBytes(),
                    Bytes32.wrap(header.getStateRoot().getBytes()),
                    rootNodeData);
              },
              onForest -> {
                onForest.saveWorldState(
                    Bytes32.wrap(header.getStateRoot().getBytes()), rootNodeData);
              });
          updater.commit();

          // Remove the blockchain observer
          blockchain.removeObserver(blockObserverId);
          // Notify that the snap sync has completed
          metricsManager.notifySnapSyncCompleted();
          // Clear the snap context
          snapContext.clear();
          internalFuture.complete(null);

          return true;
        }
      }
    }
    return false;
  }

  @Override
  protected synchronized void cleanupQueues() {
    super.cleanupQueues();
    pendingAccountRequests.clear();
    pendingStorageRequests.clear();
    pendingLargeStorageRequests.clear();
    pendingCodeRequests.clear();
    pendingTrieNodeRequests.clear();
  }

  /** Method to start the healing process of the trie */
  public synchronized void startTrieHeal() {
    if (trieHealStartedBefore.compareAndSet(false, true)) {
      syncDurationMetrics.stopTimer(
          SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);

      syncDurationMetrics.startTimer(SyncDurationMetrics.Labels.SNAP_WORLD_STATE_HEALING_DURATION);
    }
    snapContext.clearAccountRangeTasks();
    snapSyncState.setHealTrieStatus(true);
    // Try to find a new pivot block before starting the healing process
    pivotBlockSelector.switchToNewPivotBlock(
        (blockHeader, newPivotBlockFound) -> {
          snapContext.clearAccountRangeTasks();
          LOG.info(
              "Running world state heal process from peers with pivot block {}",
              blockHeader.getNumber());
          enqueueRequest(
              createAccountTrieNodeDataRequest(
                  blockHeader.getStateRoot(), Bytes.EMPTY, accountsHealingList));
        });
  }

  /** Method to reload the healing process of the trie */
  public synchronized void reloadTrieHeal() {
    // Clear the flat database and trie log from the world state storage if needed
    worldStateStorageCoordinator.applyOnMatchingStrategies(
        List.of(DataStorageFormat.BONSAI, DataStorageFormat.X_BONSAI_ARCHIVE),
        worldStateKeyValueStorage -> {
          final BonsaiWorldStateKeyValueStorage strategy =
              worldStateStorageCoordinator.getStrategy(BonsaiWorldStateKeyValueStorage.class);
          strategy.clearFlatDatabase();
          strategy.clearTrieLog();
        });
    // Clear pending trie node and code requests
    pendingTrieNodeRequests.clear();
    pendingCodeRequests.clear();

    snapSyncState.setHealTrieStatus(false);
    checkCompletion(snapSyncState.getPivotBlockHeader().orElseThrow());
  }

  public synchronized void startFlatDatabaseHeal(final BlockHeader header) {
    LOG.info("Initiating the healing process for the flat database");
    syncDurationMetrics.startTimer(SyncDurationMetrics.Labels.FLAT_DB_HEAL);
    snapSyncState.setHealFlatDatabaseInProgress(true);
    final Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(16);
    ranges.forEach(
        (key, value) ->
            enqueueRequest(
                createAccountFlatHealingRangeRequest(header.getStateRoot(), key, value)));
  }

  @Override
  public synchronized void enqueueRequest(final SnapDataRequest request) {
    if (!internalFuture.isDone()) {
      if (request instanceof BytecodeRequest) {
        pendingCodeRequests.add(request);
      } else if (request instanceof StorageRangeDataRequest) {
        if (!((StorageRangeDataRequest) request).getStartKeyHash().equals(RangeManager.MIN_RANGE)) {
          pendingLargeStorageRequests.add(request);
        } else {
          pendingStorageRequests.add(request);
        }
      } else if (request instanceof AccountRangeDataRequest) {
        pendingAccountRequests.add(request);
      } else if (request instanceof AccountFlatDatabaseHealingRangeRequest) {
        pendingAccountFlatDatabaseHealingRequests.add(request);
      } else if (request instanceof StorageFlatDatabaseHealingRangeRequest) {
        pendingStorageFlatDatabaseHealingRequests.add(request);
      } else {
        pendingTrieNodeRequests.add(request);
      }
      notifyAll();
    }
  }

  public synchronized void setAccountsHealingList(final Set<Bytes> addAccountToHealingList) {
    this.accountsHealingList = addAccountToHealingList;
  }

  /**
   * Adds an account to the list of accounts to be repaired during the healing process. If the
   * account is not already in the list, it is added to both the snap context and the internal set
   * of accounts to be repaired.
   *
   * @param account The account to be added for repair.
   */
  public synchronized void addAccountToHealingList(final Bytes account) {
    if (!accountsHealingList.contains(account)) {
      snapContext.addAccountToHealingList(account);
      accountsHealingList.add(account);
    }
  }

  public synchronized Set<Bytes> getAccountsHealingList() {
    return accountsHealingList;
  }

  @Override
  public synchronized void enqueueRequests(final Stream<SnapDataRequest> requests) {
    if (!internalFuture.isDone()) {
      requests.forEach(this::enqueueRequest);
    }
  }

  public synchronized Task<SnapDataRequest> dequeueRequestBlocking(
      final List<TaskCollection<SnapDataRequest>> queueDependencies,
      final TaskCollection<SnapDataRequest> queue,
      final Consumer<Void> unBlocked) {
    boolean isWaiting = false;
    while (!internalFuture.isDone()) {
      while (queueDependencies.stream()
          .map(TaskCollection::allTasksCompleted)
          .anyMatch(Predicate.isEqual(false))) {
        try {
          isWaiting = true;
          wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
      if (isWaiting) {
        unBlocked.accept(null);
      }
      isWaiting = false;
      Task<SnapDataRequest> task = queue.remove();
      if (task != null) {
        return task;
      }

      try {
        wait();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  public synchronized Task<SnapDataRequest> dequeueAccountRequestBlocking() {
    return dequeueRequestBlocking(
        List.of(pendingStorageRequests, pendingLargeStorageRequests, pendingCodeRequests),
        pendingAccountRequests,
        unused -> snapContext.updatePersistedTasks(pendingAccountRequests.asList()));
  }

  public synchronized Task<SnapDataRequest> dequeueLargeStorageRequestBlocking() {
    return dequeueRequestBlocking(Collections.emptyList(), pendingLargeStorageRequests, __ -> {});
  }

  public synchronized Task<SnapDataRequest> dequeueStorageRequestBlocking() {
    return dequeueRequestBlocking(Collections.emptyList(), pendingStorageRequests, __ -> {});
  }

  public synchronized Task<SnapDataRequest> dequeueCodeRequestBlocking() {
    return dequeueRequestBlocking(List.of(pendingStorageRequests), pendingCodeRequests, __ -> {});
  }

  public synchronized Task<SnapDataRequest> dequeueTrieNodeRequestBlocking() {
    return dequeueRequestBlocking(
        List.of(pendingAccountRequests, pendingStorageRequests, pendingLargeStorageRequests),
        pendingTrieNodeRequests,
        __ -> {});
  }

  public synchronized Task<SnapDataRequest> dequeueAccountFlatDatabaseHealingRequestBlocking() {
    return dequeueRequestBlocking(
        List.of(
            pendingAccountRequests,
            pendingStorageRequests,
            pendingLargeStorageRequests,
            pendingTrieNodeRequests,
            pendingStorageFlatDatabaseHealingRequests),
        pendingAccountFlatDatabaseHealingRequests,
        __ -> {});
  }

  public synchronized Task<SnapDataRequest> dequeueStorageFlatDatabaseHealingRequestBlocking() {
    return dequeueRequestBlocking(
        List.of(
            pendingAccountRequests,
            pendingStorageRequests,
            pendingLargeStorageRequests,
            pendingTrieNodeRequests),
        pendingStorageFlatDatabaseHealingRequests,
        __ -> {});
  }

  public SnapSyncMetricsManager getMetricsManager() {
    return metricsManager;
  }

  public void setPivotBlockSelector(final DynamicPivotBlockSelector pivotBlockSelector) {
    this.pivotBlockSelector = pivotBlockSelector;
  }

  public void setWorldStateHealFinishedListener(final WorldStateHealFinishedListener listener) {
    this.worldStateHealFinishedListener = listener;
  }

  /**
   * Notifies the registered listener that world state heal has finished. Once the state heal has
   * finished no new pivot blocks are selected. This notification is sent only once.
   */
  private void notifyWorldStateHealFinished() {
    if (worldStateHealFinishedNotified.compareAndSet(false, true)) {
      if (worldStateHealFinishedListener != null) {
        LOG.info("Notifying that world state download has finished");
        worldStateHealFinishedListener.onWorldStateHealFinished();
      }
    }
  }

  public BlockAddedObserver createBlockchainObserver() {
    return addedBlockContext ->
        ethContext
            .getScheduler()
            .executeServiceTask(
                () -> {
                  final AtomicBoolean foundNewPivotBlock = new AtomicBoolean(false);
                  pivotBlockSelector.check(
                      (____, isNewPivotBlock) -> {
                        if (isNewPivotBlock) {
                          foundNewPivotBlock.set(true);
                        }
                      });

                  final boolean isNewPivotBlockFound = foundNewPivotBlock.get();
                  final boolean isBlockchainCaughtUp =
                      snapSyncState.isWaitingBlockchain()
                          && !pivotBlockSelector.isBlockchainBehind();

                  if (snapSyncState.isHealTrieInProgress()
                      && (isNewPivotBlockFound || isBlockchainCaughtUp)) {
                    snapSyncState.setWaitingBlockchain(false);
                    reloadTrieHeal();
                  }
                });
  }
}
