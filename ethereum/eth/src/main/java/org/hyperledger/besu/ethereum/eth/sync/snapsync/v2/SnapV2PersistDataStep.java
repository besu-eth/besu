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

import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.canRetryOnError;
import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.errorCountAtThreshold;
import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.getRetryableErrorCounter;

import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snap/2 persist step. Owns persistence, child creation, and range tracking. */
public class SnapV2PersistDataStep {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2PersistDataStep.class);

  private final SnapSyncProcessState snapSyncState;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final SnapRequestContext downloadState;
  private final SnapSyncConfiguration snapSyncConfiguration;
  private final DownloadedAccountRangeTracker accountRangeTracker;
  private final DownloadedStorageRangeTracker storageRangeTracker;

  public SnapV2PersistDataStep(
      final SnapSyncProcessState snapSyncState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapRequestContext downloadState,
      final SnapSyncConfiguration snapSyncConfiguration,
      final DownloadedAccountRangeTracker accountRangeTracker,
      final DownloadedStorageRangeTracker storageRangeTracker) {
    this.snapSyncState = snapSyncState;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.downloadState = downloadState;
    this.snapSyncConfiguration = snapSyncConfiguration;
    this.accountRangeTracker = accountRangeTracker;
    this.storageRangeTracker = storageRangeTracker;
  }

  public List<Task<SnapDataRequest>> persist(final List<Task<SnapDataRequest>> tasks) {
    final List<Runnable> pendingUpdates = new ArrayList<>();
    try {
      final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
      for (final Task<SnapDataRequest> task : tasks) {
        final SnapDataRequest request = task.getData();
        if (request.isExpired(snapSyncState)) {
          throw new IllegalStateException(expiredRequestMessage(request));
        } else if (request.isResponseReceived()) {
          final int nbNodesSaved =
              request.persist(
                  worldStateStorageCoordinator,
                  updater,
                  downloadState,
                  snapSyncState,
                  snapSyncConfiguration);
          if (nbNodesSaved > 0) {
            downloadState.getMetricsManager().notifyNodesGenerated(nbNodesSaved);
          }
          final List<SnapDataRequest> children =
              request
                  .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
                  .toList();
          pendingUpdates.add(() -> trackRangesAndEnqueueChildren(request, children));
        }
      }
      updater.commit();
    } catch (final StorageException storageException) {
      if (canRetryOnError(storageException)) {
        if (errorCountAtThreshold()) {
          LOG.info(
              "Encountered {} retryable RocksDB errors, latest error message {}",
              getRetryableErrorCounter(),
              storageException.getMessage());
        }
        tasks.forEach(task -> task.getData().clear());
        return tasks;
      }
      throw storageException;
    }
    // Only reached after successful commit — apply tracking + enqueue atomically
    for (final Runnable update : pendingUpdates) {
      update.run();
    }
    return tasks;
  }

  private String expiredRequestMessage(final SnapDataRequest request) {
    final String currentPivot =
        snapSyncState.getPivotBlockHeader().map(header -> header.toLogString()).orElse("empty");
    final String requestPivot =
        request instanceof SnapV2DataRequest snapV2DataRequest
            ? snapV2DataRequest.getPivotBlockHeader().toLogString()
            : "unknown";
    return "Expired snap/2 request reached persist step: type "
        + request.getRequestType()
        + ", request pivot "
        + requestPivot
        + ", current pivot "
        + currentPivot;
  }

  public Task<SnapDataRequest> persist(final Task<SnapDataRequest> task) {
    return persist(List.of(task)).get(0);
  }

  private void trackRangesAndEnqueueChildren(
      final SnapDataRequest request, final List<SnapDataRequest> children) {
    // Register tracking state before enqueueing children (no race)
    if (request instanceof SnapV2AccountRangeRequest accountRequest) {
      trackAccountRange(accountRequest, children);
    } else if (request instanceof SnapV2StorageRangeRequest storageRequest) {
      trackStorageRange(storageRequest, children);
    } else if (request instanceof SnapV2BytecodeRequest codeRequest) {
      accountRangeTracker.onChildCompleted(codeRequest.getRangeStart());
    }

    downloadState.enqueueRequests(children.stream());
  }

  private void trackAccountRange(
      final SnapV2AccountRangeRequest accountRequest, final List<SnapDataRequest> children) {
    final Bytes32 rangeStart = accountRequest.getRangeStart();

    int continuationCount = 0;
    SnapV2AccountRangeRequest continuation = null;
    for (final SnapDataRequest child : children) {
      if (child instanceof SnapV2AccountRangeRequest accountRangeContinuation) {
        continuationCount++;
        if (continuationCount > 1) {
          throw new IllegalStateException(
              "Expected at most one SnapV2AccountRangeRequest continuation, got "
                  + continuationCount);
        }
        continuation = accountRangeContinuation;
      }
    }

    final Bytes32 coveredEnd;
    if (continuation != null) {
      if (accountRequest.getAccounts().isEmpty()) {
        throw new IllegalStateException("Account range continuation found for empty response");
      }

      final Bytes32 continuationStart = continuation.getStartKeyHash();
      final Bytes32 lastReceivedAccount = accountRequest.getAccounts().lastKey();

      if (continuationStart.compareTo(lastReceivedAccount) <= 0) {
        throw new IllegalStateException(
            "Account range continuation does not advance past last received account: continuation "
                + continuationStart
                + ", last received "
                + lastReceivedAccount);
      }

      coveredEnd = prevKey(continuationStart);
    } else {
      coveredEnd = accountRequest.getEndKeyHash();
    }

    final int childCount = children.size() - continuationCount;

    accountRangeTracker.registerPending(rangeStart, coveredEnd, childCount);
  }

  private void trackStorageRange(
      final SnapV2StorageRangeRequest storageRequest, final List<SnapDataRequest> children) {
    final Bytes32 rangeStart = storageRequest.getRangeStart();
    final Bytes32 startKeyHash = storageRequest.getStartKeyHash();
    final Bytes32 accountHashBytes = Bytes32.wrap(storageRequest.getAccountHash().getBytes());

    int continuationCount = 0;
    SnapV2StorageRangeRequest firstContinuation = null;
    for (final SnapDataRequest child : children) {
      if (child instanceof SnapV2StorageRangeRequest storageRangeContinuation) {
        continuationCount++;
        if (firstContinuation == null) {
          firstContinuation = storageRangeContinuation;
        }
      }
    }

    final Bytes32 coveredEnd;
    if (firstContinuation != null) {
      final Bytes32 continuationStart = firstContinuation.getStartKeyHash();
      final NavigableMap<Bytes32, Bytes> slots = storageRequest.getSlots();
      if (!slots.isEmpty() && continuationStart.compareTo(slots.lastKey()) <= 0) {
        throw new IllegalStateException(
            "Storage range continuation does not advance past last received slot: continuation "
                + continuationStart
                + ", last received "
                + slots.lastKey());
      }
      coveredEnd = prevKey(continuationStart);
    } else {
      coveredEnd = storageRequest.getEndKeyHash();
    }

    storageRangeTracker.registerSlotRange(accountHashBytes, startKeyHash, coveredEnd);

    if (continuationCount == 0) {
      accountRangeTracker.onChildCompleted(rangeStart);
    } else {
      accountRangeTracker.adjustPendingChildren(rangeStart, continuationCount - 1);
    }
  }

  private static Bytes32 prevKey(final Bytes32 key) {
    return UInt256.fromBytes(key).subtract(UInt256.ONE);
  }
}
