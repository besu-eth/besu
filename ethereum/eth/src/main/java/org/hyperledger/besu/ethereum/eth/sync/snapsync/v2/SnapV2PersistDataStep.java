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

import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.Task;

import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snap/2 persist step. Owns persistence and child creation. */
public class SnapV2PersistDataStep {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2PersistDataStep.class);

  private final SnapSyncProcessState snapSyncState;
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final SnapRequestContext downloadState;
  private final SnapSyncConfiguration snapSyncConfiguration;

  public SnapV2PersistDataStep(
      final SnapSyncProcessState snapSyncState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapRequestContext downloadState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    this.snapSyncState = snapSyncState;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.downloadState = downloadState;
    this.snapSyncConfiguration = snapSyncConfiguration;
  }

  public List<Task<SnapDataRequest>> persist(final List<Task<SnapDataRequest>> tasks) {
    try {
      final WorldStateKeyValueStorage.Updater updater = worldStateStorageCoordinator.updater();
      for (Task<SnapDataRequest> task : tasks) {
        if (task.getData().isResponseReceived()) {
          final SnapDataRequest request = task.getData();
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

          final Stream<SnapDataRequest> childRequests =
              request.getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState);
          downloadState.enqueueRequests(childRequests);
        }
      }
      updater.commit();
    } catch (StorageException storageException) {
      if (canRetryOnError(storageException)) {
        if (errorCountAtThreshold()) {
          LOG.info(
              "Encountered {} retryable RocksDB errors, latest error message {}",
              getRetryableErrorCounter(),
              storageException.getMessage());
        }
        tasks.forEach(task -> task.getData().clear());
      } else {
        throw storageException;
      }
    }
    return tasks;
  }

  public Task<SnapDataRequest> persist(final Task<SnapDataRequest> task) {
    return persist(List.of(task)).get(0);
  }
}
