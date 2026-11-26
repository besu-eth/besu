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

import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.canRetryOnError;
import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.errorCountAtThreshold;
import static org.hyperledger.besu.ethereum.eth.sync.StorageExceptionManager.getRetryableErrorCounter;

import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.UndoLog;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.TrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.tasks.Task;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadLocalDataStep {

  private static final Logger LOG = LoggerFactory.getLogger(LoadLocalDataStep.class);
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final SnapWorldDownloadState downloadState;
  private final SnapSyncProcessState snapSyncState;

  private final SnapSyncConfiguration snapSyncConfiguration;
  private final Counter existingNodeCounter;

  public LoadLocalDataStep(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapWorldDownloadState downloadState,
      final SnapSyncConfiguration snapSyncConfiguration,
      final MetricsSystem metricsSystem,
      final SnapSyncProcessState snapSyncState) {
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.downloadState = downloadState;
    this.snapSyncConfiguration = snapSyncConfiguration;
    existingNodeCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_world_state_existing_trie_nodes_total",
            "Total number of node data requests completed using existing data");
    this.snapSyncState = snapSyncState;
  }

  public Stream<Task<SnapDataRequest>> loadLocalDataTrieNode(
      final Task<SnapDataRequest> task, final Pipe<Task<SnapDataRequest>> completedTasks) {
    final TrieNodeHealingRequest request = (TrieNodeHealingRequest) task.getData();
    final UndoLog undoLog = new UndoLog();
    // check if node is already stored in the worldstate
    try {
      if (snapSyncState.hasPivotBlockHeader()) {
        Optional<Bytes> existingData = request.getExistingData(worldStateStorageCoordinator);
        if (existingData.isPresent()) {
          request.setData(existingData.get());
          final Optional<List<SnapDataRequest>> rootStorageRequests =
              rootStorageRequestsOfCompleteNode(request);
          if (rootStorageRequests.isEmpty()) {
            // the stored node can't be used, so it is downloaded like an absent one and healing
            // then walks down from it
            request.clear();
            return Stream.of(task);
          }
          existingNodeCounter.inc();
          request.setRequiresPersisting(false);
          final WorldStateKeyValueStorage.Updater updater =
              worldStateStorageCoordinator.batchUpdater();
          request.persist(
              worldStateStorageCoordinator,
              updater,
              downloadState,
              snapSyncState,
              snapSyncConfiguration,
              undoLog);
          updater.commit();
          downloadState.enqueueRequests(rootStorageRequests.get().stream());
          completedTasks.put(task);
          return Stream.empty();
        }
      }
    } catch (StorageException storageException) {
      if (canRetryOnError(storageException)) {
        // We reset the task by setting it to null. This way, it is considered as failed by the
        // pipeline, and it will attempt to execute it again later.
        if (errorCountAtThreshold()) {
          LOG.info(
              "Encountered {} retryable RocksDB errors, latest error message {}",
              getRetryableErrorCounter(),
              storageException.getMessage());
        }
        // nothing was stored, so the retried request counts in its parent again
        undoLog.undo();
        task.getData().clear();
      } else {
        throw storageException;
      }
    }
    return Stream.of(task);
  }

  /**
   * Returns the storage requests of a stored node, or empty when the stored value is not the node
   * or a node below it is missing.
   */
  private Optional<List<SnapDataRequest>> rootStorageRequestsOfCompleteNode(
      final TrieNodeHealingRequest request) {
    if (!request.isResponseReceived()) {
      // for example an empty value written under the node's hash
      LOG.atDebug()
          .setMessage("Stored value of trie node {} at {} is not the node, downloading it again")
          .addArgument(request::getNodeHash)
          .addArgument(request::getLocation)
          .log();
      return Optional.empty();
    }
    try {
      return Optional.of(request.getRootStorageRequests(worldStateStorageCoordinator).toList());
    } catch (final MerkleTrieException e) {
      LOG.atDebug()
          .setMessage("Stored trie node {} at {} is missing a descendant, downloading it again")
          .addArgument(request::getNodeHash)
          .addArgument(request::getLocation)
          .setCause(e)
          .log();
      return Optional.empty();
    }
  }
}
