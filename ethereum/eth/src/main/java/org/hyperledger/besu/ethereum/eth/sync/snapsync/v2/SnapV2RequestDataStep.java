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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBytecodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetStorageRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.Task;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** snap/2 network request step. Fetches account, storage, and code data from peers. */
public class SnapV2RequestDataStep {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2RequestDataStep.class);

  static final Duration FAILED_REQUEST_BACKOFF = Duration.ofSeconds(1);

  private static final long PEER_ERROR_WARN_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

  private final EthContext ethContext;
  private final WorldStateProofProvider worldStateProofProvider;
  private final SnapRequestContext downloadState;
  private final MetricsSystem metricsSystem;
  private final AtomicLong peerErrorCount = new AtomicLong();
  private final AtomicLong lastPeerErrorWarnMillis = new AtomicLong();

  public SnapV2RequestDataStep(
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState fastSyncState,
      final SnapRequestContext downloadState,
      final MetricsSystem metricsSystem) {
    this.ethContext = ethContext;
    this.worldStateProofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
    this.downloadState = downloadState;
    this.metricsSystem = metricsSystem;
  }

  public CompletableFuture<Task<SnapDataRequest>> requestAccount(
      final Task<SnapDataRequest> requestTask) {
    final SnapV2AccountRangeRequest request = (SnapV2AccountRangeRequest) requestTask.getData();
    final BlockHeader blockHeader = request.getPivotBlockHeader();
    final RetryingGetAccountRangeFromPeerTask getAccountTask =
        (RetryingGetAccountRangeFromPeerTask)
            RetryingGetAccountRangeFromPeerTask.forAccountRange(
                ethContext,
                request.getStartKeyHash(),
                request.getEndKeyHash(),
                blockHeader,
                metricsSystem);
    downloadState.addOutstandingTask(getAccountTask);
    return getAccountTask
        .run()
        .orTimeout(10, TimeUnit.SECONDS)
        .handle(
            (response, error) -> {
              downloadState.removeOutstandingTask(getAccountTask);
              if (response != null) {
                request.setRootHash(blockHeader.getStateRoot());
                request.addResponse(
                    worldStateProofProvider, response.accounts(), response.proofs());
                if (request.hasInvalidProof()) {
                  getAccountTask
                      .getAssignedPeer()
                      .ifPresent(
                          peer -> {
                            LOG.atDebug()
                                .setMessage(
                                    "Invalid snap/2 account range proof received from peer {}")
                                .addArgument(peer::getLoggableId)
                                .log();
                            peer.recordUselessResponse("invalid snap/2 account range proof");
                          });
                }
              }
              if (error != null) {
                LOG.debug(
                    "Error handling snap/2 account download ({} - {}): {}",
                    request.getStartKeyHash(),
                    request.getEndKeyHash(),
                    error);
                recordPeerError("account range", error);
              }
              return requestTask;
            })
        .thenCompose(this::maybeBackOffFailedRequest);
  }

  public CompletableFuture<List<Task<SnapDataRequest>>> requestStorage(
      final List<Task<SnapDataRequest>> requestTasks) {
    final List<Hash> accountHashes =
        requestTasks.stream()
            .map(Task::getData)
            .map(SnapV2StorageRangeRequest.class::cast)
            .map(SnapV2StorageRangeRequest::getAccountHash)
            .collect(Collectors.toList());
    final BlockHeader blockHeader =
        ((SnapV2StorageRangeRequest) requestTasks.getFirst().getData()).getPivotBlockHeader();
    final Bytes32 minRange =
        requestTasks.size() == 1
            ? ((SnapV2StorageRangeRequest) requestTasks.get(0).getData()).getStartKeyHash()
            : RangeManager.MIN_RANGE;
    final Bytes32 maxRange =
        requestTasks.size() == 1
            ? ((SnapV2StorageRangeRequest) requestTasks.get(0).getData()).getEndKeyHash()
            : RangeManager.MAX_RANGE;
    final List<Bytes32> accountHashesAsBytes32 =
        accountHashes.stream()
            .map(hash -> Bytes32.wrap(hash.getBytes()))
            .collect(Collectors.toList());
    final RetryingGetStorageRangeFromPeerTask getStorageRangeTask =
        RetryingGetStorageRangeFromPeerTask.forStorageRange(
            ethContext, accountHashesAsBytes32, minRange, maxRange, blockHeader, metricsSystem);
    downloadState.addOutstandingTask(getStorageRangeTask);
    return getStorageRangeTask
        .run()
        .orTimeout(10, TimeUnit.SECONDS)
        .handle(
            (response, error) -> {
              downloadState.removeOutstandingTask(getStorageRangeTask);
              if (response != null) {
                final ArrayDeque<NavigableMap<Bytes32, Bytes>> slots = new ArrayDeque<>();
                boolean invalidProofReceived = false;
                try {
                  final boolean isEmptyRange =
                      (response.slots().isEmpty() || response.slots().get(0).isEmpty())
                          && !response.proofs().isEmpty();
                  if (isEmptyRange) {
                    slots.add(new TreeMap<>());
                  } else {
                    slots.addAll(response.slots());
                  }
                  for (int i = 0; i < slots.size(); i++) {
                    final SnapV2StorageRangeRequest request =
                        (SnapV2StorageRangeRequest) requestTasks.get(i).getData();
                    request.setRootHash(blockHeader.getStateRoot());
                    request.addResponse(
                        downloadState,
                        worldStateProofProvider,
                        slots.get(i),
                        i < slots.size() - 1 ? new ArrayDeque<>() : response.proofs());
                    if (request.hasInvalidProof()) {
                      invalidProofReceived = true;
                    }
                  }
                  if (invalidProofReceived) {
                    getStorageRangeTask
                        .getAssignedPeer()
                        .ifPresent(
                            peer -> {
                              LOG.atDebug()
                                  .setMessage(
                                      "Invalid snap/2 storage range proof received from peer {}")
                                  .addArgument(peer::getLoggableId)
                                  .log();
                              peer.recordUselessResponse("invalid snap/2 storage range proof");
                            });
                  }
                } catch (final Exception e) {
                  LOG.error("Error while processing storage range response", e);
                }
              }
              if (error != null) {
                LOG.debug("Error handling snap/2 storage range request: {}", error);
                recordPeerError("storage range", error);
              }
              return requestTasks;
            })
        .thenCompose(this::maybeBackOffFailedRequests);
  }

  public CompletableFuture<List<Task<SnapDataRequest>>> requestCode(
      final List<Task<SnapDataRequest>> requestTasks) {
    final List<Bytes32> codeHashes =
        requestTasks.stream()
            .map(Task::getData)
            .map(SnapV2BytecodeRequest.class::cast)
            .map(SnapV2BytecodeRequest::getCodeHash)
            .distinct()
            .collect(Collectors.toList());
    final BlockHeader blockHeader =
        ((SnapV2BytecodeRequest) requestTasks.getFirst().getData()).getPivotBlockHeader();
    final EthTask<Map<Bytes32, Bytes>> getByteCodeTask =
        RetryingGetBytecodeFromPeerTask.forByteCode(
            ethContext, codeHashes, blockHeader, metricsSystem);
    downloadState.addOutstandingTask(getByteCodeTask);
    return getByteCodeTask
        .run()
        .orTimeout(10, TimeUnit.SECONDS)
        .handle(
            (response, error) -> {
              downloadState.removeOutstandingTask(getByteCodeTask);
              if (response != null) {
                for (Task<SnapDataRequest> requestTask : requestTasks) {
                  final SnapV2BytecodeRequest request =
                      (SnapV2BytecodeRequest) requestTask.getData();
                  request.setRootHash(blockHeader.getStateRoot());
                  if (response.containsKey(request.getCodeHash())) {
                    request.setCode(response.get(request.getCodeHash()));
                  }
                }
              }
              if (error != null) {
                LOG.debug("Error handling snap/2 code request: {}", error);
                recordPeerError("bytecode", error);
              }
              return requestTasks;
            })
        .thenCompose(this::maybeBackOffFailedRequests);
  }

  private CompletableFuture<Task<SnapDataRequest>> maybeBackOffFailedRequest(
      final Task<SnapDataRequest> task) {
    if (task.getData().isResponseReceived()) {
      return CompletableFuture.completedFuture(task);
    }
    return ethContext
        .getScheduler()
        .scheduleFutureTask(() -> CompletableFuture.completedFuture(task), FAILED_REQUEST_BACKOFF);
  }

  private CompletableFuture<List<Task<SnapDataRequest>>> maybeBackOffFailedRequests(
      final List<Task<SnapDataRequest>> tasks) {
    if (tasks.stream().anyMatch(task -> task.getData().isResponseReceived())) {
      return CompletableFuture.completedFuture(tasks);
    }
    return ethContext
        .getScheduler()
        .scheduleFutureTask(() -> CompletableFuture.completedFuture(tasks), FAILED_REQUEST_BACKOFF);
  }

  private void recordPeerError(final String context, final Throwable error) {
    peerErrorCount.incrementAndGet();
    final long now = System.currentTimeMillis();
    final long last = lastPeerErrorWarnMillis.get();
    if (now - last >= PEER_ERROR_WARN_INTERVAL_MS
        && lastPeerErrorWarnMillis.compareAndSet(last, now)) {
      final long loggedCount = peerErrorCount.getAndSet(0);
      LOG.warn(
          "snap/2 peer request failures: {} error(s) in the last {}s (most recent in '{}': {})",
          loggedCount,
          PEER_ERROR_WARN_INTERVAL_MS / 1000,
          context,
          error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName());
    }
  }
}
