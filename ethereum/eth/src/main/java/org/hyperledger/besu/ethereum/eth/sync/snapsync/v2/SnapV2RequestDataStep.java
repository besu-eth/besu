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
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snap/2 network request step. Fetches account, storage, and code data from peers. */
public class SnapV2RequestDataStep {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2RequestDataStep.class);

  private final EthContext ethContext;
  private final WorldStateProofProvider worldStateProofProvider;
  private final SnapSyncProcessState fastSyncState;
  private final SnapRequestContext downloadState;
  private final MetricsSystem metricsSystem;

  public SnapV2RequestDataStep(
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState fastSyncState,
      final SnapRequestContext downloadState,
      final MetricsSystem metricsSystem) {
    this.ethContext = ethContext;
    this.worldStateProofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
    this.fastSyncState = fastSyncState;
    this.downloadState = downloadState;
    this.metricsSystem = metricsSystem;
  }

  public CompletableFuture<Task<SnapDataRequest>> requestAccount(
      final Task<SnapDataRequest> requestTask) {
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
    final SnapV2AccountRangeRequest request = (SnapV2AccountRangeRequest) requestTask.getData();
    final EthTask<AccountRangeMessage.AccountRangeData> getAccountTask =
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
              }
              if (error != null) {
                LOG.atDebug()
                    .setMessage("Error handling account download accounts ({} - {}) task: {}")
                    .addArgument(request.getStartKeyHash())
                    .addArgument(request.getEndKeyHash())
                    .addArgument(error)
                    .log();
              }
              return requestTask;
            });
  }

  public CompletableFuture<List<Task<SnapDataRequest>>> requestStorage(
      final List<Task<SnapDataRequest>> requestTasks) {
    final List<Hash> accountHashes =
        requestTasks.stream()
            .map(Task::getData)
            .map(SnapV2StorageRangeRequest.class::cast)
            .map(SnapV2StorageRangeRequest::getAccountHash)
            .collect(Collectors.toList());
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
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
    final EthTask<StorageRangeMessage.SlotRangeData> getStorageRangeTask =
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
                  }
                } catch (final Exception e) {
                  LOG.error("Error while processing storage range response", e);
                }
              }
              if (error != null) {
                LOG.atDebug()
                    .setMessage("Error handling storage range request task: {}")
                    .addArgument(error)
                    .log();
              }
              return requestTasks;
            });
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
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
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
                LOG.atDebug()
                    .setMessage("Error handling code request task: {}")
                    .addArgument(error)
                    .log();
              }
              return requestTasks;
            });
  }
}
