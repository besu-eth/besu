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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBytecodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetStorageRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetTrieNodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.TrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestDataStep {
  private static final Logger LOG = LoggerFactory.getLogger(RequestDataStep.class);
  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final SnapSyncProcessState fastSyncState;
  private final SnapWorldDownloadState downloadState;
  private final SnapSyncConfiguration snapSyncConfiguration;
  private final MetricsSystem metricsSystem;
  private final EthContext ethContext;
  private final WorldStateProofProvider worldStateProofProvider;

  public RequestDataStep(
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState fastSyncState,
      final SnapWorldDownloadState downloadState,
      final SnapSyncConfiguration snapSyncConfiguration,
      final MetricsSystem metricsSystem) {
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.fastSyncState = fastSyncState;
    this.downloadState = downloadState;
    this.snapSyncConfiguration = snapSyncConfiguration;
    this.metricsSystem = metricsSystem;
    this.ethContext = ethContext;
    this.worldStateProofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
  }

  public CompletableFuture<Task<SnapDataRequest>> requestAccount(
      final Task<SnapDataRequest> requestTask) {

    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
    final AccountRangeDataRequest accountDataRequest =
        (AccountRangeDataRequest) requestTask.getData();
    final EthTask<AccountRangeMessage.AccountRangeData> getAccountTask =
        RetryingGetAccountRangeFromPeerTask.forAccountRange(
            ethContext,
            accountDataRequest.getStartKeyHash(),
            accountDataRequest.getEndKeyHash(),
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
                accountDataRequest.setRootHash(blockHeader.getStateRoot());
                accountDataRequest.addResponse(
                    worldStateProofProvider, response.accounts(), response.proofs());
              }
              if (error != null) {
                LOG.atDebug()
                    .setMessage("Error handling account download accounts ({} - {}) task: {}")
                    .addArgument(accountDataRequest.getStartKeyHash())
                    .addArgument(accountDataRequest.getEndKeyHash())
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
            .map(StorageRangeDataRequest.class::cast)
            .map(StorageRangeDataRequest::getAccountHash)
            .collect(Collectors.toList());
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
    final Bytes32 minRange =
        requestTasks.size() == 1
            ? ((StorageRangeDataRequest) requestTasks.get(0).getData()).getStartKeyHash()
            : RangeManager.MIN_RANGE;
    final Bytes32 maxRange =
        requestTasks.size() == 1
            ? ((StorageRangeDataRequest) requestTasks.get(0).getData()).getEndKeyHash()
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
                /*
                 * Checks if the response represents an "empty range".
                 *
                 * An "empty range" is defined as a response where at least one proof exists
                 * and either no slots are present, or the first slot is empty
                 */
                try {
                  final boolean isEmptyRange =
                      (response.slots().isEmpty() || response.slots().get(0).isEmpty())
                          && !response.proofs().isEmpty();
                  if (isEmptyRange) { // empty range detected
                    slots.add(new TreeMap<>());
                  } else {
                    slots.addAll(response.slots());
                  }
                  for (int i = 0; i < slots.size(); i++) {
                    final StorageRangeDataRequest request =
                        (StorageRangeDataRequest) requestTasks.get(i).getData();
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
            .map(BytecodeRequest.class::cast)
            .map(BytecodeRequest::getCodeHash)
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
                  final BytecodeRequest request = (BytecodeRequest) requestTask.getData();
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

  public CompletableFuture<List<Task<SnapDataRequest>>> requestTrieNodeByPath(
      final List<Task<SnapDataRequest>> requestTasks) {

    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();
    final Map<Bytes, List<Bytes>> message = new HashMap<>();
    requestTasks.stream()
        .map(Task::getData)
        .map(TrieNodeHealingRequest.class::cast)
        .map(TrieNodeHealingRequest::getTrieNodePath)
        .forEach(
            path -> {
              final List<Bytes> bytes =
                  message.computeIfAbsent(path.get(0), k -> Lists.newArrayList());
              if (path.size() > 1) {
                bytes.add(path.get(1));
              }
            });
    final EthTask<Map<Bytes, Bytes>> getTrieNodeFromPeerTask =
        RetryingGetTrieNodeFromPeerTask.forTrieNodes(
            ethContext, message, blockHeader, metricsSystem);
    downloadState.addOutstandingTask(getTrieNodeFromPeerTask);
    return getTrieNodeFromPeerTask
        .run()
        .orTimeout(10, TimeUnit.SECONDS)
        .handle(
            (response, error) -> {
              downloadState.removeOutstandingTask(getTrieNodeFromPeerTask);
              if (response != null) {
                for (final Task<SnapDataRequest> task : requestTasks) {
                  final TrieNodeHealingRequest request = (TrieNodeHealingRequest) task.getData();
                  final Bytes matchingData = response.get(request.getPathId());
                  if (matchingData != null) {
                    request.setData(matchingData);
                  }
                }
              }
              if (error != null) {
                LOG.atDebug()
                    .setMessage("Error handling trie node request task: {}")
                    .addArgument(error)
                    .log();
              }
              return requestTasks;
            });
  }

  /**
   * Retrieves local accounts from the flat database and generates the necessary proof, updates the
   * data request with the retrieved information, and returns the modified data request task.
   *
   * @param requestTask request data to fill
   * @return data request with local accounts
   */
  public CompletableFuture<Task<SnapDataRequest>> requestLocalFlatAccounts(
      final Task<SnapDataRequest> requestTask) {

    final AccountFlatDatabaseHealingRangeRequest accountDataRequest =
        (AccountFlatDatabaseHealingRangeRequest) requestTask.getData();
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();

    // retrieve accounts from flat database
    final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();

    worldStateStorageCoordinator.applyOnMatchingFlatMode(
        FlatDbMode.FULL,
        onBonsai -> {
          accounts.putAll(
              onBonsai.streamFlatAccounts(
                  accountDataRequest.getStartKeyHash(),
                  accountDataRequest.getEndKeyHash(),
                  snapSyncConfiguration.getLocalFlatAccountCountToHealPerRequest()));
        });

    final List<Bytes> proofs = new ArrayList<>();
    if (!accounts.isEmpty()) {
      // generate range proof if accounts are present
      proofs.addAll(
          worldStateProofProvider.getAccountProofRelatedNodes(
              blockHeader.getStateRoot(), accounts.firstKey()));
      proofs.addAll(
          worldStateProofProvider.getAccountProofRelatedNodes(
              blockHeader.getStateRoot(), accounts.lastKey()));
    }

    accountDataRequest.setRootHash(blockHeader.getStateRoot());
    accountDataRequest.addLocalData(worldStateProofProvider, accounts, new ArrayDeque<>(proofs));

    return CompletableFuture.completedFuture(requestTask);
  }

  /**
   * Retrieves local storage slots from the flat database and generates the necessary proof, updates
   * the data request with the retrieved information, and returns the modified data request task.
   *
   * @param requestTask request data to fill
   * @return data request with local slots
   */
  public CompletableFuture<Task<SnapDataRequest>> requestLocalFlatStorages(
      final Task<SnapDataRequest> requestTask) {

    final StorageFlatDatabaseHealingRangeRequest storageDataRequest =
        (StorageFlatDatabaseHealingRangeRequest) requestTask.getData();
    final BlockHeader blockHeader = fastSyncState.getPivotBlockHeader().get();

    storageDataRequest.setRootHash(blockHeader.getStateRoot());

    // retrieve slots from flat database
    final TreeMap<Bytes32, Bytes> slots = new TreeMap<>();
    worldStateStorageCoordinator.applyOnMatchingFlatMode(
        FlatDbMode.FULL,
        onBonsai -> {
          slots.putAll(
              onBonsai.streamFlatStorages(
                  storageDataRequest.getAccountHash(),
                  storageDataRequest.getStartKeyHash(),
                  storageDataRequest.getEndKeyHash(),
                  snapSyncConfiguration.getLocalFlatStorageCountToHealPerRequest()));
        });

    final List<Bytes> proofs = new ArrayList<>();
    if (!slots.isEmpty()) {
      // generate range proof if slots are present
      proofs.addAll(
          worldStateProofProvider.getStorageProofRelatedNodes(
              storageDataRequest.getStorageRoot(),
              Bytes32.wrap(storageDataRequest.getAccountHash().getBytes()),
              slots.firstKey()));
      proofs.addAll(
          worldStateProofProvider.getStorageProofRelatedNodes(
              storageDataRequest.getStorageRoot(),
              Bytes32.wrap(storageDataRequest.getAccountHash().getBytes()),
              slots.lastKey()));
    }
    storageDataRequest.addLocalData(worldStateProofProvider, slots, new ArrayDeque<>(proofs));

    return CompletableFuture.completedFuture(requestTask);
  }
}
