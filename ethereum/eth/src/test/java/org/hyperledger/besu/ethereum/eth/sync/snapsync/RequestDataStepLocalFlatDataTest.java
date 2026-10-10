/*
 * Copyright contributors to Besu.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.Task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RequestDataStepLocalFlatDataTest {

  private static final int ACCOUNT_COUNT = 15;

  private final BonsaiWorldStateKeyValueStorage worldStateStorage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      new WorldStateStorageCoordinator(worldStateStorage);

  private final EthContext ethContext = mock(EthContext.class);
  private final EthScheduler ethScheduler = mock(EthScheduler.class);
  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final SnapWorldDownloadState downloadState = mock(SnapWorldDownloadState.class);
  private final SnapSyncConfiguration snapSyncConfiguration = mock(SnapSyncConfiguration.class);

  // what was handed to the computation threads, to be run by the test
  private final List<Runnable> scheduled = new ArrayList<>();
  private Hash stateRoot;
  private RequestDataStep requestDataStep;

  @BeforeEach
  public void setUp() {
    final MerkleTrie<Bytes, Bytes> accountTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, ACCOUNT_COUNT);
    stateRoot = Hash.wrap(accountTrie.getRootHash());
    final BlockHeader pivot = new BlockHeaderTestFixture().stateRoot(stateRoot).buildHeader();
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(pivot));
    when(snapSyncConfiguration.getLocalFlatAccountCountToHealPerRequest()).thenReturn(10);
    when(snapSyncConfiguration.getLocalFlatStorageCountToHealPerRequest()).thenReturn(2);
    when(downloadState.getMetricsManager()).thenReturn(mock(SnapSyncMetricsManager.class));
    when(downloadState.getAccountsHealingList()).thenReturn(new HashSet<>());
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
    when(ethScheduler.scheduleComputationTask(any()))
        .thenAnswer(
            invocation -> {
              final Supplier<Object> computation = invocation.getArgument(0);
              final CompletableFuture<Object> result = new CompletableFuture<>();
              scheduled.add(() -> result.complete(computation.get()));
              return result;
            });
    requestDataStep =
        new RequestDataStep(
            ethContext,
            worldStateStorageCoordinator,
            snapSyncState,
            downloadState,
            snapSyncConfiguration,
            new NoOpMetricsSystem());
  }

  @Test
  public void shouldCheckLocalFlatAccountsOnTheComputationThreads() {
    final AccountFlatDatabaseHealingRangeRequest request =
        SnapDataRequest.createAccountFlatHealingRangeRequest(
            stateRoot, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    final Task<SnapDataRequest> task = new StubTask(request);

    final CompletableFuture<Task<SnapDataRequest>> result =
        requestDataStep.requestLocalFlatAccounts(task);

    // nothing is read by the caller, which is the thread that feeds the pipeline
    assertThat(result).isNotDone();
    assertThat(childRequests(request)).isEmpty();
    verify(ethScheduler).scheduleComputationTask(any());

    scheduled.forEach(Runnable::run);

    assertThat(result).isCompletedWithValue(task);
    // the first ten accounts were read, so the rest of the range is left to a follow-up request
    assertThat(childRequests(request))
        .singleElement()
        .isInstanceOf(AccountFlatDatabaseHealingRangeRequest.class);
  }

  @Test
  public void shouldCheckLocalFlatStorageOnTheComputationThreads() {
    final Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
    final Bytes32 storageRoot =
        Bytes32.wrap(
            PmtStateTrieAccountValue.readFrom(
                    RLP.input(worldStateStorage.getAccount(accountHash).orElseThrow()))
                .getStorageRoot()
                .getBytes());
    final StorageFlatDatabaseHealingRangeRequest request =
        SnapDataRequest.createStorageFlatHealingRangeRequest(
            stateRoot,
            Bytes32.wrap(accountHash.getBytes()),
            storageRoot,
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);
    final Task<SnapDataRequest> task = new StubTask(request);

    final CompletableFuture<Task<SnapDataRequest>> result =
        requestDataStep.requestLocalFlatStorages(task);

    assertThat(result).isNotDone();
    verify(ethScheduler).scheduleComputationTask(any());

    scheduled.forEach(Runnable::run);

    assertThat(result).isCompletedWithValue(task);
    // two of the three slots were read, so the rest of the range is left to follow-up requests
    assertThat(childRequests(request))
        .isNotEmpty()
        .allSatisfy(
            child -> assertThat(child).isInstanceOf(StorageFlatDatabaseHealingRangeRequest.class));
  }

  private List<SnapDataRequest> childRequests(final SnapDataRequest request) {
    return request
        .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
        .toList();
  }
}
