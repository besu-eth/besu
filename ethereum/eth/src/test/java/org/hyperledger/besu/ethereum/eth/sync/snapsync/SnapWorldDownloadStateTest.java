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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloadProcess;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.testutil.TestClock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

@SuppressWarnings("unchecked")
public class SnapWorldDownloadStateTest {

  private static final Bytes ROOT_NODE_DATA = Bytes.of(1, 2, 3, 4);
  private static final Hash ROOT_NODE_HASH = Hash.hash(ROOT_NODE_DATA);
  private static final int MAX_REQUESTS_WITHOUT_PROGRESS = 10;
  private static final long MIN_MILLIS_BEFORE_STALLING = 50_000;

  private WorldStateKeyValueStorage worldStateKeyValueStorage;

  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final BlockHeader header =
      new BlockHeaderTestFixture().stateRoot(ROOT_NODE_HASH).buildHeader();
  private final InMemoryTasksPriorityQueues<SnapDataRequest> pendingRequests =
      new InMemoryTasksPriorityQueues<>();
  private final WorldStateDownloadProcess worldStateDownloadProcess =
      mock(WorldStateDownloadProcess.class);
  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final SnapSyncStatePersistenceManager snapContext =
      mock(SnapSyncStatePersistenceManager.class);
  private final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final DynamicPivotBlockSelector dynamicPivotBlockManager =
      mock(DynamicPivotBlockSelector.class);
  private final EthContext ethContext = mock(EthContext.class);

  private final TestClock clock = new TestClock();
  private SnapWorldDownloadState downloadState;

  private CompletableFuture<Void> future;

  static class SnapWorldDownloadStateTestArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageFormat.BONSAI, true),
          Arguments.of(DataStorageFormat.BONSAI, false),
          Arguments.of(DataStorageFormat.FOREST, false));
    }
  }

  public void setUp(final DataStorageFormat storageFormat) {

    when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());

    if (storageFormat == DataStorageFormat.BONSAI) {
      worldStateKeyValueStorage =
          new BonsaiWorldStateKeyValueStorage(
              new InMemoryKeyValueStorageProvider(),
              new NoOpMetricsSystem(),
              DataStorageConfiguration.DEFAULT_BONSAI_PARTIAL_DB_CONFIG);
    } else {
      worldStateKeyValueStorage =
          new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    }
    worldStateStorageCoordinator = new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    downloadState =
        new SnapWorldDownloadState(
            worldStateStorageCoordinator,
            snapContext,
            blockchain,
            snapSyncState,
            pendingRequests,
            MAX_REQUESTS_WITHOUT_PROGRESS,
            MIN_MILLIS_BEFORE_STALLING,
            metricsManager,
            clock,
            ethContext,
            SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    final DynamicPivotBlockSelector dynamicPivotBlockManager =
        mock(DynamicPivotBlockSelector.class);
    doAnswer(
            invocation -> {
              BiConsumer<BlockHeader, Boolean> callback = invocation.getArgument(0);
              callback.accept(header, false);
              return null;
            })
        .when(dynamicPivotBlockManager)
        .switchToNewPivotBlock(any());
    downloadState.setPivotBlockSelector(dynamicPivotBlockManager);
    downloadState.setRootNodeData(ROOT_NODE_DATA);
    future = downloadState.getDownloadFuture();
    assertThat(downloadState.isDownloading()).isTrue();

    final EthScheduler ethScheduler = mock(EthScheduler.class);
    when(ethContext.getScheduler()).thenReturn(ethScheduler);
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(ethScheduler)
        .executeServiceTask(any(Runnable.class));
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldCompleteReturnedFutureWhenNoPendingTasksRemain(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    when(snapSyncState.isHealFlatDatabaseInProgress()).thenReturn(true);
    downloadState.checkCompletion(header);

    assertThat(future).isCompleted();
    assertThat(downloadState.isDownloading()).isFalse();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldStartHealWhenNoSnapsyncPendingTasksRemain(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(false);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(mock(BlockHeader.class)));
    assertThat(downloadState.pendingTrieNodeRequests.isEmpty()).isTrue();

    downloadState.checkCompletion(header);
    assertThat(downloadState.isDownloading()).isTrue();
    assertThat(downloadState.pendingTrieNodeRequests.isEmpty()).isFalse();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldStoreRootNodeBeforeReturnedFutureCompletes(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    when(snapSyncState.isHealFlatDatabaseInProgress()).thenReturn(true);
    final CompletableFuture<Void> postFutureChecks =
        future.thenAccept(
            result ->
                assertThat(
                        worldStateStorageCoordinator.getAccountStateTrieNode(
                            Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
                    .contains(ROOT_NODE_DATA));

    downloadState.checkCompletion(header);

    assertThat(future).isCompleted();
    assertThat(postFutureChecks).isCompleted();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldNotCompleteWhenThereAreAccountPendingTasks(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(false);
    downloadState.pendingAccountRequests.add(
        SnapDataRequest.createAccountDataRequest(
            Hash.EMPTY_TRIE_HASH,
            Hash.wrap(Bytes32.random()),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE));

    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldNotCompleteWhenThereAreStoragePendingTasks(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(false);
    downloadState.pendingStorageRequests.add(
        SnapDataRequest.createStorageTrieNodeDataRequest(
            Hash.EMPTY_TRIE_HASH, Hash.wrap(Bytes32.random()), Hash.EMPTY_TRIE_HASH, Bytes.EMPTY));

    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();

    downloadState.pendingLargeStorageRequests.add(
        SnapDataRequest.createStorageTrieNodeDataRequest(
            Hash.EMPTY_TRIE_HASH, Hash.wrap(Bytes32.random()), Hash.EMPTY_TRIE_HASH, Bytes.EMPTY));

    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldNotCompleteWhenThereAreTriePendingTasks(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    downloadState.pendingTrieNodeRequests.add(
        SnapDataRequest.createAccountTrieNodeDataRequest(
            Hash.wrap(Bytes32.random()), Bytes.EMPTY, new HashSet<>()));

    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldNotCompleteWhenThereAreFlatDBHealingPendingTasks(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    when(snapSyncState.isHealFlatDatabaseInProgress()).thenReturn(true);
    downloadState.pendingAccountFlatDatabaseHealingRequests.add(
        SnapDataRequest.createAccountFlatHealingRangeRequest(
            Hash.wrap(Bytes32.random()), Bytes32.ZERO, Bytes32.ZERO));

    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldCancelOutstandingTasksWhenFutureIsCancelled(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    final EthTask<?> outstandingTask1 = mock(EthTask.class);
    final EthTask<?> outstandingTask2 = mock(EthTask.class);
    downloadState.addOutstandingTask(outstandingTask1);
    downloadState.addOutstandingTask(outstandingTask2);

    downloadState.pendingAccountRequests.add(
        SnapDataRequest.createAccountDataRequest(
            Hash.EMPTY_TRIE_HASH,
            Hash.wrap(Bytes32.random()),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE));
    downloadState.pendingStorageRequests.add(
        SnapDataRequest.createStorageTrieNodeDataRequest(
            Hash.EMPTY_TRIE_HASH, Hash.wrap(Bytes32.random()), Hash.EMPTY_TRIE_HASH, Bytes.EMPTY));
    downloadState.setWorldStateDownloadProcess(worldStateDownloadProcess);

    future.cancel(true);

    verify(outstandingTask1).cancel();
    verify(outstandingTask2).cancel();

    assertThat(downloadState.pendingAccountRequests.isEmpty()).isTrue();
    assertThat(downloadState.pendingStorageRequests.isEmpty()).isTrue();
    verify(worldStateDownloadProcess).abort();
    assertThat(downloadState.isDownloading()).isFalse();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldRestartHealWhenNewPivotBlock(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(mock(BlockHeader.class)));
    when(snapSyncState.isHealTrieInProgress()).thenReturn(false);
    assertThat(downloadState.pendingTrieNodeRequests.isEmpty()).isTrue();
    // start heal
    downloadState.checkCompletion(header);
    verify(snapSyncState).setHealTrieStatus(true);
    assertThat(downloadState.pendingTrieNodeRequests.isEmpty()).isFalse();
    // add useless requests
    downloadState.pendingTrieNodeRequests.add(
        BytecodeRequest.createAccountTrieNodeDataRequest(Hash.EMPTY, Bytes.EMPTY, new HashSet<>()));
    downloadState.pendingCodeRequests.add(
        BytecodeRequest.createBytecodeRequest(Bytes32.ZERO, Hash.EMPTY, Bytes32.ZERO));
    // reload the heal
    downloadState.reloadTrieHeal();
    verify(snapSyncState).setHealTrieStatus(false);
    assertThat(downloadState.pendingTrieNodeRequests.size()).isEqualTo(1);
    assertThat(downloadState.pendingCodeRequests.isEmpty()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldListeningBlockchainDuringHeal(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    when(snapSyncState.isHealTrieInProgress()).thenReturn(false);

    downloadState.setPivotBlockSelector(dynamicPivotBlockManager);

    downloadState.checkCompletion(header);

    downloadState.checkCompletion(header);

    // should register only one time
    verify(blockchain, times(1)).observeBlockAdded(any());
    verify(snapSyncState, atLeastOnce()).setHealTrieStatus(true);

    assertThat(future).isNotDone();
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldStopWaitingBlockchainWhenNewPivotBlockAvailable(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);

    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);

    downloadState.setPivotBlockSelector(dynamicPivotBlockManager);
    when(dynamicPivotBlockManager.isBlockchainBehind()).thenReturn(true);

    downloadState.checkCompletion(header);

    verify(snapSyncState).setWaitingBlockchain(true);
    when(snapSyncState.isWaitingBlockchain()).thenReturn(true);

    final BlockHeaderTestFixture blockHeaderTestFixture = new BlockHeaderTestFixture();
    final BlockHeader newPivotBlock = blockHeaderTestFixture.number(550L).buildHeader();
    doAnswer(
            invocation -> {
              BiConsumer<BlockHeader, Boolean> callback =
                  invocation.getArgument(0, BiConsumer.class);
              callback.accept(newPivotBlock, true);
              return null;
            })
        .when(dynamicPivotBlockManager)
        .check(any());

    final Block newBlock =
        new Block(
            new BlockHeaderTestFixture().number(500).buildHeader(),
            new BlockBody(emptyList(), emptyList()));

    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(newBlock.getHeader()));

    final BlockAddedObserver blockAddedListener = downloadState.createBlockchainObserver();
    blockAddedListener.onBlockAdded(
        BlockAddedEvent.createForHeadAdvancement(
            new Block(
                new BlockHeaderTestFixture().number(500).buildHeader(),
                new BlockBody(emptyList(), emptyList())),
            Collections.emptyList(),
            Collections.emptyList()));

    // reload heal
    verify(snapSyncState).setWaitingBlockchain(false);
    verify(snapSyncState).setHealTrieStatus(false);
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldStopWaitingBlockchainWhenCloseToTheHead(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);

    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);

    downloadState.setPivotBlockSelector(dynamicPivotBlockManager);

    when(dynamicPivotBlockManager.isBlockchainBehind()).thenReturn(true);

    downloadState.checkCompletion(header);

    verify(snapSyncState).setWaitingBlockchain(true);
    when(snapSyncState.isWaitingBlockchain()).thenReturn(true);

    final Block newBlock =
        new Block(
            new BlockHeaderTestFixture().number(500).buildHeader(),
            new BlockBody(emptyList(), emptyList()));

    when(dynamicPivotBlockManager.isBlockchainBehind()).thenReturn(false);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(newBlock.getHeader()));

    final BlockAddedObserver blockAddedListener = downloadState.createBlockchainObserver();
    blockAddedListener.onBlockAdded(
        BlockAddedEvent.createForHeadAdvancement(
            newBlock, Collections.emptyList(), Collections.emptyList()));

    verify(snapSyncState).setWaitingBlockchain(false);
    verify(snapSyncState).setHealTrieStatus(false);
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldCompleteReturnedFutureWhenNoPendingTasksRemainAndFlatDBHealNotNeeded(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    Assumptions.assumeTrue(
        storageFormat == DataStorageFormat.FOREST
            || (storageFormat == DataStorageFormat.BONSAI && !isFlatDbHealingEnabled));
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    downloadState.checkCompletion(header);

    assertThat(future).isCompleted();
    assertThat(downloadState.isDownloading()).isFalse();
  }

  @ParameterizedTest
  @ArgumentsSource(SnapWorldDownloadStateTestArguments.class)
  public void shouldNotCompleteReturnedFutureWhenNoPendingTasksRemainAndFlatDBHealNeeded(
      final DataStorageFormat storageFormat, final boolean isFlatDbHealingEnabled) {
    setUp(storageFormat);
    Assumptions.assumeTrue(storageFormat == DataStorageFormat.BONSAI);
    Assumptions.assumeTrue(isFlatDbHealingEnabled);
    ((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage).upgradeToFullFlatDbMode();
    when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
    downloadState.checkCompletion(header);

    assertThat(future).isNotDone();
    verify(snapSyncState).setHealFlatDatabaseInProgress(true);
    assertThat(
            worldStateStorageCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(ROOT_NODE_HASH.getBytes())))
        .isEmpty();
    assertThat(downloadState.isDownloading()).isTrue();
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }
}
