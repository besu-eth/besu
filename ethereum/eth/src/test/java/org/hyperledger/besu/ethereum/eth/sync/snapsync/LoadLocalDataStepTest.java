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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.noop.NoOpMetricsSystem.NO_OP_COUNTER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.UndoLog;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.TrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.tasks.Task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class LoadLocalDataStepTest {

  private static final Bytes DATA = Bytes.of(1, 2, 3);
  private static final Hash HASH = Hash.hash(DATA);

  private final BlockHeader blockHeader =
      new BlockHeaderTestFixture().stateRoot(HASH).buildHeader();
  private final AccountTrieNodeHealingRequest request =
      SnapDataRequest.createAccountTrieNodeDataRequest(
          HASH, Bytes.fromHexString("0x01"), new HashSet<>());
  private final Task<SnapDataRequest> task = new StubTask(request);

  private final Pipe<Task<SnapDataRequest>> completedTasks =
      new Pipe<>(10, NO_OP_COUNTER, NO_OP_COUNTER, NO_OP_COUNTER, "test_pipe");

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final SnapWorldDownloadState downloadState = mock(SnapWorldDownloadState.class);
  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      mock(WorldStateStorageCoordinator.class);
  private final WorldStateKeyValueStorage.Updater updater =
      mock(WorldStateKeyValueStorage.Updater.class);

  private final SnapSyncConfiguration snapSyncConfiguration = mock(SnapSyncConfiguration.class);

  private final LoadLocalDataStep loadLocalDataStep =
      new LoadLocalDataStep(
          worldStateStorageCoordinator,
          downloadState,
          snapSyncConfiguration,
          new NoOpMetricsSystem(),
          snapSyncState);

  @BeforeEach
  public void setup() {
    when(snapSyncState.hasPivotBlockHeader()).thenReturn(true);
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(blockHeader));
  }

  @Test
  public void shouldReturnStreamWithUnchangedTaskWhenDataNotPresent() {

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isNull();
    assertThat(output).containsExactly(task);
  }

  @Test
  public void shouldReturnStreamWithSameRootHashTaskWhenDataArePresent() {

    task.getData().setRootHash(blockHeader.getStateRoot());

    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.of(DATA));
    when(worldStateStorageCoordinator.batchUpdater())
        .thenReturn(mock(WorldStateKeyValueStorage.Updater.class));

    final BlockHeader newBlockHeader =
        new BlockHeaderTestFixture().stateRoot(Hash.EMPTY).buildHeader();
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(newBlockHeader));

    loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isEqualTo(task);
    assertThat(task.getData().getRootHash()).isEqualTo(blockHeader.getStateRoot());
  }

  @Test
  public void shouldReturnEmptyStreamAndSendTaskToCompletedPipeWhenDataIsPresent() {
    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.of(DATA));
    when(worldStateStorageCoordinator.batchUpdater()).thenReturn(updater);

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(completedTasks.poll()).isSameAs(task);
    assertThat(request.isResponseReceived()).isTrue();
    assertThat(output).isEmpty();

    verify(updater).commit();
    Mockito.reset(updater);

    // Should not require persisting.
    request.persist(
        worldStateStorageCoordinator,
        updater,
        downloadState,
        snapSyncState,
        snapSyncConfiguration,
        new UndoLog());
    verifyNoInteractions(updater);
  }

  @Test
  public void shouldDownloadAStoredNodeWhoseValueIsNotTheNode() {
    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.of(Bytes.EMPTY));
    when(worldStateStorageCoordinator.batchUpdater()).thenReturn(updater);

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(task, completedTasks);

    assertThat(output).containsExactly(task);
    assertThat(completedTasks.poll()).isNull();
    assertThat(request.isResponseReceived()).isFalse();
    verify(updater, never()).commit();
  }

  @Test
  public void shouldDownloadAStoredNodeWhenANodeBelowItIsMissing() {
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final Map<Bytes, Bytes32> hashByLocation = new HashMap<>();
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.ofNullable(nodes.get(hash)),
            Function.identity(),
            Function.identity());
    // 40-byte values keep every leaf large enough to be stored by its hash
    final Bytes value = Bytes.wrap(new byte[40]);
    final Bytes32 missingAccount = Bytes32.fromHexString("0x12" + "00".repeat(31));
    trie.put(Bytes32.fromHexString("0x11" + "00".repeat(31)), value);
    trie.put(missingAccount, value);
    trie.put(Bytes32.fromHexString("0x20" + "00".repeat(31)), value);
    trie.commit(
        (location, hash, node) -> {
          nodes.put(hash, node);
          hashByLocation.put(location, hash);
        });
    // the branch at 0x01 stays stored, but the leaf below it for 0x12... is lost
    nodes.remove(hashByLocation.get(Bytes.of(1, 2)));

    final AccountTrieNodeHealingRequest storedBranch =
        SnapDataRequest.createAccountTrieNodeDataRequest(
            Hash.wrap(hashByLocation.get(Bytes.of(1))),
            Bytes.of(1),
            new HashSet<>(Set.of(CompactEncoding.bytesToPath(missingAccount))));
    final Task<SnapDataRequest> storedBranchTask = new StubTask(storedBranch);
    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenAnswer(
            invocation -> Optional.ofNullable(nodes.get(invocation.<Bytes32>getArgument(1))));
    when(worldStateStorageCoordinator.batchUpdater()).thenReturn(updater);

    final Stream<Task<SnapDataRequest>> output =
        loadLocalDataStep.loadLocalDataTrieNode(storedBranchTask, completedTasks);

    assertThat(output).containsExactly(storedBranchTask);
    assertThat(completedTasks.poll()).isNull();
    assertThat(storedBranch.isResponseReceived()).isFalse();
    verify(updater, never()).commit();
  }

  @Test
  public void shouldCountAStoredNodeInItsParentAgainWhenItsCommitFails() {
    final ForestWorldStateKeyValueStorage storage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator coordinator = spy(new WorldStateStorageCoordinator(storage));
    final LoadLocalDataStep step =
        new LoadLocalDataStep(
            coordinator,
            downloadState,
            snapSyncConfiguration,
            new NoOpMetricsSystem(),
            snapSyncState);
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.ofNullable(nodes.get(hash)),
            Function.identity(),
            Function.identity());
    // 40-byte values keep both leaves large enough to be stored by their hash
    trie.put(Bytes32.fromHexString("0x10" + "00".repeat(31)), Bytes.wrap(new byte[40]));
    trie.put(Bytes32.fromHexString("0x20" + "00".repeat(31)), Bytes.wrap(new byte[40]));
    trie.commit((location, hash, node) -> nodes.put(hash, node));
    final TrieNodeHealingRequest parent =
        SnapDataRequest.createStorageTrieNodeDataRequest(
            Hash.wrap(trie.getRootHash()), HASH, HASH, Bytes.EMPTY);
    parent.setData(nodes.get(trie.getRootHash()));
    final List<TrieNodeHealingRequest> leaves =
        parent
            .getChildRequests(downloadState, coordinator, snapSyncState)
            .map(TrieNodeHealingRequest.class::cast)
            .toList();
    final TrieNodeHealingRequest downloadedLeaf = leaves.get(0);
    downloadedLeaf.setData(nodes.get(downloadedLeaf.getNodeHash()));
    final WorldStateKeyValueStorage.Updater leafUpdater = coordinator.batchUpdater();
    downloadedLeaf.persist(
        coordinator,
        leafUpdater,
        downloadState,
        snapSyncState,
        snapSyncConfiguration,
        new UndoLog());
    leafUpdater.commit();
    final TrieNodeHealingRequest storedLeaf = leaves.get(1);
    storage
        .updater()
        .putAccountStorageTrieNode(storedLeaf.getNodeHash(), nodes.get(storedLeaf.getNodeHash()))
        .commit();

    // the stored leaf completes the parent, but the commit that would store the parent fails
    final WorldStateKeyValueStorage.Updater failingUpdater = spy(storage.batchUpdater());
    doThrow(
            new StorageException(
                new RocksDBException(
                    "Busy", new Status(Status.Code.Busy, Status.SubCode.None, null))))
        .when(failingUpdater)
        .commit();
    doReturn(failingUpdater).doCallRealMethod().when(coordinator).batchUpdater();
    final Task<SnapDataRequest> storedLeafTask = new StubTask(storedLeaf);
    assertThat(step.loadLocalDataTrieNode(storedLeafTask, completedTasks))
        .containsExactly(storedLeafTask);
    assertThat(storage.getAccountStorageTrieNode(parent.getNodeHash())).isEmpty();

    assertThat(step.loadLocalDataTrieNode(storedLeafTask, completedTasks)).isEmpty();
    assertThat(storage.getAccountStorageTrieNode(parent.getNodeHash())).isPresent();
  }
}
