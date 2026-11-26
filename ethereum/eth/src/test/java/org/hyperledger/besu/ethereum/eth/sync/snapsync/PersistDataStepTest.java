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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.TrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.Task;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class PersistDataStepTest {

  private final WorldStateKeyValueStorage worldStateKeyValueStorage =
      spy(
          new InMemoryKeyValueStorageProvider()
              .createWorldStateStorage(DataStorageConfiguration.DEFAULT_CONFIG));
  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      spy(new WorldStateStorageCoordinator(worldStateKeyValueStorage));

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final SnapWorldDownloadState downloadState = mock(SnapWorldDownloadState.class);

  private final SnapSyncConfiguration snapSyncConfiguration = mock(SnapSyncConfiguration.class);

  private final PersistDataStep persistDataStep =
      new PersistDataStep(
          snapSyncState, worldStateStorageCoordinator, downloadState, snapSyncConfiguration);

  @BeforeEach
  public void setUp() {
    when(downloadState.getMetricsManager()).thenReturn(mock(SnapSyncMetricsManager.class));
  }

  @Test
  public void shouldPersistDataWhenPresent() {
    final List<Task<SnapDataRequest>> tasks = TaskGenerator.createAccountRequest(true, false);
    final List<Task<SnapDataRequest>> result = persistDataStep.persist(tasks);

    assertThat(result).isSameAs(tasks);

    assertDataPersisted(tasks);
  }

  @Test
  public void shouldPersistTrieNodeHealDataOnlyOnce() {

    // a leaf holding an account without storage or code
    final Bytes stateTrieNode =
        RLP.encode(
            out -> {
              out.startList();
              out.writeBytes(
                  Bytes.fromHexString(
                      "0x310e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6"));
              out.writeBytes(
                  RLP.encode(
                      new PatriciaTrieAccountValue(0, Wei.ZERO, Hash.EMPTY_TRIE_HASH, Hash.EMPTY)
                          ::writeTo));
              out.endList();
            });
    final Hash hash = Hash.hash(stateTrieNode);
    final Bytes location = Bytes.of(0x02);
    final AccountTrieNodeHealingRequest accountTrieNodeDataRequest =
        SnapDataRequest.createAccountTrieNodeDataRequest(hash, location, Collections.emptySet());
    accountTrieNodeDataRequest.setData(stateTrieNode);

    final BonsaiWorldStateKeyValueStorage.Updater updater =
        (BonsaiWorldStateKeyValueStorage.Updater) spy(worldStateKeyValueStorage.updater());
    final BonsaiWorldStateKeyValueStorage.Updater secondUpdater =
        mock(BonsaiWorldStateKeyValueStorage.Updater.class);
    doReturn(updater).doReturn(secondUpdater).when(worldStateStorageCoordinator).batchUpdater();

    List<Task<SnapDataRequest>> result =
        persistDataStep.persist(List.of(new StubTask(accountTrieNodeDataRequest)));

    persistDataStep.persist(List.of(new StubTask(accountTrieNodeDataRequest)));

    verify(updater, times(1)).putTrieNode(location, Bytes32.wrap(hash.getBytes()), stateTrieNode);
    verify(secondUpdater, never()).putTrieNode(any(), any(), any());
    assertDataPersisted(result);
  }

  @Test
  public void shouldSkipPersistDataWhenNoData() {
    final List<Task<SnapDataRequest>> tasks = TaskGenerator.createAccountRequest(false, false);
    final List<Task<SnapDataRequest>> result = persistDataStep.persist(tasks);

    assertThat(result).isSameAs(tasks);
    assertThat(
            worldStateStorageCoordinator
                .getStrategy(BonsaiWorldStateKeyValueStorage.class)
                .getTrieNodeUnsafe(tasks.get(0).getData().getRootHash().getBytes()))
        .isEmpty();
  }

  @Test
  public void shouldHandleNullTaskElementInTrie() {
    // Create a StorageRangeDataRequest where taskElement might be null or incomplete
    List<Task<SnapDataRequest>> tasks = TaskGenerator.createAccountRequest(false, true);

    try {
      List<Task<SnapDataRequest>> result = persistDataStep.persist(tasks);

      // check for proper handling of null taskElement
      assertThat(result).isSameAs(tasks);
      assertThat(result)
          .isNotNull(); // Make sure the result isn't null even with the bad taskElement
    } catch (NullPointerException e) {
      fail(
          "NullPointerException occurred during persist step, taskElement might be null: "
              + e.getMessage());
    }
  }

  @Test
  public void shouldStoreANodeAndItsParentWhenTheirFailedBatchIsRetried() {
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final TrieNodeHealingRequest parent = storageTrieRootRequest(nodes, "0x10", "0x20");
    final List<TrieNodeHealingRequest> leaves = childRequests(parent, nodes);
    persistDataStep.persist(List.of(new StubTask(leaves.get(0))));

    // the last leaf completes the parent in a batch whose commit fails
    final TrieNodeHealingRequest lastLeaf = leaves.get(1);
    failNextCommit(() -> {});
    final List<Task<SnapDataRequest>> failed =
        persistDataStep.persist(List.of(new StubTask(lastLeaf)));
    assertThat(failed.get(0).getData().isResponseReceived()).isFalse();
    assertStored(lastLeaf, false);
    assertStored(parent, false);

    lastLeaf.setData(nodes.get(lastLeaf.getNodeHash()));
    persistDataStep.persist(List.of(new StubTask(lastLeaf)));

    assertStored(lastLeaf, true);
    assertStored(parent, true);
  }

  @Test
  public void shouldEnqueueChildRequestsOnlyWhenTheirParentsBatchIsCommitted() {
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final TrieNodeHealingRequest parent = storageTrieRootRequest(nodes, "0x10", "0x20");

    failNextCommit(() -> {});
    persistDataStep.persist(List.of(new StubTask(parent)));
    verify(downloadState, never()).enqueueRequests(any());

    parent.setData(nodes.get(parent.getNodeHash()));
    persistDataStep.persist(List.of(new StubTask(parent)));
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<Stream<SnapDataRequest>> enqueued = ArgumentCaptor.forClass(Stream.class);
    verify(downloadState).enqueueRequests(enqueued.capture());
    final List<TrieNodeHealingRequest> children =
        enqueued.getValue().map(TrieNodeHealingRequest.class::cast).toList();
    assertThat(children).hasSize(2);

    for (final TrieNodeHealingRequest child : children) {
      child.setData(nodes.get(child.getNodeHash()));
      persistDataStep.persist(List.of(new StubTask(child)));
    }
    assertStored(parent, true);
  }

  @Test
  public void shouldCountANodeOnceInItsParentWhenAnotherBatchStoredItBeforeTheRetry() {
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final TrieNodeHealingRequest root = storageTrieRootRequest(nodes, "0x11", "0x12", "0x20");
    final List<TrieNodeHealingRequest> rootChildren = childRequests(root, nodes);
    final TrieNodeHealingRequest branch = atLocation(rootChildren, Bytes.of(1));
    final TrieNodeHealingRequest otherLeaf = atLocation(rootChildren, Bytes.of(2));
    final List<TrieNodeHealingRequest> leaves = childRequests(branch, nodes);

    // while the first leaf's batch commits, the second leaf's batch completes the branch, then the
    // first leaf's commit fails
    failNextCommit(() -> persistDataStep.persist(List.of(new StubTask(leaves.get(1)))));
    persistDataStep.persist(List.of(new StubTask(leaves.get(0))));
    assertStored(branch, true);

    leaves.get(0).setData(nodes.get(leaves.get(0).getNodeHash()));
    persistDataStep.persist(List.of(new StubTask(leaves.get(0))));
    assertStored(leaves.get(0), true);
    assertStored(root, false);

    persistDataStep.persist(List.of(new StubTask(otherLeaf)));
    assertStored(root, true);
  }

  private static final Hash STORAGE_ACCOUNT_HASH = Hash.hash(Bytes.of(1));

  /** Builds a storage trie with a leaf under each key prefix and returns a request for its root. */
  private static TrieNodeHealingRequest storageTrieRootRequest(
      final Map<Bytes32, Bytes> nodes, final String... keyPrefixes) {
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.ofNullable(nodes.get(hash)),
            Function.identity(),
            Function.identity());
    // a 32-byte slot value keeps each leaf large enough to be stored by its hash
    final Bytes value = RLP.encodeOne(Bytes.repeat((byte) 1, 32));
    for (final String keyPrefix : keyPrefixes) {
      trie.put(Bytes32.fromHexString(keyPrefix + "00".repeat(31)), value);
    }
    trie.commit((location, hash, node) -> nodes.put(hash, node));
    final TrieNodeHealingRequest request =
        SnapDataRequest.createStorageTrieNodeDataRequest(
            Hash.wrap(trie.getRootHash()), STORAGE_ACCOUNT_HASH, Hash.EMPTY, Bytes.EMPTY);
    request.setData(nodes.get(trie.getRootHash()));
    return request;
  }

  private List<TrieNodeHealingRequest> childRequests(
      final TrieNodeHealingRequest parent, final Map<Bytes32, Bytes> nodes) {
    final List<TrieNodeHealingRequest> children =
        parent
            .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
            .map(TrieNodeHealingRequest.class::cast)
            .toList();
    children.forEach(child -> child.setData(nodes.get(child.getNodeHash())));
    return children;
  }

  private static TrieNodeHealingRequest atLocation(
      final List<TrieNodeHealingRequest> requests, final Bytes location) {
    return requests.stream()
        .filter(request -> request.getLocation().equals(location))
        .findFirst()
        .orElseThrow();
  }

  /** Makes the next batch fail its commit with a retryable error after running the action. */
  private void failNextCommit(final Runnable whileCommitting) {
    final WorldStateKeyValueStorage.Updater updater = spy(worldStateKeyValueStorage.updater());
    doAnswer(
            invocation -> {
              whileCommitting.run();
              throw new StorageException(
                  new RocksDBException(
                      "Busy", new Status(Status.Code.Busy, Status.SubCode.None, null)));
            })
        .when(updater)
        .commit();
    doReturn(updater).doCallRealMethod().when(worldStateStorageCoordinator).batchUpdater();
  }

  private void assertStored(final TrieNodeHealingRequest request, final boolean stored) {
    assertThat(
            worldStateStorageCoordinator
                .getAccountStorageTrieNode(
                    STORAGE_ACCOUNT_HASH, request.getLocation(), request.getNodeHash())
                .isPresent())
        .as("node at %s stored", request.getLocation())
        .isEqualTo(stored);
  }

  private void assertDataPersisted(final List<Task<SnapDataRequest>> tasks) {
    tasks.forEach(
        task -> {
          if (task.getData() instanceof AccountRangeDataRequest) {
            final AccountRangeDataRequest data = (AccountRangeDataRequest) task.getData();
            StoredMerklePatriciaTrie<Bytes, Bytes> trie =
                new StoredMerklePatriciaTrie<>(
                    worldStateStorageCoordinator::getAccountStateTrieNode,
                    Bytes32.wrap(data.getRootHash().getBytes()),
                    b -> b,
                    b -> b);
            data.getAccounts().forEach((key, value) -> assertThat(trie.get(key)).isPresent());
          } else if (task.getData() instanceof StorageRangeDataRequest) {
            final StorageRangeDataRequest data = (StorageRangeDataRequest) task.getData();
            final StoredMerklePatriciaTrie<Bytes, Bytes> trie =
                new StoredMerklePatriciaTrie<>(
                    (location, hash) ->
                        worldStateStorageCoordinator.getAccountStorageTrieNode(
                            data.getAccountHash(), location, hash),
                    data.getStorageRoot(),
                    b -> b,
                    b -> b);
            data.getSlots().forEach((key, value) -> assertThat(trie.get(key)).isPresent());
          } else if (task.getData() instanceof BytecodeRequest) {
            final BytecodeRequest data = (BytecodeRequest) task.getData();
            assertThat(
                    worldStateStorageCoordinator
                        .getStrategy(BonsaiWorldStateKeyValueStorage.class)
                        .getCode(Hash.wrap(data.getCodeHash()), Hash.wrap(data.getAccountHash())))
                .isPresent();
          } else if (task.getData() instanceof AccountTrieNodeHealingRequest) {
            final AccountTrieNodeHealingRequest data =
                (AccountTrieNodeHealingRequest) task.getData();
            assertThat(
                    worldStateStorageCoordinator
                        .getStrategy(BonsaiWorldStateKeyValueStorage.class)
                        .getTrieNodeUnsafe(data.getLocation()))
                .isPresent();
          } else {
            fail("not expected message");
          }
        });
  }
}
