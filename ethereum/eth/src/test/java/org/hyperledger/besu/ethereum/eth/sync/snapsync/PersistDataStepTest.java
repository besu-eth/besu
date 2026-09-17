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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.Task;

import java.util.Collections;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PersistDataStepTest {

  private final WorldStateKeyValueStorage worldStateKeyValueStorage =
      spy(
          new InMemoryKeyValueStorageProvider()
              .createWorldStateStorage(DataStorageConfiguration.DEFAULT_CONFIG));
  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      new WorldStateStorageCoordinator(worldStateKeyValueStorage);

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

    final Bytes stateTrieNode =
        Bytes.fromHexString(
            "0xe2a0310e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf602");
    final Hash hash = Hash.hash(stateTrieNode);
    final Bytes location = Bytes.of(0x02);
    final AccountTrieNodeHealingRequest accountTrieNodeDataRequest =
        SnapDataRequest.createAccountTrieNodeDataRequest(hash, location, Collections.emptySet());
    accountTrieNodeDataRequest.setData(stateTrieNode);

    final BonsaiWorldStateKeyValueStorage.Updater updater =
        (BonsaiWorldStateKeyValueStorage.Updater) spy(worldStateKeyValueStorage.updater());
    when(worldStateKeyValueStorage.updater())
        .thenReturn(updater)
        .thenReturn(mock(BonsaiWorldStateKeyValueStorage.Updater.class));

    List<Task<SnapDataRequest>> result =
        persistDataStep.persist(List.of(new StubTask(accountTrieNodeDataRequest)));

    persistDataStep.persist(List.of(new StubTask(accountTrieNodeDataRequest)));

    verify(updater, times(1))
        .putAccountStateTrieNode(location, Bytes32.wrap(hash.getBytes()), stateTrieNode);
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
