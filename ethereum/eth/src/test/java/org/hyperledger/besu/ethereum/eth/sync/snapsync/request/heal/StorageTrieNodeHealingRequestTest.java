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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.UndoLog;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StorageTrieNodeHealingRequestTest {

  final List<Address> accounts =
      List.of(
          Address.fromHexString("0xdeadbeef"),
          Address.fromHexString("0xdeadbeee"),
          Address.fromHexString("0xdeadbeea"),
          Address.fromHexString("0xdeadbeeb"));

  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private Hash account0Hash;
  private Hash account0StorageRoot;

  static class StorageFormatArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageFormat.BONSAI), Arguments.of(DataStorageFormat.FOREST));
    }
  }

  public void setup(final DataStorageFormat storageFormat) {
    if (storageFormat.equals(DataStorageFormat.FOREST)) {
      worldStateStorageCoordinator =
          new WorldStateStorageCoordinator(
              new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage()));
    } else {
      final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();
      worldStateStorageCoordinator =
          new WorldStateStorageCoordinator(
              new BonsaiWorldStateKeyValueStorage(
                  storageProvider,
                  new NoOpMetricsSystem(),
                  DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
    }
    final MerkleTrie<Bytes, Bytes> trie =
        TrieGenerator.generateTrie(
            worldStateStorageCoordinator,
            accounts.stream().map(Address::addressHash).collect(Collectors.toList()));

    account0Hash = accounts.get(0).addressHash();
    account0StorageRoot =
        trie.get(account0Hash.getBytes())
            .map(RLP::input)
            .map(PatriciaTrieAccountValue::readFrom)
            .map(PatriciaTrieAccountValue::getStorageRoot)
            .orElseThrow();
  }

  @ParameterizedTest
  @ArgumentsSource(StorageFormatArguments.class)
  void shouldDetectExistingData(final DataStorageFormat storageFormat) {
    setup(storageFormat);

    final StorageTrieNodeHealingRequest request =
        new StorageTrieNodeHealingRequest(
            account0StorageRoot, account0Hash, Hash.EMPTY, Bytes.EMPTY);

    Assertions.assertThat(request.getExistingData(worldStateStorageCoordinator)).isPresent();
  }

  @ParameterizedTest
  @ArgumentsSource(StorageFormatArguments.class)
  void shouldDetectMissingData(final DataStorageFormat storageFormat) {
    setup(storageFormat);
    final StorageTrieNodeHealingRequest request =
        new StorageTrieNodeHealingRequest(Hash.EMPTY, account0Hash, Hash.EMPTY, Bytes.EMPTY);

    Assertions.assertThat(request.getExistingData(worldStateStorageCoordinator)).isEmpty();
  }

  @Test
  void shouldNotStoreAClearedParentWhenItsLastChildIsPersisted() {
    final ForestWorldStateKeyValueStorage storage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator coordinator = new WorldStateStorageCoordinator(storage);
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.ofNullable(nodes.get(hash)),
            Function.identity(),
            Function.identity());
    // 40-byte values keep both leaves large enough to be stored by their hash
    final Bytes value = Bytes.wrap(new byte[40]);
    trie.put(Bytes32.fromHexString("0x11" + "00".repeat(31)), value);
    trie.put(Bytes32.fromHexString("0x20" + "00".repeat(31)), value);
    trie.commit((location, hash, node) -> nodes.put(hash, node));

    final StorageTrieNodeHealingRequest parent =
        new StorageTrieNodeHealingRequest(
            Hash.wrap(trie.getRootHash()), accounts.get(0).addressHash(), Hash.EMPTY, Bytes.EMPTY);
    parent.setData(nodes.get(trie.getRootHash()));
    final SnapRequestContext downloadState = mock(SnapRequestContext.class);
    final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
    final SnapSyncConfiguration snapSyncConfiguration = mock(SnapSyncConfiguration.class);
    final List<TrieNodeHealingRequest> children =
        parent
            .getChildRequests(downloadState, coordinator, snapSyncState)
            .map(TrieNodeHealingRequest.class::cast)
            .toList();
    // a failed write clears the parent for a retry after its children were queued
    parent.clear();

    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    for (final TrieNodeHealingRequest child : children) {
      child.setData(nodes.get(child.getNodeHash()));
      child.persist(
          coordinator, updater, downloadState, snapSyncState, snapSyncConfiguration, new UndoLog());
    }
    updater.commit();

    Assertions.assertThat(children).hasSize(2);
    Assertions.assertThat(storage.getAccountStorageTrieNode(parent.getNodeHash())).isEmpty();
    for (final TrieNodeHealingRequest child : children) {
      Assertions.assertThat(storage.getAccountStorageTrieNode(child.getNodeHash())).isPresent();
    }
  }
}
