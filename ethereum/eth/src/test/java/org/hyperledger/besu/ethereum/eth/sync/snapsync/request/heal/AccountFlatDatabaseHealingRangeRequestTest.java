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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AccountFlatDatabaseHealingRangeRequestTest {

  @Mock private SnapWorldDownloadState downloadState;
  @Mock private SnapSyncProcessState snapSyncState;

  @BeforeEach
  public void setup() {
    Mockito.when(downloadState.getMetricsManager())
        .thenReturn(Mockito.mock(SnapSyncMetricsManager.class));
    Mockito.when(downloadState.getAccountsHealingList()).thenReturn(new HashSet<>());
  }

  @Test
  public void shouldReturnChildRequests() {
    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    final WorldStateProofProvider proofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);
    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    // Create a collector to gather account entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, 10, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the account entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), Bytes32.ZERO);
    proofs.addAll(
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), accounts.lastKey()));

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final AccountFlatDatabaseHealingRangeRequest request =
        new AccountFlatDatabaseHealingRangeRequest(
            Hash.EMPTY, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    // Add local data to the request, including the proof provider, accounts TreeMap, and proofs as
    // an ArrayDeque
    request.addLocalData(proofProvider, accounts, new ArrayDeque<>(proofs));

    // Verify that the start key hash of the snapDataRequest is greater than the last key in the
    // accounts TreeMap
    List<SnapDataRequest> childRequests =
        request
            .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
            .toList();
    Assertions.assertThat(childRequests).hasSize(1);
    AccountFlatDatabaseHealingRangeRequest snapDataRequest =
        (AccountFlatDatabaseHealingRangeRequest) childRequests.get(0);
    Assertions.assertThat(snapDataRequest.getStartKeyHash()).isGreaterThan(accounts.lastKey());

    // Verify that we have storage healing request when the account need to be repaired
    Mockito.when(downloadState.getAccountsHealingList())
        .thenReturn(
            new HashSet<>(
                accounts.keySet().stream()
                    .map(CompactEncoding::bytesToPath)
                    .collect(Collectors.toList())));
    childRequests =
        request
            .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
            .toList();
    Assertions.assertThat(childRequests).hasSizeGreaterThan(1);
    Assertions.assertThat(childRequests)
        .hasAtLeastOneElementOfType(AccountFlatDatabaseHealingRangeRequest.class);
    Assertions.assertThat(childRequests)
        .hasAtLeastOneElementOfType(StorageFlatDatabaseHealingRangeRequest.class);
  }

  @Test
  public void shouldNotReturnChildRequestsWhenNoMoreAccounts() {
    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    final WorldStateProofProvider proofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);
    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);

    // Create a collector to gather account entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, 15, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the account entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Create a request for healing the flat database with no more accounts
    final AccountFlatDatabaseHealingRangeRequest request =
        new AccountFlatDatabaseHealingRangeRequest(
            Hash.EMPTY, accounts.lastKey(), RangeManager.MAX_RANGE);

    // Add local data to the request
    request.addLocalData(proofProvider, new TreeMap<>(), new ArrayDeque<>());

    // Verify that no child requests are returned from the request
    final Stream<SnapDataRequest> childRequests =
        request.getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState);
    Assertions.assertThat(childRequests).isEmpty();
  }

  @Test
  public void doNotPersistWhenProofIsValid() {

    final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();

    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider,
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    final WorldStateProofProvider proofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);
    // Create a collector to gather account entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, 10, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the account entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), Bytes32.ZERO);
    proofs.addAll(
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), accounts.lastKey()));

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final AccountFlatDatabaseHealingRangeRequest request =
        new AccountFlatDatabaseHealingRangeRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);
    // Add local data to the request, including the proof provider, accounts TreeMap, and proofs as
    // an ArrayDeque
    request.addLocalData(proofProvider, accounts, new ArrayDeque<>(proofs));

    WorldStateKeyValueStorage.Updater updater = Mockito.spy(worldStateKeyValueStorage.updater());
    request.doPersist(
        worldStateStorageCoordinator,
        updater,
        downloadState,
        snapSyncState,
        SnapSyncConfiguration.getDefault());
    Mockito.verifyNoInteractions(updater);
  }

  @Test
  public void doHealAndPersistWhenProofIsInvalid() {

    final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();

    final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider,
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    final WorldStateProofProvider proofProvider =
        new WorldStateProofProvider(worldStateStorageCoordinator);

    final MerkleTrie<Bytes, Bytes> accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, 15);
    // Create a collector to gather account entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, 15, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the account entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> accounts =
        (TreeMap<Bytes32, Bytes>)
            accountStateTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), Bytes32.ZERO);
    proofs.addAll(
        proofProvider.getAccountProofRelatedNodes(
            Hash.wrap(accountStateTrie.getRootHash()), accounts.lastKey()));

    // Remove an account in the middle of the range
    final Iterator<Map.Entry<Bytes32, Bytes>> iterator = accounts.entrySet().iterator();
    Map.Entry<Bytes32, Bytes> removedAccount = null;
    int i = 0;
    while (iterator.hasNext()) {
      if (i == 7) {
        removedAccount = Map.Entry.copyOf(iterator.next());
        iterator.remove();
      } else {
        iterator.next();
      }
      i++;
    }

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final AccountFlatDatabaseHealingRangeRequest request =
        new AccountFlatDatabaseHealingRangeRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);
    // Add local data to the request, including the proof provider, accounts TreeMap, and proofs as
    // an ArrayDeque
    request.addLocalData(proofProvider, accounts, new ArrayDeque<>(proofs));

    BonsaiWorldStateKeyValueStorage.Updater updater =
        Mockito.spy(worldStateKeyValueStorage.updater());
    request.doPersist(
        worldStateStorageCoordinator,
        updater,
        downloadState,
        snapSyncState,
        SnapSyncConfiguration.getDefault());
    // check add the missing account to the updater
    Mockito.verify(updater)
        .putAccountInfoState(Hash.wrap(removedAccount.getKey()), removedAccount.getValue());
  }
}
