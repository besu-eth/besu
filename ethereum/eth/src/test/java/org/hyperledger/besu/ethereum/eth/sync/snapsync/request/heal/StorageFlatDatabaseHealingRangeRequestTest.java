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

import static org.apache.tuweni.rlp.RLP.decodeValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

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

@ExtendWith(MockitoExtension.class)
class StorageFlatDatabaseHealingRangeRequestTest {

  @Mock private SnapWorldDownloadState downloadState;
  @Mock private SnapSyncProcessState snapSyncState;

  final List<Address> accounts =
      List.of(
          Address.fromHexString("0xdeadbeef"),
          Address.fromHexString("0xdeadbeee"),
          Address.fromHexString("0xdeadbeea"),
          Address.fromHexString("0xdeadbeeb"));

  private MerkleTrie<Bytes, Bytes> trie;

  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  private WorldStateProofProvider proofProvider;
  private Hash account0Hash;
  private Hash account0StorageRoot;

  @BeforeEach
  public void setup() {
    final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();

    worldStateKeyValueStorage =
        new BonsaiWorldStateKeyValueStorage(
            storageProvider,
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    worldStateStorageCoordinator = new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    proofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
    trie =
        TrieGenerator.generateTrie(
            worldStateStorageCoordinator,
            accounts.stream().map(Address::addressHash).collect(Collectors.toList()));
    account0Hash = accounts.get(0).addressHash();
    account0StorageRoot =
        trie.get(account0Hash.getBytes())
            .map(RLP::input)
            .map(PmtStateTrieAccountValue::readFrom)
            .map(PmtStateTrieAccountValue::getStorageRoot)
            .orElseThrow();
  }

  @Test
  void shouldReturnChildRequests() {

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                worldStateKeyValueStorage.getAccountStorageTrieNode(account0Hash, location, hash),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            b -> b,
            b -> b);

    // Create a collector to gather slot entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, 1, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the slot entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> slots =
        (TreeMap<Bytes32, Bytes>)
            storageTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.firstKey());
    proofs.addAll(
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.lastKey()));

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final StorageFlatDatabaseHealingRangeRequest request =
        new StorageFlatDatabaseHealingRangeRequest(
            Hash.EMPTY,
            Bytes32.wrap(account0Hash.getBytes()),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);
    // Add local data to the request, including the proof provider, accounts TreeMap, and proofs as
    // an ArrayDeque
    request.addLocalData(proofProvider, slots, new ArrayDeque<>(proofs));

    // Verify that the start key hash of the snapDataRequest is greater than the last key in the
    // slots TreeMap
    List<SnapDataRequest> childRequests =
        request
            .getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState)
            .toList();
    Assertions.assertThat(childRequests).hasSizeGreaterThan(1);
    StorageFlatDatabaseHealingRangeRequest snapDataRequest =
        (StorageFlatDatabaseHealingRangeRequest) childRequests.get(0);
    Assertions.assertThat(snapDataRequest.getStartKeyHash()).isGreaterThan(slots.lastKey());
  }

  @Test
  void shouldNotReturnChildRequestsWhenNoMoreSlots() {

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                worldStateKeyValueStorage.getAccountStorageTrieNode(account0Hash, location, hash),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            b -> b,
            b -> b);

    // Create a collector to gather slot entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the slots entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> slots =
        (TreeMap<Bytes32, Bytes>)
            storageTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Create a request for healing the flat database with no more slots
    final StorageFlatDatabaseHealingRangeRequest request =
        new StorageFlatDatabaseHealingRangeRequest(
            Hash.EMPTY,
            Bytes32.wrap(account0Hash.getBytes()),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            slots.lastKey(),
            RangeManager.MAX_RANGE);

    // Add local data to the request
    request.addLocalData(proofProvider, new TreeMap<>(), new ArrayDeque<>());

    // Verify that no child requests are returned from the request
    final Stream<SnapDataRequest> childRequests =
        request.getChildRequests(downloadState, worldStateStorageCoordinator, snapSyncState);
    Assertions.assertThat(childRequests).isEmpty();
  }

  @Test
  void doNotPersistWhenProofIsValid() {

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                worldStateKeyValueStorage.getAccountStorageTrieNode(account0Hash, location, hash),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            b -> b,
            b -> b);

    // Create a collector to gather slots entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the slot entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> slots =
        (TreeMap<Bytes32, Bytes>)
            storageTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.firstKey());
    proofs.addAll(
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.lastKey()));

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final StorageFlatDatabaseHealingRangeRequest request =
        new StorageFlatDatabaseHealingRangeRequest(
            Hash.wrap(trie.getRootHash()),
            Bytes32.wrap(account0Hash.getBytes()),
            storageTrie.getRootHash(),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);

    // Add local data to the request
    request.addLocalData(proofProvider, slots, new ArrayDeque<>(proofs));

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
  void doHealAndPersistWhenProofIsInvalid() {

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                worldStateKeyValueStorage.getAccountStorageTrieNode(account0Hash, location, hash),
            Bytes32.wrap(account0StorageRoot.getBytes()),
            b -> b,
            b -> b);

    // Create a collector to gather slots entries within a specific range
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            Bytes32.ZERO, RangeManager.MAX_RANGE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    // Create a visitor for the range collector
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);

    // Collect the slot entries within the specified range using the trie and range collector
    final TreeMap<Bytes32, Bytes> slots =
        (TreeMap<Bytes32, Bytes>)
            storageTrie.entriesFrom(
                root ->
                    RangeStorageEntriesCollector.collectEntries(
                        collector, visitor, root, Bytes32.ZERO));

    // Retrieve the proof related nodes for the account trie
    final List<Bytes> proofs =
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.firstKey());
    proofs.addAll(
        proofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(account0Hash.getBytes()), slots.lastKey()));

    // Remove a slot in the middle of the range
    final Iterator<Map.Entry<Bytes32, Bytes>> iterator = slots.entrySet().iterator();
    Map.Entry<Bytes32, Bytes> removedSlot = null;
    int i = 0;
    while (iterator.hasNext()) {
      if (i == 1) {
        removedSlot = Map.Entry.copyOf(iterator.next());
        iterator.remove();
      } else {
        iterator.next();
      }
      i++;
    }

    // Create a request for healing the flat database with a range from MIN_RANGE to MAX_RANGE
    final StorageFlatDatabaseHealingRangeRequest request =
        new StorageFlatDatabaseHealingRangeRequest(
            Hash.wrap(trie.getRootHash()),
            Bytes32.wrap(account0Hash.getBytes()),
            storageTrie.getRootHash(),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE);
    // Add local data to the request
    request.addLocalData(proofProvider, slots, new ArrayDeque<>(proofs));

    BonsaiWorldStateKeyValueStorage.Updater updater =
        Mockito.spy(worldStateKeyValueStorage.updater());
    request.doPersist(
        worldStateStorageCoordinator,
        updater,
        downloadState,
        snapSyncState,
        SnapSyncConfiguration.getDefault());
    // check add the missing slot to the updater
    Mockito.verify(updater)
        .putStorageValueBySlotHash(
            account0Hash,
            Hash.wrap(removedSlot.getKey()),
            Bytes32.leftPad(decodeValue(removedSlot.getValue())));
  }
}
