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

import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AccountHealingTrackingTest {

  private final List<Address> accounts = List.of(Address.fromHexString("0xdeadbeef"));
  private final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
      InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateStorage();

  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private WorldStateProofProvider worldStateProofProvider;
  private MerkleTrie<Bytes, Bytes> accountStateTrie;

  @Mock SnapWorldDownloadState snapWorldDownloadState;

  @BeforeEach
  public void setup() {
    worldStateStorageCoordinator = new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    accountStateTrie =
        TrieGenerator.generateTrie(
            worldStateStorageCoordinator,
            accounts.stream().map(Address::addressHash).collect(Collectors.toList()));
    worldStateProofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
  }

  @Test
  void shouldMarkAccountForHealingWhenStorageProofIsReceived() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(
                (location, hash) ->
                    worldStateKeyValueStorage.getAccountStorageTrieNode(
                        accountHash, location, hash),
                Function.identity(),
                Function.identity()),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()));

    final NavigableMap<Bytes32, Bytes> slots =
        TrieGenerator.collectEntries(
            storageTrie, Bytes32.wrap(Hash.ZERO.getBytes()), MAX_RANGE, 10);

    final List<Bytes> proofs =
        worldStateProofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(),
            Bytes32.wrap(accountHash.getBytes()),
            Bytes32.wrap(Hash.ZERO.getBytes()));
    proofs.addAll(
        worldStateProofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(accountHash.getBytes()), slots.lastKey()));

    final StorageRangeDataRequest storageRangeDataRequest =
        SnapDataRequest.createStorageRangeDataRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes32.wrap(accountHash.getBytes()),
            storageTrie.getRootHash(),
            Bytes32.wrap(Hash.ZERO.getBytes()),
            MAX_RANGE);
    storageRangeDataRequest.addResponse(
        snapWorldDownloadState, worldStateProofProvider, slots, new ArrayDeque<>(proofs));
    storageRangeDataRequest.getChildRequests(
        snapWorldDownloadState, worldStateStorageCoordinator, null);

    verify(snapWorldDownloadState).addAccountToHealingList(any(Bytes.class));
  }

  @Test
  void shouldNotMarkAccountForHealingWhenAllStorageIsReceivedWithoutProof() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(
                (location, hash) ->
                    worldStateKeyValueStorage.getAccountStorageTrieNode(
                        accountHash, location, hash),
                Function.identity(),
                Function.identity()),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()));

    final NavigableMap<Bytes32, Bytes> slots =
        TrieGenerator.collectEntries(
            storageTrie, Bytes32.wrap(Hash.ZERO.getBytes()), MAX_RANGE, 10);

    final StorageRangeDataRequest storageRangeDataRequest =
        SnapDataRequest.createStorageRangeDataRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes32.wrap(accountHash.getBytes()),
            storageTrie.getRootHash(),
            Bytes32.wrap(Hash.ZERO.getBytes()),
            MAX_RANGE);
    storageRangeDataRequest.addResponse(
        snapWorldDownloadState, worldStateProofProvider, slots, new ArrayDeque<>());
    storageRangeDataRequest.getChildRequests(
        snapWorldDownloadState, worldStateStorageCoordinator, null);

    verify(snapWorldDownloadState, never()).addAccountToHealingList(any(Bytes.class));
  }

  @Test
  void shouldMarkAccountForHealingOnInvalidStorageProof() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));

    final List<Bytes> proofs =
        List.of(
            worldStateKeyValueStorage
                .getAccountStorageTrieNode(
                    accountHash,
                    Bytes.EMPTY,
                    Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()))
                .orElseThrow());

    final StorageRangeDataRequest storageRangeDataRequest =
        SnapDataRequest.createStorageRangeDataRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes32.wrap(accountHash.getBytes()),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()),
            Bytes32.wrap(Hash.ZERO.getBytes()),
            MAX_RANGE);
    storageRangeDataRequest.addResponse(
        snapWorldDownloadState, worldStateProofProvider, new TreeMap<>(), new ArrayDeque<>(proofs));

    verify(snapWorldDownloadState).addAccountToHealingList(any(Bytes.class));
  }

  @Test
  void shouldMarkAccountForHealingOnInvalidStorageWithoutProof() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(
                (location, hash) ->
                    worldStateKeyValueStorage.getAccountStorageTrieNode(
                        accountHash, location, hash),
                Function.identity(),
                Function.identity()),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()));

    final NavigableMap<Bytes32, Bytes> slots =
        TrieGenerator.collectEntries(storageTrie, Bytes32.wrap(Hash.ZERO.getBytes()), MAX_RANGE, 1);

    final StorageRangeDataRequest storageRangeDataRequest =
        SnapDataRequest.createStorageRangeDataRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes32.wrap(accountHash.getBytes()),
            storageTrie.getRootHash(),
            Bytes32.wrap(Hash.ZERO.getBytes()),
            MAX_RANGE);
    storageRangeDataRequest.addResponse(
        snapWorldDownloadState, worldStateProofProvider, slots, new ArrayDeque<>());

    verify(snapWorldDownloadState).addAccountToHealingList(any(Bytes.class));
  }

  @Test
  void shouldMarkAccountForHealingOnPartialStorageRange() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));

    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
        new StoredMerklePatriciaTrie<>(
            new StoredNodeFactory<>(
                (location, hash) ->
                    worldStateKeyValueStorage.getAccountStorageTrieNode(
                        accountHash, location, hash),
                Function.identity(),
                Function.identity()),
            Bytes32.wrap(stateTrieAccountValue.getStorageRoot().getBytes()));

    final NavigableMap<Bytes32, Bytes> slots =
        TrieGenerator.collectEntries(storageTrie, Bytes32.wrap(Hash.ZERO.getBytes()), MAX_RANGE, 1);

    final List<Bytes> proofs =
        worldStateProofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(),
            Bytes32.wrap(accountHash.getBytes()),
            Bytes32.wrap(Hash.ZERO.getBytes()));
    proofs.addAll(
        worldStateProofProvider.getStorageProofRelatedNodes(
            storageTrie.getRootHash(), Bytes32.wrap(accountHash.getBytes()), slots.lastKey()));

    final StorageRangeDataRequest storageRangeDataRequest =
        SnapDataRequest.createStorageRangeDataRequest(
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes32.wrap(accountHash.getBytes()),
            storageTrie.getRootHash(),
            Bytes32.wrap(Hash.ZERO.getBytes()),
            MAX_RANGE);
    storageRangeDataRequest.addResponse(
        snapWorldDownloadState, worldStateProofProvider, slots, new ArrayDeque<>(proofs));
    verify(snapWorldDownloadState, never()).addAccountToHealingList(any(Bytes.class));

    storageRangeDataRequest.getChildRequests(
        snapWorldDownloadState, worldStateStorageCoordinator, null);

    verify(snapWorldDownloadState).addAccountToHealingList(any(Bytes.class));
  }

  @Test
  void shouldNotMarkAccountForHealingOnValidStorageTrieNodeDetection() {
    final Hash accountHash = Hash.hash(accounts.getFirst().getBytes());
    final PmtStateTrieAccountValue stateTrieAccountValue =
        PmtStateTrieAccountValue.readFrom(
            RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()));
    final StorageTrieNodeHealingRequest storageTrieNodeHealingRequest =
        SnapDataRequest.createStorageTrieNodeDataRequest(
            stateTrieAccountValue.getStorageRoot(),
            accountHash,
            Hash.wrap(accountStateTrie.getRootHash()),
            Bytes.EMPTY);
    storageTrieNodeHealingRequest.getExistingData(worldStateStorageCoordinator);

    verify(snapWorldDownloadState, never()).addAccountToHealingList(any(Bytes.class));
  }
}
