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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StubTask;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2StorageRangeRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SnapV2PersistDataStepTest {

  // serving side: the "network" world state that responses and proofs come from
  private BonsaiWorldStateKeyValueStorage servingStorage;
  private WorldStateStorageCoordinator servingCoordinator;
  private WorldStateProofProvider proofProvider;
  private Hash stateRoot;

  // local side: the syncing node's storage that persist writes to
  private BonsaiWorldStateKeyValueStorage localStorage;
  private WorldStateStorageCoordinator localCoordinator;

  private BlockHeader pivot;
  private DownloadedAccountRangeTracker accountTracker;
  private DownloadedStorageRangeTracker storageTracker;
  private SnapSyncProcessState snapSyncState;
  private SnapRequestContext downloadState;
  private SnapV2PersistDataStep persistStep;

  @BeforeEach
  void setup() {
    servingStorage = newBonsaiStorage();
    servingCoordinator = new WorldStateStorageCoordinator(servingStorage);
    proofProvider = new WorldStateProofProvider(servingCoordinator);
    final MerkleTrie<Bytes, Bytes> trie = TrieGenerator.generateTrie(servingCoordinator, 4);
    stateRoot = Hash.wrap(trie.getRootHash());

    localStorage = newBonsaiStorage();
    localCoordinator = new WorldStateStorageCoordinator(localStorage);

    pivot = new BlockHeaderTestFixture().stateRoot(stateRoot).buildHeader();
    accountTracker = new DownloadedAccountRangeTracker();
    storageTracker = new DownloadedStorageRangeTracker();

    snapSyncState = mock(SnapSyncProcessState.class);
    when(snapSyncState.getPivotBlockHash()).thenReturn(Optional.of(pivot.getHash()));
    downloadState = mock(SnapRequestContext.class);
    when(downloadState.getMetricsManager()).thenReturn(mock(SnapSyncMetricsManager.class));

    persistStep =
        new SnapV2PersistDataStep(
            snapSyncState,
            localCoordinator,
            downloadState,
            mock(SnapSyncConfiguration.class),
            accountTracker,
            storageTracker);
  }

  @Test
  void persistsAccountRangeAndRegistersTrackers() {
    final NavigableMap<Bytes32, Bytes> accounts =
        TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot);
    final SnapV2AccountRangeRequest request = accountRangeRequest(accounts);

    persistStep.persist(new StubTask(request));

    // the whole trie, including nodes whose children lie outside any single range, must be
    // committed: the account trie must be fully traversable from local storage
    final MerkleTrie<Bytes, Bytes> localTrie =
        new StoredMerklePatriciaTrie<>(
            localCoordinator::getAccountStateTrieNode,
            Bytes32.wrap(stateRoot.getBytes()),
            b -> b,
            b -> b);
    assertThat(Hash.wrap(localTrie.getRootHash())).isEqualTo(stateRoot);
    for (final Bytes32 accountHash : accounts.keySet()) {
      assertThat(localTrie.get(accountHash)).isPresent();
      assertThat(localStorage.getAccount(Hash.wrap(accountHash))).isPresent();
    }

    // every generated account has storage and code: one storage and one bytecode child each
    final List<SnapDataRequest> children = enqueuedChildren();
    assertThat(children)
        .filteredOn(SnapV2StorageRangeRequest.class::isInstance)
        .hasSize(accounts.size());
    assertThat(children)
        .filteredOn(SnapV2BytecodeRequest.class::isInstance)
        .hasSize(accounts.size());

    for (final Bytes32 accountHash : accounts.keySet()) {
      assertThat(accountTracker.isAccountHashPersisted(accountHash)).isTrue();
      assertThat(accountTracker.isAccountHashPending(accountHash)).isTrue();
    }
    assertThat(accountTracker.pendingRangeCount()).isEqualTo(1);
  }

  @Test
  void tracksRangeOnlyUpToContinuationStart() {
    // a truncated response carries only the first accounts plus boundary proofs
    final NavigableMap<Bytes32, Bytes> allAccounts =
        TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot);
    final NavigableMap<Bytes32, Bytes> partialAccounts = new TreeMap<>(allAccounts);
    partialAccounts.remove(partialAccounts.lastKey());
    partialAccounts.remove(partialAccounts.lastKey());
    final List<Bytes> proofs = new ArrayList<>();
    proofs.addAll(proofProvider.getAccountProofRelatedNodes(stateRoot, MIN_RANGE));
    proofs.addAll(proofProvider.getAccountProofRelatedNodes(stateRoot, partialAccounts.lastKey()));

    final SnapV2AccountRangeRequest request =
        new SnapV2AccountRangeRequest(pivot, MIN_RANGE, MAX_RANGE);
    request.addResponse(proofProvider, partialAccounts, proofs);

    persistStep.persist(new StubTask(request));

    final SnapV2AccountRangeRequest continuation =
        enqueuedChildren().stream()
            .filter(SnapV2AccountRangeRequest.class::isInstance)
            .map(SnapV2AccountRangeRequest.class::cast)
            .findFirst()
            .orElseThrow();
    assertThat(continuation.getStartKeyHash()).isGreaterThan(partialAccounts.lastKey());

    // only the range covered by the response is tracked; the continuation owns the rest
    assertThat(accountTracker.isAccountHashPersisted(partialAccounts.firstKey())).isTrue();
    assertThat(accountTracker.isAccountHashPersisted(partialAccounts.lastKey())).isTrue();
    assertThat(accountTracker.isAccountHashPersisted(continuation.getStartKeyHash())).isFalse();
  }

  @Test
  void accountWithoutStorageRegistersFullSlotRange() {
    final Bytes32 plainAccount = Bytes32.fromHexString("0xdead");
    final PmtStateTrieAccountValue plainValue =
        new PmtStateTrieAccountValue(1L, Wei.ONE, Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    addServingAccount(plainAccount, plainValue);

    final NavigableMap<Bytes32, Bytes> accounts =
        TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot);
    persistStep.persist(new StubTask(accountRangeRequest(accounts)));

    // an account with an empty storage trie needs no storage child: all its slots count as
    // downloaded for selective BAL application
    assertThat(storageTracker.isSlotHashDownloaded(plainAccount, MIN_RANGE)).isTrue();
    assertThat(storageTracker.isSlotHashDownloaded(plainAccount, MAX_RANGE)).isTrue();
    assertThat(enqueuedChildren().stream().filter(SnapV2StorageRangeRequest.class::isInstance))
        .hasSize(accounts.size() - 1);
  }

  @Test
  void persistsStorageRangeAndRegistersSlotRange() {
    final Bytes32 accountHash =
        TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot).firstKey();
    final PmtStateTrieAccountValue account = TrieGenerator.readAccount(servingStorage, accountHash);
    accountTracker.registerPending(MIN_RANGE, MAX_RANGE, 1);

    final SnapV2StorageRangeRequest request =
        new SnapV2StorageRangeRequest(
            pivot,
            accountHash,
            Bytes32.wrap(account.getStorageRoot().getBytes()),
            MIN_RANGE,
            MAX_RANGE,
            MIN_RANGE);
    final NavigableMap<Bytes32, Bytes> slots =
        TrieGenerator.collectStorageEntries(
            servingCoordinator,
            Hash.wrap(accountHash),
            Bytes32.wrap(account.getStorageRoot().getBytes()));
    request.addResponse(downloadState, proofProvider, slots, new ArrayDeque<>());

    persistStep.persist(new StubTask(request));

    final MerkleTrie<Bytes, Bytes> localStorageTrie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) ->
                localCoordinator.getAccountStorageTrieNode(Hash.wrap(accountHash), location, hash),
            Bytes32.wrap(account.getStorageRoot().getBytes()),
            b -> b,
            b -> b);
    assertThat(Hash.wrap(localStorageTrie.getRootHash())).isEqualTo(account.getStorageRoot());
    // TrieGenerator writes slot keys 1, 2, 3 with values 2, 4, 6
    assertThat(
            localStorage
                .getStorageValueByStorageSlotKey(
                    Hash.wrap(accountHash), new StorageSlotKey(UInt256.ONE))
                .map(UInt256::fromBytes))
        .contains(UInt256.valueOf(2));
    for (final Bytes32 slotHash : slots.keySet()) {
      assertThat(storageTracker.isSlotHashDownloaded(accountHash, slotHash)).isTrue();
    }

    // the storage child was the only pending child, so the account range is now complete
    assertThat(accountTracker.isAccountHashDownloaded(accountHash)).isTrue();
    assertThat(accountTracker.pendingRangeCount()).isZero();
  }

  @Test
  void persistsBytecodeAndCompletesChild() {
    final Bytes32 accountHash =
        TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot).firstKey();
    final PmtStateTrieAccountValue account = TrieGenerator.readAccount(servingStorage, accountHash);
    accountTracker.registerPending(MIN_RANGE, MAX_RANGE, 1);

    final Bytes code =
        servingStorage.getCode(account.getCodeHash(), Hash.wrap(accountHash)).orElseThrow();
    final SnapV2BytecodeRequest request =
        new SnapV2BytecodeRequest(
            pivot, accountHash, Bytes32.wrap(account.getCodeHash().getBytes()), MIN_RANGE);
    request.setCode(code);

    persistStep.persist(new StubTask(request));

    assertThat(localStorage.getCode(account.getCodeHash(), Hash.wrap(accountHash))).contains(code);
    assertThat(accountTracker.isAccountHashDownloaded(accountHash)).isTrue();
  }

  @Test
  void rejectsExpiredRequest() {
    when(snapSyncState.getPivotBlockHash()).thenReturn(Optional.of(Hash.hash(Bytes.of(9, 9, 9))));

    final SnapV2AccountRangeRequest request =
        accountRangeRequest(TrieGenerator.collectAccountEntries(servingCoordinator, stateRoot));

    assertThatThrownBy(() -> persistStep.persist(new StubTask(request)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expired snap/2 request");
  }

  @Test
  void skipsRequestWithoutResponse() {
    final SnapV2AccountRangeRequest request =
        new SnapV2AccountRangeRequest(pivot, MIN_RANGE, MAX_RANGE);

    persistStep.persist(new StubTask(request));

    assertThat(
            localCoordinator.getAccountStateTrieNode(
                Bytes.EMPTY, Bytes32.wrap(stateRoot.getBytes())))
        .isEmpty();
    assertThat(accountTracker.pendingRangeCount()).isZero();
    assertThat(accountTracker.completedRangeCount()).isZero();
    verify(downloadState, never()).enqueueRequests(any());
  }

  private SnapV2AccountRangeRequest accountRangeRequest(
      final NavigableMap<Bytes32, Bytes> accounts) {
    final SnapV2AccountRangeRequest request =
        new SnapV2AccountRangeRequest(pivot, MIN_RANGE, MAX_RANGE);
    request.addResponse(proofProvider, accounts, List.of());
    return request;
  }

  private void addServingAccount(
      final Bytes32 accountHash, final PmtStateTrieAccountValue accountValue) {
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            servingCoordinator::getAccountStateTrieNode,
            Bytes32.wrap(stateRoot.getBytes()),
            b -> b,
            b -> b);
    final WorldStateKeyValueStorage.Updater updater = servingCoordinator.updater();
    final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater =
        (BonsaiWorldStateKeyValueStorage.Updater) updater;
    final Bytes encoded = RLP.encode(accountValue::writeTo);
    trie.put(accountHash, encoded);
    trie.commit(bonsaiUpdater::putAccountStateTrieNode);
    bonsaiUpdater.putAccountInfoState(Hash.wrap(accountHash), encoded);
    updater.commit();
    stateRoot = Hash.wrap(trie.getRootHash());
    pivot = new BlockHeaderTestFixture().stateRoot(stateRoot).buildHeader();
    when(snapSyncState.getPivotBlockHash()).thenReturn(Optional.of(pivot.getHash()));
  }

  @SuppressWarnings("unchecked")
  private List<SnapDataRequest> enqueuedChildren() {
    final ArgumentCaptor<Stream<SnapDataRequest>> captor = ArgumentCaptor.forClass(Stream.class);
    verify(downloadState).enqueueRequests(captor.capture());
    return captor.getValue().toList();
  }

  private static BonsaiWorldStateKeyValueStorage newBonsaiStorage() {
    return InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateStorage();
  }
}
