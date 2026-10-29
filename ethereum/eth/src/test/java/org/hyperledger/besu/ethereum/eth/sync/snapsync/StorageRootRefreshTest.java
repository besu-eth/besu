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
import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.TrieGenerator;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseCallback;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountRangeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageRangeDataRequest;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * A pivot change makes the storage root of an account with frequently changing storage out of date.
 * Snap sync then refreshes that root once per account and retries the affected storage requests
 * unchanged, instead of creating a new request for every failed response.
 */
class StorageRootRefreshTest {

  private final Address account = Address.fromHexString("0xdeadbeef");
  private final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private WorldStateProofProvider worldStateProofProvider;
  private MerkleTrie<Bytes, Bytes> accountStateTrie;
  private Hash accountHash;
  private Hash stateRoot;
  private Bytes32 storageRoot;

  private static final Hash OLDER_STATE_ROOT = Hash.hash(Bytes.of(1));
  private static final Hash NEWER_STATE_ROOT = Hash.hash(Bytes.of(2));
  private static final Bytes32 STALE_STORAGE_ROOT = Bytes32.fromHexString("0x01");

  @BeforeEach
  void setup() {
    worldStateStorageCoordinator = new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    accountStateTrie =
        TrieGenerator.generateTrie(worldStateStorageCoordinator, List.of(account.addressHash()));
    worldStateProofProvider = new WorldStateProofProvider(worldStateStorageCoordinator);
    accountHash = Hash.hash(account.getBytes());
    stateRoot = Hash.wrap(accountStateTrie.getRootHash());
    storageRoot =
        Bytes32.wrap(
            PatriciaTrieAccountValue.readFrom(
                    RLP.input(accountStateTrie.get(accountHash.getBytes()).orElseThrow()))
                .getStorageRoot()
                .getBytes());
  }

  @Nested
  class StorageRequests {
    private final SnapRequestContext context = mock(SnapRequestContext.class);

    @BeforeEach
    void noRefreshedRoot() {
      when(context.getRefreshedStorageRoot(any())).thenReturn(Optional.empty());
    }

    @Test
    void invalidProofAfterPivotChangeRequestsRefreshAndRetriesTheSameRequest() {
      // the storage root was learned at an older pivot; the peer answered for the current one
      final StorageRangeDataRequest request = storageRequest(OLDER_STATE_ROOT, STALE_STORAGE_ROOT);
      request.setRootHash(stateRoot);

      request.addResponse(context, worldStateProofProvider, slots(3), proofs(slots(3)));

      verify(context).requestStorageRootRefresh(accountHash);
      verify(context, never()).enqueueRequest(any());
      assertThat(request.isResponseReceived()).isFalse(); // so CompleteTaskStep requeues it
    }

    @Test
    void invalidProofWithCurrentStorageRootDoesNotRequestRefresh() {
      // the storage root belongs to the state root the peer answered for: the response is bad
      final StorageRangeDataRequest request = storageRequest(stateRoot, STALE_STORAGE_ROOT);
      request.setRootHash(stateRoot);

      request.addResponse(context, worldStateProofProvider, slots(3), proofs(slots(3)));

      verify(context, never()).requestStorageRootRefresh(any());
      verify(context, never()).enqueueRequest(any());
      assertThat(request.isResponseReceived()).isFalse();
    }

    @Test
    void responseIsVerifiedAgainstTheRefreshedStorageRoot() {
      when(context.getRefreshedStorageRoot(accountHash))
          .thenReturn(
              Optional.of(new SnapRequestContext.RefreshedStorageRoot(stateRoot, storageRoot)));
      final StorageRangeDataRequest request = storageRequest(OLDER_STATE_ROOT, STALE_STORAGE_ROOT);
      request.setRootHash(stateRoot);

      // a partial response: one slot plus boundary proofs, so a follow-up request is needed
      final NavigableMap<Bytes32, Bytes> firstSlot = slots(1);
      request.addResponse(context, worldStateProofProvider, firstSlot, proofs(firstSlot));

      assertThat(request.isResponseReceived()).isTrue();
      assertThat(request.getStorageRoot()).isEqualTo(storageRoot);
      final List<StorageRangeDataRequest> followUps =
          request
              .getChildRequests(context, worldStateStorageCoordinator, null)
              .map(StorageRangeDataRequest.class::cast)
              .toList();
      assertThat(followUps).isNotEmpty();
      assertThat(followUps).allMatch(child -> child.getStorageRoot().equals(storageRoot));
      verify(context, never()).requestStorageRootRefresh(any());
    }

    @Test
    void lateResponseForAnOlderPivotIsVerifiedAgainstTheRequestsOwnStorageRoot() {
      // a refresh at a newer pivot learned another storage root, but this response was served for
      // the pivot the request's own storage root belongs to
      when(context.getRefreshedStorageRoot(accountHash))
          .thenReturn(
              Optional.of(
                  new SnapRequestContext.RefreshedStorageRoot(
                      NEWER_STATE_ROOT, STALE_STORAGE_ROOT)));
      final StorageRangeDataRequest request = storageRequest(stateRoot, storageRoot);
      request.setRootHash(stateRoot);

      request.addResponse(context, worldStateProofProvider, slots(3), proofs(slots(3)));

      assertThat(request.isResponseReceived()).isTrue();
      assertThat(request.getStorageRoot()).isEqualTo(storageRoot);
      verify(context, never()).requestStorageRootRefresh(any());
      verify(context, never()).addAccountToHealingList(any());
    }

    @Test
    void refreshThatFoundTheSameStorageRootPreventsFurtherRefreshes() {
      // the pivot moved but the account's storage did not change
      when(context.getRefreshedStorageRoot(accountHash))
          .thenReturn(
              Optional.of(new SnapRequestContext.RefreshedStorageRoot(stateRoot, storageRoot)));
      final StorageRangeDataRequest request = storageRequest(OLDER_STATE_ROOT, storageRoot);
      request.setRootHash(stateRoot);

      // a bad response: one slot value does not match the proof
      final NavigableMap<Bytes32, Bytes> tampered = new TreeMap<>(slots(3));
      tampered.put(tampered.firstKey(), Bytes.of(0x42));
      request.addResponse(context, worldStateProofProvider, tampered, proofs(slots(3)));

      assertThat(request.isResponseReceived()).isFalse();
      verify(context, never()).requestStorageRootRefresh(any());
    }

    private StorageRangeDataRequest storageRequest(
        final Hash knownAtStateRoot, final Bytes32 knownStorageRoot) {
      return SnapDataRequest.createStorageRangeDataRequest(
          knownAtStateRoot,
          Bytes32.wrap(accountHash.getBytes()),
          knownStorageRoot,
          MIN_RANGE,
          MAX_RANGE);
    }
  }

  @Nested
  class Refresh {
    private final SnapRequestContext context = mock(SnapRequestContext.class);

    @Test
    void refreshRecordsTheStorageRootAndCreatesNoRequests() {
      final AccountRangeDataRequest refresh =
          SnapDataRequest.createStorageRootRefreshRequest(OLDER_STATE_ROOT, accountHash);
      assertThat(refresh.isStorageRootRefresh()).isTrue();
      refresh.setRootHash(stateRoot);

      final Bytes32 key = Bytes32.wrap(accountHash.getBytes());
      final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
      accounts.put(key, accountStateTrie.get(accountHash.getBytes()).orElseThrow());
      refresh.addResponse(
          worldStateProofProvider,
          accounts,
          new ArrayDeque<>(worldStateProofProvider.getAccountProofRelatedNodes(stateRoot, key)));

      assertThat(refresh.isResponseReceived()).isTrue();
      assertThat(refresh.getChildRequests(context, worldStateStorageCoordinator, null)).isEmpty();
      verify(context).completeStorageRootRefresh(accountHash, stateRoot, storageRoot);
      verify(context, never()).enqueueRequest(any());
    }

    @Test
    void refreshOfARemovedAccountWithNoAccountAfterItCompletesWithTheEmptyStorageRoot() {
      // no account exists at or after the requested key: the peer sends only the proof
      final Bytes32 lastKey = Bytes32.fromHexString("0x" + "ff".repeat(32));
      final Hash removedAccount = Hash.wrap(lastKey);
      final AccountRangeDataRequest refresh =
          SnapDataRequest.createStorageRootRefreshRequest(stateRoot, removedAccount);

      refresh.addResponse(
          worldStateProofProvider,
          new TreeMap<>(),
          new ArrayDeque<>(
              worldStateProofProvider.getAccountProofRelatedNodes(stateRoot, lastKey)));

      assertThat(refresh.isResponseReceived()).isTrue();
      assertThat(refresh.getChildRequests(context, worldStateStorageCoordinator, null)).isEmpty();
      verify(context)
          .completeStorageRootRefresh(
              removedAccount, stateRoot, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()));
    }

    @Test
    void refreshAnsweredWithoutTheNextAccountIsRetriedWithoutThrowing() {
      // the peer omits the account after the requested key, so the proof is incomplete
      final AccountRangeDataRequest refresh =
          SnapDataRequest.createStorageRootRefreshRequest(stateRoot, Hash.wrap(Bytes32.ZERO));

      refresh.addResponse(
          worldStateProofProvider,
          new TreeMap<>(),
          new ArrayDeque<>(
              worldStateProofProvider.getAccountProofRelatedNodes(stateRoot, Bytes32.ZERO)));

      assertThat(refresh.isResponseReceived()).isFalse();
      verifyNoInteractions(context);
    }

    @Test
    void refreshOfARemovedAccountAnsweredWithTheNextAccountCompletesWithTheEmptyStorageRoot() {
      // the peer answers with the first account after the requested key, as geth does
      final Hash removedAccount = Hash.wrap(Bytes32.ZERO);
      final Bytes32 nextKey = Bytes32.wrap(accountHash.getBytes());
      final AccountRangeDataRequest refresh =
          SnapDataRequest.createStorageRootRefreshRequest(stateRoot, removedAccount);
      final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
      accounts.put(nextKey, accountStateTrie.get(accountHash.getBytes()).orElseThrow());
      final List<Bytes> proofs =
          new ArrayList<>(
              worldStateProofProvider.getAccountProofRelatedNodes(stateRoot, Bytes32.ZERO));
      proofs.addAll(worldStateProofProvider.getAccountProofRelatedNodes(stateRoot, nextKey));

      refresh.addResponse(worldStateProofProvider, accounts, new ArrayDeque<>(proofs));

      assertThat(refresh.isResponseReceived()).isTrue();
      assertThat(refresh.getChildRequests(context, worldStateStorageCoordinator, null)).isEmpty();
      verify(context)
          .completeStorageRootRefresh(
              removedAccount, stateRoot, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()));
    }

    @Test
    void refreshAnsweredWithoutDataIsRetried() {
      // e.g. the peer no longer has the state root; the refresh stays pending and is retried
      final AccountRangeDataRequest refresh =
          SnapDataRequest.createStorageRootRefreshRequest(stateRoot, accountHash);

      refresh.addResponse(worldStateProofProvider, new TreeMap<>(), new ArrayDeque<>());

      assertThat(refresh.isResponseReceived()).isFalse();
      verifyNoInteractions(context);
    }
  }

  @Nested
  class DownloadState {
    private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
    private final SnapSyncStatePersistenceManager snapContext =
        mock(SnapSyncStatePersistenceManager.class);
    private final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);
    private SnapWorldDownloadState downloadState;
    private BlockHeader pivot;

    @BeforeEach
    void createDownloadState() {
      when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());
      pivot = new BlockHeaderTestFixture().stateRoot(stateRoot).buildHeader();
      when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(pivot));
      downloadState =
          new SnapWorldDownloadState(
              worldStateStorageCoordinator,
              snapContext,
              mock(Blockchain.class),
              snapSyncState,
              new InMemoryTasksPriorityQueues<>(),
              10,
              50_000,
              metricsManager,
              new TestClock(),
              mock(EthContext.class),
              SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    }

    @Test
    void onlyOneRefreshIsQueuedPerAccountWhilePending() {
      downloadState.requestStorageRootRefresh(accountHash);
      downloadState.requestStorageRootRefresh(accountHash);
      downloadState.requestStorageRootRefresh(accountHash);

      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isTrue();
      assertThat(dequeueAccountRequest()).isNotNull();
      assertThat(downloadState.pendingStorageRootRefreshRequests.size()).isZero();
    }

    @Test
    void refreshIsTakenWhileStorageRequestsAreStillQueued() {
      downloadState.enqueueRequest(
          SnapDataRequest.createStorageRangeDataRequest(
              stateRoot,
              Bytes32.wrap(accountHash.getBytes()),
              storageRoot,
              Bytes32.fromHexString("0x10"),
              MAX_RANGE));
      downloadState.requestStorageRootRefresh(accountHash);

      final Task<SnapDataRequest> task = dequeueAccountRequest();

      assertThat(task.getData()).isInstanceOf(AccountRangeDataRequest.class);
      assertThat(((AccountRangeDataRequest) task.getData()).isStorageRootRefresh()).isTrue();
      assertThat(downloadState.pendingLargeStorageRequests.size()).isEqualTo(1);
    }

    @Test
    void refreshIsNotSavedForRestart() {
      downloadState.requestStorageRootRefresh(accountHash);

      dequeueAccountRequest();

      assertThat(downloadState.pendingAccountRequests.size()).isZero();
      verify(snapContext, never()).updatePersistedTasks(any());
    }

    @Test
    void completingTheRefreshStoresTheRootAndClearsThePendingMarker() {
      downloadState.requestStorageRootRefresh(accountHash);

      downloadState.completeStorageRootRefresh(accountHash, stateRoot, storageRoot);

      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isFalse();
      assertThat(downloadState.getRefreshedStorageRoot(accountHash))
          .contains(new SnapRequestContext.RefreshedStorageRoot(stateRoot, storageRoot));
    }

    @Test
    void anAccountIsRefreshedAgainOnlyAtANewPivot() {
      downloadState.requestStorageRootRefresh(accountHash);
      dequeueAccountRequest().markCompleted();
      downloadState.completeStorageRootRefresh(accountHash, stateRoot, storageRoot);

      // e.g. after a late response for an older pivot: requests retried at this pivot use the
      // refreshed storage root
      downloadState.requestStorageRootRefresh(accountHash);
      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isFalse();
      assertThat(downloadState.pendingStorageRootRefreshRequests.size()).isZero();

      // at a new pivot the storage root may have changed again
      when(snapSyncState.getPivotBlockHeader())
          .thenReturn(
              Optional.of(new BlockHeaderTestFixture().stateRoot(NEWER_STATE_ROOT).buildHeader()));
      downloadState.requestStorageRootRefresh(accountHash);
      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isTrue();
    }

    @Test
    void downloadIsNotCompleteWhileARefreshIsQueued() {
      downloadState.requestStorageRootRefresh(accountHash);
      when(snapSyncState.isHealTrieInProgress()).thenReturn(true);
      when(snapSyncState.isHealFlatDatabaseInProgress()).thenReturn(true);

      downloadState.checkCompletion(pivot);

      assertThat(downloadState.getDownloadFuture()).isNotDone();
    }

    @Test
    void storageRequestsOfAnAccountBeingRefreshedAreParkedWithoutCountingAsFailures() {
      final StorageRangeDataRequest refreshing = largeStorageRequest(accountHash);
      final StorageRangeDataRequest other = largeStorageRequest(Hash.wrap(Bytes32.ZERO));
      downloadState.enqueueRequest(refreshing);
      downloadState.enqueueRequest(other);
      downloadState.requestStorageRootRefresh(accountHash);

      final Task<SnapDataRequest> task = dequeueLargeStorageRequest();

      assertThat(task.getData()).isSameAs(other);
      assertThat(downloadState.pendingLargeStorageRequests.size()).isZero();
      // with the handed out request done, the parked one keeps the queue from being completed
      task.markCompleted();
      assertThat(downloadState.pendingLargeStorageRequests.allTasksCompleted()).isFalse();
      assertThat(downloadState.getRequestsSinceLastProgress()).isZero();
    }

    @Test
    void smallStorageRequestsOfAnAccountBeingRefreshedAreParked() {
      final StorageRangeDataRequest refreshing =
          SnapDataRequest.createStorageRangeDataRequest(
              stateRoot, Bytes32.wrap(accountHash.getBytes()), storageRoot, MIN_RANGE, MAX_RANGE);
      downloadState.enqueueRequest(refreshing);
      downloadState.requestStorageRootRefresh(accountHash);

      final CompletableFuture<Task<SnapDataRequest>> dequeued =
          dequeueOnWaitingThread(downloadState::dequeueStorageRequestBlocking);

      assertThat(dequeued).isNotDone();
      // the parked request left the queue but stays outstanding
      assertThat(downloadState.pendingStorageRequests.size()).isZero();
      assertThat(downloadState.pendingStorageRequests.allTasksCompleted()).isFalse();
      downloadState.completeStorageRootRefresh(accountHash, stateRoot, storageRoot);
      assertThat(dequeued)
          .succeedsWithin(Duration.ofSeconds(5))
          .extracting(Task::getData)
          .isSameAs(refreshing);
    }

    @Test
    void completingTheRefreshPutsParkedRequestsBackOnceAndWakesTheWaitingThread() {
      final StorageRangeDataRequest refreshing = largeStorageRequest(accountHash);
      downloadState.enqueueRequest(refreshing);
      downloadState.requestStorageRootRefresh(accountHash);

      // only parked requests left: the dequeue waits instead of handing them out
      final CompletableFuture<Task<SnapDataRequest>> dequeued =
          dequeueOnWaitingThread(downloadState::dequeueLargeStorageRequestBlocking);
      assertThat(dequeued).isNotDone();

      downloadState.completeStorageRootRefresh(accountHash, stateRoot, storageRoot);

      assertThat(dequeued)
          .succeedsWithin(Duration.ofSeconds(5))
          .extracting(Task::getData)
          .isSameAs(refreshing);
      // a second completion has nothing left to put back
      downloadState.completeStorageRootRefresh(accountHash, stateRoot, storageRoot);
      assertThat(downloadState.pendingLargeStorageRequests.size()).isZero();
      dequeued.join().markCompleted();
      assertThat(downloadState.pendingLargeStorageRequests.allTasksCompleted()).isTrue();
      assertThat(downloadState.getRequestsSinceLastProgress()).isZero();
    }

    @Test
    void stoppingTheDownloadClearsPendingRefreshesAndParkedRequests() {
      downloadState.enqueueRequest(largeStorageRequest(accountHash));
      downloadState.enqueueRequest(largeStorageRequest(Hash.wrap(Bytes32.ZERO)));
      downloadState.requestStorageRootRefresh(accountHash);
      dequeueLargeStorageRequest(); // parks the refreshing account's request

      downloadState.getDownloadFuture().cancel(true);

      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isFalse();
      assertThat(downloadState.getRefreshedStorageRoot(accountHash)).isEmpty();
      assertThat(downloadState.pendingLargeStorageRequests.allTasksCompleted()).isTrue();
      assertThat(downloadState.pendingStorageRootRefreshRequests.size()).isZero();
      // a late refresh request is ignored and not left pending without a refresh behind it
      downloadState.requestStorageRootRefresh(accountHash);
      assertThat(downloadState.isStorageRootRefreshPending(accountHash)).isFalse();
    }

    @Test
    void requestsParkedForARefreshThatFindsNoStorageAreCompletedWithoutBeingSent() {
      final EthContext ethContext = mock(EthContext.class);
      final SnapSyncConfiguration config = SnapSyncConfiguration.getDefault();
      final RequestDataStep requestDataStep =
          new RequestDataStep(
              ethContext,
              worldStateStorageCoordinator,
              snapSyncState,
              downloadState,
              config,
              new NoOpMetricsSystem());
      final PersistDataStep persistDataStep =
          new PersistDataStep(snapSyncState, worldStateStorageCoordinator, downloadState, config);
      final CompleteTaskStep completeTaskStep =
          new CompleteTaskStep(snapSyncState, new NoOpMetricsSystem());
      // made at an older pivot, when the account still had storage
      final StorageRangeDataRequest parked = smallStorageRequest(accountHash);
      final StorageRangeDataRequest other = smallStorageRequest(Hash.wrap(Bytes32.ZERO));
      downloadState.enqueueRequest(parked);
      downloadState.enqueueRequest(other);
      downloadState.requestStorageRootRefresh(accountHash);
      final Task<SnapDataRequest> otherTask = dequeueStorageRequest(); // parks the first request
      assertThat(otherTask.getData()).isSameAs(other);

      // the refresh is taken and finds that the account has no storage at the pivot
      dequeueAccountRequest().markCompleted();
      downloadState.completeStorageRootRefresh(
          accountHash, stateRoot, MerkleTrie.EMPTY_TRIE_NODE_HASH);

      final Task<SnapDataRequest> task = dequeueStorageRequest();
      assertThat(task.getData()).isSameAs(parked);
      final List<Task<SnapDataRequest>> tasks = List.of(task);
      assertThat(requestDataStep.requestStorage(tasks).join()).isSameAs(tasks);
      persistDataStep.persist(tasks);
      completeTaskStep.markAsCompleteOrFailed(downloadState, task);

      assertThat(parked.isResponseReceived()).isTrue();
      verifyNoInteractions(ethContext);
      // nothing is left to download for the account, but what was saved of its storage needs
      // healing
      assertThat(downloadState.pendingStorageRequests.size()).isZero();
      assertThat(downloadState.pendingLargeStorageRequests.size()).isZero();
      assertThat(downloadState.getAccountsHealingList())
          .contains(CompactEncoding.bytesToPath(accountHash.getBytes()));
      // the other request, still outstanding, is the only one keeping the queue from completing
      otherTask.markCompleted();
      assertThat(downloadState.pendingStorageRequests.allTasksCompleted()).isTrue();
    }

    private StorageRangeDataRequest largeStorageRequest(final Hash account) {
      return SnapDataRequest.createStorageRangeDataRequest(
          stateRoot,
          Bytes32.wrap(account.getBytes()),
          storageRoot,
          Bytes32.fromHexString("0x10"),
          MAX_RANGE);
    }

    private StorageRangeDataRequest smallStorageRequest(final Hash account) {
      return SnapDataRequest.createStorageRangeDataRequest(
          OLDER_STATE_ROOT, Bytes32.wrap(account.getBytes()), storageRoot, MIN_RANGE, MAX_RANGE);
    }

    private Task<SnapDataRequest> dequeueLargeStorageRequest() {
      return assertTimeoutPreemptively(
          Duration.ofSeconds(5), () -> downloadState.dequeueLargeStorageRequestBlocking());
    }

    private Task<SnapDataRequest> dequeueStorageRequest() {
      return assertTimeoutPreemptively(
          Duration.ofSeconds(5), () -> downloadState.dequeueStorageRequestBlocking());
    }

    private Task<SnapDataRequest> dequeueAccountRequest() {
      return assertTimeoutPreemptively(
          Duration.ofSeconds(5), () -> downloadState.dequeueAccountRequestBlocking());
    }

    /** Starts the dequeue on its own thread and returns once the thread waits for a request. */
    private CompletableFuture<Task<SnapDataRequest>> dequeueOnWaitingThread(
        final Supplier<Task<SnapDataRequest>> dequeue) {
      final CompletableFuture<Task<SnapDataRequest>> dequeued = new CompletableFuture<>();
      final Thread thread = new Thread(() -> dequeued.complete(dequeue.get()));
      thread.setDaemon(true);
      thread.start();
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> thread.getState() == Thread.State.WAITING || dequeued.isDone());
      return dequeued;
    }
  }

  @Nested
  class RequestStorage {
    private final SnapRequestContext context = mock(SnapRequestContext.class);
    private final EthContext ethContext = mock(EthContext.class);
    private RequestDataStep requestDataStep;

    @BeforeEach
    void createRequestDataStep() {
      final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
      when(snapSyncState.getPivotBlockHeader())
          .thenReturn(Optional.of(new BlockHeaderTestFixture().stateRoot(stateRoot).buildHeader()));
      requestDataStep =
          new RequestDataStep(
              ethContext,
              worldStateStorageCoordinator,
              snapSyncState,
              context,
              SnapSyncConfiguration.getDefault(),
              new NoOpMetricsSystem());
    }

    @Test
    void requestOfAnAccountProvenWithoutStorageAtThePivotIsCompletedWithoutBeingSent() {
      when(context.getRefreshedStorageRoot(accountHash))
          .thenReturn(
              Optional.of(
                  new SnapRequestContext.RefreshedStorageRoot(
                      stateRoot, MerkleTrie.EMPTY_TRIE_NODE_HASH)));
      final StorageRangeDataRequest request = fullRangeRequest(OLDER_STATE_ROOT);
      final List<Task<SnapDataRequest>> tasks = List.of(new StubTask(request));

      assertThat(requestDataStep.requestStorage(tasks).join()).isSameAs(tasks);

      assertThat(request.isResponseReceived()).isTrue();
      assertThat(request.getStorageRoot()).isEqualTo(MerkleTrie.EMPTY_TRIE_NODE_HASH);
      verify(context).addAccountToHealingList(any());
      verifyNoInteractions(ethContext);
    }

    // The tests below run the real retrying peer task: only the peer's answer is canned.
    @Test
    void aNoStorageAnswerForAStorageRootFromAnOlderPivotRequestsARefresh() {
      when(context.getRefreshedStorageRoot(any())).thenReturn(Optional.empty());
      final StorageRangeDataRequest request = fullRangeRequest(OLDER_STATE_ROOT);

      sendWithPeerAnswering(noStorageAnswer(), request);

      assertThat(request.isResponseReceived()).isFalse();
      verify(context).requestStorageRootRefresh(accountHash);
    }

    @Test
    void aNoStorageAnswerForAStorageRootValidAtThePivotDoesNotRequestARefresh() {
      when(context.getRefreshedStorageRoot(any())).thenReturn(Optional.empty());
      final StorageRangeDataRequest request = fullRangeRequest(stateRoot);

      sendWithPeerAnswering(noStorageAnswer(), request);

      assertThat(request.isResponseReceived()).isFalse();
      verify(context, never()).requestStorageRootRefresh(any());
    }

    @Test
    void aNoStorageAnswerAfterARefreshForAnotherPivotRequestsARefresh() {
      // a refresh proves nothing for a pivot it was not made at
      when(context.getRefreshedStorageRoot(accountHash))
          .thenReturn(
              Optional.of(
                  new SnapRequestContext.RefreshedStorageRoot(
                      NEWER_STATE_ROOT, MerkleTrie.EMPTY_TRIE_NODE_HASH)));
      final StorageRangeDataRequest request = fullRangeRequest(OLDER_STATE_ROOT);

      sendWithPeerAnswering(noStorageAnswer(), request);

      assertThat(request.isResponseReceived()).isFalse();
      verify(context).requestStorageRootRefresh(accountHash);
    }

    // what a peer answers to a full range request for an account without storage: no slots and no
    // proofs
    private MessageData noStorageAnswer() {
      return StorageRangeMessage.create(new ArrayDeque<>(), List.of())
          .wrapMessageData(BigInteger.ONE);
    }

    // Sends the request through the real retrying peer task. Only the peer is faked: it is the one
    // peer available and it answers the request with the given message.
    private void sendWithPeerAnswering(
        final MessageData answer, final StorageRangeDataRequest request) {
      final EthPeers ethPeers = mock(EthPeers.class);
      final EthPeer peer = mock(EthPeer.class);
      final ChainState chainState = mock(ChainState.class);
      when(chainState.getEstimatedTotalDifficulty()).thenReturn(Difficulty.ZERO);
      when(peer.chainState()).thenReturn(chainState);
      when(peer.getReputation()).thenReturn(mock(PeerReputation.class));
      when(peer.getConnection()).thenReturn(mock(PeerConnection.class));
      when(peer.isServingSnap()).thenReturn(true);
      when(ethPeers.streamBestPeers())
          .thenAnswer(invocation -> Stream.of(EthPeerImmutableAttributes.from(peer)));

      final ResponseStream responseStream = mock(ResponseStream.class);
      when(responseStream.getPeer()).thenReturn(peer);
      when(responseStream.then(any()))
          .thenAnswer(
              invocation -> {
                final ResponseCallback callback = invocation.getArgument(0);
                callback.exec(false, answer, peer);
                return responseStream;
              });
      final PendingPeerRequest pendingRequest = mock(PendingPeerRequest.class);
      when(pendingRequest.abort()).thenReturn(Optional.of(responseStream));
      doAnswer(
              invocation -> {
                final Consumer<ResponseStream> onSent = invocation.getArgument(0);
                onSent.accept(responseStream);
                return null;
              })
          .when(pendingRequest)
          .then(any(), any());
      when(ethPeers.executePeerRequest(any(), anyLong(), any())).thenReturn(pendingRequest);

      when(ethContext.getEthPeers()).thenReturn(ethPeers);
      when(ethContext.getScheduler()).thenReturn(new DeterministicEthScheduler());

      final List<Task<SnapDataRequest>> tasks = List.of(new StubTask(request));

      assertThat(requestDataStep.requestStorage(tasks).join()).isSameAs(tasks);
    }

    private StorageRangeDataRequest fullRangeRequest(final Hash knownAtStateRoot) {
      return SnapDataRequest.createStorageRangeDataRequest(
          knownAtStateRoot,
          Bytes32.wrap(accountHash.getBytes()),
          storageRoot,
          MIN_RANGE,
          MAX_RANGE);
    }
  }

  @Nested
  class Restart {
    @Test
    void savedAccountRangesWithTheSameBoundsLoadOnce() {
      final SnapSyncStatePersistenceManager persistence =
          new SnapSyncStatePersistenceManager(new InMemoryKeyValueStorageProvider());
      final Bytes32 key = Bytes32.wrap(accountHash.getBytes());
      final List<AccountRangeDataRequest> saved = new ArrayList<>();
      // re-fetches saved by older versions: same account, different state roots
      for (int i = 0; i < 1_000; i++) {
        saved.add(
            SnapDataRequest.createAccountRangeDataRequest(
                Hash.hash(Bytes.ofUnsignedInt(i)), key, key));
      }
      saved.add(SnapDataRequest.createAccountRangeDataRequest(stateRoot, MIN_RANGE, key));
      persistence.updatePersistedTasks(saved);

      final List<AccountRangeDataRequest> loaded = persistence.getCurrentAccountRange();

      assertThat(loaded).hasSize(2);
      assertThat(loaded)
          .extracting(AccountRangeDataRequest::getStartKeyHash)
          .containsExactlyInAnyOrder(key, MIN_RANGE);
    }
  }

  /** The first {@code limit} storage slots of the test account. */
  private NavigableMap<Bytes32, Bytes> slots(final int limit) {
    final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie = storageTrie();
    final RangeStorageEntriesCollector collector =
        RangeStorageEntriesCollector.createCollector(
            MIN_RANGE, MAX_RANGE, limit, Integer.MAX_VALUE);
    final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
    return (TreeMap<Bytes32, Bytes>)
        storageTrie.entriesFrom(
            root ->
                RangeStorageEntriesCollector.collectEntries(collector, visitor, root, MIN_RANGE));
  }

  /** Boundary proofs for a response starting at {@code MIN_RANGE}. */
  private ArrayDeque<Bytes> proofs(final NavigableMap<Bytes32, Bytes> slots) {
    final Bytes32 key = Bytes32.wrap(accountHash.getBytes());
    final List<Bytes> proofs =
        new ArrayList<>(
            worldStateProofProvider.getStorageProofRelatedNodes(storageRoot, key, MIN_RANGE));
    proofs.addAll(
        worldStateProofProvider.getStorageProofRelatedNodes(storageRoot, key, slots.lastKey()));
    return new ArrayDeque<>(proofs);
  }

  private StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie() {
    return new StoredMerklePatriciaTrie<>(
        new StoredNodeFactory<>(
            (location, hash) ->
                worldStateKeyValueStorage.getTrieNode(TrieNodeKey.of(accountHash, location), hash),
            Function.identity(),
            Function.identity()),
        storageRoot);
  }
}
