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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.STORAGE_RANGE;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie.FlatDatabaseUpdater.noop;
import static org.hyperledger.besu.ethereum.trie.RangeManager.findNewBeginElementInRange;
import static org.hyperledger.besu.ethereum.trie.RangeManager.getRangeCount;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.v2.SnapV2DataRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Snap/2 storage range data request. Commits all trie nodes including incomplete ones. */
public class SnapV2StorageRangeRequest extends SnapV2DataRequest {

  private final Hash accountHash;
  private final Bytes32 storageRoot;
  private final Bytes32 startKeyHash;
  private final Bytes32 endKeyHash;
  private final StackTrie stackTrie;
  private ResponseProofStatus responseProofStatus;

  public SnapV2StorageRangeRequest(
      final BlockHeader pivotBlockHeader,
      final Bytes32 accountHash,
      final Bytes32 storageRoot,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final Bytes32 rangeStart) {
    super(STORAGE_RANGE, pivotBlockHeader, rangeStart);
    this.accountHash = Hash.wrap(accountHash);
    this.storageRoot = storageRoot;
    this.startKeyHash = startKeyHash;
    this.endKeyHash = endKeyHash;
    this.responseProofStatus = ResponseProofStatus.PENDING;
    this.stackTrie = new StackTrie(Hash.wrap(getStorageRoot()), startKeyHash);
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapRequestContext downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    final AtomicInteger nbNodesSaved = new AtomicInteger();
    final NodeUpdater nodeUpdater =
        (location, hash, value) -> {
          applyForStrategy(
              updater,
              onBonsai -> onBonsai.putAccountStorageTrieNode(accountHash, location, hash, value),
              onForest -> onForest.putAccountStorageTrieNode(hash, value));
          nbNodesSaved.incrementAndGet();
        };

    final AtomicReference<StackTrie.FlatDatabaseUpdater> flatDatabaseUpdater =
        new AtomicReference<>(noop());

    worldStateStorageCoordinator.applyOnMatchingFlatModes(
        List.of(FlatDbMode.FULL, FlatDbMode.ARCHIVE),
        bonsaiWorldStateStorageStrategy -> {
          flatDatabaseUpdater.set(
              (key, value) ->
                  ((BonsaiWorldStateKeyValueStorage.Updater) updater)
                      .putStorageValueBySlotHash(
                          accountHash,
                          Hash.wrap(key),
                          Bytes32.leftPad(org.apache.tuweni.rlp.RLP.decodeValue(value))));
        });

    stackTrie.commit(flatDatabaseUpdater.get(), nodeUpdater, true);
    downloadState.getMetricsManager().notifySlotsDownloaded(stackTrie.getElementsCount().get());
    return nbNodesSaved.get();
  }

  public void addResponse(
      final SnapRequestContext downloadState,
      final WorldStateProofProvider worldStateProofProvider,
      final NavigableMap<Bytes32, Bytes> slots,
      final ArrayDeque<Bytes> proofs) {
    if (!slots.isEmpty() || !proofs.isEmpty()) {
      if (!worldStateProofProvider.isValidRangeProof(
          startKeyHash, endKeyHash, storageRoot, proofs, slots)) {
        responseProofStatus = ResponseProofStatus.INVALID;
      } else {
        stackTrie.addElement(startKeyHash, proofs, slots);
        responseProofStatus = ResponseProofStatus.VALID;
      }
    }
  }

  @Override
  public boolean isResponseReceived() {
    return responseProofStatus == ResponseProofStatus.VALID;
  }

  public boolean hasInvalidProof() {
    return responseProofStatus == ResponseProofStatus.INVALID;
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapRequestContext downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    if (responseProofStatus != ResponseProofStatus.VALID) {
      return Stream.empty();
    }

    final List<SnapDataRequest> childRequests = new ArrayList<>();
    final StackTrie.TaskElement taskElement = stackTrie.getElement(startKeyHash);

    if (taskElement == null) {
      return Stream.empty();
    }

    findNewBeginElementInRange(storageRoot, taskElement.proofs(), taskElement.keys(), endKeyHash)
        .ifPresent(
            missingRightElement -> {
              final int nbRanges = getRangeCount(startKeyHash, endKeyHash, taskElement.keys());
              RangeManager.generateRanges(missingRightElement, endKeyHash, nbRanges)
                  .forEach(
                      (key, value) -> {
                        childRequests.add(
                            new SnapV2StorageRangeRequest(
                                getPivotBlockHeader(),
                                Bytes32.wrap(accountHash.getBytes()),
                                storageRoot,
                                key,
                                value,
                                getRangeStart()));
                      });
            });

    return childRequests.stream();
  }

  public Hash getAccountHash() {
    return accountHash;
  }

  public Bytes32 getStorageRoot() {
    return storageRoot;
  }

  public NavigableMap<Bytes32, Bytes> getSlots() {
    return stackTrie.getElement(startKeyHash).keys();
  }

  public SnapV2StorageRangeRequest retarget(
      final BlockHeader newPivotBlockHeader, final Bytes32 newStorageRoot) {
    return new SnapV2StorageRangeRequest(
        newPivotBlockHeader,
        Bytes32.wrap(accountHash.getBytes()),
        newStorageRoot,
        startKeyHash,
        endKeyHash,
        getRangeStart());
  }

  public Bytes32 getStartKeyHash() {
    return startKeyHash;
  }

  public Bytes32 getEndKeyHash() {
    return endKeyHash;
  }

  @Override
  public void clear() {
    this.responseProofStatus = ResponseProofStatus.PENDING;
    this.stackTrie.removeElement(startKeyHash);
  }
}
