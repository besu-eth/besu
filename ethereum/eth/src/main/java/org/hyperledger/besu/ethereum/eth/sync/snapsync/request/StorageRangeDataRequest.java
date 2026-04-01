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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.STORAGE_RANGE;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie.FlatDatabaseUpdater.noop;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.findNewBeginElementInRange;
import static org.hyperledger.besu.ethereum.trie.RangeManager.getRangeCount;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.StackTrie;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Returns a list of storages and the merkle proofs of an entire range */
public class StorageRangeDataRequest extends SnapDataRequest {

  private static final Logger LOG = LoggerFactory.getLogger(StorageRangeDataRequest.class);

  private final Hash accountHash;
  private final Bytes32 storageRoot;
  private final Bytes32 startKeyHash;
  private final Bytes32 endKeyHash;

  private final StackTrie stackTrie;
  private Optional<Boolean> isProofValid;

  protected StorageRangeDataRequest(
      final Hash rootHash,
      final Bytes32 accountHash,
      final Bytes32 storageRoot,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    super(STORAGE_RANGE, rootHash);
    this.accountHash = Hash.wrap(accountHash);
    this.storageRoot = storageRoot;
    this.startKeyHash = startKeyHash;
    this.endKeyHash = endKeyHash;
    this.isProofValid = Optional.empty();
    this.stackTrie = new StackTrie(Hash.wrap(getStorageRoot()), startKeyHash);
    LOG.trace(
        "create get storage range data request for account {} with root hash={} from {} to {}",
        accountHash,
        rootHash,
        startKeyHash,
        endKeyHash);
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    // search incomplete nodes in the range
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
                          accountHash, Hash.wrap(key), Bytes32.leftPad(RLP.decodeValue(value))));
        });

    stackTrie.commit(flatDatabaseUpdater.get(), nodeUpdater);

    downloadState.getMetricsManager().notifySlotsDownloaded(stackTrie.getElementsCount().get());

    return nbNodesSaved.get();
  }

  public void addResponse(
      final SnapWorldDownloadState downloadState,
      final WorldStateProofProvider worldStateProofProvider,
      final NavigableMap<Bytes32, Bytes> slots,
      final ArrayDeque<Bytes> proofs) {
    if (!slots.isEmpty() || !proofs.isEmpty()) {
      if (!worldStateProofProvider.isValidRangeProof(
          startKeyHash, endKeyHash, storageRoot, proofs, slots)) {
        // If the proof is invalid, it means that the storage will be a mix of several blocks.
        // Therefore, it will be necessary to heal the account's storage subsequently
        LOG.atDebug()
            .setMessage("invalid storage range proof received for account hash {} range {} {}")
            .addArgument(() -> accountHash)
            .addArgument(() -> slots.isEmpty() ? "none" : slots.firstKey())
            .addArgument(() -> slots.isEmpty() ? "none" : slots.lastKey())
            .log();

        downloadState.addAccountToHealingList(CompactEncoding.bytesToPath(accountHash.getBytes()));
        // We will request the new storage root of the account because it is apparently no longer
        // valid with the new pivot block.
        downloadState.enqueueRequest(
            createAccountDataRequest(getRootHash(), accountHash, startKeyHash, endKeyHash));
        isProofValid = Optional.of(false);
      } else {
        stackTrie.addElement(startKeyHash, proofs, slots);
        isProofValid = Optional.of(true);
      }
    }
  }

  @Override
  public boolean isResponseReceived() {
    return isProofValid.isPresent();
  }

  @Override
  public boolean isExpired(final SnapSyncProcessState snapSyncState) {
    return snapSyncState.isExpired(this);
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    final List<SnapDataRequest> childRequests = new ArrayList<>();

    if (!isProofValid.orElse(false)) {
      return Stream.empty();
    }

    final StackTrie.TaskElement taskElement = stackTrie.getElement(startKeyHash);

    if (null == taskElement) {
      return Stream.empty();
    }

    findNewBeginElementInRange(storageRoot, taskElement.proofs(), taskElement.keys(), endKeyHash)
        .ifPresent(
            missingRightElement -> {
              final int nbRanges = getRangeCount(startKeyHash, endKeyHash, taskElement.keys());
              RangeManager.generateRanges(missingRightElement, endKeyHash, nbRanges)
                  .forEach(
                      (key, value) -> {
                        final StorageRangeDataRequest storageRangeDataRequest =
                            createStorageRangeDataRequest(
                                getRootHash(),
                                Bytes32.wrap(accountHash.getBytes()),
                                storageRoot,
                                key,
                                value);
                        childRequests.add(storageRangeDataRequest);
                      });
            });

    if (startKeyHash.equals(MIN_RANGE) && !taskElement.proofs().isEmpty()) {
      // need to heal this account storage
      downloadState.addAccountToHealingList(CompactEncoding.bytesToPath(accountHash.getBytes()));
    }

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

  public Bytes32 getStartKeyHash() {
    return startKeyHash;
  }

  public Bytes32 getEndKeyHash() {
    return endKeyHash;
  }

  @Override
  public void clear() {
    this.isProofValid = Optional.of(false);
    this.stackTrie.removeElement(startKeyHash);
  }

  @VisibleForTesting
  public void setProofValid(final boolean isProofValid) {
    this.isProofValid = Optional.of(isProofValid);
  }
}
