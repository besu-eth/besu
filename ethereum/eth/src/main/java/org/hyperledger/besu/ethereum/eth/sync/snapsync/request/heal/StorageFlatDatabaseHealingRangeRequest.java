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

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.STORAGE_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.getRangeCount;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/**
 * The StorageFlatDatabaseHealingRangeRequest class represents a request to heal a range of storage
 * in the flat databases. It encapsulates the necessary information to identify the range and
 * initiate the healing process.
 */
public class StorageFlatDatabaseHealingRangeRequest extends SnapDataRequest {

  private final Hash accountHash;
  private final Bytes32 storageRoot;
  private final Bytes32 startKeyHash;
  private final Bytes32 endKeyHash;
  private NavigableMap<Bytes32, Bytes> slots;
  private boolean isProofValid;

  public StorageFlatDatabaseHealingRangeRequest(
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
    this.isProofValid = false;
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    final List<SnapDataRequest> childRequests = new ArrayList<>();
    if (!slots.isEmpty()) {
      // new request is added if the response does not match all the requested range
      final int nbRanges = getRangeCount(startKeyHash, endKeyHash, slots);
      RangeManager.generateRanges(
              slots.lastKey().toUnsignedBigInteger().add(BigInteger.ONE),
              endKeyHash.toUnsignedBigInteger(),
              nbRanges)
          .forEach(
              (key, value) -> {
                final StorageFlatDatabaseHealingRangeRequest storageRangeDataRequest =
                    createStorageFlatHealingRangeRequest(
                        getRootHash(),
                        Bytes32.wrap(accountHash.getBytes()),
                        storageRoot,
                        key,
                        value);
                childRequests.add(storageRangeDataRequest);
              });
    }
    return childRequests.stream();
  }

  public Hash getAccountHash() {
    return accountHash;
  }

  public Bytes32 getStorageRoot() {
    return storageRoot;
  }

  public Bytes32 getStartKeyHash() {
    return startKeyHash;
  }

  public Bytes32 getEndKeyHash() {
    return endKeyHash;
  }

  @Override
  public boolean isResponseReceived() {
    return true;
  }

  public void addLocalData(
      final WorldStateProofProvider worldStateProofProvider,
      final NavigableMap<Bytes32, Bytes> slots,
      final ArrayDeque<Bytes> proofs) {
    if (!slots.isEmpty() && !proofs.isEmpty()) {
      // verify proof in order to check if the local flat database is valid or not
      isProofValid =
          worldStateProofProvider.isValidRangeProof(
              startKeyHash, endKeyHash, storageRoot, proofs, slots);
    }
    this.slots = slots;
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {

    if (!isProofValid) {
      // If the proof is not valid, it indicates that the flat database needs to be fixed.

      final BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater =
          (BonsaiWorldStateKeyValueStorage.Updater) updater;

      final MerkleTrie<Bytes, Bytes> storageTrie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) ->
                  worldStateStorageCoordinator.getAccountStorageTrieNode(
                      accountHash, location, hash),
              storageRoot,
              Function.identity(),
              Function.identity());

      Map<Bytes32, Bytes> flatDbSlots = new TreeMap<>(slots);

      // Retrieve the data from the trie in order to know what needs to be fixed in the flat
      // database
      final RangeStorageEntriesCollector collector =
          RangeStorageEntriesCollector.createCollector(
              startKeyHash,
              slots.isEmpty() ? endKeyHash : slots.lastKey(),
              slots.isEmpty()
                  ? snapSyncConfiguration.getLocalFlatStorageCountToHealPerRequest()
                  : Integer.MAX_VALUE,
              Integer.MAX_VALUE);
      final TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
      slots =
          (TreeMap<Bytes32, Bytes>)
              storageTrie.entriesFrom(
                  root ->
                      RangeStorageEntriesCollector.collectEntries(
                          collector, visitor, root, startKeyHash));

      // Process each slot
      slots.forEach(
          (key, value) -> {
            // Remove the key from the flat db and get its associated value
            final Bytes flatDbEntry = flatDbSlots.remove(key);
            // If the key was not in flat db and its associated value is different from the
            // current value
            if (!value.equals(flatDbEntry)) {
              // Update the storage value
              bonsaiUpdater.putStorageValueBySlotHash(
                  accountHash, Hash.wrap(key), Bytes32.leftPad(RLP.decodeValue(value)));
            }
          });
      // For each remaining key, remove the storage value
      flatDbSlots
          .keySet()
          .forEach(key -> bonsaiUpdater.removeStorageValueBySlotHash(accountHash, Hash.wrap(key)));
    }
    return slots.size();
  }
}
