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

import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/** Represents a healing request for a storage trie node. */
public class StorageTrieNodeHealingRequest extends TrieNodeHealingRequest {

  final Hash accountHash;

  public StorageTrieNodeHealingRequest(
      final Hash nodeHash, final Hash accountHash, final Hash rootHash, final Bytes location) {
    super(nodeHash, rootHash, location);
    this.accountHash = accountHash;
  }

  @Override
  protected int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    applyForStrategy(
        updater,
        onBonsai -> {
          onBonsai.putAccountStorageTrieNode(getAccountHash(), getLocation(), getNodeHash(), data);
        },
        onForest -> {
          onForest.putAccountStorageTrieNode(getNodeHash(), data);
        });
    return 1;
  }

  @Override
  public Optional<Bytes> getExistingData(
      final WorldStateStorageCoordinator worldStateStorageCoordinator) {
    return worldStateStorageCoordinator.getAccountStorageTrieNode(
        getAccountHash(), getLocation(), getNodeHash());
  }

  @Override
  protected SnapDataRequest createChildNodeDataRequest(final Hash childHash, final Bytes location) {
    return SnapDataRequest.createStorageTrieNodeDataRequest(
        childHash, getAccountHash(), getRootHash(), location);
  }

  @Override
  protected Stream<SnapDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapWorldDownloadState downloadState,
      final Bytes location,
      final Bytes path,
      final Bytes value) {
    worldStateStorageCoordinator.applyWhenFlatModeEnabled(
        onBonsai -> {
          onBonsai
              .updater()
              .putStorageValueBySlotHash(
                  accountHash, getSlotHash(location, path), Bytes32.leftPad(RLP.decodeValue(value)))
              .commit();
        });
    return Stream.empty();
  }

  public Hash getAccountHash() {
    return accountHash;
  }

  private Hash getSlotHash(final Bytes location, final Bytes path) {
    return Hash.wrap(Bytes32.wrap(CompactEncoding.pathToBytes(Bytes.concatenate(location, path))));
  }

  @Override
  public List<Bytes> getTrieNodePath() {
    return List.of(accountHash.getBytes(), CompactEncoding.encode(getLocation()));
  }
}
