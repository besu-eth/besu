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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.AccountTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageFlatDatabaseHealingRangeRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.StorageTrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.heal.TrieNodeHealingRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.TasksPriorityProvider;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public abstract class SnapDataRequest implements TasksPriorityProvider {

  // Number of children in a hex (Patricia) trie node
  private static final int MAX_CHILDREN = 16;

  protected Optional<TrieNodeHealingRequest> possibleParent = Optional.empty();
  protected int depth;
  protected long priority;
  protected final AtomicInteger pendingChildren = new AtomicInteger(0);

  private final RequestType requestType;
  private Hash rootHash;

  protected SnapDataRequest(final RequestType requestType, final Hash originalRootHash) {
    this.requestType = requestType;
    this.rootHash = originalRootHash;
  }

  public static AccountRangeDataRequest createAccountRangeDataRequest(
      final Hash rootHash, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    return new AccountRangeDataRequest(rootHash, startKeyHash, endKeyHash);
  }

  public static AccountFlatDatabaseHealingRangeRequest createAccountFlatHealingRangeRequest(
      final Hash rootHash, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    return new AccountFlatDatabaseHealingRangeRequest(rootHash, startKeyHash, endKeyHash);
  }

  public static StorageFlatDatabaseHealingRangeRequest createStorageFlatHealingRangeRequest(
      final Hash rootHash,
      final Bytes32 accountHash,
      final Bytes32 storageRoot,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    return new StorageFlatDatabaseHealingRangeRequest(
        rootHash, accountHash, storageRoot, startKeyHash, endKeyHash);
  }

  public static AccountRangeDataRequest createAccountDataRequest(
      final Hash rootHash,
      final Hash accountHash,
      final Bytes32 startStorageRange,
      final Bytes32 endStorageRange) {
    return new AccountRangeDataRequest(rootHash, accountHash, startStorageRange, endStorageRange);
  }

  public static StorageRangeDataRequest createStorageRangeDataRequest(
      final Hash rootHash,
      final Bytes32 accountHash,
      final Bytes32 storageRoot,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    return new StorageRangeDataRequest(
        rootHash, accountHash, storageRoot, startKeyHash, endKeyHash);
  }

  public static AccountTrieNodeHealingRequest createAccountTrieNodeDataRequest(
      final Hash hash, final Bytes location, final Set<Bytes> inconsistentAccounts) {
    return new AccountTrieNodeHealingRequest(hash, hash, location, inconsistentAccounts);
  }

  public static AccountTrieNodeHealingRequest createAccountTrieNodeDataRequest(
      final Hash hash,
      final Hash rootHash,
      final Bytes location,
      final Set<Bytes> inconsistentAccounts) {
    return new AccountTrieNodeHealingRequest(hash, rootHash, location, inconsistentAccounts);
  }

  public static StorageTrieNodeHealingRequest createStorageTrieNodeDataRequest(
      final Hash hash, final Hash accountHash, final Hash rootHash, final Bytes location) {
    return new StorageTrieNodeHealingRequest(hash, accountHash, rootHash, location);
  }

  public static BytecodeRequest createBytecodeRequest(
      final Bytes32 accountHash, final Hash rootHash, final Bytes32 codeHash) {
    return new BytecodeRequest(rootHash, accountHash, codeHash);
  }

  public int persist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    return doPersist(
        worldStateStorageCoordinator, updater, downloadState, snapSyncState, snapSyncConfiguration);
  }

  protected abstract int doPersist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration);

  public abstract boolean isResponseReceived();

  public boolean isExpired(final SnapSyncProcessState snapSyncState) {
    return false;
  }

  public abstract Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState);

  public void registerParent(final TrieNodeHealingRequest parent) {
    if (this.possibleParent.isPresent()) {
      throw new WorldStateDownloaderException("Cannot set parent twice");
    }
    this.possibleParent = Optional.of(parent);
    this.depth = parent.depth + 1;
    this.priority = parent.priority * MAX_CHILDREN + parent.incrementChildren();
  }

  protected int incrementChildren() {
    return pendingChildren.incrementAndGet();
  }

  protected int saveParent(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    if (pendingChildren.decrementAndGet() == 0) {
      return persist(
          worldStateStorageCoordinator,
          updater,
          downloadState,
          snapSyncState,
          snapSyncConfiguration);
    }
    return 0;
  }

  public void clear() {}

  public RequestType getRequestType() {
    return requestType;
  }

  public Hash getRootHash() {
    return rootHash;
  }

  public void setRootHash(final Hash rootHash) {
    this.rootHash = rootHash;
  }

  @Override
  public long getPriority() {
    return 0;
  }

  @Override
  public int getDepth() {
    return 0;
  }
}
