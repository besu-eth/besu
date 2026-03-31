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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.TRIE_NODE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapWorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.patricia.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.services.tasks.TasksPriorityProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public abstract class TrieNodeHealingRequest extends SnapDataRequest
    implements TasksPriorityProvider {

  private final Bytes32 nodeHash;
  private final Bytes location;
  protected Bytes data;

  protected AtomicBoolean requiresPersisting = new AtomicBoolean(true);

  protected TrieNodeHealingRequest(final Hash nodeHash, final Hash rootHash, final Bytes location) {
    super(TRIE_NODE, rootHash);
    this.nodeHash = Bytes32.wrap(nodeHash.getBytes());
    this.location = location;
    this.data = Bytes.EMPTY;
  }

  @Override
  public int persist(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateKeyValueStorage.Updater updater,
      final SnapWorldDownloadState downloadState,
      final SnapSyncProcessState snapSyncState,
      final SnapSyncConfiguration snapSyncConfiguration) {
    if (isExpired(snapSyncState) || pendingChildren.get() > 0) {
      // we do nothing. Our last child will eventually persist us.
      return 0;
    }
    int saved = 0;
    if (requiresPersisting.getAndSet(false)) {
      checkNotNull(data, "Must set data before node can be persisted.");
      saved =
          doPersist(
              worldStateStorageCoordinator,
              updater,
              downloadState,
              snapSyncState,
              snapSyncConfiguration);
    }
    if (possibleParent.isPresent()) {
      return possibleParent
              .get()
              .saveParent(
                  worldStateStorageCoordinator,
                  updater,
                  downloadState,
                  snapSyncState,
                  snapSyncConfiguration)
          + saved;
    }
    return saved;
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(
      final SnapWorldDownloadState downloadState,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState) {
    if (!isResponseReceived()) {
      // If this node hasn't been downloaded yet, we can't return any child data
      return Stream.empty();
    }

    final List<Node<Bytes>> nodes = TrieNodeDecoder.decodeNodes(location, data);
    return nodes.stream()
        .flatMap(
            node -> {
              if (nodeIsHashReferencedDescendant(node)) {
                return Stream.of(
                    createChildNodeDataRequest(
                        Hash.wrap(node.getHash()), node.getLocation().orElse(Bytes.EMPTY)));
              } else {
                return node.getValue()
                    .map(
                        value ->
                            getRequestsFromTrieNodeValue(
                                worldStateStorageCoordinator,
                                downloadState,
                                node.getLocation().orElse(Bytes.EMPTY),
                                node.getPath(),
                                value))
                    .orElseGet(Stream::empty);
              }
            })
        .peek(request -> request.registerParent(this));
  }

  public boolean isRoot() {
    return possibleParent.isEmpty();
  }

  @Override
  public boolean isResponseReceived() {
    return !data.isEmpty() && Hash.hash(data).getBytes().equals(getNodeHash());
  }

  @Override
  public void clear() {
    setData(Bytes.EMPTY);
  }

  @Override
  public boolean isExpired(final SnapSyncProcessState snapSyncState) {
    return snapSyncState.isExpired(this);
  }

  public boolean isRequiresPersisting() {
    return requiresPersisting.get();
  }

  public Bytes32 getNodeHash() {
    return nodeHash;
  }

  public Bytes getLocation() {
    return location;
  }

  @Override
  public int getDepth() {
    return depth;
  }

  @Override
  public long getPriority() {
    return priority;
  }

  public Bytes getPathId() {
    return Bytes.concatenate(new ArrayList<>(getTrieNodePath()));
  }

  public void setData(final Bytes data) {
    this.data = data;
  }

  public void setRequiresPersisting(final boolean requiresPersisting) {
    this.requiresPersisting.set(requiresPersisting);
  }

  private boolean nodeIsHashReferencedDescendant(final Node<Bytes> node) {
    return !Objects.equals(node.getHash(), nodeHash) && node.isReferencedByHash();
  }

  public abstract Optional<Bytes> getExistingData(
      final WorldStateStorageCoordinator worldStateStorageCoordinator);

  public abstract List<Bytes> getTrieNodePath();

  protected abstract SnapDataRequest createChildNodeDataRequest(
      final Hash childHash, final Bytes location);

  public Stream<SnapDataRequest> getRootStorageRequests(
      final WorldStateStorageCoordinator worldStateStorageCoordinator) {
    return Stream.empty();
  }

  protected abstract Stream<SnapDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapWorldDownloadState downloadState,
      final Bytes location,
      final Bytes path,
      final Bytes value);
}
