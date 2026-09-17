/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.p2p.peers;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.util.Subscribers;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/** Represents a set of peers for which connections should be actively maintained. */
public class MaintainedPeers {
  private final Set<Peer> maintainedPeers = ConcurrentHashMap.newKeySet();
  private final Subscribers<PeerAddedCallback> addedSubscribers = Subscribers.create();
  private final Subscribers<PeerRemovedCallback> removedCallbackSubscribers = Subscribers.create();

  public boolean add(final Peer peer) {
    checkArgument(
        peer.getEnodeURL().isListening(),
        "Invalid enode url.  Enode url must contain a non-zero listening port.");
    final boolean wasAdded = maintainedPeers.add(peer);
    addedSubscribers.forEach(s -> s.onPeerAdded(peer, wasAdded));
    return wasAdded;
  }

  public boolean remove(final Peer peer) {
    final boolean wasRemoved = maintainedPeers.remove(peer);
    removedCallbackSubscribers.forEach(s -> s.onPeerRemoved(peer, wasRemoved));
    return wasRemoved;
  }

  public boolean contains(final Peer peer) {
    return maintainedPeers.contains(peer);
  }

  public int size() {
    return maintainedPeers.size();
  }

  public void subscribeAdd(final PeerAddedCallback callback) {
    addedSubscribers.subscribe(callback);
  }

  public void subscribeRemove(final PeerRemovedCallback callback) {
    removedCallbackSubscribers.subscribe(callback);
  }

  public Stream<Peer> streamPeers() {
    return maintainedPeers.stream();
  }

  @FunctionalInterface
  public interface PeerAddedCallback {
    void onPeerAdded(Peer peer, boolean wasAdded);
  }

  @FunctionalInterface
  public interface PeerRemovedCallback {
    void onPeerRemoved(Peer peer, boolean wasRemoved);
  }
}
