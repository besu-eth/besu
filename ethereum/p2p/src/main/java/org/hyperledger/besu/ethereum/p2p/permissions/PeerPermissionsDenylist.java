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
package org.hyperledger.besu.ethereum.p2p.permissions;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.util.LimitedSet;
import org.hyperledger.besu.util.LimitedSet.Mode;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.tuweni.bytes.Bytes;

public class PeerPermissionsDenylist extends PeerPermissions {
  private static final int DEFAULT_INITIAL_CAPACITY = 20;

  private final Set<Bytes> denylist;

  private PeerPermissionsDenylist(final int initialCapacity, final OptionalInt maxSize) {
    if (maxSize.isPresent()) {
      denylist =
          LimitedSet.create(initialCapacity, maxSize.getAsInt(), Mode.DROP_LEAST_RECENTLY_ACCESSED);
    } else {
      denylist = new ConcurrentHashSet<>(initialCapacity);
    }
  }

  private PeerPermissionsDenylist(final OptionalInt maxSize) {
    this(DEFAULT_INITIAL_CAPACITY, maxSize);
  }

  public static PeerPermissionsDenylist create() {
    return new PeerPermissionsDenylist(OptionalInt.empty());
  }

  public static PeerPermissionsDenylist create(final int maxSize) {
    return new PeerPermissionsDenylist(OptionalInt.of(maxSize));
  }

  @Override
  public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
    return !EnodeURLImpl.sameListeningEndpoint(localNode.getEnodeURL(), remotePeer.getEnodeURL())
        && !denylist.contains(remotePeer.getId());
  }

  @Override
  public boolean isPermitted(final Peer localNode, final PeerId remotePeerId, final Action action) {
    return !denylist.contains(remotePeerId.getId());
  }

  public void add(final Peer peer) {
    if (denylist.add(peer.getId())) {
      dispatchUpdate(true, Optional.of(Collections.singletonList(peer)));
    }
  }

  public void remove(final Peer peer) {
    if (denylist.remove(peer.getId())) {
      dispatchUpdate(false, Optional.of(Collections.singletonList(peer)));
    }
  }

  public void add(final Bytes peerId) {
    if (denylist.add(peerId)) {
      dispatchUpdate(true, Optional.empty());
    }
  }

  public void remove(final Bytes peerId) {
    if (denylist.remove(peerId)) {
      dispatchUpdate(false, Optional.empty());
    }
  }

  @Override
  public void close() {
    denylist.clear();
  }
}
