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

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.util.Subscribers;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

public abstract class PeerPermissions implements AutoCloseable {
  private final Subscribers<PermissionsUpdateCallback> updateSubscribers = Subscribers.create();

  public static final PeerPermissions NOOP = new NoopPeerPermissions();

  public static PeerPermissions noop() {
    return NOOP;
  }

  public static PeerPermissions combine(final PeerPermissions... permissions) {
    return combine(Arrays.asList(permissions));
  }

  public static PeerPermissions combine(final List<PeerPermissions> permissions) {
    return CombinedPeerPermissions.create(permissions);
  }

  // Defines what actions can be queried for permissions checks
  public enum Action {
    DISCOVERY_ALLOW_IN_PEER_TABLE,
    DISCOVERY_ACCEPT_INBOUND_BONDING,
    DISCOVERY_ALLOW_OUTBOUND_BONDING,
    DISCOVERY_SERVE_INBOUND_NEIGHBORS_REQUEST,
    DISCOVERY_ALLOW_OUTBOUND_NEIGHBORS_REQUEST,
    RLPX_ALLOW_NEW_INBOUND_CONNECTION,
    RLPX_ALLOW_NEW_OUTBOUND_CONNECTION,
    RLPX_ALLOW_ONGOING_REMOTELY_INITIATED_CONNECTION,
    RLPX_ALLOW_ONGOING_LOCALLY_INITIATED_CONNECTION
  }

  /**
   * Checks whether the local node is permitted to engage in some action with the given remote peer.
   *
   * @param localNode The local node that is querying for permissions.
   * @param remotePeer The remote peer that the local node is checking for permissions
   * @param action The action for which the local node is checking permissions
   * @return {@code true} If the given action is allowed with the given peer
   */
  public abstract boolean isPermitted(
      final Peer localNode, final Peer remotePeer, final Action action);

  /**
   * Checks whether a connection to/from the given address is permitted. This is a pre-identity
   * IP-level check that can be used before the remote peer's node ID is known.
   *
   * @param address the remote socket address to check
   * @return {@code true} if the address is permitted (default: always true)
   */
  public boolean isPermitted(final InetSocketAddress address) {
    return true;
  }

  @Override
  public void close() {
    // Do nothing by default
  }

  public void subscribeUpdate(final PermissionsUpdateCallback callback) {
    updateSubscribers.subscribe(callback);
  }

  protected void dispatchUpdate(
      final boolean permissionsRestricted, final Optional<List<Peer>> affectedPeers) {
    updateSubscribers.forEach(s -> s.onUpdate(permissionsRestricted, affectedPeers));
  }

  private static class NoopPeerPermissions extends PeerPermissions {
    @Override
    public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
      return true;
    }
  }

  private static class CombinedPeerPermissions extends PeerPermissions {
    private final ImmutableList<PeerPermissions> permissions;

    private CombinedPeerPermissions(final ImmutableList<PeerPermissions> permissions) {
      this.permissions = permissions;
    }

    public static PeerPermissions create(final List<PeerPermissions> permissions) {
      final ImmutableList<PeerPermissions> filteredPermissions =
          permissions.stream()
              .flatMap(
                  p -> {
                    if (p instanceof CombinedPeerPermissions) {
                      return ((CombinedPeerPermissions) p).permissions.stream();
                    } else {
                      return Stream.of(p);
                    }
                  })
              .filter(p -> !(p instanceof NoopPeerPermissions))
              .collect(ImmutableList.toImmutableList());

      if (filteredPermissions.isEmpty()) {
        return PeerPermissions.NOOP;
      } else if (filteredPermissions.size() == 1) {
        return filteredPermissions.get(0);
      } else {
        return new CombinedPeerPermissions(filteredPermissions);
      }
    }

    @Override
    public void subscribeUpdate(final PermissionsUpdateCallback callback) {
      for (final PeerPermissions permission : permissions) {
        permission.subscribeUpdate(callback);
      }
    }

    @Override
    public boolean isPermitted(final Peer localNode, final Peer remotePeer, final Action action) {
      for (final PeerPermissions permission : permissions) {
        if (!permission.isPermitted(localNode, remotePeer, action)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean isPermitted(final InetSocketAddress address) {
      for (final PeerPermissions permission : permissions) {
        if (!permission.isPermitted(address)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void close() {
      for (final PeerPermissions permission : permissions) {
        permission.close();
      }
    }
  }
}
