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
package org.hyperledger.besu.ethereum.p2p.network;

import org.hyperledger.besu.ethereum.p2p.peers.MaintainedPeers;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerDenylistManager implements DisconnectCallback {
  private static final Logger LOG = LoggerFactory.getLogger(PeerDenylistManager.class);
  private static final Set<DisconnectReason> locallyTriggeredDisconnectReasons =
      ImmutableSet.of(
          DisconnectReason.BREACH_OF_PROTOCOL,
          DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION,
          DisconnectReason.SUBPROTOCOL_TRIGGERED_MISMATCHED_NETWORK,
          DisconnectReason.SUBPROTOCOL_TRIGGERED_MISMATCHED_GENESIS_HASH,
          DisconnectReason.NULL_NODE_ID,
          DisconnectReason.LOCAL_IDENTITY,
          DisconnectReason.UNEXPECTED_ID);

  private static final Set<DisconnectReason> remotelyTriggeredDisconnectReasons =
      ImmutableSet.of(DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION);

  private final PeerPermissionsDenylist denylist;

  private final MaintainedPeers maintainedPeers;

  public PeerDenylistManager(
      final PeerPermissionsDenylist denylist, final MaintainedPeers maintainedPeers) {
    this.denylist = denylist;
    this.maintainedPeers = maintainedPeers;
  }

  @Override
  public void onDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    if (shouldBlock(reason, initiatedByPeer)) {
      if (maintainedPeers.contains(connection.getPeer())) {
        LOG.debug(
            "Skip adding maintained peer {} to peer denylist for reason {}",
            connection,
            reason.name());
      } else {
        LOG.debug("Added peer {} to peer denylist for reason {}", connection, reason.name());
        denylist.add(connection.getPeer());
      }
    }
  }

  private boolean shouldBlock(final DisconnectReason reason, final boolean initiatedByPeer) {
    final DisconnectReason normalized = DisconnectReason.forCode(reason.getCode());
    return (!initiatedByPeer
            && (locallyTriggeredDisconnectReasons.contains(reason)
                || locallyTriggeredDisconnectReasons.contains(normalized)))
        || (initiatedByPeer
            && (remotelyTriggeredDisconnectReasons.contains(reason)
                || remotelyTriggeredDisconnectReasons.contains(normalized)));
  }
}
