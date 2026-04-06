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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.MaintainedPeers;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionsDenylist;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PeerDenylistManagerTest {
  private int ipModifier;
  private Peer localNode;
  private PeerDenylistManager peerDenylistManager;
  private PeerPermissionsDenylist denylist;
  private MaintainedPeers maintainedPeers;

  @BeforeEach
  public void beforeTest() {
    ipModifier = 1;
    localNode = generatePeer();
    denylist = PeerPermissionsDenylist.create();
    maintainedPeers = new MaintainedPeers();
    peerDenylistManager = new PeerDenylistManager(denylist, maintainedPeers);
  }

  @Test
  public void doesNotDenylistPeerForNormalDisconnect() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);

    peerDenylistManager.onDisconnect(peer, DisconnectReason.TOO_MANY_PEERS, false);

    checkPermissions(denylist, peer.getPeer(), true);
  }

  @Test
  public void denylistPeerForBadBehavior() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(peer, DisconnectReason.BREACH_OF_PROTOCOL, false);
    checkPermissions(denylist, peer.getPeer(), false);
  }

  @Test
  public void denylistPeerForBadBehaviorWithDifferentMessage() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(
        peer, DisconnectReason.BREACH_OF_PROTOCOL_INVALID_MESSAGE_CODE_FOR_PROTOCOL, false);
    checkPermissions(denylist, peer.getPeer(), false);
  }

  @Test
  public void doesNotDenylistPeerForOurBadBehavior() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(peer, DisconnectReason.BREACH_OF_PROTOCOL, true);
    checkPermissions(denylist, peer.getPeer(), true);
  }

  @Test
  public void doesNotDenylistMaintainedPeer() {
    final PeerConnection peer = generatePeerConnection();
    maintainedPeers.add(peer.getPeer());

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(peer, DisconnectReason.BREACH_OF_PROTOCOL, false);
    checkPermissions(denylist, peer.getPeer(), true);
  }

  @Test
  public void denylistIncompatiblePeer() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(
        peer, DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION, false);
    checkPermissions(denylist, peer.getPeer(), false);
  }

  @Test
  public void denylistIncompatiblePeerWhoIssuesDisconnect() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(
        peer, DisconnectReason.INCOMPATIBLE_P2P_PROTOCOL_VERSION, true);
    checkPermissions(denylist, peer.getPeer(), false);
  }

  @Test
  public void disconnectReasonWithEmptyValue_doesNotAddToDenylist() {
    final PeerConnection peer = generatePeerConnection();

    checkPermissions(denylist, peer.getPeer(), true);
    peerDenylistManager.onDisconnect(peer, DisconnectReason.UNKNOWN, false);
    checkPermissions(denylist, peer.getPeer(), true);
  }

  private void checkPermissions(
      final PeerPermissionsDenylist denylist, final Peer remotePeer, final boolean expectedResult) {
    for (PeerPermissions.Action action : PeerPermissions.Action.values()) {
      assertThat(denylist.isPermitted(localNode, remotePeer, action)).isEqualTo(expectedResult);
    }
  }

  private PeerConnection generatePeerConnection() {
    final Bytes nodeId = Peer.randomId();
    final PeerConnection conn = mock(PeerConnection.class);
    final PeerInfo peerInfo = mock(PeerInfo.class);
    final Peer peer = generatePeer();

    when(peerInfo.getNodeId()).thenReturn(nodeId);
    when(conn.getPeerInfo()).thenReturn(peerInfo);
    when(conn.getPeer()).thenReturn(peer);

    return conn;
  }

  private Peer generatePeer() {
    return DefaultPeer.fromEnodeURL(
        EnodeURLImpl.builder()
            .nodeId(Peer.randomId())
            .ipAddress("10.9.8." + ipModifier++)
            .discoveryPort(65535)
            .listeningPort(65534)
            .build());
  }
}
