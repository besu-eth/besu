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
package org.hyperledger.besu.ethereum.permissioning.node;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.util.Subscribers;

import java.util.Collection;
import java.util.Optional;

/**
 * A permissioning provider that only provides an answer when we have no peers outside of our
 * bootnodes
 */
public class InsufficientPeersPermissioningProvider implements ContextualNodePermissioningProvider {
  private final P2PNetwork p2pNetwork;
  private final Collection<? extends NodeIdentifier> bootnodeIdentifiers;
  private long nonBootnodePeerConnections;
  private final Subscribers<Runnable> permissioningUpdateSubscribers = Subscribers.create();

  /**
   * Creates the provider observing the provided p2p network
   *
   * @param p2pNetwork the p2p network to observe
   * @param bootnodeIdentifiers the bootnodes that this node is configured to connect to
   */
  public InsufficientPeersPermissioningProvider(
      final P2PNetwork p2pNetwork, final Collection<? extends NodeIdentifier> bootnodeIdentifiers) {
    this.p2pNetwork = p2pNetwork;
    this.bootnodeIdentifiers = bootnodeIdentifiers;
    this.nonBootnodePeerConnections = countP2PNetworkNonBootnodeConnections();
    p2pNetwork.subscribeConnect(this::handleConnect);
    p2pNetwork.subscribeDisconnect(this::handleDisconnect);
  }

  private boolean isNotABootnode(final PeerConnection peerConnection) {
    return bootnodeIdentifiers.stream()
        .noneMatch(
            (bootNode) ->
                EnodeURLImpl.sameListeningEndpoint(peerConnection.getRemoteEnode(), bootNode));
  }

  private long countP2PNetworkNonBootnodeConnections() {
    return p2pNetwork.getPeers().stream().filter(this::isNotABootnode).count();
  }

  @Override
  public Optional<Boolean> isPermitted(
      final NodeIdentifier sourceEnode, final NodeIdentifier destinationEnode) {
    final Optional<EnodeURLImpl> maybeSelfEnode = p2pNetwork.getLocalEnode();
    if (nonBootnodePeerConnections > 0) {
      return Optional.empty();
    } else if (maybeSelfEnode.isEmpty()) {
      // The local node is not yet ready, so we can't validate enodes yet
      return Optional.empty();
    } else if (checkEnode(maybeSelfEnode.get(), sourceEnode)
        && checkEnode(maybeSelfEnode.get(), destinationEnode)) {
      return Optional.of(true);
    } else {
      return Optional.empty();
    }
  }

  private boolean checkEnode(final NodeIdentifier localEnode, final NodeIdentifier enode) {
    return (NodeIdentifier.isSameListeningEndpoint(localEnode, enode)
        || bootnodeIdentifiers.stream()
            .anyMatch(bootNode -> NodeIdentifier.isSameListeningEndpoint(bootNode, enode)));
  }

  private void handleConnect(final PeerConnection peerConnection) {
    if (isNotABootnode(peerConnection)) {
      // if the first non bootnode peer seen
      if (++nonBootnodePeerConnections == 1) {
        permissioningUpdateSubscribers.forEach(Runnable::run);
      }
    }
  }

  private void handleDisconnect(
      final PeerConnection peerConnection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    if (isNotABootnode(peerConnection)) {
      // if we just lost the last non bootnode
      if (--nonBootnodePeerConnections == 0) {
        permissioningUpdateSubscribers.forEach(Runnable::run);
      }
    }
  }

  @Override
  public long subscribeToUpdates(final Runnable callback) {
    return permissioningUpdateSubscribers.subscribe(callback);
  }

  @Override
  public boolean unsubscribeFromUpdates(final long id) {
    return permissioningUpdateSubscribers.unsubscribe(id);
  }
}
