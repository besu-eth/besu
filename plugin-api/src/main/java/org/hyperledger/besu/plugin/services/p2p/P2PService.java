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
package org.hyperledger.besu.plugin.services.p2p;

import org.hyperledger.besu.datatypes.p2p.MessageData;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.p2p.Capability;
import org.hyperledger.besu.plugin.data.p2p.Message;
import org.hyperledger.besu.plugin.data.p2p.Peer;
import org.hyperledger.besu.plugin.data.p2p.PeerConnection;
import org.hyperledger.besu.plugin.services.BesuService;

import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;

/** Service to enable and disable P2P service. */
@Unstable
public interface P2PService extends BesuService {

  /** Enables P2P discovery. */
  void enableDiscovery();

  /** Disables P2P discovery. */
  void disableDiscovery();

  /**
   * Returns whether the P2P network is enabled. When disabled (e.g. started with {@code
   * --p2p-enabled=false}), peer-count checks should be skipped because no peers will ever connect.
   *
   * @return {@code true} if the P2P network is enabled, {@code false} otherwise
   */
  boolean isP2pEnabled();

  /**
   * Get the number of connected peers.
   *
   * @return the number of connected peers
   */
  int getPeerCount();

  /**
   * Get all connected peer connections.
   *
   * @return list of connected peer connections
   */
  Collection<? extends PeerConnection> getPeerConnections();

  /**
   * Get all maintained connection peers.
   *
   * @return list of maintained connection peers
   */
  Collection<? extends Peer> getMaintainedConnectionPeers();

  /**
   * Subscribe to connection events.
   *
   * @param networkSubscriber the subscriber to receive connection events
   */
  void subscribeConnect(ConnectionListener networkSubscriber);

  /**
   * Subscribe to disconnection events.
   *
   * @param networkSubscriber the subscriber to receive disconnection events
   */
  void subscribeDisconnect(DisconnectionListener networkSubscriber);

  /**
   * Subscribe to messages on a specific capability.
   *
   * @param capability the capability to subscribe to
   * @param networkSubscriber the subscriber to receive messages for the specified capability
   */
  void subscribeMessage(final Capability capability, final MessageListener networkSubscriber);

  /**
   * Send a message to a specific peer.
   *
   * @param protocol the protocol to use
   * @param peerId the peer id to send the message to
   * @param messageData the message data to send
   * @throws PeerConnection.PeerNotConnected if the peer is not connected
   */
  @Unstable
  void send(String protocol, Bytes peerId, MessageData messageData)
      throws PeerConnection.PeerNotConnected;

  /**
   * Disconnect from a specific peer.
   *
   * @param peerId the peer id to disconnect from
   */
  @Unstable
  void disconnect(Bytes peerId);

  /** Consumer of network connection events. */
  @FunctionalInterface
  interface ConnectionListener {
    /**
     * Called when a new peer connection is established.
     *
     * @param connection the newly established peer connection
     */
    void onConnect(final PeerConnection connection);
  }

  /** Consumer of network disconnection events. */
  @FunctionalInterface
  interface DisconnectionListener {
    /**
     * Called when a peer connection is terminated.
     *
     * @param connection the connection that was closed
     * @param disconnectionCode the code indicating the reason for disconnection
     * @param disconnectionMessage a human-readable message explaining the disconnection reason
     * @param initiatedByPeer {@code true} if the remote peer initiated the disconnect
     */
    void onDisconnect(
        final PeerConnection connection,
        final Bytes disconnectionCode,
        final String disconnectionMessage,
        final boolean initiatedByPeer);
  }

  /** Consumer of network message events. */
  @FunctionalInterface
  interface MessageListener {
    /**
     * Called for each incoming message on a subscribed capability.
     *
     * @param capability the capability of the message
     * @param message the message
     */
    void onMessage(final Capability capability, final Message message);
  }
}
