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

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.util.List;
import java.util.function.Consumer;

/** Represents an object responsible for managing a wire subprotocol. */
public interface ProtocolManager extends AutoCloseable {

  String getSupportedProtocol();

  /**
   * Defines the list of capabilities supported by this manager.
   *
   * @return the list of capabilities supported by this manager
   */
  List<Capability> getSupportedCapabilities();

  /**
   * Subscribes to the capabilities the manager adds once it runs, which only the connections made
   * afterwards may agree on. A manager whose capabilities never change ignores it.
   *
   * @param listener receives each capability added to {@link #getSupportedCapabilities()}
   */
  default void subscribeCapabilityAdded(final Consumer<Capability> listener) {}

  /** Stops the protocol manager. */
  void stop();

  /**
   * Blocks until protocol manager has stopped.
   *
   * @throws InterruptedException if interrupted while waiting
   */
  void awaitStop() throws InterruptedException;

  /**
   * Processes a message from a peer.
   *
   * @param cap the capability that corresponds to the message
   * @param message the message from the peer
   */
  void processMessage(Capability cap, Message message);

  /**
   * Handles new peer connections.
   *
   * @param peerConnection the new peer connection
   */
  void handleNewConnection(PeerConnection peerConnection);

  /**
   * Handles peer disconnects.
   *
   * @param peerConnection the connection that is being closed
   * @param disconnectReason the reason given for closing the connection
   * @param initiatedByPeer true if the peer requested to disconnect, false if this node requested
   *     the disconnect
   */
  void handleDisconnect(
      PeerConnection peerConnection, DisconnectReason disconnectReason, boolean initiatedByPeer);

  /**
   * Returns the highest capability in the list of capabilities supported by this manager.
   *
   * @return the highest capability
   */
  int getHighestProtocolVersion();

  @Override
  default void close() {
    stop();
  }
}
