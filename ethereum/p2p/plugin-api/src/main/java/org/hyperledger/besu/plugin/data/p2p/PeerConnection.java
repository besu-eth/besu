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
package org.hyperledger.besu.plugin.data.p2p;

import org.hyperledger.besu.plugin.data.EnodeURL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/** A P2P connection to another node for plugin use. */
public interface PeerConnection {
  /**
   * Agreed capabilities between us and the peer.
   *
   * @return a set of shared capabilities between this node and the connected peer
   */
  Set<? extends Capability> getAgreedCapabilities();

  /**
   * Returns the agreed capability corresponding to given protocol.
   *
   * @param protocol the name of the protocol
   * @return the agreed capability corresponding to this protocol, returns null if no matching
   *     capability is supported
   */
  Capability capability(final String protocol);

  /**
   * Data about the peer on the other side of this connection.
   *
   * @return A representation of the remote peer this node is connected to.
   */
  Peer getPeer();

  /**
   * Returns the remote node's identity as advertised during the connection handshake.
   *
   * @return the advertised identity of the remote node
   */
  PeerInfo getPeerInfo();

  /**
   * Get local address.
   *
   * @return local address
   */
  InetSocketAddress getLocalAddress();

  /**
   * Get remote address.
   *
   * @return remote address
   */
  InetSocketAddress getRemoteAddress();

  /**
   * Get remote enode URL.
   *
   * @return remote enode URL
   */
  EnodeURL getRemoteEnode();

  /**
   * Get the difference, measured in milliseconds, between the time this connection was initiated
   * and midnight, January 1, 1970 UTC
   *
   * @return the time when this connection was initiated.
   */
  long getInitiatedAt();

  /**
   * Check if connection was initiated inbound.
   *
   * @return true if inbound initiated
   */
  boolean inboundInitiated();

  /**
   * Check if status has been exchanged.
   *
   * @return true if status exchanged
   */
  boolean getStatusExchanged();

  /** Exception thrown when attempting to send to a disconnected peer. */
  class PeerNotConnected extends IOException {

    /**
     * Constructs a new PeerNotConnected exception with the specified detail message.
     *
     * @param message the detail message explaining why the peer is not connected
     */
    public PeerNotConnected(final String message) {
      super(message);
    }
  }
}
