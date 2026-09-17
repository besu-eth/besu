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

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;

import java.util.List;

public interface LocalNode {

  static LocalNode create(
      final String clientId, final int p2pVersion, final List<Capability> supportedCapabilities) {
    return DefaultLocalNode.create(clientId, p2pVersion, supportedCapabilities);
  }

  static LocalNode create(
      final String clientId,
      final int p2pVersion,
      final List<Capability> supportedCapabilities,
      final EnodeURLImpl enode) {
    DefaultLocalNode localNode =
        DefaultLocalNode.create(clientId, p2pVersion, supportedCapabilities);
    localNode.setEnode(enode);
    return localNode;
  }

  /**
   * Information of this node as a peer.
   *
   * @return the {@link PeerInfo} associated with the local node.
   * @throws NodeNotReadyException If the local node is not ready, throws an exception.
   */
  PeerInfo getPeerInfo() throws NodeNotReadyException;

  /**
   * This node as a Peer.
   *
   * @return a {@link Peer} representing the local node.
   * @throws NodeNotReadyException If the local node is not ready, throws an exception.
   */
  Peer getPeer() throws NodeNotReadyException;

  /**
   * This method can be called prior to invoking any method that may throw a {@link
   * NodeNotReadyException}. If this method returns true, the node is ready and will not throw an
   * exception.
   *
   * @return True if the local node is up and running.
   */
  boolean isReady();

  interface ReadyCallback {
    void onReady(LocalNode localNode);
  }

  class NodeNotReadyException extends RuntimeException {
    public NodeNotReadyException(final String message) {
      super(message);
    }
  }
}
