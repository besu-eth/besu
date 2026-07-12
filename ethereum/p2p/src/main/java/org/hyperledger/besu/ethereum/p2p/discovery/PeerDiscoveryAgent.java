/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.ethereum.beacon.discovery.schema.NodeRecord;

/** Service interface for peer discovery in a P2P network. */
public interface PeerDiscoveryAgent {

  /**
   * Starts the peer discovery service on the specified TCP port.
   *
   * @param tcpPort The TCP port to start the service on.
   * @return A future that completes with the actual port the service started on.
   */
  CompletableFuture<Integer> start(int tcpPort);

  /**
   * Stops the peer discovery service.
   *
   * @return A future that completes when the service has stopped.
   */
  CompletableFuture<?> stop();

  /** Updates the node's record in the discovery system. */
  void updateNodeRecord();

  /**
   * Checks if the given peer's fork ID is compatible with the local node.
   *
   * @param peer The peer to check.
   * @return true if the peer's fork ID is compatible, false otherwise.
   */
  boolean checkForkId(DiscoveryPeer peer);

  /**
   * Streams the currently discovered peers.
   *
   * @return A stream of discovered peers.
   */
  Stream<? extends DiscoveryPeer> streamDiscoveredPeers();

  /**
   * Drops the specified peer from the discovery service.
   *
   * @param peer The peer to drop.
   */
  void dropPeer(PeerId peer);

  /**
   * Checks if the discovery service is enabled.
   *
   * @return true if the service is enabled, false otherwise.
   */
  boolean isEnabled();

  /**
   * Checks if the discovery service has been stopped.
   *
   * @return true if the service is stopped, false otherwise.
   */
  boolean isStopped();

  /**
   * Adds a peer to the discovery service.
   *
   * @param peer The peer to bond with.
   */
  void addPeer(Peer peer);

  /**
   * Retrieves a peer by its PeerId.
   *
   * @param peerId The PeerId of the peer to retrieve.
   * @return An Optional containing the peer if found, or empty if not found.
   */
  Optional<Peer> getPeer(PeerId peerId);

  /**
   * Returns the local node record (ENR), if available.
   *
   * @return an Optional containing the local NodeRecord, or empty if not available.
   */
  default Optional<NodeRecord> getLocalNodeRecord() {
    return Optional.empty();
  }
}
