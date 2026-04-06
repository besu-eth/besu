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

import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** P2P Network Interface. */
public interface P2PNetwork extends Closeable {

  void start();

  /**
   * Returns a snapshot of the currently connected peer connections.
   *
   * @return Peers currently connected.
   */
  Collection<PeerConnection> getPeers();

  /**
   * Returns the number of currently connected peers.
   *
   * @return the number of connected peers.
   */
  default int getPeerCount() {
    return getPeers().size();
  }

  /**
   * Returns a stream of peers that have been discovered on the network. These peers are not
   * necessarily connected.
   *
   * @return A stream of discovered peers on the network.
   */
  Stream<? extends DiscoveryPeer> streamDiscoveredPeers();

  /**
   * Connects to a {@link Peer}.
   *
   * @param peer Peer to connect to.
   * @return Future of the established {@link PeerConnection}
   */
  CompletableFuture<PeerConnection> connect(Peer peer);

  /**
   * Subscribe a {@link Consumer} to all incoming {@link Message} of a given sub-protocol. Calling
   * {@link #start()} on an implementation without at least having one subscribed {@link Consumer}
   * per supported sub-protocol should throw a {@link RuntimeException}.
   *
   * @param capability Capability (sub-protocol) to subscribe to.
   * @param callback The callback to invoke when a message for the given capability arrives
   */
  void subscribe(final Capability capability, final MessageCallback callback);

  /**
   * Subscribe a {@link Consumer} to all incoming new Peer connection events.
   *
   * @param callback The callback to invoke when a new connection is established
   */
  void subscribeConnect(final ConnectCallback callback);

  /**
   * Subscribe a {@link Consumer} to all incoming new Peer disconnect events.
   *
   * @param callback The callback to invoke when a peer disconnects
   */
  void subscribeDisconnect(DisconnectCallback callback);

  /**
   * Adds a {@link Peer} to a list indicating efforts should be made to always stay connected
   * regardless of maxPeer limits. Non-permitted peers may be added to this list, but will not
   * actually be connected to as long as they are prohibited.
   *
   * @param peer The peer that should be connected to
   * @return boolean representing whether or not the peer has been added to the list, false is
   *     returned if the peer was already on the list
   */
  boolean addMaintainedConnectionPeer(final Peer peer);

  /**
   * Disconnect and remove the given {@link Peer} from the maintained peer list. Peer is
   * disconnected even if it is not in the maintained peer list. See {@link
   * #addMaintainedConnectionPeer(Peer)} for details on the maintained peer list.
   *
   * @param peer The peer to which connections are no longer required
   * @return boolean representing whether the peer was removed from the maintained peer list
   */
  boolean removeMaintainedConnectionPeer(final Peer peer);

  /** Returns a collection of peers that are maintained connections. */
  Collection<Peer> getMaintainedConnectionPeers();

  /** Stops the P2P network layer. */
  void stop();

  /** Blocks until the P2P network layer has stopped. */
  void awaitStop();

  /**
   * Checks if the node is listening for network connections
   *
   * @return true if the node is listening for network connections, false, otherwise.
   */
  boolean isListening();

  /**
   * Returns whether the P2P network is enabled
   *
   * @return true if the P2P network is enabled, false, otherwise.
   */
  boolean isP2pEnabled();

  /**
   * Is discovery enabled?
   *
   * @return Return true if peer discovery is enabled.
   */
  boolean isDiscoveryEnabled();

  /**
   * Is discovery stopped?
   *
   * @return Return true if peer discovery is stopped.
   */
  boolean isStopped();

  /**
   * Returns the EnodeURL used to identify this peer in the network.
   *
   * @return the enodeURL associated with this node if P2P has been enabled. Returns empty
   *     otherwise.
   */
  Optional<EnodeURLImpl> getLocalEnode();

  void updateNodeRecord();

  /**
   * Returns the local Ethereum Node Record (ENR) as a string, if available.
   *
   * @return an Optional containing the ENR string, or empty if not available.
   */
  default Optional<String> getLocalEnr() {
    return Optional.empty();
  }

  /**
   * IPv6 address information extracted from the local ENR.
   *
   * @param address the IPv6 address string
   * @param listeningPort the IPv6 TCP listening port, if available
   * @param discoveryPort the IPv6 UDP discovery port, if available
   */
  record IPv6AddressInfo(
      String address, Optional<Integer> listeningPort, Optional<Integer> discoveryPort) {}

  /**
   * Returns IPv6 address information from the local ENR, if available.
   *
   * @return an Optional containing IPv6 address info, or empty if not configured.
   */
  default Optional<IPv6AddressInfo> getIPv6AddressInfo() {
    return Optional.empty();
  }

  Optional<RlpxAgent> getRlpxAgent();
}
