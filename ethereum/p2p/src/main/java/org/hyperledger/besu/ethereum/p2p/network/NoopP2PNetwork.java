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
import org.hyperledger.besu.ethereum.p2p.network.exceptions.P2PDisabledException;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.ConnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.DisconnectCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class NoopP2PNetwork implements P2PNetwork {
  @Override
  public Collection<PeerConnection> getPeers() {
    return Collections.emptyList();
  }

  @Override
  public Stream<DiscoveryPeer> streamDiscoveredPeers() {
    return Stream.empty();
  }

  @Override
  public CompletableFuture<PeerConnection> connect(final Peer peer) {
    return CompletableFuture.failedFuture(
        new P2PDisabledException("P2P network is not enabled, cannot connect to peer: " + peer));
  }

  @Override
  public void subscribe(final Capability capability, final MessageCallback callback) {}

  @Override
  public void subscribeConnect(final ConnectCallback callback) {}

  @Override
  public void subscribeDisconnect(final DisconnectCallback callback) {}

  @Override
  public boolean addMaintainedConnectionPeer(final Peer peer) {
    return false;
  }

  @Override
  public boolean removeMaintainedConnectionPeer(final Peer peer) {
    return false;
  }

  @Override
  public Collection<Peer> getMaintainedConnectionPeers() {
    return Collections.emptyList();
  }

  @Override
  public void stop() {}

  @Override
  public void awaitStop() {}

  @Override
  public boolean isListening() {
    return false;
  }

  @Override
  public boolean isP2pEnabled() {
    return false;
  }

  @Override
  public boolean isDiscoveryEnabled() {
    return false;
  }

  @Override
  public boolean isStopped() {
    return true;
  }

  @Override
  public Optional<EnodeURLImpl> getLocalEnode() {
    return Optional.empty();
  }

  @Override
  public void updateNodeRecord() {}

  @Override
  public void close() throws IOException {}

  @Override
  public void start() {}

  @Override
  public Optional<RlpxAgent> getRlpxAgent() {
    return Optional.empty();
  }
}
