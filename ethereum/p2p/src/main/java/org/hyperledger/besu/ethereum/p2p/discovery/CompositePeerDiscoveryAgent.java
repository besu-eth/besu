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

import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PeerDiscoveryAgent} that fans out to a V4 agent and (optionally) a V5 agent, sharing a
 * single UDP socket via {@link SharedDiscoveryTransport}. The V5 agent is null when the node key is
 * on a curve that DiscV5 cannot use (only secp256k1 is supported); the composite then operates as
 * V4-only on top of the shared transport.
 *
 * <p>Lifecycle order:
 *
 * <ol>
 *   <li>{@link SharedDiscoveryTransport#start()} — binds the shared UDP channel(s)
 *   <li>Both agents start in parallel
 *   <li>Both agents stop, then transport stops
 * </ol>
 */
public final class CompositePeerDiscoveryAgent implements PeerDiscoveryAgent {

  private static final Logger LOG = LoggerFactory.getLogger(CompositePeerDiscoveryAgent.class);

  private final PeerDiscoveryAgent agentV4;
  private final PeerDiscoveryAgent agentV5;
  private final SharedDiscoveryTransport transport;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public CompositePeerDiscoveryAgent(
      final PeerDiscoveryAgent agentV4,
      final PeerDiscoveryAgent agentV5,
      final SharedDiscoveryTransport transport) {
    this.agentV4 = agentV4;
    this.agentV5 = agentV5;
    this.transport = transport;
  }

  @Override
  public CompletableFuture<Integer> start(final int tcpPort) {
    if (!isEnabled()) {
      return CompletableFuture.completedFuture(0);
    }
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Composite peer discovery agent already started"));
    }
    LOG.info(
        "Starting discovery on shared UDP socket ({})", agentV5 == null ? "V4 only" : "V4 + V5");
    // Wire inbound handlers BEFORE binding the shared channel so packets arriving in the
    // bind → agent.start window are not silently dropped. V5 uses a buffered publisher
    // managed by the discv5 library, so its prepareHandlers is a no-op.
    agentV4.prepareHandlers();
    if (agentV5 != null) {
      agentV5.prepareHandlers();
    }
    final CompletableFuture<Integer> result =
        transport
            .start()
            .thenCompose(
                ignored -> {
                  final CompletableFuture<Integer> v4Future = agentV4.start(tcpPort);
                  if (agentV5 == null) {
                    return v4Future;
                  }
                  final CompletableFuture<Integer> v5Future = agentV5.start(tcpPort);
                  // Return V5's port — it carries the ENR and is the authoritative UDP port.
                  // If either agent fails to start, allOf propagates the failure and we surface
                  // it to the caller rather than continuing in a half-started state.
                  return CompletableFuture.allOf(v4Future, v5Future)
                      .thenApply(done -> v5Future.join());
                });
    // Best-effort rollback: if startup fails at any stage, stop whatever was started so
    // channels, event loops, and executors don't leak. The original failure is still
    // surfaced to the caller via `result`.
    result.whenComplete(
        (v, ex) -> {
          if (ex != null) {
            LOG.warn("Discovery startup failed; rolling back", ex);
            stop();
          }
        });
    return result;
  }

  @Override
  public CompletableFuture<?> stop() {
    if (!stopped.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("Stopping discovery ...");
    final CompletableFuture<?> v4Stop = agentV4.stop().toCompletableFuture();
    final CompletableFuture<?> stops =
        agentV5 == null
            ? CompletableFuture.allOf(v4Stop)
            : CompletableFuture.allOf(v4Stop, agentV5.stop().toCompletableFuture());
    // Stop the transport even if agent shutdown failed — otherwise channels and the shared
    // event loop would leak.
    return stops
        .handle(
            (v, ex) -> {
              if (ex != null) {
                LOG.warn("Agent stop failed; continuing with transport shutdown", ex);
              }
              return transport.stop();
            })
        .thenCompose(f -> f);
  }

  @Override
  public void updateNodeRecord() {
    agentV4.updateNodeRecord();
    if (agentV5 != null) {
      agentV5.updateNodeRecord();
    }
  }

  @Override
  public boolean checkForkId(final DiscoveryPeer peer) {
    // V5 has ENR-based fork check; V4 checks ENR attachment on DiscoveryPeerV4
    if (agentV5 == null) {
      return agentV4.checkForkId(peer);
    }
    return agentV5.checkForkId(peer) && agentV4.checkForkId(peer);
  }

  @Override
  public Stream<? extends DiscoveryPeer> streamDiscoveredPeers() {
    if (agentV5 == null) {
      return agentV4.streamDiscoveredPeers();
    }
    // Dedup by peerId, not by full DefaultPeer equality (which includes the EnodeURL).
    // The same peer can appear in both streams with different endpoints — e.g. V5 has the
    // ENR-advertised TCP port while V4 may carry the port observed during bonding — and
    // .distinct() would keep both, leaving RlpxAgent.connect to pick arbitrarily. V5 wins
    // here for the same reason as getPeer(peerId): it carries the authoritative ENR.
    return Stream.concat(agentV5.streamDiscoveredPeers(), agentV4.streamDiscoveredPeers())
        .collect(Collectors.toMap(DiscoveryPeer::getId, p -> p, (v5, v4) -> v5, LinkedHashMap::new))
        .values()
        .stream();
  }

  @Override
  public void dropPeer(final PeerId peer) {
    agentV4.dropPeer(peer);
    if (agentV5 != null) {
      agentV5.dropPeer(peer);
    }
  }

  @Override
  public boolean isEnabled() {
    if (agentV5 == null) {
      return agentV4.isEnabled();
    }
    return agentV4.isEnabled() || agentV5.isEnabled();
  }

  @Override
  public boolean isStopped() {
    if (agentV5 == null) {
      return agentV4.isStopped();
    }
    return agentV4.isStopped() && agentV5.isStopped();
  }

  @Override
  public void addPeer(final Peer peer) {
    agentV4.addPeer(peer);
    if (agentV5 != null) {
      agentV5.addPeer(peer);
    }
  }

  @Override
  public Optional<Peer> getPeer(final PeerId peerId) {
    if (agentV5 == null) {
      return agentV4.getPeer(peerId);
    }
    // V5 is preferred (holds ENR); fall back to V4 if not found
    return agentV5.getPeer(peerId).or(() -> agentV4.getPeer(peerId));
  }

  @Override
  public Optional<NodeRecord> getLocalNodeRecord() {
    // V5 holds the authoritative ENR when enabled; fall back to V4's NodeRecord
    // (e.g. when the node key is not secp256k1 and V5 is disabled).
    return agentV5 == null ? agentV4.getLocalNodeRecord() : agentV5.getLocalNodeRecord();
  }
}
