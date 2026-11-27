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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PeerDiscoveryAgent} that fans out to a V4 agent and/or a V5 agent, sharing a single UDP
 * socket via {@link SharedDiscoveryTransport}. Either agent may be null:
 *
 * <ul>
 *   <li>{@code agentV5 = null}: V4-only (node key is not secp256k1, or {@code --discovery-mode=V4})
 *   <li>{@code agentV4 = null}: V5-only ({@code --discovery-mode=V5} with secp256k1 key)
 * </ul>
 *
 * <p>The constructor resolves the configured agents once into non-null fields: a primary agent (V5
 * when configured, since it carries the authoritative ENR, otherwise V4) and an optional secondary
 * agent (V4 when both are configured).
 *
 * <p>Lifecycle order:
 *
 * <ol>
 *   <li>{@link SharedDiscoveryTransport#start()} — binds the shared UDP channel(s)
 *   <li>Configured agents start in parallel
 *   <li>Configured agents stop, then transport stops
 * </ol>
 */
public final class CompositePeerDiscoveryAgent implements PeerDiscoveryAgent {

  private static final Logger LOG = LoggerFactory.getLogger(CompositePeerDiscoveryAgent.class);

  private final Optional<PeerDiscoveryAgent> agentV4;
  private final Optional<PeerDiscoveryAgent> agentV5;

  /** V5 when configured, otherwise V4. Never null. */
  private final PeerDiscoveryAgent primary;

  /** V4 when both agents are configured, otherwise empty. */
  private final Optional<PeerDiscoveryAgent> secondary;

  /** Every configured agent, V4 first then V5. Never empty. */
  private final List<PeerDiscoveryAgent> agents;

  private final String mode;
  private final SharedDiscoveryTransport transport;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public CompositePeerDiscoveryAgent(
      final PeerDiscoveryAgent agentV4,
      final PeerDiscoveryAgent agentV5,
      final SharedDiscoveryTransport transport) {
    if (agentV4 == null && agentV5 == null) {
      throw new IllegalArgumentException(
          "At least one discovery agent (V4 or V5) must be non-null");
    }
    this.agentV4 = Optional.ofNullable(agentV4);
    this.agentV5 = Optional.ofNullable(agentV5);
    this.primary = this.agentV5.orElse(agentV4);
    this.secondary = agentV5 != null ? this.agentV4 : Optional.empty();
    this.agents = Stream.of(agentV4, agentV5).filter(Objects::nonNull).toList();
    this.mode = agentV4 == null ? "V5 only" : agentV5 == null ? "V4 only" : "V4 + V5";
    this.transport = transport;
  }

  @Override
  public CompletableFuture<Integer> start(final int rlpxTcpPort) {
    if (!isEnabled()) {
      return CompletableFuture.completedFuture(0);
    }
    if (!started.compareAndSet(false, true)) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Composite peer discovery agent already started"));
    }
    LOG.info("Starting discovery on shared UDP socket ({})", mode);
    // Wire inbound handlers BEFORE binding the shared channel so packets arriving in the
    // bind → agent.start window aren't dropped. V5's prepareHandlers is a no-op; instead
    // SharedDiscoveryTransport buffers V5 packets in a bounded replay sink until the discv5
    // library subscribes inside agentV5.start().
    agents.forEach(PeerDiscoveryAgent::prepareHandlers);
    final CompletableFuture<Integer> result =
        transport
            .start()
            .thenCompose(
                ignored -> {
                  if (secondary.isEmpty()) {
                    return primary.start(rlpxTcpPort);
                  }
                  final CompletableFuture<Integer> secondaryFuture =
                      secondary.get().start(rlpxTcpPort);
                  final CompletableFuture<Integer> primaryFuture = primary.start(rlpxTcpPort);
                  // Return the primary (V5) port - it carries the ENR and is authoritative. allOf
                  // propagates either agent's failure rather than continuing half-started.
                  return CompletableFuture.allOf(secondaryFuture, primaryFuture)
                      .thenApply(done -> primaryFuture.join());
                });
    // Best-effort rollback: if startup fails, stop whatever was started so channels, event
    // loops, and executors don't leak. The original failure still surfaces via `result`.
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
    final CompletableFuture<?>[] agentStops =
        agents.stream()
            .map(agent -> agent.stop().toCompletableFuture())
            .toArray(CompletableFuture<?>[]::new);
    return CompletableFuture.allOf(agentStops)
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
    agents.forEach(PeerDiscoveryAgent::updateNodeRecord);
  }

  @Override
  public boolean checkForkId(final DiscoveryPeer peer) {
    // V4 and V5 both delegate to the same shared ForkIdManager check, so only one call is
    // needed. V4 is used when configured. If the checks ever diverge, decide explicitly between
    // AND/OR before calling both.
    return agentV4.orElse(primary).checkForkId(peer);
  }

  @Override
  public Stream<? extends DiscoveryPeer> streamDiscoveredPeers() {
    if (secondary.isEmpty()) {
      return primary.streamDiscoveredPeers();
    }
    // Dedup by peerId, not full DefaultPeer equality - the same peer can appear in both streams
    // with different endpoints (e.g. V5's ENR TCP port vs V4's bonding-observed port). V5 wins,
    // same reason as getPeer(peerId): it carries the authoritative ENR.
    return Stream.concat(primary.streamDiscoveredPeers(), secondary.get().streamDiscoveredPeers())
        .collect(Collectors.toMap(DiscoveryPeer::getId, p -> p, (v5, v4) -> v5, LinkedHashMap::new))
        .values()
        .stream();
  }

  @Override
  public void dropPeer(final PeerId peer) {
    agents.forEach(agent -> agent.dropPeer(peer));
  }

  @Override
  public boolean isEnabled() {
    return agents.stream().anyMatch(PeerDiscoveryAgent::isEnabled);
  }

  @Override
  public boolean isStopped() {
    return agents.stream().allMatch(PeerDiscoveryAgent::isStopped);
  }

  @Override
  public void addPeer(final Peer peer) {
    agents.forEach(agent -> agent.addPeer(peer));
  }

  @Override
  public Optional<Peer> getPeer(final PeerId peerId) {
    // V5 is preferred (holds ENR); fall back to V4 if not found
    return primary.getPeer(peerId).or(() -> secondary.flatMap(agent -> agent.getPeer(peerId)));
  }

  @Override
  public Optional<NodeRecord> getLocalNodeRecord() {
    // V5 holds the authoritative ENR when enabled; fall back to V4's NodeRecord
    // (e.g. when the node key is not secp256k1 and V5 is disabled).
    return primary.getLocalNodeRecord();
  }
}
