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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrrequest.EnrRequestPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse.EnrResponsePacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.findneighbors.FindNeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.neighbors.NeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping.PingPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.pong.PongPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This component is the entrypoint for managing the lifecycle of peers.
 *
 * <p>It keeps track of the interactions with each peer, including the expectations of what we
 * expect to receive next from each peer. In other words, it implements the state machine for
 * (discovery) peers.
 *
 * <p>When necessary, it updates the underlying {@link PeerTable}, particularly with additions which
 * may succeed or not depending on the contents of the target bucket for the peer.
 *
 * <h3>Peer state machine</h3>
 *
 * <pre>{@code
 *                                                                +--------------------+
 *                                                                |                    |
 *                                                    +-----------+  MESSAGE_EXPECTED  +-----------+
 *                                                    |           |                    |           |
 *                                                    |           +---+----------------+           |
 * +------------+         +-----------+         +-----+----+          |                      +-----v-----+
 * |            |         |           |         |          <----------+                      |           |
 * |  KNOWN  +--------->  BONDING  +--------->  BONDED     |                                 |  DROPPED  |
 * |            |         |           |         |          ^                                 |           |
 * +------------+         +-----------+         +----------+                                 +-----------+
 *
 * }</pre>
 *
 * <ul>
 *   <li><em>KNOWN:</em> the peer is known but there is no ongoing interaction with it.
 *   <li><em>BONDING:</em> an attempt to bond is being made (e.g. a PING has been sent).
 *   <li><em>BONDED:</em> the bonding handshake has taken place (e.g. an expected PONG has been
 *       received after having sent a PING). This is the same as having an "active" channel.
 *   <li><em>MESSAGE_EXPECTED (*)</em>: a message has been sent and a response is expected.
 *   <li><em>DROPPED (*):</em> the peer is no longer in our peer table.
 * </ul>
 *
 * <p>(*) It is worthy to note that the <code>MESSAGE_EXPECTED</code> and <code>DROPPED</code>
 * states are not modelled explicitly in {@link PeerDiscoveryStatus}, but they have been included in
 * the diagram for clarity. These two states define the elimination path for a peer from the
 * underlying table.
 *
 * <p>If an expectation to receive a message was unmet, following the evaluation of a failure
 * condition, the peer will be physically dropped (eliminated) from the table.
 */
public class PeerDiscoveryController {
  private static final Logger LOG = LoggerFactory.getLogger(PeerDiscoveryController.class);
  private static final long REFRESH_CHECK_INTERVAL_MILLIS = MILLISECONDS.convert(30, SECONDS);
  private static final int PEER_REFRESH_ROUND_TIMEOUT_IN_SECONDS = 5;
  protected final TimerUtil timerUtil;
  private final PeerTable peerTable;
  private final Cache<Bytes, DiscoveryPeerV4> bondingPeers =
      CacheBuilder.newBuilder().maximumSize(50).expireAfterWrite(10, TimeUnit.MINUTES).build();
  private final Cache<Bytes, Packet> cachedEnrRequests;

  private final Collection<DiscoveryPeerV4> bootstrapNodes;

  /* A tracker for inflight interactions and the state machine of a peer. */
  private final Map<Bytes, Map<PacketType, PeerInteractionState>> inflightInteractions =
      new ConcurrentHashMap<>();

  private final AtomicBoolean started = new AtomicBoolean(false);

  private final NodeKey nodeKey;
  // The peer representation of this node
  private final DiscoveryPeerV4 localPeer;
  private final OutboundMessageHandler outboundMessageHandler;
  private final PeerDiscoveryPermissions peerPermissions;
  private final DiscoveryProtocolLogger discoveryProtocolLogger;
  private final LabelledMetric<Counter> interactionCounter;
  private final LabelledMetric<Counter> interactionRetryCounter;
  private final boolean filterOnEnrForkId;
  private final RlpxAgent rlpxAgent;

  private RetryDelayFunction retryDelayFunction = RetryDelayFunction.linear(1.5, 2000, 60000);

  private final AsyncExecutor workerExecutor;

  private final PeerRequirement peerRequirement;
  private final long tableRefreshIntervalMs;
  private OptionalLong tableRefreshTimerId = OptionalLong.empty();
  private long lastRefreshTime = -1;

  private final long cleanPeerTableIntervalMs;
  private final AtomicBoolean peerTableIsDirty = new AtomicBoolean(false);
  private OptionalLong cleanTableTimerId = OptionalLong.empty();
  private RecursivePeerRefreshState recursivePeerRefreshState;
  private final boolean includeBootnodesOnPeerRefresh;

  private final PacketFactory packetFactory;
  private final PingPacketDataFactory pingPacketDataFactory;
  private final PongPacketDataFactory pongPacketDataFactory;
  private final FindNeighborsPacketDataFactory findNeighborsPacketDataFactory;
  private final NeighborsPacketDataFactory neighborsPacketDataFactory;
  private final EnrRequestPacketDataFactory enrRequestPacketDataFactory;
  private final EnrResponsePacketDataFactory enrResponsePacketDataFactory;

  PeerDiscoveryController(
      final NodeKey nodeKey,
      final DiscoveryPeerV4 localPeer,
      final PeerTable peerTable,
      final Collection<DiscoveryPeerV4> bootstrapNodes,
      final OutboundMessageHandler outboundMessageHandler,
      final TimerUtil timerUtil,
      final AsyncExecutor workerExecutor,
      final long tableRefreshIntervalMs,
      final long cleanPeerTableIntervalMs,
      final PeerRequirement peerRequirement,
      final PeerPermissions peerPermissions,
      final MetricsSystem metricsSystem,
      final Optional<Cache<Bytes, Packet>> maybeCacheForEnrRequests,
      final boolean filterOnEnrForkId,
      final RlpxAgent rlpxAgent,
      final boolean includeBootnodesOnPeerRefresh,
      final PacketFactory packetFactory,
      final PingPacketDataFactory pingPacketDataFactory,
      final PongPacketDataFactory pongPacketDataFactory,
      final FindNeighborsPacketDataFactory findNeighborsPacketDataFactory,
      final NeighborsPacketDataFactory neighborsPacketDataFactory,
      final EnrRequestPacketDataFactory enrRequestPacketDataFactory,
      final EnrResponsePacketDataFactory enrResponsePacketDataFactory) {
    this.timerUtil = timerUtil;
    this.nodeKey = nodeKey;
    this.localPeer = localPeer;
    this.bootstrapNodes = bootstrapNodes;
    this.peerTable = peerTable;
    this.workerExecutor = workerExecutor;
    this.tableRefreshIntervalMs = tableRefreshIntervalMs;
    this.cleanPeerTableIntervalMs = cleanPeerTableIntervalMs;
    this.peerRequirement = peerRequirement;
    this.outboundMessageHandler = outboundMessageHandler;
    this.discoveryProtocolLogger = new DiscoveryProtocolLogger(metricsSystem);
    this.peerPermissions = new PeerDiscoveryPermissions(localPeer, peerPermissions);
    this.rlpxAgent = rlpxAgent;
    this.includeBootnodesOnPeerRefresh = includeBootnodesOnPeerRefresh;

    this.packetFactory = packetFactory;
    this.pingPacketDataFactory = pingPacketDataFactory;
    this.pongPacketDataFactory = pongPacketDataFactory;
    this.findNeighborsPacketDataFactory = findNeighborsPacketDataFactory;
    this.neighborsPacketDataFactory = neighborsPacketDataFactory;
    this.enrRequestPacketDataFactory = enrRequestPacketDataFactory;
    this.enrResponsePacketDataFactory = enrResponsePacketDataFactory;

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "discovery_inflight_interactions_current",
        "Current number of inflight discovery interactions",
        inflightInteractions::size);

    this.interactionCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "discovery_interaction_count",
            "Total number of discovery interactions initiated",
            "type");

    this.interactionRetryCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "discovery_interaction_retry_count",
            "Total number of interaction retries performed",
            "type");

    this.cachedEnrRequests =
        maybeCacheForEnrRequests.orElse(
            CacheBuilder.newBuilder().maximumSize(50).expireAfterWrite(10, SECONDS).build());

    this.filterOnEnrForkId = filterOnEnrForkId;
  }

  public static Builder builder() {
    return new Builder();
  }

  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("The peer table had already been started");
    }

    LOG.debug("Starting with filterOnEnrForkId = {}", filterOnEnrForkId);
    final List<DiscoveryPeerV4> initialDiscoveryPeerV4s =
        bootstrapNodes.stream()
            .filter(peerPermissions::isAllowedInPeerTable)
            .collect(Collectors.toList());
    initialDiscoveryPeerV4s.forEach(peerTable::tryAdd);

    recursivePeerRefreshState =
        new RecursivePeerRefreshState(
            this::bond,
            this::findNodes,
            timerUtil,
            localPeer,
            peerTable,
            peerPermissions,
            PEER_REFRESH_ROUND_TIMEOUT_IN_SECONDS,
            100);

    peerPermissions.subscribeUpdate(this::handlePermissionsUpdate);

    recursivePeerRefreshState.start(initialDiscoveryPeerV4s, localPeer.getId());

    final long refreshTimerId =
        timerUtil.setPeriodic(
            Math.min(REFRESH_CHECK_INTERVAL_MILLIS, tableRefreshIntervalMs),
            "peer-table-refresh",
            this::refreshTableIfRequired);
    tableRefreshTimerId = OptionalLong.of(refreshTimerId);

    cleanTableTimerId =
        OptionalLong.of(
            timerUtil.setPeriodic(
                cleanPeerTableIntervalMs, "peer-table-clean", this::cleanPeerTableIfRequired));
  }

  public CompletableFuture<?> stop() {
    if (!started.compareAndSet(true, false)) {
      return CompletableFuture.completedFuture(null);
    }

    tableRefreshTimerId.ifPresent(timerUtil::cancelTimer);
    tableRefreshTimerId = OptionalLong.empty();
    cleanTableTimerId.ifPresent(timerUtil::cancelTimer);
    cleanTableTimerId = OptionalLong.empty();
    inflightInteractions
        .values()
        .forEach(
            l -> {
              l.values().forEach(s -> s.cancelTimers());
              l.clear();
            });
    inflightInteractions.clear();
    recursivePeerRefreshState.cancel();
    return CompletableFuture.completedFuture(null);
  }

  private void handlePermissionsUpdate(
      final boolean addRestrictions, final Optional<List<Peer>> affectedPeers) {
    if (!addRestrictions) {
      // Nothing to do if permissions were relaxed
      return;
    }

    // If we have an explicit list of peers, drop each peer from our discovery table
    if (affectedPeers.isPresent()) {
      affectedPeers.get().forEach(this::dropPeerIfDisallowed);
      return;
    }

    // Otherwise, signal that we need to clean up the peer table
    peerTableIsDirty.set(true);
  }

  private void dropPeerIfDisallowed(final Peer peer) {
    if (!peerPermissions.isAllowedInPeerTable(peer)) {
      dropPeer(peer);
    }
  }

  public void dropPeer(final PeerId peer) {
    peerTable.tryEvict(peer);
  }

  /**
   * Handles an incoming message and processes it based on the state machine for the {@link
   * DiscoveryPeerV4}.
   *
   * <p>The callback will be called with the canonical representation of the sender Peer as stored
   * in our table, or with an empty Optional if the message was out of band and we didn't process
   * it.
   *
   * @param packet The incoming message.
   * @param sender The sender.
   */
  public void onMessage(final Packet packet, final DiscoveryPeerV4 sender) {
    discoveryProtocolLogger.logReceivedPacket(sender, packet);

    // Message from self. This should not happen.
    if (sender.getId().equals(localPeer.getId())) {
      return;
    }

    final DiscoveryPeerV4 peer = resolvePeer(sender);
    if (peer == null) {
      return;
    }
    final Bytes peerId = peer.getId();
    switch (packet.getType()) {
      case PING:
        if (peerPermissions.allowInboundBonding(peer)) {
          final PingPacketData ping = packet.getPacketData(PingPacketData.class).get();
          if (!PeerDiscoveryStatus.BONDED.equals(peer.getStatus())
              && (bondingPeers.getIfPresent(sender.getId()) == null)) {
            bond(peer);
          }
          respondToPing(ping, packet.getHash(), peer);
        }
        break;
      case PONG:
        matchInteraction(packet)
            .ifPresent(
                interaction -> {
                  if (filterOnEnrForkId) {
                    requestENR(peer);
                  }
                  bondingPeers.invalidate(peerId);
                  checkBeforeAddingToPeerTable(peer);
                  recursivePeerRefreshState.onBondingComplete(peer);
                  Optional.ofNullable(cachedEnrRequests.getIfPresent(peerId))
                      .ifPresent(cachedEnrRequest -> processEnrRequest(peer, cachedEnrRequest));
                });
        break;
      case NEIGHBORS:
        matchInteraction(packet)
            .ifPresent(
                interaction ->
                    recursivePeerRefreshState.onNeighboursReceived(
                        peer, getPeersFromNeighborsPacket(packet)));
        break;
      case FIND_NEIGHBORS:
        if (PeerDiscoveryStatus.BONDED.equals(peer.getStatus())
            && peerPermissions.allowInboundNeighborsRequest(peer)) {
          final FindNeighborsPacketData fn =
              packet.getPacketData(FindNeighborsPacketData.class).get();
          respondToFindNeighbors(fn, peer);
        }

        break;
      case ENR_REQUEST:
        if (PeerDiscoveryStatus.BONDED.equals(peer.getStatus())) {
          processEnrRequest(peer, packet);
        } else if (PeerDiscoveryStatus.BONDING.equals(peer.getStatus())) {
          LOG.trace("ENR_REQUEST cached for bonding peer Id: {}", peerId);
          // Due to UDP, it may happen that we receive the ENR_REQUEST just before the PONG.
          // Because peers want to send the ENR_REQUEST directly after the pong.
          // If this happens we don't want to ignore the request but process when bonded.
          // this cache allows to keep the request and to respond after having processed the PONG
          cachedEnrRequests.put(peerId, packet);
        }
        break;
      case ENR_RESPONSE:
        matchInteraction(packet)
            .ifPresent(
                interaction -> {
                  final Optional<EnrResponsePacketData> packetData =
                      packet.getPacketData(EnrResponsePacketData.class);
                  final NodeRecord enr = packetData.get().getEnr();
                  peer.setNodeRecord(enr);
                });
        break;
    }
  }

  private void processEnrRequest(final DiscoveryPeerV4 peer, final Packet packet) {
    LOG.trace("ENR_REQUEST received from bonded peer Id: {}", peer.getId());
    packet
        .getPacketData(EnrRequestPacketData.class)
        .ifPresent(p -> respondToENRRequest(p, packet.getHash(), peer));
  }

  private List<DiscoveryPeerV4> getPeersFromNeighborsPacket(final Packet packet) {
    final Optional<NeighborsPacketData> maybeNeighborsData =
        packet.getPacketData(NeighborsPacketData.class);
    if (maybeNeighborsData.isEmpty()) {
      return Collections.emptyList();
    }
    final NeighborsPacketData neighborsData = maybeNeighborsData.get();

    return neighborsData.getNodes().stream()
        .map(p -> peerTable.get(p).orElse(p))
        .collect(Collectors.toList());
  }

  private void checkBeforeAddingToPeerTable(final DiscoveryPeerV4 peer) {
    if (peerTable.isIpAddressInvalid(peer.getEndpoint())) {
      return;
    }

    if (peer.getFirstDiscovered() == 0L) {
      connectOnRlpxLayer(peer)
          .whenComplete(
              (pc, th) -> {
                if (th == null || !(th.getCause() instanceof TimeoutException)) {
                  peer.setStatus(PeerDiscoveryStatus.BONDED);
                  peer.setFirstDiscovered(System.currentTimeMillis());
                  addToPeerTable(peer);
                } else {
                  if (LOG.isTraceEnabled()) {
                    LOG.trace(
                        "Handshake timed out with peer {}: {}",
                        peer.getLoggableId(),
                        th.getMessage());
                  } else {
                    LOG.debug("Handshake timed out with peer {}", peer.getLoggableId());
                  }
                  peerTable.invalidateIP(peer.getEndpoint());
                }
              });
    } else {
      peer.setStatus(PeerDiscoveryStatus.BONDED);
      addToPeerTable(peer);
    }
  }

  public void addToPeerTable(final DiscoveryPeerV4 peer) {
    final PeerTable.AddResult result = peerTable.tryAdd(peer);

    if (result.getOutcome() == PeerTable.AddResult.AddOutcome.ALREADY_EXISTED) {
      // Bump peer.
      peerTable.tryEvict(peer);
      peerTable.tryAdd(peer);
    } else if (result.getOutcome() == PeerTable.AddResult.AddOutcome.BUCKET_FULL) {
      peerTable.tryEvict(result.getEvictionCandidate());
      peerTable.tryAdd(peer);
    }
  }

  CompletableFuture<PeerConnection> connectOnRlpxLayer(final DiscoveryPeerV4 peer) {
    return rlpxAgent.connect(peer);
  }

  private Optional<PeerInteractionState> matchInteraction(final Packet packet) {
    final Bytes nodeId = packet.getNodeId();
    final Map<PacketType, PeerInteractionState> stateMap = inflightInteractions.get(nodeId);
    if (stateMap == null) {
      return Optional.empty();
    }
    final PacketType packetType = packet.getType();
    final PeerInteractionState interaction = stateMap.get(packetType);
    if (interaction == null || !interaction.test(packet)) {
      return Optional.empty();
    }
    interaction.cancelTimers();
    stateMap.remove(packetType);
    if (stateMap.isEmpty()) {
      inflightInteractions.remove(nodeId);
    }
    return Optional.of(interaction);
  }

  private void refreshTableIfRequired() {
    final long now = System.currentTimeMillis();
    if (lastRefreshTime + tableRefreshIntervalMs <= now) {
      LOG.debug("Refreshing peer table after {} ms", tableRefreshIntervalMs);
      refreshTable();
    } else if (!peerRequirement.hasSufficientPeers()) {
      LOG.debug("Refreshing peer table: seeking more peers. peer count < max");
      refreshTable();
    }
  }

  private void cleanPeerTableIfRequired() {
    if (peerTableIsDirty.compareAndSet(true, false)) {
      peerTable.streamAllPeers().forEach(this::dropPeerIfDisallowed);
    }
  }

  @VisibleForTesting
  RecursivePeerRefreshState getRecursivePeerRefreshState() {
    return recursivePeerRefreshState;
  }

  /**
   * Refreshes the peer table by generating a random ID and interrogating the closest nodes for it.
   * Currently the refresh process is NOT recursive.
   */
  private void refreshTable() {
    final Bytes target = Peer.randomId();

    final List<DiscoveryPeerV4> initialPeers = peerTable.nearestBondedPeers(Peer.randomId(), 16);
    if (includeBootnodesOnPeerRefresh) {
      bootstrapNodes.stream()
          .filter(p -> p.getStatus() != PeerDiscoveryStatus.BONDED)
          .forEach(p -> p.setStatus(PeerDiscoveryStatus.KNOWN));

      // If configured to retry bootnodes during peer table refresh, include them
      // in the initial peers list.
      initialPeers.addAll(bootstrapNodes);
    }
    recursivePeerRefreshState.start(initialPeers, target);
    lastRefreshTime = System.currentTimeMillis();
  }

  /**
   * Initiates a bonding PING-PONG cycle with a peer.
   *
   * @param peer The targeted peer.
   */
  @VisibleForTesting
  void bond(final DiscoveryPeerV4 peer) {
    if (!peerPermissions.isAllowedInPeerTable(peer)) {
      return;
    }

    peer.setStatus(PeerDiscoveryStatus.BONDING);
    bondingPeers.put(peer.getId(), peer);

    final Consumer<PeerInteractionState> action =
        interaction -> {
          final PingPacketData data =
              pingPacketDataFactory.create(
                  Optional.of(localPeer.getEndpoint()),
                  peer.getEndpoint(),
                  localPeer.getNodeRecord().map(NodeRecord::getSeq).orElse(null));
          createPacket(
              PacketType.PING,
              data,
              pingPacket -> {
                final Bytes pingHash = pingPacket.getHash();
                // Update the matching filter to only accept the PONG if it echoes the hash of our
                // PING.
                final Predicate<Packet> newFilter =
                    packet ->
                        packet
                            .getPacketData(PongPacketData.class)
                            .map(pong -> pong.getPingHash().equals(pingHash))
                            .orElse(false);
                interaction.updateFilter(newFilter);

                sendPacket(peer, pingPacket);
              });
        };

    // The filter condition will be updated as soon as the action is performed.
    final PeerInteractionState peerInteractionState =
        new PeerInteractionState(action, peer.getId(), PacketType.PONG, packet -> false);
    dispatchInteraction(peer, peerInteractionState);
  }

  /**
   * Initiates an enr request cycle with a peer.
   *
   * @param peer The targeted peer.
   */
  @VisibleForTesting
  void requestENR(final DiscoveryPeerV4 peer) {
    final Consumer<PeerInteractionState> action =
        interaction -> {
          final EnrRequestPacketData data = enrRequestPacketDataFactory.create();
          createPacket(
              PacketType.ENR_REQUEST,
              data,
              enrPacket -> {
                final Bytes enrHash = enrPacket.getHash();
                // Update the matching filter to only accept the ENRResponse if it echoes the hash
                // of our request.
                final Predicate<Packet> newFilter =
                    packet ->
                        packet
                            .getPacketData(EnrResponsePacketData.class)
                            .map(enr -> enr.getRequestHash().equals(enrHash))
                            .orElse(false);
                interaction.updateFilter(newFilter);

                sendPacket(peer, enrPacket);
              });
        };

    // The filter condition will be updated as soon as the action is performed.
    final PeerInteractionState peerInteractionState =
        new PeerInteractionState(action, peer.getId(), PacketType.ENR_RESPONSE, packet -> false);
    dispatchInteraction(peer, peerInteractionState);
  }

  private void sendPacket(
      final DiscoveryPeerV4 peer, final PacketType type, final PacketData data) {
    createPacket(
        type,
        data,
        packet -> {
          discoveryProtocolLogger.logSendingPacket(peer, packet);
          outboundMessageHandler.send(peer, packet);
        });
  }

  private void sendPacket(final DiscoveryPeerV4 peer, final Packet packet) {
    discoveryProtocolLogger.logSendingPacket(peer, packet);
    outboundMessageHandler.send(peer, packet);
  }

  @VisibleForTesting
  void createPacket(final PacketType type, final PacketData data, final Consumer<Packet> handler) {
    // Creating packets is quite expensive because they have to be cryptographically signed
    // So ensure the work is done on a worker thread to avoid blocking the vertx event thread.
    workerExecutor
        .execute(() -> packetFactory.create(type, data, nodeKey))
        .thenAccept(handler)
        .exceptionally(
            error -> {
              LOG.error("Error while creating packet", error);
              return null;
            });
  }

  /**
   * Sends a FIND_NEIGHBORS message to a {@link DiscoveryPeerV4}, in search of a target value.
   *
   * @param peer the peer to interrogate
   * @param target the target node ID to find
   */
  private void findNodes(final DiscoveryPeerV4 peer, final Bytes target) {
    final Consumer<PeerInteractionState> action =
        interaction -> {
          final FindNeighborsPacketData data = findNeighborsPacketDataFactory.create(target);
          sendPacket(peer, PacketType.FIND_NEIGHBORS, data);
        };
    final PeerInteractionState interaction =
        new PeerInteractionState(action, peer.getId(), PacketType.NEIGHBORS, packet -> true);
    dispatchInteraction(peer, interaction);
  }

  /**
   * Dispatches a new tracked interaction with a peer, adding it to the {@link
   * #inflightInteractions} map and executing the action for the first time.
   *
   * <p>If a previous inflightInteractions interaction existed, we cancel any associated timers.
   *
   * @param peer The peer.
   * @param state The state.
   */
  private void dispatchInteraction(final Peer peer, final PeerInteractionState state) {
    final Bytes id = peer.getId();
    final PeerInteractionState previous =
        inflightInteractions
            .computeIfAbsent(id, k -> new ConcurrentHashMap<>())
            .put(state.expectedType, state);
    if (previous != null) {
      previous.cancelTimers();
    }
    state.execute();
  }

  private void respondToPing(
      final PingPacketData packetData, final Bytes pingHash, final DiscoveryPeerV4 sender) {
    if (packetData.getExpiration() < Instant.now().getEpochSecond()) {
      LOG.debug("ignoring expired PING");
      return;
    }
    // We don't care about the `from` field of the ping, we pong to the `sender`
    final PongPacketData data =
        pongPacketDataFactory.create(
            sender.getEndpoint(),
            pingHash,
            localPeer.getNodeRecord().map(NodeRecord::getSeq).orElse(null));

    sendPacket(sender, PacketType.PONG, data);
  }

  private void respondToFindNeighbors(
      final FindNeighborsPacketData packetData, final DiscoveryPeerV4 sender) {
    if (packetData.getExpiration() < Instant.now().getEpochSecond()) {
      return;
    }
    // Each peer is encoded as 16 bytes for address, 4 bytes for port, 4 bytes for tcp port
    // and 64 bytes for id. This is prepended by 97 bytes of hash, signature and type.
    // 16 + 4 + 4 + 64 = 88 bytes
    // 88 * 13 = 1144 bytes
    // To fit under 1280 bytes, we must return just 13 peers maximum.
    final List<DiscoveryPeerV4> peers = peerTable.nearestBondedPeers(packetData.getTarget(), 13);
    final PacketData data = neighborsPacketDataFactory.create(peers);
    sendPacket(sender, PacketType.NEIGHBORS, data);
  }

  private void respondToENRRequest(
      final EnrRequestPacketData enrRequestPacketData,
      final Bytes requestHash,
      final DiscoveryPeerV4 sender) {
    if (enrRequestPacketData.getExpiration() >= Instant.now().getEpochSecond()) {
      final EnrResponsePacketData data =
          enrResponsePacketDataFactory.create(requestHash, localPeer.getNodeRecord().orElse(null));
      sendPacket(sender, PacketType.ENR_RESPONSE, data);
    }
  }

  /**
   * Returns a copy of the known peers. Modifications to the list will not update the table's state,
   * but modifications to the Peers themselves will.
   *
   * @return List of peers.
   */
  public Stream<DiscoveryPeerV4> streamDiscoveredPeers() {
    return peerTable.streamAllPeers().filter(peerPermissions::isAllowedInPeerTable);
  }

  public void setRetryDelayFunction(final RetryDelayFunction retryDelayFunction) {
    this.retryDelayFunction = retryDelayFunction;
  }

  public void handleBondingRequest(final DiscoveryPeerV4 peer) {
    final DiscoveryPeerV4 peerToBond = resolvePeer(peer);
    if (peerToBond == null) {
      return;
    }
    if (peerPermissions.allowOutboundBonding(peerToBond)
        && PeerDiscoveryStatus.KNOWN.equals(peerToBond.getStatus())) {
      bond(peerToBond);
    }
  }

  // Load the peer first from the table, then from bonding cache or use the instance that comes in.
  private DiscoveryPeerV4 resolvePeer(final DiscoveryPeerV4 peer) {
    if (peerTable.isIpAddressInvalid(peer.getEndpoint())) {
      return null;
    }
    final Optional<DiscoveryPeerV4> maybeKnownPeer =
        peerTable.get(peer).filter(known -> known.discoveryEndpointMatches(peer));
    DiscoveryPeerV4 resolvedPeer = maybeKnownPeer.orElse(peer);
    if (maybeKnownPeer.isEmpty()) {
      final DiscoveryPeerV4 bondingPeer = bondingPeers.getIfPresent(peer.getId());
      if (bondingPeer != null) {
        resolvedPeer = bondingPeer;
      }
    }

    return resolvedPeer;
  }

  /** Holds the state machine data for a peer interaction. */
  private class PeerInteractionState implements Predicate<Packet> {

    private static final int MAX_RETRIES = 5;

    /**
     * The action that led to the peer being in this state (e.g. sending a PING or NEIGHBORS
     * message), in case it needs to be retried.
     */
    private final Consumer<PeerInteractionState> action;

    private final Bytes peerId;

    /** The expected type of the message that will transition the peer out of this state. */
    private final PacketType expectedType;

    private final Counter retryCounter;

    /** A custom filter to accept transitions out of this state. */
    private Predicate<Packet> filter;

    /** Timers associated with this entry. */
    private OptionalLong timerId = OptionalLong.empty();

    private long delay = 0;
    private int retryCount = 0;

    PeerInteractionState(
        final Consumer<PeerInteractionState> action,
        final Bytes peerId,
        final PacketType expectedType,
        final Predicate<Packet> filter) {
      this.action = action;
      this.peerId = peerId;
      this.expectedType = expectedType;
      this.filter = filter;
      interactionCounter.labels(expectedType.name()).inc();
      retryCounter = interactionRetryCounter.labels(expectedType.name());
    }

    @Override
    public boolean test(final Packet packet) {
      return expectedType == packet.getType() && (filter == null || filter.test(packet));
    }

    void updateFilter(final Predicate<Packet> filter) {
      this.filter = filter;
    }

    /** Executes the action associated with this state. Sets a "boomerang" timer to itself. */
    void execute() {
      action.accept(this);
      if (retryCount < MAX_RETRIES) {
        this.delay = retryDelayFunction.apply(this.delay);
        timerId =
            OptionalLong.of(
                timerUtil.setTimer(
                    this.delay,
                    () -> {
                      retryCounter.inc();
                      retryCount++;
                      execute();
                    }));
      } else {
        Optional.ofNullable(inflightInteractions.get(peerId))
            .ifPresent(
                peerInterationStateMap -> {
                  peerInterationStateMap.remove(expectedType);
                  if (peerInterationStateMap.isEmpty()) {
                    inflightInteractions.remove(peerId);
                  }
                });
      }
    }

    /** Cancels any timers associated with this entry. */
    void cancelTimers() {
      timerId.ifPresent(timerUtil::cancelTimer);
    }
  }

  public interface AsyncExecutor {
    <T> CompletableFuture<T> execute(Supplier<T> action);
  }

  public static class Builder {
    // Options with default values
    private OutboundMessageHandler outboundMessageHandler = OutboundMessageHandler.NOOP;
    private PeerRequirement peerRequirement = PeerRequirement.NOOP;
    private PeerPermissions peerPermissions = PeerPermissions.noop();
    private long tableRefreshIntervalMs = MILLISECONDS.convert(30, TimeUnit.MINUTES);
    private long cleanPeerTableIntervalMs = MILLISECONDS.convert(1, TimeUnit.MINUTES);
    private final List<DiscoveryPeerV4> bootstrapNodes = new ArrayList<>();
    private PeerTable peerTable;
    private boolean includeBootnodesOnPeerRefresh = true;

    // Required dependencies
    private NodeKey nodeKey;
    private DiscoveryPeerV4 localPeer;
    private TimerUtil timerUtil;
    private AsyncExecutor workerExecutor;
    private MetricsSystem metricsSystem;
    private boolean filterOnEnrForkId;

    private Cache<Bytes, Packet> cachedEnrRequests =
        CacheBuilder.newBuilder().maximumSize(50).expireAfterWrite(10, SECONDS).build();
    private RlpxAgent rlpxAgent;

    // set defaults for all PacketPackage classes, allowing calling code to override if needed
    private final PacketPackage packetPackage = DaggerPacketPackage.create();
    private PacketFactory packetFactory = packetPackage.packetFactory();
    private PingPacketDataFactory pingPacketDataFactory = packetPackage.pingPacketDataFactory();
    private PongPacketDataFactory pongPacketDataFactory = packetPackage.pongPacketDataFactory();
    private FindNeighborsPacketDataFactory findNeighborsPacketDataFactory =
        packetPackage.findNeighborsPacketDataFactory();
    private NeighborsPacketDataFactory neighborsPacketDataFactory =
        packetPackage.neighborsPacketDataFactory();
    private EnrRequestPacketDataFactory enrRequestPacketDataFactory =
        packetPackage.enrRequestPacketDataFactory();
    private EnrResponsePacketDataFactory enrResponsePacketDataFactory =
        packetPackage.enrResponsePacketDataFactory();

    private Builder() {}

    public PeerDiscoveryController build() {
      validate();

      return new PeerDiscoveryController(
          nodeKey,
          localPeer,
          peerTable,
          bootstrapNodes,
          outboundMessageHandler,
          timerUtil,
          workerExecutor,
          tableRefreshIntervalMs,
          cleanPeerTableIntervalMs,
          peerRequirement,
          peerPermissions,
          metricsSystem,
          Optional.of(cachedEnrRequests),
          filterOnEnrForkId,
          rlpxAgent,
          includeBootnodesOnPeerRefresh,
          packetFactory,
          pingPacketDataFactory,
          pongPacketDataFactory,
          findNeighborsPacketDataFactory,
          neighborsPacketDataFactory,
          enrRequestPacketDataFactory,
          enrResponsePacketDataFactory);
    }

    private void validate() {
      validateRequiredDependency(nodeKey, "nodeKey");
      validateRequiredDependency(localPeer, "LocalPeer");
      validateRequiredDependency(timerUtil, "TimerUtil");
      validateRequiredDependency(workerExecutor, "AsyncExecutor");
      validateRequiredDependency(metricsSystem, "MetricsSystem");
      validateRequiredDependency(rlpxAgent, "RlpxAgent");
      validateRequiredDependency(peerTable, "PeerTable");
    }

    private void validateRequiredDependency(final Object object, final String name) {
      checkState(object != null, name + " must be configured.");
    }

    public Builder nodeKey(final NodeKey nodeKey) {
      checkNotNull(nodeKey);
      this.nodeKey = nodeKey;
      return this;
    }

    public Builder localPeer(final DiscoveryPeerV4 localPeer) {
      checkNotNull(localPeer);
      this.localPeer = localPeer;
      return this;
    }

    public Builder peerTable(final PeerTable peerTable) {
      checkNotNull(peerTable);
      this.peerTable = peerTable;
      return this;
    }

    public Builder bootstrapNodes(final Collection<DiscoveryPeerV4> bootstrapNodes) {
      this.bootstrapNodes.addAll(bootstrapNodes);
      return this;
    }

    public Builder outboundMessageHandler(final OutboundMessageHandler outboundMessageHandler) {
      checkNotNull(outboundMessageHandler);
      this.outboundMessageHandler = outboundMessageHandler;
      return this;
    }

    public Builder timerUtil(final TimerUtil timerUtil) {
      checkNotNull(timerUtil);
      this.timerUtil = timerUtil;
      return this;
    }

    public Builder workerExecutor(final AsyncExecutor workerExecutor) {
      checkNotNull(workerExecutor);
      this.workerExecutor = workerExecutor;
      return this;
    }

    public Builder tableRefreshIntervalMs(final long tableRefreshIntervalMs) {
      checkArgument(tableRefreshIntervalMs >= 0);
      this.tableRefreshIntervalMs = tableRefreshIntervalMs;
      return this;
    }

    public Builder cleanPeerTableIntervalMs(final long cleanPeerTableIntervalMs) {
      checkArgument(cleanPeerTableIntervalMs >= 0);
      this.cleanPeerTableIntervalMs = cleanPeerTableIntervalMs;
      return this;
    }

    public Builder peerRequirement(final PeerRequirement peerRequirement) {
      checkNotNull(peerRequirement);
      this.peerRequirement = peerRequirement;
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      checkNotNull(peerPermissions);
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      checkNotNull(metricsSystem);
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder filterOnEnrForkId(final boolean filterOnEnrForkId) {
      this.filterOnEnrForkId = filterOnEnrForkId;
      return this;
    }

    public Builder cacheForEnrRequests(final Cache<Bytes, Packet> cacheToUse) {
      checkNotNull(cacheToUse);
      this.cachedEnrRequests = cacheToUse;
      return this;
    }

    public Builder rlpxAgent(final RlpxAgent rlpxAgent) {
      checkNotNull(rlpxAgent);
      this.rlpxAgent = rlpxAgent;
      return this;
    }

    public Builder includeBootnodesOnPeerRefresh(final boolean includeBootnodesOnPeerRefresh) {
      this.includeBootnodesOnPeerRefresh = includeBootnodesOnPeerRefresh;
      return this;
    }

    public Builder setPacketFactory(final PacketFactory packetFactory) {
      this.packetFactory = packetFactory;
      return this;
    }

    public Builder setPingPacketDataFactory(final PingPacketDataFactory pingPacketDataFactory) {
      this.pingPacketDataFactory = pingPacketDataFactory;
      return this;
    }

    public Builder setPongPacketDataFactory(final PongPacketDataFactory pongPacketDataFactory) {
      this.pongPacketDataFactory = pongPacketDataFactory;
      return this;
    }

    public Builder setFindNeighborsPacketDataFactory(
        final FindNeighborsPacketDataFactory findNeighborsPacketDataFactory) {
      this.findNeighborsPacketDataFactory = findNeighborsPacketDataFactory;
      return this;
    }

    public Builder setNeighborsPacketDataFactory(
        final NeighborsPacketDataFactory neighborsPacketDataFactory) {
      this.neighborsPacketDataFactory = neighborsPacketDataFactory;
      return this;
    }

    public Builder setEnrRequestPacketDataFactory(
        final EnrRequestPacketDataFactory enrRequestPacketDataFactory) {
      this.enrRequestPacketDataFactory = enrRequestPacketDataFactory;
      return this;
    }

    public Builder setEnrResponsePacketDataFactory(
        final EnrResponsePacketDataFactory enrResponsePacketDataFactory) {
      this.enrResponsePacketDataFactory = enrResponsePacketDataFactory;
      return this;
    }
  }
}
