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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer.DisconnectCallback;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerSelector;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.ChainHeadTracker;
import org.hyperledger.besu.ethereum.eth.sync.SnapServerChecker;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerClientName;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.util.Subscribers;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EthPeers implements PeerSelector {
  private static final Logger LOG = LoggerFactory.getLogger(EthPeers.class);
  public static final Comparator<EthPeerImmutableAttributes> TOTAL_DIFFICULTY =
      Comparator.comparing((final EthPeerImmutableAttributes p) -> p.estimatedTotalDifficulty());

  public static final Comparator<EthPeerImmutableAttributes> CHAIN_HEIGHT =
      Comparator.comparing((final EthPeerImmutableAttributes p) -> p.estimatedChainHeight());

  public static final Comparator<EthPeerImmutableAttributes> MOST_USEFUL_PEER =
      Comparator.comparing((final EthPeerImmutableAttributes p) -> p.reputationScore())
          .thenComparing(CHAIN_HEIGHT);

  public static final Comparator<EthPeerImmutableAttributes> TOTAL_DIFFICULTY_THEN_HEIGHT =
      TOTAL_DIFFICULTY.thenComparing(CHAIN_HEIGHT);

  public static final Comparator<EthPeerImmutableAttributes> LEAST_TO_MOST_BUSY =
      Comparator.comparing(EthPeerImmutableAttributes::outstandingRequests)
          .thenComparing(EthPeerImmutableAttributes::lastRequestTimestamp);
  public static final int NODE_ID_LENGTH = 64;
  public static final int USEFULL_PEER_SCORE_THRESHOLD = 102;

  private final Map<Bytes, EthPeer> activeConnections = new ConcurrentHashMap<>();

  private final Cache<PeerConnection, EthPeer> incompleteConnections =
      CacheBuilder.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(20L))
          .concurrencyLevel(1)
          .removalListener(this::onCacheRemoval)
          .build();
  private final Clock clock;
  private final List<NodeMessagePermissioningProvider> permissioningProviders;
  private final int maxMessageSize;
  private final Subscribers<ConnectCallback> connectCallbacks = Subscribers.create();
  private final Subscribers<DisconnectCallback> disconnectCallbacks = Subscribers.create();
  private final Collection<PendingPeerRequest> pendingRequests = new CopyOnWriteArrayList<>();
  private final int peerUpperBound;
  private final int maxRemotelyInitiatedConnections;
  private final Boolean randomPeerPriority;
  private final Bytes nodeIdMask = Bytes.random(NODE_ID_LENGTH);
  private final Supplier<ProtocolSpec> currentProtocolSpecSupplier;
  private final SyncMode syncMode;
  private final ForkIdManager forkIdManager;
  private final int snapServerTargetNumber;
  private final boolean shouldLimitRemoteConnections;

  private Comparator<EthPeerImmutableAttributes> bestPeerComparator;
  private final Bytes localNodeId;
  private RlpxAgent rlpxAgent;

  private final Counter connectedPeersCounter;
  private ChainHeadTracker tracker;
  private SnapServerChecker snapServerChecker;
  private boolean snapServerPeersNeeded = false;
  private Supplier<TrailingPeerRequirements> trailingPeerRequirementsSupplier =
      () -> TrailingPeerRequirements.UNRESTRICTED;

  public EthPeers(
      final Supplier<ProtocolSpec> currentProtocolSpecSupplier,
      final Clock clock,
      final MetricsSystem metricsSystem,
      final int maxMessageSize,
      final List<NodeMessagePermissioningProvider> permissioningProviders,
      final Bytes localNodeId,
      final int peerUpperBound,
      final int maxRemotelyInitiatedConnections,
      final Boolean randomPeerPriority,
      final SyncMode syncMode,
      final ForkIdManager forkIdManager) {
    this.currentProtocolSpecSupplier = currentProtocolSpecSupplier;
    this.clock = clock;
    this.permissioningProviders = permissioningProviders;
    this.maxMessageSize = maxMessageSize;
    this.bestPeerComparator = TOTAL_DIFFICULTY_THEN_HEIGHT;
    this.localNodeId = localNodeId;
    this.peerUpperBound = peerUpperBound;
    this.maxRemotelyInitiatedConnections = maxRemotelyInitiatedConnections;
    this.randomPeerPriority = randomPeerPriority;
    LOG.trace("MaxPeers: {}, Max Remote: {}", peerUpperBound, maxRemotelyInitiatedConnections);
    this.syncMode = syncMode;
    this.forkIdManager = forkIdManager;
    this.snapServerTargetNumber =
        peerUpperBound / 2; // 50% of peers should be snap servers while snap syncing
    this.shouldLimitRemoteConnections = maxRemotelyInitiatedConnections < peerUpperBound;

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.ETHEREUM,
        "peer_count",
        "The current number of peers connected",
        activeConnections::size);
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.ETHEREUM,
        "peer_count_snap_server",
        "The current number of peers connected that serve snap data",
        () ->
            (int) streamAvailablePeers().filter(EthPeerImmutableAttributes::isServingSnap).count());
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.PEERS,
        "pending_peer_requests_current",
        "Number of peer requests currently pending because peers are busy",
        pendingRequests::size);
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.ETHEREUM,
        "peer_limit",
        "The maximum number of peers this node allows to connect",
        () -> peerUpperBound);

    connectedPeersCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PEERS, "connected_total", "Total number of peers connected");

    final LabelledSuppliedMetric peerClientLabelledGauge =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.PEERS,
            "peer_count_by_client",
            "The number of clients connected by client",
            "client");

    for (final var clientName : PeerClientName.values()) {
      peerClientLabelledGauge.labels(
          () -> countConnectedPeersByClientName(clientName), clientName.getDisplayName());
    }
  }

  private double countConnectedPeersByClientName(final PeerClientName clientName) {
    return streamAllActiveConnections()
        .map(PeerConnection::getPeerInfo)
        .map(PeerInfo::getClientName)
        .filter(clientName::equals)
        .count();
  }

  public void registerNewConnection(
      final PeerConnection newConnection, final List<PeerValidator> peerValidators) {
    final Bytes id = newConnection.getPeer().getId();
    synchronized (this) {
      EthPeer ethPeer = activeConnections.get(id);
      if (ethPeer == null) {
        final Optional<EthPeer> peerInList =
            incompleteConnections.asMap().values().stream()
                .filter(p -> p.getId().equals(id))
                .findFirst();
        ethPeer =
            peerInList.orElseGet(
                () ->
                    new EthPeer(
                        newConnection,
                        this::ethPeerStatusExchanged,
                        peerValidators,
                        maxMessageSize,
                        clock,
                        permissioningProviders,
                        localNodeId));
      }
      incompleteConnections.put(newConnection, ethPeer);
    }
  }

  @NotNull
  private List<PeerConnection> getIncompleteConnections(final Bytes id) {
    return incompleteConnections.asMap().keySet().stream()
        .filter(nrc -> nrc.getPeer().getId().equals(id))
        .collect(Collectors.toList());
  }

  public boolean registerDisconnect(final PeerConnection connection) {
    final EthPeer peer = peer(connection);
    return registerDisconnect(peer, connection);
  }

  private boolean registerDisconnect(final EthPeer peer, final PeerConnection connection) {
    incompleteConnections.invalidate(connection);
    boolean removed = false;
    if (peer == null) {
      LOG.atTrace()
          .setMessage("attempt to remove null peer with connection {}")
          .addArgument(connection)
          .log();
      return false;
    }
    if (peer.getConnection().equals(connection)) {
      final Bytes id = peer.getId();
      if (!peerHasIncompleteConnection(id)) {
        removed = activeConnections.remove(id, peer);
        disconnectCallbacks.forEach(callback -> callback.onDisconnect(peer));
        peer.handleDisconnect();
        abortPendingRequestsAssignedToDisconnectedPeers();
        if (peer.getReputation().getScore() > USEFULL_PEER_SCORE_THRESHOLD) {
          LOG.atDebug().setMessage("Disconnected USEFUL peer {}").addArgument(peer).log();
        } else {
          LOG.atDebug()
              .setMessage("Disconnected EthPeer {}")
              .addArgument(peer.getLoggableId())
              .log();
        }
      }
    }
    reattemptPendingPeerRequests();
    return removed;
  }

  private boolean peerHasIncompleteConnection(final Bytes id) {
    return getIncompleteConnections(id).stream().anyMatch(conn -> !conn.isDisconnected());
  }

  private void abortPendingRequestsAssignedToDisconnectedPeers() {
    synchronized (this) {
      for (final PendingPeerRequest request : pendingRequests) {
        if (request.getAssignedPeer().map(EthPeer::isDisconnected).orElse(false)) {
          request.abort();
        }
      }
    }
  }

  public EthPeer peer(final PeerConnection connection) {
    final EthPeer ethPeer = incompleteConnections.getIfPresent(connection);
    return ethPeer != null ? ethPeer : activeConnections.get(connection.getPeer().getId());
  }

  public PendingPeerRequest executePeerRequest(
      final PeerRequest request, final long minimumBlockNumber, final Optional<EthPeer> peer) {
    final long actualMinBlockNumber;
    if (minimumBlockNumber > 0 && currentProtocolSpecSupplier.get().isPoS()) {
      // if on PoS do not enforce a min block number, since the estimated chain height of the remote
      // peer is not updated anymore.
      actualMinBlockNumber = 0;
    } else {
      actualMinBlockNumber = minimumBlockNumber;
    }
    final PendingPeerRequest pendingPeerRequest =
        new PendingPeerRequest(this, request, actualMinBlockNumber, peer);
    synchronized (this) {
      if (!pendingPeerRequest.attemptExecution()) {
        pendingRequests.add(pendingPeerRequest);
      }
    }
    return pendingPeerRequest;
  }

  public void dispatchMessage(
      final EthPeer peer, final EthMessage ethMessage, final String protocolName) {
    final Optional<RequestManager> maybeRequestManager = peer.dispatch(ethMessage, protocolName);
    if (maybeRequestManager.isPresent() && peer.hasAvailableRequestCapacity()) {
      reattemptPendingPeerRequests();
    }
  }

  public void dispatchMessage(final EthPeer peer, final EthMessage ethMessage) {
    dispatchMessage(peer, ethMessage, EthProtocol.NAME);
  }

  @VisibleForTesting
  void reattemptPendingPeerRequests() {
    synchronized (this) {
      final Iterator<PendingPeerRequest> iterator = pendingRequests.iterator();
      while (iterator.hasNext()
          && streamAvailablePeers()
              .anyMatch(EthPeerImmutableAttributes::hasAvailableRequestCapacity)) {
        final PendingPeerRequest request = iterator.next();
        if (request.attemptExecution()) {
          pendingRequests.remove(request);
        }
      }
    }
  }

  public long subscribeConnect(final ConnectCallback callback) {
    return connectCallbacks.subscribe(callback);
  }

  public void unsubscribeConnect(final long id) {
    connectCallbacks.unsubscribe(id);
  }

  public void subscribeDisconnect(final DisconnectCallback callback) {
    disconnectCallbacks.subscribe(callback);
  }

  public int peerCount() {
    removeDisconnectedPeers();
    return activeConnections.size();
  }

  public int getMaxPeers() {
    return peerUpperBound;
  }

  public Stream<EthPeerImmutableAttributes> streamAllPeers() {
    return activeConnections.values().stream().map(EthPeerImmutableAttributes::from);
  }

  private void removeDisconnectedPeers() {
    activeConnections
        .values()
        .forEach(
            ep -> {
              if (ep.isDisconnected()) {
                registerDisconnect(ep, ep.getConnection());
              }
            });
  }

  public Stream<EthPeerImmutableAttributes> streamAvailablePeers() {
    return streamAllPeers().filter(peer -> !peer.isDisconnected());
  }

  public Stream<EthPeerImmutableAttributes> streamBestPeers() {
    return streamAvailablePeers()
        .filter(EthPeerImmutableAttributes::isFullyValidated)
        .sorted(getBestPeerComparator().reversed());
  }

  public Optional<EthPeer> bestPeer() {
    return streamAvailablePeers()
        .max(getBestPeerComparator())
        .map(EthPeerImmutableAttributes::ethPeer);
  }

  public Optional<EthPeer> bestPeerWithHeightEstimate() {
    return bestPeerMatchingCriteria(p -> p.isFullyValidated() && p.hasEstimatedChainHeight());
  }

  public Optional<EthPeer> bestPeerMatchingCriteria(
      final Predicate<EthPeerImmutableAttributes> matchesCriteria) {
    return streamAvailablePeers()
        .filter(matchesCriteria)
        .max(getBestPeerComparator())
        .map(EthPeerImmutableAttributes::ethPeer);
  }

  public void setBestPeerComparator(final Comparator<EthPeerImmutableAttributes> comparator) {
    LOG.info("Updating the default best peer comparator");
    bestPeerComparator = comparator;
  }

  public Comparator<EthPeerImmutableAttributes> getBestPeerComparator() {
    return bestPeerComparator;
  }

  public void setRlpxAgent(final RlpxAgent rlpxAgent) {
    this.rlpxAgent = rlpxAgent;
  }

  public Stream<PeerConnection> streamAllActiveConnections() {
    return activeConnections.values().stream()
        .map(EthPeer::getConnection)
        .filter(c -> !c.isDisconnected());
  }

  public Stream<PeerConnection> streamAllConnections() {
    return Stream.concat(
            activeConnections.values().stream().map(EthPeer::getConnection),
            incompleteConnections.asMap().keySet().stream())
        .distinct()
        .filter(c -> !c.isDisconnected());
  }

  public Optional<DisconnectReason> gatePeerConnection(final Peer peer, final boolean inbound) {

    if (peer.getForkId().isPresent()) {
      final ForkId forkId = peer.getForkId().get();
      if (!forkIdManager.peerCheck(forkId)) {
        LOG.atDebug()
            .setMessage("Wrong fork id, not trying to connect to peer {}")
            .addArgument(peer::getId)
            .log();

        return Optional.of(DisconnectReason.USELESS_PEER_BY_CHAIN_COMPARATOR);
      }
    }

    final Bytes id = peer.getId();
    if (alreadyConnectedOrConnecting(inbound, id)) {
      LOG.atTrace()
          .setMessage("not connecting to peer {} - already connected")
          .addArgument(peer.getLoggableId())
          .log();
      return Optional.of(DisconnectReason.ALREADY_CONNECTED);
    }

    if (peerCount() < getMaxPeers() || needMoreSnapServers() || canExceedPeerLimits(id)) {
      return Optional.empty();
    } else {
      return Optional.of(DisconnectReason.TOO_MANY_PEERS);
    }
  }

  private boolean alreadyConnectedOrConnecting(final boolean inbound, final Bytes id) {
    final EthPeer ethPeer = activeConnections.get(id);
    if (ethPeer != null && !ethPeer.isDisconnected()) {
      return true;
    }
    final List<PeerConnection> incompleteConnections = getIncompleteConnections(id);
    return incompleteConnections.stream()
        .anyMatch(c -> !c.isDisconnected() && (!inbound || (inbound && c.inboundInitiated())));
  }

  public void disconnectWorstUselessPeer() {
    streamAvailablePeers()
        .filter(p -> !canExceedPeerLimits(p.ethPeer().getId()))
        .min(getBestPeerComparator())
        .ifPresent(
            peer -> {
              LOG.atDebug()
                  .setMessage(
                      "disconnecting peer {}. Waiting for better peers. Current {} of max {}")
                  .addArgument(peer.ethPeer().getLoggableId())
                  .addArgument(this::peerCount)
                  .addArgument(this::getMaxPeers)
                  .log();
              peer.ethPeer()
                  .disconnect(DisconnectMessage.DisconnectReason.USELESS_PEER_BY_CHAIN_COMPARATOR);
            });
  }

  public void setChainHeadTracker(final ChainHeadTracker tracker) {
    this.tracker = tracker;
  }

  public void setSnapServerChecker(final SnapServerChecker checker) {
    this.snapServerChecker = checker;
  }

  public void snapServerPeersNeeded(final boolean b) {
    this.snapServerPeersNeeded = b;
  }

  public void setTrailingPeerRequirementsSupplier(
      final Supplier<TrailingPeerRequirements> tprSupplier) {
    this.trailingPeerRequirementsSupplier = tprSupplier;
  }

  // Part of the PeerSelector interface, to be split apart later
  @Override
  public Optional<EthPeer> getPeer(final Predicate<EthPeerImmutableAttributes> filter) {
    return streamAvailablePeers()
        .filter(filter)
        .filter(EthPeerImmutableAttributes::hasAvailableRequestCapacity)
        .filter(EthPeerImmutableAttributes::isFullyValidated)
        .max(getBestPeerComparator())
        .map(EthPeerImmutableAttributes::ethPeer);
  }

  // Part of the PeerSelector interface, to be split apart later
  @Override
  public CompletableFuture<EthPeer> waitForPeer(
      final Predicate<EthPeerImmutableAttributes> filter) {
    final CompletableFuture<EthPeer> future = new CompletableFuture<>();
    LOG.debug("Waiting for peer matching filter. {} peers currently connected.", peerCount());
    // check for an existing peer matching the filter and use that if one is found
    Optional<EthPeer> maybePeer = getPeer(filter);
    if (maybePeer.isPresent()) {
      LOG.debug("Found peer matching filter already connected!");
      future.complete(maybePeer.get());
    } else {
      // no existing peer matches our filter. Subscribe to new connections until we find one
      LOG.debug("Subscribing to new peer connections to wait until one matches filter");
      final long subscriptionId =
          subscribeConnect(
              (peer) -> {
                if (!future.isDone() && filter.test(EthPeerImmutableAttributes.from(peer))) {
                  LOG.debug("Found new peer matching filter!");
                  future.complete(peer);
                } else {
                  LOG.debug("New peer does not match filter");
                }
              });
      future.handle(
          (peer, throwable) -> {
            LOG.debug("Unsubscribing from new peer connections with ID {}", subscriptionId);
            unsubscribeConnect(subscriptionId);
            return null;
          });
    }

    return future;
  }

  // Part of the PeerSelector interface, to be split apart later
  @Override
  public Optional<EthPeer> getPeerByPeerId(final PeerId peerId) {
    return Optional.ofNullable(activeConnections.get(peerId.getId()));
  }

  @FunctionalInterface
  public interface ConnectCallback {
    void onPeerConnected(EthPeer newPeer);
  }

  @Override
  public String toString() {
    if (activeConnections.isEmpty()) {
      return "0 EthPeers {}";
    }
    final String connectionsList =
        activeConnections.values().stream()
            .sorted()
            .map(EthPeer::toString)
            .collect(Collectors.joining(", \n"));
    return activeConnections.size() + " EthPeers {\n" + connectionsList + '}';
  }

  private void ethPeerStatusExchanged(final EthPeer peer) {
    // We have a connection to a peer that is on the right chain and is willing to connect to us.
    // Find out what the EthPeer block height is and whether it can serve snap data (if we are doing
    // snap sync)
    LOG.debug("Peer {} status exchanged", peer);
    assert tracker != null : "ChainHeadTracker must be set before EthPeers can be used";
    CompletableFuture<BlockHeader> future = tracker.getBestHeaderFromPeer(peer);

    future.whenComplete(
        (peerHeadBlockHeader, error) -> {
          if (peerHeadBlockHeader == null) {
            LOG.debug(
                "Failed to retrieve chain head info. Disconnecting {}... {}",
                peer.getLoggableId(),
                error);
            peer.disconnect(
                DisconnectMessage.DisconnectReason.USELESS_PEER_FAILED_TO_RETRIEVE_CHAIN_HEAD);
          } else {

            // we can check trailing peers now
            final TrailingPeerRequirements trailingPeerRequirements =
                trailingPeerRequirementsSupplier.get();
            if (trailingPeerRequirements != null) {
              if (peer.chainState().getEstimatedHeight()
                  < trailingPeerRequirements.getMinimumHeightToBeUpToDate()) {
                if (!(getNumTrailingPeers(trailingPeerRequirements.getMinimumHeightToBeUpToDate())
                    < trailingPeerRequirements.getMaxTrailingPeers())) {
                  LOG.atTrace()
                      .setMessage(
                          "Adding trailing peer {} would exceed max trailing peers {}. Disconnecting...")
                      .addArgument(peer.getLoggableId())
                      .addArgument(trailingPeerRequirements.getMaxTrailingPeers())
                      .log();
                  peer.disconnect(
                      DisconnectMessage.DisconnectReason.USELESS_PEER_EXCEEDS_TRAILING_PEERS);
                  return;
                }
              }
            }

            peer.chainState().updateHeightEstimate(peerHeadBlockHeader.getNumber());
            CompletableFuture<Void> isServingSnapFuture;
            if (syncMode == SyncMode.SNAP) {
              // even if we have finished the snap sync, we still want to know if the peer is a snap
              // server
              isServingSnapFuture =
                  CompletableFuture.runAsync(
                      () -> {
                        try {
                          checkIsSnapServer(peer, peerHeadBlockHeader);
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
            } else {
              isServingSnapFuture = CompletableFuture.completedFuture(null);
            }
            isServingSnapFuture.thenRun(
                () -> {
                  if (!peer.getConnection().isDisconnected() && addPeerToEthPeers(peer)) {
                    connectedPeersCounter.inc();
                    connectCallbacks.forEach(cb -> cb.onPeerConnected(peer));
                  }
                });
          }
        });
  }

  private void checkIsSnapServer(final EthPeer peer, final BlockHeader peersHeadBlockHeader) {
    if (peer.getAgreedCapabilities().contains(SnapProtocol.SNAP1)) {
      if (snapServerChecker != null) {
        // set that peer is a snap server for doing the test
        peer.setIsServingSnap(true);
        Boolean isServer;
        try {
          isServer = snapServerChecker.check(peer, peersHeadBlockHeader).get(6L, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOG.atTrace()
              .setMessage("Error checking if peer {} is a snap server. Setting to false.")
              .addArgument(peer.getLoggableId())
              .log();
          peer.setIsServingSnap(false);
          return;
        }
        peer.setIsServingSnap(isServer);
        LOG.atTrace()
            .setMessage("{}: peer {}")
            .addArgument(isServer ? "Is a snap server" : "Is NOT a snap server")
            .addArgument(peer.getLoggableId())
            .log();
      }
    }
  }

  private int comparePeerPriorities(final EthPeer p1, final EthPeer p2) {
    final PeerConnection a = p1.getConnection();
    final PeerConnection b = p2.getConnection();
    final boolean aCanExceedPeerLimits = canExceedPeerLimits(a.getPeer().getId());
    final boolean bCanExceedPeerLimits = canExceedPeerLimits(b.getPeer().getId());
    if (aCanExceedPeerLimits && !bCanExceedPeerLimits) {
      return -1;
    } else if (bCanExceedPeerLimits && !aCanExceedPeerLimits) {
      return 1;
    } else {
      return randomPeerPriority
          ? compareByMaskedNodeId(a, b)
          : compareConnectionInitiationTimes(a, b);
    }
  }

  private boolean canExceedPeerLimits(final Bytes peerId) {
    if (rlpxAgent == null) {
      return false;
    }
    return rlpxAgent.canExceedConnectionLimits(peerId);
  }

  private int compareConnectionInitiationTimes(final PeerConnection a, final PeerConnection b) {
    return Math.toIntExact(a.getInitiatedAt() - b.getInitiatedAt());
  }

  private int compareByMaskedNodeId(final PeerConnection a, final PeerConnection b) {
    return a.getPeer().getId().xor(nodeIdMask).compareTo(b.getPeer().getId().xor(nodeIdMask));
  }

  private void enforceRemoteConnectionLimits() {
    if (!shouldLimitRemoteConnections || peerCount() < maxRemotelyInitiatedConnections) {
      // Nothing to do
      return;
    }

    getActivePrioritizedPeers()
        .filter(p -> p.getConnection().inboundInitiated())
        .filter(p -> !canExceedPeerLimits(p.getId()))
        .skip(maxRemotelyInitiatedConnections)
        .forEach(
            conn -> {
              LOG.trace(
                  "Too many remotely initiated connections. Disconnect low-priority connection: {}, maxRemote={}",
                  conn,
                  maxRemotelyInitiatedConnections);
              conn.disconnect(DisconnectMessage.DisconnectReason.TOO_MANY_PEERS);
            });
  }

  private Stream<EthPeer> getActivePrioritizedPeers() {
    return activeConnections.values().stream()
        .filter(p -> !p.isDisconnected())
        .sorted(this::comparePeerPriorities);
  }

  private void enforceConnectionLimits() {
    if (peerCount() < peerUpperBound) {
      // Nothing to do - we're under our limits
      return;
    }
    getActivePrioritizedPeers()
        .skip(peerUpperBound)
        .map(EthPeer::getConnection)
        .filter(c -> !canExceedPeerLimits(c.getPeer().getId()))
        .forEach(
            conn -> {
              LOG.trace(
                  "Too many connections. Disconnect low-priority connection: {}, maxConnections={}",
                  conn,
                  peerUpperBound);
              conn.disconnect(DisconnectMessage.DisconnectReason.TOO_MANY_PEERS);
            });
  }

  private boolean inboundInitiatedConnectionLimitExceeded() {
    return shouldLimitRemoteConnections
        && countUntrustedRemotelyInitiatedConnections() > maxRemotelyInitiatedConnections;
  }

  private long countUntrustedRemotelyInitiatedConnections() {
    return activeConnections.values().stream()
        .map(EthPeer::getConnection)
        .filter(PeerConnection::inboundInitiated)
        .filter(c -> !c.isDisconnected())
        .filter(conn -> !canExceedPeerLimits(conn.getPeer().getId()))
        .count();
  }

  private void onCacheRemoval(
      final RemovalNotification<PeerConnection, EthPeer> removalNotification) {
    if (removalNotification.wasEvicted()) {
      final PeerConnection peerConnectionRemoved = removalNotification.getKey();
      final EthPeer peer = removalNotification.getValue();
      if (peer == null) {
        return;
      }
      final PeerConnection peerConnectionOfEthPeer = peer.getConnection();
      if (peerConnectionRemoved != null) {
        if (!peerConnectionRemoved.equals(peerConnectionOfEthPeer)) {
          // If this connection is not the connection of the EthPeer by now we can disconnect
          peerConnectionRemoved.disconnect(DisconnectMessage.DisconnectReason.ALREADY_CONNECTED);
        }
      }
    }
  }

  boolean addPeerToEthPeers(final EthPeer peer) {
    // We have a connection to a peer that is on the right chain and is willing to connect to us.
    // Figure out whether we want to add it to the active connections.
    final PeerConnection connection = peer.getConnection();
    if (activeConnections.containsValue(peer)) {
      return false;
    }

    final Bytes id = peer.getId();
    if (!randomPeerPriority) {

      if (peerCount() >= peerUpperBound) {
        final long numSnapServers = numberOfSnapServers();
        final boolean inboundLimitExceeded = inboundInitiatedConnectionLimitExceeded();
        // three reasons why we would disconnect an existing peer to accommodate the new peer
        if (canExceedPeerLimits(id)
            || (snapServerPeersNeeded
                && numSnapServers < snapServerTargetNumber
                && peer.isServingSnap())
            || (inboundLimitExceeded && !peer.getConnection().inboundInitiated())) {

          final boolean filterOutSnapServers =
              snapServerPeersNeeded && (numSnapServers <= snapServerTargetNumber);

          // find and disconnect the least useful peer we can disconnect
          activeConnections.values().stream()
              .filter(p -> !canExceedPeerLimits(p.getId()))
              .map(EthPeerImmutableAttributes::from)
              .filter(filterOutSnapServers ? p -> !p.isServingSnap() : p -> true)
              .filter(inboundLimitExceeded ? p -> p.isInboundInitiated() : p -> true)
              .min(MOST_USEFUL_PEER)
              .ifPresentOrElse(
                  pe -> {
                    pe.ethPeer().disconnect(DisconnectMessage.DisconnectReason.TOO_MANY_PEERS);
                    LOG.atTrace()
                        .setMessage("Disconnecting peer {} to be replaced by prioritised peer {}")
                        .addArgument(pe.ethPeer().getLoggableId())
                        .addArgument(peer.getLoggableId())
                        .log();
                  },
                  () -> // disconnect the least useful peer
                  activeConnections.values().stream()
                          .filter(p -> !canExceedPeerLimits(p.getId()))
                          .map(EthPeerImmutableAttributes::from)
                          .min(MOST_USEFUL_PEER)
                          .ifPresent(
                              p -> {
                                p.ethPeer()
                                    .disconnect(DisconnectMessage.DisconnectReason.TOO_MANY_PEERS);
                                LOG.atTrace()
                                    .setMessage(
                                        "Disconnecting peer {} to be replaced by prioritised peer {}")
                                    .addArgument(p.ethPeer().getLoggableId())
                                    .addArgument(peer.getLoggableId())
                                    .log();
                              }));
        } else {
          LOG.atTrace()
              .setMessage(
                  "Too many peers. Disconnect peer {} with connection: {}, max connections {}")
              .addArgument(peer.getLoggableId())
              .addArgument(connection)
              .addArgument(peerUpperBound)
              .log();
          connection.disconnect(DisconnectMessage.DisconnectReason.TOO_MANY_PEERS);
          return false;
        }
      }

      final boolean added = (activeConnections.putIfAbsent(id, peer) == null);
      if (added) {
        LOG.atTrace()
            .setMessage("Added peer {} with connection {} to activeConnections")
            .addArgument(id)
            .addArgument(connection)
            .log();
      } else {
        LOG.atTrace()
            .setMessage("Did not add peer {} with connection {} to activeConnections")
            .addArgument(id)
            .addArgument(connection)
            .log();
      }
      return added;

    } else {
      // randomPeerPriority! Add the peer and if there are too many connections fix it
      // TODO: random peer priority does not care yet about snap server peers -> check later
      activeConnections.putIfAbsent(id, peer);
      enforceRemoteConnectionLimits();
      enforceConnectionLimits();
      return activeConnections.containsKey(id);
    }
  }

  private long getNumTrailingPeers(final long minimumHeightToBeUpToDate) {
    return streamAvailablePeers()
        .filter(p -> p.estimatedChainHeight() < minimumHeightToBeUpToDate)
        .count();
  }

  private boolean needMoreSnapServers() {
    return snapServerPeersNeeded && numberOfSnapServers() < snapServerTargetNumber;
  }

  private long numberOfSnapServers() {
    return activeConnections.values().stream().filter(EthPeer::isServingSnap).count();
  }
}
