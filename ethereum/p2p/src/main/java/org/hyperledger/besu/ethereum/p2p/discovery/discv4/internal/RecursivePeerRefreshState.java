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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecursivePeerRefreshState {
  private static final Logger LOG = LoggerFactory.getLogger(RecursivePeerRefreshState.class);
  private static final int MAX_CONCURRENT_REQUESTS = 3;
  private Bytes target;
  private final PeerDiscoveryPermissions peerPermissions;
  private final PeerTable peerTable;
  private final DiscoveryPeerV4 localPeer;

  private final BondingAgent bondingAgent;
  private final FindNeighbourDispatcher findNeighbourDispatcher;
  private Optional<RoundTimeout> currentRoundTimeout = Optional.empty();
  private boolean iterativeSearchInProgress = false;
  private final int maxRounds;
  private int currentRound;

  private final NavigableMap<Bytes, MetadataPeer> oneTrueMap = new TreeMap<>();

  private final TimerUtil timerUtil;
  private final int timeoutPeriodInSeconds;

  List<DiscoveryPeerV4> initialPeers;

  RecursivePeerRefreshState(
      final BondingAgent bondingAgent,
      final FindNeighbourDispatcher neighborFinder,
      final TimerUtil timerUtil,
      final DiscoveryPeerV4 localPeer,
      final PeerTable peerTable,
      final PeerDiscoveryPermissions peerPermissions,
      final int timeoutPeriodInSeconds,
      final int maxRounds) {
    this.bondingAgent = bondingAgent;
    this.findNeighbourDispatcher = neighborFinder;
    this.timerUtil = timerUtil;
    this.localPeer = localPeer;
    this.peerTable = peerTable;
    this.peerPermissions = peerPermissions;
    this.timeoutPeriodInSeconds = timeoutPeriodInSeconds;
    this.maxRounds = maxRounds;
  }

  void start(final List<DiscoveryPeerV4> initialPeers, final Bytes target) {
    // TODO check this flag earlier
    if (iterativeSearchInProgress) {
      LOG.debug(
          "Skip peer search because previous search ({}) is still in progress.", currentRound);
      return;
    }
    LOG.debug("Start peer search.");
    iterativeSearchInProgress = true;
    this.target = target;
    currentRoundTimeout.ifPresent(RoundTimeout::cancelTimeout);
    currentRound = 0;
    oneTrueMap.clear();
    addInitialPeers(initialPeers);
    bondingInitiateRound();
  }

  private boolean reachedMaximumNumberOfRounds() {
    return currentRound >= maxRounds;
  }

  private void addInitialPeers(final List<DiscoveryPeerV4> initialPeers) {
    LOG.debug("{} INITIAL PEERS: {}", initialPeers.size(), initialPeers);
    this.initialPeers = initialPeers;
    for (final DiscoveryPeerV4 peer : initialPeers) {
      final MetadataPeer iterationParticipant =
          new MetadataPeer(peer, PeerDistanceCalculator.distance(target, peer.getId()));
      oneTrueMap.put(peer.getId(), iterationParticipant);
    }
  }

  private void bondingInitiateRound() {
    if (!iterativeSearchInProgress) {
      // cancelled so we can ignore
      return;
    }
    currentRoundTimeout.ifPresent(RoundTimeout::cancelTimeout);
    final List<MetadataPeer> candidates = bondingRoundCandidates();
    if (candidates.isEmpty()) {
      // All peers are already bonded (or failed to bond) so immediately switch to neighbours round
      LOG.debug("Skipping bonding round because no candidates are available");
      neighboursInitiateRound();
      return;
    }
    LOG.debug("Initiating bonding round with {} candidates", candidates.size());
    for (final MetadataPeer peer : candidates) {
      peer.bondingStarted();
      bondingAgent.performBonding(peer.getPeer());
    }
    currentRoundTimeout = Optional.of(scheduleTimeout(this::bondingCancelOutstandingRequests));
  }

  private RoundTimeout scheduleTimeout(final Runnable onTimeout) {
    final AtomicBoolean timeoutCancelled = new AtomicBoolean(false);
    final long timerId =
        timerUtil.setTimer(
            TimeUnit.SECONDS.toMillis(this.timeoutPeriodInSeconds),
            () -> performIfNotCancelled(onTimeout, timeoutCancelled));
    return new RoundTimeout(timeoutCancelled, timerId);
  }

  private void performIfNotCancelled(final Runnable action, final AtomicBoolean cancelled) {
    if (!cancelled.get()) {
      action.run();
    }
  }

  private void bondingCancelOutstandingRequests() {
    if (!iterativeSearchInProgress) {
      // cancelled so we can ignore
      return;
    }
    LOG.debug("Bonding round timed out");
    for (final Map.Entry<Bytes, MetadataPeer> entry : oneTrueMap.entrySet()) {
      final MetadataPeer metadataPeer = entry.getValue();
      if (metadataPeer.hasOutstandingBondRequest()) {
        // We're setting bonding to "complete" here, by "cancelling" the outstanding request.
        metadataPeer.bondingComplete();
      }
    }
    neighboursInitiateRound();
  }

  private void neighboursInitiateRound() {
    if (!iterativeSearchInProgress) {
      // cancelled so we can ignore
      return;
    }
    currentRoundTimeout.ifPresent(RoundTimeout::cancelTimeout);
    final List<MetadataPeer> candidates = neighboursRoundCandidates();
    if (candidates.isEmpty() || reachedMaximumNumberOfRounds()) {
      LOG.debug(
          "Iterative peer search complete.  {} peers processed over {} rounds.",
          oneTrueMap.size(),
          currentRound + 1);
      iterativeSearchInProgress = false;
      return;
    }
    LOG.debug(
        "Initiating neighbours round with {} candidates from {} tracked nodes",
        candidates.size(),
        oneTrueMap.size());
    for (final MetadataPeer peer : candidates) {
      peer.findNeighboursStarted();
      findNeighbourDispatcher.findNeighbours(peer.getPeer(), target);
    }
    currentRoundTimeout = Optional.of(scheduleTimeout(this::neighboursCancelOutstandingRequests));
    currentRound++;
  }

  private void neighboursCancelOutstandingRequests() {
    if (!iterativeSearchInProgress) {
      // cancelled so we can ignore
      return;
    }
    LOG.debug("Neighbours round timed out");
    for (final Map.Entry<Bytes, MetadataPeer> entry : oneTrueMap.entrySet()) {
      final MetadataPeer metadataPeer = entry.getValue();
      if (metadataPeer.hasOutstandingNeighboursRequest()) {
        metadataPeer.findNeighboursFailed();
      }
    }
    bondingInitiateRound();
  }

  private boolean satisfiesMapAdditionCriteria(final DiscoveryPeerV4 discoPeer) {
    return !oneTrueMap.containsKey(discoPeer.getId())
        && (initialPeers.contains(discoPeer) || !peerTable.get(discoPeer).isPresent())
        && !discoPeer.getId().equals(localPeer.getId())
        && !peerTable.isIpAddressInvalid(discoPeer.getEndpoint());
  }

  void onNeighboursReceived(final DiscoveryPeerV4 peer, final List<DiscoveryPeerV4> peers) {
    final MetadataPeer metadataPeer = oneTrueMap.get(peer.getId());
    if (metadataPeer == null) {
      return;
    }
    LOG.debug(
        "Received neighbours packet with {} neighbours from {}",
        peers.size(),
        peer.getEnodeURLString());
    for (final DiscoveryPeerV4 receivedDiscoPeer : peers) {
      if (satisfiesMapAdditionCriteria(receivedDiscoPeer)) {
        final MetadataPeer receivedMetadataPeer =
            new MetadataPeer(
                receivedDiscoPeer,
                PeerDistanceCalculator.distance(target, receivedDiscoPeer.getId()));
        oneTrueMap.put(receivedDiscoPeer.getId(), receivedMetadataPeer);
      }
    }

    if (!metadataPeer.hasOutstandingNeighboursRequest()) {
      return;
    }
    metadataPeer.findNeighboursComplete();
    if (neighboursRoundTermination()) {
      bondingInitiateRound();
    }
  }

  void onBondingComplete(final DiscoveryPeerV4 peer) {
    if (!iterativeSearchInProgress) {
      // cancelled so we can ignore
      return;
    }
    final MetadataPeer iterationParticipant = oneTrueMap.get(peer.getId());
    if (iterationParticipant == null) {
      return;
    }
    if (!iterationParticipant.hasOutstandingBondRequest()) {
      return;
    }
    iterationParticipant.bondingComplete();
    if (bondingRoundTermination()) {
      neighboursInitiateRound();
    }
  }

  private boolean neighboursRoundTermination() {
    for (final Map.Entry<Bytes, MetadataPeer> entry : oneTrueMap.entrySet()) {
      final MetadataPeer metadataPeer = entry.getValue();
      if (metadataPeer.hasOutstandingNeighboursRequest()) {
        return false;
      }
    }
    return true;
  }

  private boolean bondingRoundTermination() {
    for (final Map.Entry<Bytes, MetadataPeer> entry : oneTrueMap.entrySet()) {
      final MetadataPeer metadataPeer = entry.getValue();
      if (metadataPeer.hasOutstandingBondRequest()) {
        return false;
      }
    }
    return true;
  }

  private List<MetadataPeer> bondingRoundCandidates() {
    return oneTrueMap.values().stream()
        .filter(MetadataPeer::isBondingCandidate)
        .filter(p -> peerPermissions.allowOutboundBonding(p.getPeer()))
        .collect(Collectors.toList());
  }

  private List<MetadataPeer> neighboursRoundCandidates() {
    return oneTrueMap.values().stream()
        .filter(MetadataPeer::isNeighboursRoundCandidate)
        .filter(p -> peerPermissions.allowOutboundNeighborsRequest(p.getPeer()))
        .limit(MAX_CONCURRENT_REQUESTS)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  void cancel() {
    iterativeSearchInProgress = false;
  }

  /**
   * Whether an iterative peer search is currently running. While it is, {@link #start} is a no-op.
   *
   * @return true if a search is in progress
   */
  boolean isSearchInProgress() {
    return iterativeSearchInProgress;
  }

  public static class MetadataPeer implements Comparable<MetadataPeer> {

    DiscoveryPeerV4 peer;
    int distance;

    boolean bondingStarted = false;
    boolean bondingComplete = false;
    boolean findNeighboursStarted = false;
    boolean findNeighboursComplete = false;

    MetadataPeer(final DiscoveryPeerV4 peer, final int distance) {
      this.peer = peer;
      this.distance = distance;
    }

    DiscoveryPeerV4 getPeer() {
      return peer;
    }

    void bondingStarted() {
      this.bondingStarted = true;
    }

    void bondingComplete() {
      this.bondingComplete = true;
    }

    void findNeighboursStarted() {
      this.findNeighboursStarted = true;
    }

    void findNeighboursComplete() {
      this.findNeighboursComplete = true;
    }

    void findNeighboursFailed() {
      this.findNeighboursComplete = true;
    }

    private boolean isBondingCandidate() {
      return !bondingComplete && !bondingStarted && peer.getStatus() == PeerDiscoveryStatus.KNOWN;
    }

    private boolean isNeighboursRoundCandidate() {
      return peer.getStatus() == PeerDiscoveryStatus.BONDED && !findNeighboursStarted;
    }

    private boolean hasOutstandingBondRequest() {
      return bondingStarted && !bondingComplete;
    }

    private boolean hasOutstandingNeighboursRequest() {
      return findNeighboursStarted && !findNeighboursComplete;
    }

    @Override
    public int compareTo(final MetadataPeer o) {
      if (this.distance > o.distance) {
        return 1;
      }
      return -1;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final MetadataPeer that = (MetadataPeer) o;
      return Objects.equals(peer.getId(), that.peer.getId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(peer.getId());
    }

    @Override
    public String toString() {
      return peer + ": " + distance;
    }
  }

  @FunctionalInterface
  public interface FindNeighbourDispatcher {
    /**
     * Sends a FIND_NEIGHBORS message to a {@link DiscoveryPeerV4}, in search of a target value.
     *
     * @param peer the peer to interrogate
     * @param target the target node ID to find
     */
    void findNeighbours(final DiscoveryPeerV4 peer, final Bytes target);
  }

  @FunctionalInterface
  public interface BondingAgent {
    /**
     * Initiates a bonding PING-PONG cycle with a peer.
     *
     * @param peer The targeted peer.
     */
    void performBonding(final DiscoveryPeerV4 peer);
  }

  private class RoundTimeout {
    private final AtomicBoolean timeoutCancelled;
    private final long timerId;

    private RoundTimeout(final AtomicBoolean timeoutCancelled, final long timerId) {
      this.timeoutCancelled = timeoutCancelled;
      this.timerId = timerId;
    }

    public void cancelTimeout() {
      timerUtil.cancelTimer(timerId);
      timeoutCancelled.set(true);
    }
  }
}
