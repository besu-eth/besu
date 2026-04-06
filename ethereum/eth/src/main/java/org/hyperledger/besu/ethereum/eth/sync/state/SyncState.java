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
package org.hyperledger.besu.ethereum.eth.sync.state;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.DefaultSyncStatus;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.core.Synchronizer.InSyncListener;
import org.hyperledger.besu.ethereum.eth.manager.ChainHeadEstimate;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloadStatus;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.BesuEvents.SyncStatusListener;
import org.hyperledger.besu.plugin.services.BesuEvents.TTDReachedListener;
import org.hyperledger.besu.util.Subscribers;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SyncState {

  private final Blockchain blockchain;
  private final EthPeers ethPeers;

  private final AtomicLong inSyncSubscriberId = new AtomicLong();
  private final Map<Long, InSyncTracker> inSyncTrackers = new ConcurrentHashMap<>();
  private final Subscribers<SyncStatusListener> syncStatusListeners = Subscribers.create();
  private final Subscribers<TTDReachedListener> ttdReachedListeners = Subscribers.create();

  private final Subscribers<InitialSyncCompletionListener> completionListenerSubscribers =
      Subscribers.create();

  private volatile long chainHeightListenerId;
  private volatile Optional<SyncTarget> syncTarget = Optional.empty();
  private Optional<WorldStateDownloadStatus> worldStateDownloadStatus = Optional.empty();
  private Optional<Long> newPeerListenerId;
  private Optional<Boolean> reachedTerminalDifficulty = Optional.empty();
  private final Optional<Checkpoint> checkpoint;
  private volatile boolean isInitialSyncPhaseDone;

  private volatile boolean isResyncNeeded;

  private Optional<Address> maybeAccountToRepair = Optional.empty();

  public SyncState(final Blockchain blockchain, final EthPeers ethPeers) {
    this(blockchain, ethPeers, false, Optional.empty());
  }

  public SyncState(
      final Blockchain blockchain,
      final EthPeers ethPeers,
      final boolean hasInitialSyncPhase,
      final Optional<Checkpoint> checkpoint) {
    this.blockchain = blockchain;
    this.ethPeers = ethPeers;
    isInitialSyncPhaseDone = !hasInitialSyncPhase;

    blockchain.observeBlockAdded(
        event -> {
          if (event.isNewCanonicalHead()) {
            checkInSync();
          }
        });

    // Add new peer listener to prevent permissioned PoA network stalling on start-up.
    // https://github.com/hyperledger/besu/issues/528
    newPeerListenerId =
        Optional.of(
            ethPeers.subscribeConnect(
                newPeer -> {
                  if (newPeer.readyForRequests()) {
                    checkInSync();
                  }
                }));
    this.checkpoint = checkpoint;
  }

  /**
   * Add a listener that will be notified when this node's sync status changes. A node is considered
   * in-sync if the local chain height is no more than {@code SYNC_TOLERANCE} behind the highest
   * estimated remote chain height.
   *
   * @param listener The callback to invoke when the sync status changes
   * @return An {@code Unsubscriber} that can be used to stop listening for these events
   */
  public long subscribeInSync(final InSyncListener listener) {
    return subscribeInSync(listener, Synchronizer.DEFAULT_IN_SYNC_TOLERANCE);
  }

  /**
   * Add a listener that will be notified when this node's sync status changes. A node is considered
   * in-sync if the local chain height is no more than {@code syncTolerance} behind the highest
   * estimated remote chain height.
   *
   * @param listener The callback to invoke when the sync status changes
   * @param syncTolerance The tolerance used to determine whether this node is in-sync. A value of
   *     zero means that the node is considered in-sync only when the local chain height is greater
   *     than or equal to the best estimated remote chain height.
   * @return An {@code Unsubscriber} that can be used to stop listening for these events
   */
  public long subscribeInSync(final InSyncListener listener, final long syncTolerance) {
    final InSyncTracker inSyncTracker = InSyncTracker.create(listener, syncTolerance);
    final long id = inSyncSubscriberId.incrementAndGet();
    inSyncTrackers.put(id, inSyncTracker);

    return id;
  }

  public boolean unsubscribeInSync(final long subscriberId) {
    return inSyncTrackers.remove(subscriberId) != null;
  }

  public long subscribeSyncStatus(final SyncStatusListener listener) {
    return syncStatusListeners.subscribe(listener);
  }

  public long subscribeTTDReached(final TTDReachedListener listener) {
    return ttdReachedListeners.subscribe(listener);
  }

  public long subscribeCompletionReached(final InitialSyncCompletionListener listener) {
    return completionListenerSubscribers.subscribe(listener);
  }

  public boolean unsubscribeSyncStatus(final long listenerId) {
    return syncStatusListeners.unsubscribe(listenerId);
  }

  public boolean unsubscribeTTDReached(final long listenerId) {
    return ttdReachedListeners.unsubscribe(listenerId);
  }

  public boolean unsubscribeInitialConditionReached(final long listenerId) {
    return completionListenerSubscribers.unsubscribe(listenerId);
  }

  public Optional<SyncStatus> syncStatus() {
    return syncStatus(syncTarget);
  }

  public Optional<SyncTarget> syncTarget() {
    return syncTarget;
  }

  public void setSyncTarget(final EthPeer peer, final BlockHeader commonAncestor) {
    final SyncTarget syncTarget = new SyncTarget(peer, commonAncestor);
    replaceSyncTarget(Optional.of(syncTarget));
  }

  public void setSyncProgress(
      final long startingBlock, final long currentBlock, final long highestBlock) {
    final SyncStatus status =
        new DefaultSyncStatus(
            startingBlock, currentBlock, highestBlock, Optional.empty(), Optional.empty());
    syncStatusListeners.forEach(c -> c.onSyncStatusChanged(Optional.of(status)));
  }

  public void setWorldStateDownloadStatus(final WorldStateDownloadStatus worldStateDownloadStatus) {
    this.worldStateDownloadStatus = Optional.ofNullable(worldStateDownloadStatus);
  }

  public boolean isInSync() {
    return isInSync(Synchronizer.DEFAULT_IN_SYNC_TOLERANCE);
  }

  public boolean isInSync(final long syncTolerance) {
    return isInSync(
        getLocalChainHead(), getSyncTargetChainHead(), getBestPeerChainHead(), syncTolerance);
  }

  public void setReachedTerminalDifficulty(final boolean stoppedAtTerminalDifficulty) {
    this.reachedTerminalDifficulty = Optional.of(stoppedAtTerminalDifficulty);
    ttdReachedListeners.forEach(listener -> listener.onTTDReached(stoppedAtTerminalDifficulty));
  }

  public Optional<Boolean> hasReachedTerminalDifficulty() {
    if (isInitialSyncPhaseDone) {
      return reachedTerminalDifficulty;
    }
    return Optional.of(Boolean.FALSE);
  }

  private boolean isInSync(
      final ChainHead localChain,
      final Optional<ChainHeadEstimate> syncTargetChain,
      final Optional<ChainHeadEstimate> bestPeerChain,
      final long syncTolerance) {
    return isInitialSyncPhaseDone
        && reachedTerminalDifficulty.orElse(true)
        // Sync target may be temporarily empty while we switch sync targets during a sync, so
        // check both the sync target and our best peer to determine if we're in sync or not
        && isInSync(localChain, syncTargetChain, syncTolerance)
        && isInSync(localChain, bestPeerChain, syncTolerance);
  }

  private boolean isInSync(
      final ChainHead localChain,
      final Optional<ChainHeadEstimate> remoteChain,
      final long syncTolerance) {
    return remoteChain
        .map(remoteState -> InSyncTracker.isInSync(localChain, remoteState, syncTolerance))
        .orElse(true);
  }

  private ChainHead getLocalChainHead() {
    return blockchain.getChainHead();
  }

  private Optional<ChainHeadEstimate> getSyncTargetChainHead() {
    return syncTarget.map(SyncTarget::peer).map(EthPeer::chainStateSnapshot);
  }

  public Optional<ChainHeadEstimate> getBestPeerChainHead() {
    return ethPeers.bestPeerWithHeightEstimate().map(EthPeer::chainStateSnapshot);
  }

  public void disconnectSyncTarget(final DisconnectReason reason) {
    syncTarget.ifPresent(syncTarget -> syncTarget.peer().disconnect(reason));
  }

  public void clearSyncTarget() {
    replaceSyncTarget(Optional.empty());
  }

  private synchronized void replaceSyncTarget(final Optional<SyncTarget> newTarget) {
    if (syncTarget.equals(newTarget)) {
      // Nothing to do
      return;
    }
    syncTarget.ifPresent(this::removeEstimatedHeightListener);
    syncTarget = newTarget;
    newTarget.ifPresent(this::addEstimatedHeightListener);
    publishSyncStatus(newTarget);
    checkInSync();
  }

  private void publishSyncStatus(final Optional<SyncTarget> newTarget) {
    final Optional<SyncStatus> syncStatus = syncStatus(newTarget);
    syncStatusListeners.forEach(c -> c.onSyncStatusChanged(syncStatus));
  }

  private Optional<SyncStatus> syncStatus(final Optional<SyncTarget> maybeTarget) {
    return maybeTarget.map(
        target -> {
          final long chainHeadBlockNumber = blockchain.getChainHeadBlockNumber();
          final long commonAncestor = target.commonAncestor().getNumber();
          final long highestKnownBlock = bestChainHeight(chainHeadBlockNumber);
          return new DefaultSyncStatus(
              commonAncestor,
              chainHeadBlockNumber,
              highestKnownBlock,
              worldStateDownloadStatus.flatMap(WorldStateDownloadStatus::getPulledStates),
              worldStateDownloadStatus.flatMap(WorldStateDownloadStatus::getKnownStates));
        });
  }

  private void removeEstimatedHeightListener(final SyncTarget target) {
    target.removePeerChainEstimatedHeightListener(chainHeightListenerId);
  }

  private void addEstimatedHeightListener(final SyncTarget target) {
    chainHeightListenerId =
        target.addPeerChainEstimatedHeightListener(estimatedHeight -> checkInSync());
  }

  public long getLocalChainHeight() {
    return blockchain.getChainHeadBlockNumber();
  }

  public long bestChainHeight() {
    final long localChainHeight = blockchain.getChainHeadBlockNumber();
    return bestChainHeight(localChainHeight);
  }

  public long bestChainHeight(final long localChainHeight) {
    return Math.max(
        localChainHeight,
        ethPeers
            .bestPeerWithHeightEstimate()
            .map(p -> p.chainState().getEstimatedHeight())
            .orElse(localChainHeight));
  }

  private synchronized void checkInSync() {
    final ChainHead localChain = getLocalChainHead();
    final Optional<ChainHeadEstimate> syncTargetChain = getSyncTargetChainHead();
    final Optional<ChainHeadEstimate> bestPeerChain = getBestPeerChainHead();

    // Remove listener when we've found a peer.
    newPeerListenerId.ifPresent(
        listenerId -> {
          ethPeers.unsubscribeConnect(listenerId);
          newPeerListenerId = Optional.empty();
        });

    inSyncTrackers
        .values()
        .forEach(
            (syncTracker) -> syncTracker.checkState(localChain, syncTargetChain, bestPeerChain));
  }

  public Optional<Checkpoint> getCheckpoint() {
    return checkpoint;
  }

  public boolean isInitialSyncPhaseDone() {
    return isInitialSyncPhaseDone;
  }

  public void markInitialSyncPhaseAsDone() {
    isInitialSyncPhaseDone = true;
    isResyncNeeded = false;
    completionListenerSubscribers.forEach(InitialSyncCompletionListener::onInitialSyncCompleted);
  }

  public boolean isResyncNeeded() {
    return isResyncNeeded;
  }

  public void markResyncNeeded() {
    isResyncNeeded = true;
  }

  public Optional<Address> getAccountToRepair() {
    return maybeAccountToRepair;
  }

  public void markAccountToRepair(final Optional<Address> address) {
    maybeAccountToRepair = address;
  }

  public void markInitialSyncRestart() {
    isInitialSyncPhaseDone = false;
    completionListenerSubscribers.forEach(InitialSyncCompletionListener::onInitialSyncRestart);
  }
}
