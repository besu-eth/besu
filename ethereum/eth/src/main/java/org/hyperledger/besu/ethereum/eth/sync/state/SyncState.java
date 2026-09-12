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

import org.hyperledger.besu.consensus.merge.NewPayloadListener;
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

public class SyncState implements NewPayloadListener {

  private final Blockchain blockchain;
  private final EthPeers ethPeers;

  // Ensures checkInSync() re-evaluation gives a consistent view of sync status. A
  // standalone lock is used instead of the object monitor to prevent checkInSync()
  // causing a deadlock while synchronized on the object monitor.
  private final Object inSyncLock = new Object();

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

  private volatile long lastPayloadBlockNumber = 0L;
  private volatile boolean payloadReceived = false;

  // Progress reported by a sync that does not use a sync target, i.e. snap sync. Retained so that
  // eth_syncing can report progress during the initial sync phase; cleared once that phase ends.
  // Null until the first report. Only the blocks are retained: the highest block is resolved per
  // read, see targetlessSyncStatus().
  private volatile TargetlessSyncProgress targetlessSyncProgress;

  // Set once markInitialSyncPhaseAsDone() has run, so that any later progress report can be
  // dropped rather than reinstating targetlessSyncProgress. Tracked separately from
  // isInitialSyncPhaseDone, which is already true from construction on a node that has no initial
  // sync phase.
  private boolean initialSyncPhaseCompleted;

  // Guards the two fields above together, so that a progress report cannot interleave with
  // markInitialSyncPhaseAsDone() clearing the progress.
  private final Object targetlessSyncProgressLock = new Object();

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

  /**
   * The current sync status, or empty when this node is not syncing.
   *
   * <p>Falls back to {@link #setSyncProgress(long, long)} reporting when no sync target is set.
   * Snap sync does not use a sync target — only {@code PipelineChainDownloader}, used by full sync,
   * sets one — so without this fallback {@code eth_syncing} reports "not syncing" for the whole of
   * a snap sync.
   *
   * @return the current sync status, or empty when not syncing
   */
  public Optional<SyncStatus> syncStatus() {
    return syncStatus(syncTarget).or(this::targetlessSyncStatus);
  }

  /**
   * The status of a sync that does not use a sync target, built from the last reported progress.
   *
   * <p>The highest block is resolved on every read rather than stored, because snap sync reports
   * progress only while a stage 2 pipeline is running. Between cycles — in particular while the
   * chain download waits for the world state heal to finish — no report arrives, and a stored
   * height would stay frozen at the pivot the last cycle reached, making the node look fully caught
   * up.
   */
  private Optional<SyncStatus> targetlessSyncStatus() {
    return Optional.ofNullable(targetlessSyncProgress)
        .map(
            progress ->
                new DefaultSyncStatus(
                    progress.startingBlock(),
                    progress.currentBlock(),
                    bestChainHeight(),
                    Optional.empty(),
                    Optional.empty()));
  }

  public Optional<SyncTarget> syncTarget() {
    return syncTarget;
  }

  public void setSyncTarget(final EthPeer peer, final BlockHeader commonAncestor) {
    final SyncTarget syncTarget = new SyncTarget(peer, commonAncestor);
    replaceSyncTarget(Optional.of(syncTarget));
  }

  /**
   * Reports the progress of a sync that does not use a sync target, i.e. snap sync.
   *
   * <p>Progress reported after {@link #markInitialSyncPhaseAsDone()} is ignored: only that method
   * clears the retained progress, so a later report would make {@code syncStatus()} non-empty for
   * the rest of the process lifetime, leaving {@code eth_syncing} claiming an in-progress sync with
   * no way to recover short of a restart. Callers are expected to report only while the initial
   * sync phase is running; this guard keeps the consequence of getting that wrong proportionate.
   *
   * @param startingBlock the block the sync started from
   * @param currentBlock the block the sync has reached
   */
  public void setSyncProgress(final long startingBlock, final long currentBlock) {
    synchronized (targetlessSyncProgressLock) {
      if (initialSyncPhaseCompleted) {
        return;
      }
      targetlessSyncProgress = new TargetlessSyncProgress(startingBlock, currentBlock);
    }
    final Optional<SyncStatus> status = targetlessSyncStatus();
    syncStatusListeners.forEach(c -> c.onSyncStatusChanged(status));
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

  /**
   * Notified for each {@code engine_newPayload} received from the consensus layer. Once the first
   * payload arrives this node is being driven by a CL, so the payload head becomes the
   * authoritative best chain height.
   *
   * @param header the header reconstructed from the payload
   */
  @Override
  public void onNewPayload(final BlockHeader header) {
    lastPayloadBlockNumber = header.getNumber();
    payloadReceived = true;
  }

  public long bestChainHeight() {
    if (payloadReceived) {
      return lastPayloadBlockNumber;
    }
    return bestChainHeight(blockchain.getChainHeadBlockNumber());
  }

  public long bestChainHeight(final long localChainHeight) {
    if (payloadReceived) {
      return lastPayloadBlockNumber;
    }
    return Math.max(
        localChainHeight,
        ethPeers
            .bestPeerWithHeightEstimate()
            .map(p -> p.chainState().getEstimatedHeight())
            .orElse(localChainHeight));
  }

  /** Evaluates whether this node is in sync and notifies any tracker whose verdict changed. */
  private void checkInSync() {
    synchronized (inSyncLock) {
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
    synchronized (targetlessSyncProgressLock) {
      initialSyncPhaseCompleted = true;
      // Otherwise the last progress reported by snap sync would be returned by syncStatus()
      // forever, making eth_syncing report a permanently in-progress sync.
      targetlessSyncProgress = null;
    }
    completionListenerSubscribers.forEach(InitialSyncCompletionListener::onInitialSyncCompleted);
  }

  public boolean isResyncNeeded() {
    return isResyncNeeded;
  }

  public void markResyncNeeded() {
    isResyncNeeded = true;
  }

  public void markInitialSyncRestart() {
    isInitialSyncPhaseDone = false;
    synchronized (targetlessSyncProgressLock) {
      initialSyncPhaseCompleted = false;
    }
    completionListenerSubscribers.forEach(InitialSyncCompletionListener::onInitialSyncRestart);
  }

  /**
   * The blocks last reported by a sync that does not use a sync target.
   *
   * @param startingBlock the block the sync started from
   * @param currentBlock the block the sync had reached when it last reported
   */
  private record TargetlessSyncProgress(long startingBlock, long currentBlock) {}
}
