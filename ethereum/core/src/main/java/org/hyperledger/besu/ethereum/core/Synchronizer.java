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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairRequester;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairResult;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Provides an interface to block synchronization processes. */
public interface Synchronizer {

  // Default tolerance used to determine whether or not this node is "in sync"
  long DEFAULT_IN_SYNC_TOLERANCE = 5;

  CompletableFuture<Void> start();

  void stop();

  void awaitStop() throws InterruptedException;

  /**
   * Current status of a sync, if syncing.
   *
   * @return the status, based on SyncingResult When actively synchronizing blocks, alternatively
   *     empty
   */
  Optional<SyncStatus> getSyncStatus();

  /**
   * Returns true if the node is considered to be in sync with the network.
   *
   * @return true if the node is in sync
   */
  boolean isInSync();

  /**
   * Whether the initial sync phase (e.g. snap sync including the world state heal) has finished.
   *
   * @return true once the initial sync phase is done, or if there is none
   */
  boolean isInitialSyncPhaseDone();

  /**
   * Returns the best known block height of the network.
   *
   * @return the best known block height of the network, or empty if not known
   */
  Optional<Long> getBestPeerChainHead();

  boolean resyncWorldState();

  /**
   * Attempts bounded repair without restarting synchronization or downloading a replacement state.
   *
   * <p>The future completes only after repair workers have stopped accessing the budget and state.
   * Cancellation is cooperative through {@code budget}; cancelling the returned future is not an
   * acknowledgement that workers have drained. Implementations without targeted repair decline it.
   *
   * @param blockHash the known block whose state is needed
   * @param stateRoot the state root committed by that block
   * @param budget cumulative work limits and cancellation for this attempt
   * @return the terminal repair outcome, not an indication that canonical forkchoice was changed
   */
  default CompletableFuture<WorldStateRepairResult> repairWorldState(
      final Hash blockHash, final Hash stateRoot, final WorldStateRepairBudget budget) {
    return repairWorldState(new WorldStateRepairRequest(blockHash, stateRoot), budget);
  }

  /** Repairs a target and verifies its explicitly identified missing dependency, if present. */
  default CompletableFuture<WorldStateRepairResult> repairWorldState(
      final WorldStateRepairRequest request, final WorldStateRepairBudget budget) {
    return CompletableFuture.completedFuture(WorldStateRepairResult.UNSUPPORTED);
  }

  /** Returns the synchronizer-owned repair admission module. */
  default WorldStateRepairRequester getWorldStateRepairRequester() {
    return WorldStateRepairRequester.DISABLED;
  }

  long subscribeSyncStatus(final BesuEvents.SyncStatusListener listener);

  boolean unsubscribeSyncStatus(long observerId);

  /**
   * Add a listener that will be notified when this node's sync status changes. A node is considered
   * in-sync if the local chain height is no more than {@code DEFAULT_IN_SYNC_TOLERANCE} behind the
   * highest estimated remote chain height.
   *
   * @param listener The callback to invoke when the sync status changes
   * @return A subscription id that can be used to unsubscribe from these events
   */
  long subscribeInSync(final InSyncListener listener);

  /**
   * Add a listener that will be notified when this node's sync status changes. A node is considered
   * in-sync if the local chain height is no more than {@code syncTolerance} behind the highest
   * estimated remote chain height.
   *
   * @param listener The callback to invoke when the sync status changes
   * @param syncTolerance The tolerance used to determine whether this node is in-sync. A value of
   *     zero means that the node is considered in-sync only when the local chain height is greater
   *     than or equal to the best estimated remote chain height.
   * @return A subscription id that can be used to unsubscribe from these events
   */
  long subscribeInSync(final InSyncListener listener, final long syncTolerance);

  /**
   * Unsubscribe from in sync events.
   *
   * @param listenerId The id returned when subscribing
   * @return {@code true} if a subscription was cancelled
   */
  boolean unsubscribeInSync(final long listenerId);

  /**
   * Add a listener that will be notified when this node initial sync status changes.
   *
   * @param listener The callback to invoke when the initial sync status changes
   * @return A subscription id that can be used to unsubscribe from these events
   */
  long subscribeInitialSync(final InitialSyncCompletionListener listener);

  /**
   * Unsubscribe from initial sync events.
   *
   * @param listenerId The id returned when subscribing
   * @return {@code true} if a subscription was cancelled
   */
  boolean unsubscribeInitialSync(final long listenerId);

  @FunctionalInterface
  interface InSyncListener {
    void onInSyncStatusChange(boolean newSyncStatus);
  }
}
