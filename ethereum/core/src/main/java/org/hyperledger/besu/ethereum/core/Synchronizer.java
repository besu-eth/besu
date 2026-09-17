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
   * Returns the best known block height of the network.
   *
   * @return the best known block height of the network, or empty if not known
   */
  Optional<Long> getBestPeerChainHead();

  boolean resyncWorldState();

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
