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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ForkChangeTracker;
import org.hyperledger.besu.ethereum.mainnet.ForkChangeTracker.ForkChangeListener;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advertises snap/2 on a node asked to sync with SNAP2 that synced with SNAP, as the chain had no
 * block access lists yet: once the sync is done and the chain head reached the fork that adds them.
 * Before, peers would agree on snap/2, which drops the trie node requests the SNAP sync heals with.
 */
public final class Snap2Advertiser implements ForkChangeListener, InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2Advertiser.class);

  private final SnapProtocolManager snapProtocolManager;
  private final SyncState syncState;
  private final ForkChangeTracker forks;
  // guarded by this, set before any check
  private long syncSubscription;
  // guarded by this: the fork of the last fork change, as a reorg tells it before the head moves
  private HardforkId changedTo;
  private boolean done;

  private Snap2Advertiser(
      final SnapProtocolManager snapProtocolManager,
      final SyncState syncState,
      final ForkChangeTracker forks) {
    this.snapProtocolManager = snapProtocolManager;
    this.syncState = syncState;
    this.forks = forks;
  }

  /**
   * Advertises snap/2 once the node synced and its chain head reached the fork that adds block
   * access lists, now when both already hold.
   *
   * @param snapProtocolManager the manager that advertises snap/2
   * @param syncState tells when the initial sync is done
   * @param forks tells when the chain head reaches another fork, stopped once snap/2 is advertised
   * @return the advertiser
   */
  public static Snap2Advertiser start(
      final SnapProtocolManager snapProtocolManager,
      final SyncState syncState,
      final ForkChangeTracker forks) {
    final Snap2Advertiser advertiser = new Snap2Advertiser(snapProtocolManager, syncState, forks);
    synchronized (advertiser) {
      forks.subscribe(advertiser);
      advertiser.syncSubscription = syncState.subscribeCompletionReached(advertiser);
      advertiser.check();
    }
    return advertiser;
  }

  /**
   * Returns whether the fork has block access lists: Amsterdam or a later fork.
   *
   * @param fork the fork
   * @return whether its blocks have block access lists
   */
  public static boolean hasBlockAccessLists(final HardforkId fork) {
    return fork instanceof MainnetHardforkId mainnetFork
        && mainnetFork.ordinal() >= MainnetHardforkId.AMSTERDAM.ordinal();
  }

  @Override
  public synchronized void onForkChange(final HardforkId fork, final BlockHeader head) {
    changedTo = fork;
    check();
  }

  @Override
  public synchronized void onInitialSyncCompleted() {
    check();
  }

  @Override
  public void onInitialSyncRestart() {}

  /** Returns whether snap/2 is advertised. */
  synchronized boolean isDone() {
    return done;
  }

  private synchronized void check() {
    if (done || !syncState.isInitialSyncPhaseDone()) {
      return;
    }
    // the sync moves the head without a fork change
    final HardforkId headFork = hasBlockAccessLists(changedTo) ? changedTo : forks.currentFork();
    if (!hasBlockAccessLists(headFork)) {
      return;
    }
    done = true;
    forks.stop();
    syncState.unsubscribeInitialConditionReached(syncSubscription);
    try {
      if (snapProtocolManager.advertiseSnap2()) {
        LOG.info("Chain head reached {}, advertising snap/2 to new connections", headFork);
      }
    } catch (final RuntimeException e) {
      LOG.warn("Could not advertise snap/2, peers syncing with SNAP2 cannot use this node", e);
    }
  }
}
