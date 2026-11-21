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

import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advertises snap/2 on a node asked to sync with SNAP2 that synced with SNAP, as the chain had no
 * block access lists yet: once the sync is done, the snap server runs and the node holds the block
 * access lists SNAP2 peers ask for, those of the last 128 blocks. Before, peers would agree on
 * snap/2, which drops the trie node requests the SNAP sync heals with.
 */
public final class Snap2Advertiser implements BlockAddedObserver, InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2Advertiser.class);
  // a SNAP2 peer picks a pivot 64 blocks behind the head, and moves it once the head is up to about
  // 120 blocks ahead, with the block access lists of the blocks after it
  private static final int SERVED_BLOCKS = 128;

  private final SnapProtocolManager snapProtocolManager;
  private final SyncState syncState;
  private final Blockchain blockchain;
  // guarded by this, set before any check
  private long chainSubscription;
  private long syncSubscription;
  private boolean done;

  private Snap2Advertiser(
      final SnapProtocolManager snapProtocolManager,
      final SyncState syncState,
      final Blockchain blockchain) {
    this.snapProtocolManager = snapProtocolManager;
    this.syncState = syncState;
    this.blockchain = blockchain;
  }

  /**
   * Advertises snap/2 once the node synced, serves snap requests and holds the block access lists
   * of the last 128 blocks, now when all already hold.
   *
   * @param snapProtocolManager the manager that advertises snap/2
   * @param syncState tells when the initial sync is done
   * @param blockchain the chain whose head is followed until snap/2 is advertised
   * @return the advertiser
   */
  public static Snap2Advertiser start(
      final SnapProtocolManager snapProtocolManager,
      final SyncState syncState,
      final Blockchain blockchain) {
    final Snap2Advertiser advertiser =
        new Snap2Advertiser(snapProtocolManager, syncState, blockchain);
    synchronized (advertiser) {
      advertiser.chainSubscription = blockchain.observeBlockAdded(advertiser);
      advertiser.syncSubscription = syncState.subscribeCompletionReached(advertiser);
      advertiser.check(blockchain.getChainHeadHeader());
    }
    return advertiser;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    if (event.isNewCanonicalHead()) {
      check(event.getHeader());
    }
  }

  @Override
  public void onInitialSyncCompleted() {
    // the sync moves the chain head without a block added event
    check(blockchain.getChainHeadHeader());
  }

  @Override
  public void onInitialSyncRestart() {}

  /** Returns whether snap/2 is advertised. */
  synchronized boolean isDone() {
    return done;
  }

  private synchronized void check(final BlockHeader head) {
    // without a snap server, SNAP2 peers would send it requests it answers empty
    if (done
        || !syncState.isInitialSyncPhaseDone()
        || head.getBalHash().isEmpty()
        || !snapProtocolManager.isServing()) {
      return;
    }
    // a failure here would fail the import of a block already stored, or the end of the sync
    try {
      if (!holdsTheBlockAccessListsBelow(head)) {
        return;
      }
      done = true;
      blockchain.removeObserver(chainSubscription);
      syncState.unsubscribeInitialConditionReached(syncSubscription);
      if (snapProtocolManager.advertiseSnap2()) {
        LOG.info(
            "Blocks {} to {} have block access lists, advertising snap/2 to new connections",
            head.getNumber() - SERVED_BLOCKS + 1,
            head.getNumber());
      }
    } catch (final RuntimeException e) {
      LOG.warn("Could not advertise snap/2, peers syncing with SNAP2 cannot use this node", e);
    }
  }

  // the SNAP sync stores no block access lists, so only blocks executed past the fork have one
  private boolean holdsTheBlockAccessListsBelow(final BlockHeader head) {
    final long oldest = head.getNumber() - SERVED_BLOCKS + 1;
    return oldest > 0
        && blockchain
            .getBlockHeader(oldest)
            .flatMap(header -> blockchain.getBlockAccessListRlp(header.getHash()))
            .isPresent();
  }
}
