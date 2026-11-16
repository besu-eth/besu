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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.util.Subscribers;

/** Tells its listeners when the chain head moves to a block of another fork. */
public final class ForkChangeTracker implements BlockAddedObserver {

  /** Receives the fork the chain head moved to. */
  @FunctionalInterface
  public interface ForkChangeListener {
    /**
     * Called on the thread that moved the chain head, once it moved to a block of another fork.
     *
     * @param fork the fork of the new chain head
     * @param head the new chain head
     */
    void onForkChange(HardforkId fork, BlockHeader head);
  }

  private final Blockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  // a failing listener must not fail the block import that moved the head
  private final Subscribers<ForkChangeListener> listeners = Subscribers.create(true);
  // guarded by this: the fork of the last chain head seen
  private HardforkId fork;

  private ForkChangeTracker(final Blockchain blockchain, final ProtocolSchedule protocolSchedule) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.fork = forkOf(blockchain.getChainHeadHeader());
  }

  /**
   * Creates a tracker that follows the chain head of the blockchain.
   *
   * @param blockchain the blockchain whose head is followed
   * @param protocolSchedule gives the fork of a block
   * @return the tracker
   */
  public static ForkChangeTracker create(
      final Blockchain blockchain, final ProtocolSchedule protocolSchedule) {
    final ForkChangeTracker tracker = new ForkChangeTracker(blockchain, protocolSchedule);
    blockchain.observeBlockAdded(tracker);
    return tracker;
  }

  /**
   * Returns the fork of the current chain head, which a sync may have set without a block added
   * event.
   *
   * @return the fork of the chain head
   */
  public HardforkId currentFork() {
    return forkOf(blockchain.getChainHeadHeader());
  }

  /**
   * Subscribes a listener to the fork changes of the chain head.
   *
   * @param listener the listener
   * @return the id of the subscription
   */
  public long subscribe(final ForkChangeListener listener) {
    return listeners.subscribe(listener);
  }

  /**
   * Removes a listener.
   *
   * @param subscriptionId the id {@link #subscribe} returned
   * @return whether the listener was subscribed
   */
  public boolean unsubscribe(final long subscriptionId) {
    return listeners.unsubscribe(subscriptionId);
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    if (!event.isNewCanonicalHead()) {
      return;
    }
    final BlockHeader head = event.getHeader();
    final HardforkId headFork = forkOf(head);
    synchronized (this) {
      if (headFork.equals(fork)) {
        return;
      }
      fork = headFork;
    }
    listeners.forEach(listener -> listener.onForkChange(headFork, head));
  }

  private HardforkId forkOf(final BlockHeader header) {
    return protocolSchedule.getByBlockHeader(header).getHardforkId();
  }
}
