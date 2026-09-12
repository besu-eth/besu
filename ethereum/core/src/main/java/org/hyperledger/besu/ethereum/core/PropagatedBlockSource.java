/*
 * Copyright contributors to Hyperledger Besu.
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

/**
 * Source of block-propagated notifications: a block whose header has been validated and is about to
 * be gossiped to peers. Declared in core so the chain plugin adapter can subscribe without
 * depending on the eth module, where the broadcaster that implements it lives.
 */
public interface PropagatedBlockSource {

  /**
   * Subscribes to propagated blocks.
   *
   * @param listener the listener to notify
   * @return the id to pass to {@link #unsubscribe(long)}
   */
  long subscribe(PropagatedBlockListener listener);

  /**
   * Removes a subscription.
   *
   * @param subscriptionId the id returned by {@link #subscribe(PropagatedBlockListener)}
   * @return {@code true} if a subscription with that id was found and removed
   */
  boolean unsubscribe(long subscriptionId);

  /** Receives propagated blocks. */
  @FunctionalInterface
  interface PropagatedBlockListener {

    /**
     * Invoked when a block is about to be propagated to peers.
     *
     * @param block the block
     * @param totalDifficulty the total difficulty of the chain including this block
     */
    void onBlockPropagated(Block block, Difficulty totalDifficulty);
  }
}
