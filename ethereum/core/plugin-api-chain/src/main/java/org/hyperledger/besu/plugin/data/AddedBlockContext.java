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
package org.hyperledger.besu.plugin.data;

import java.util.List;

/**
 * Provides context information about a block that has been added to the blockchain.
 *
 * <p>This interface extends {@link BlockContext} to include additional metadata about how and why
 * the block was added to the chain. It is primarily used by plugins to receive notifications about
 * block addition events and to understand the nature of these events.
 *
 * @see BlockContext
 */
public interface AddedBlockContext extends BlockContext {

  /**
   * Defines the different types of block addition events that can occur in the blockchain.
   *
   * <p>These event types help distinguish between normal chain progression and exceptional
   * scenarios like forks and reorganizations.
   */
  enum EventType {
    /**
     * Indicates that the block was added to the main chain and advanced the chain head to this
     * block. This is the normal case for sequential block addition.
     */
    HEAD_ADVANCED,

    /**
     * Indicates that the block was added as part of a fork, creating an alternative chain branch
     * that does not become the main chain.
     */
    FORK,

    /**
     * Indicates that the block was added as part of a chain reorganization, where the canonical
     * chain switched to a different fork that includes this block.
     */
    CHAIN_REORG,

    /**
     * Indicates that the block was stored but did not affect the canonical chain. This typically
     * occurs when importing historical blocks or when a block is stored for reference without being
     * part of the active chain progression.
     */
    STORED_ONLY
  }

  /**
   * Returns the type of event that occurred when this block was added.
   *
   * <p>The event type provides important context about how the block relates to the blockchain
   * state and whether it represents normal progression, a fork, a reorganization, or just storage.
   *
   * @return the {@link EventType} indicating how this block was added
   */
  EventType getEventType();

  /**
   * A list of transaction receipts for the added block.
   *
   * @return A List of {@link TransactionReceipt}
   */
  List<? extends TransactionReceipt> getTransactionReceipts();
}
