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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Syncs the chain up to a head the consensus client sent and the local chain does not hold. */
public interface BackwardSync {

  /**
   * Whether a sync is running.
   *
   * @return true while a sync is running
   */
  boolean isSyncing();

  /**
   * Updates the height shown in the progress logs from a head that is already being synced.
   *
   * @param headHash the head hash
   */
  void maybeUpdateTargetHeight(Hash headHash);

  /**
   * Records the finalized block the consensus client sent with a head to sync to.
   *
   * @param finalizedBlockHash the finalized block hash, zero if none
   */
  default void updateFinalizedBlockHash(final Hash finalizedBlockHash) {}

  /**
   * Syncs up to the head {@code newBlockHash}, downloading it.
   *
   * @param newBlockHash the head hash
   * @return completes once the chain holds the latest head
   */
  CompletableFuture<Void> syncBackwardsUntil(Hash newBlockHash);

  /**
   * Syncs up to the head {@code newPivot}, received with its block access list if any.
   *
   * @param newPivot the head block
   * @param blockAccessList its block access list
   * @return completes once the chain holds the latest head
   */
  CompletableFuture<Void> syncBackwardsUntil(
      Block newPivot, Optional<BlockAccessList> blockAccessList);

  /**
   * Returns a header the sync holds and the chain does not, to link a head to a bad ancestor.
   *
   * @param blockHash the block hash
   * @return the header, empty if the sync does not hold it
   */
  Optional<BlockHeader> getHeader(Hash blockHash);

  /**
   * Subscribes to the bad chains the sync finds.
   *
   * @param badChainListener the listener
   */
  void subscribeBadChainListener(BadChainListener badChainListener);
}
