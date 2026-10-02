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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.List;

/** Processes a block. */
public interface BlockProcessor {

  /** A block processing result. */
  interface Result {

    /**
     * The receipts generated for the transactions in a block
     *
     * <p>This is only valid when {@code BlockProcessor#isSuccessful} returns {@code true}.
     *
     * @return the receipts generated for the transactions in a block
     */
    List<TransactionReceipt> getReceipts();

    /**
     * Returns whether the block was successfully processed.
     *
     * @return {@code true} if the block was processed successfully; otherwise {@code false}
     */
    boolean isSuccessful();

    default boolean isFailed() {
      return !isSuccessful();
    }
  }

  /**
   * Processes the block.
   *
   * @param context the block execution context containing all required inputs
   * @return the block processing result
   */
  BlockProcessingResult processBlock(final BlockExecutionContext context);

  /**
   * Get ommer reward in ${@link Wei}
   *
   * @param blockReward reward of the block
   * @param blockNumber number of the block
   * @param ommerBlockNumber number of the block ommer
   * @return ommer reward
   */
  default Wei getOmmerReward(
      final Wei blockReward, final long blockNumber, final long ommerBlockNumber) {
    final long distance = blockNumber - ommerBlockNumber;
    return blockReward.subtract(blockReward.multiply(distance).divide(8));
  }

  /**
   * Get coinbase reward in ${@link Wei}
   *
   * @param blockReward reward of the block
   * @param blockNumber number of the block
   * @param numberOfOmmers number of ommers for this block
   * @return coinbase reward
   */
  default Wei getCoinbaseReward(
      final Wei blockReward, final long blockNumber, final int numberOfOmmers) {
    return blockReward.add(blockReward.multiply(numberOfOmmers).divide(32));
  }
}
