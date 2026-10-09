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

import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

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
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @return the block processing result
   */
  BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block);

  /**
   * Processes the block with an optional block access list. The canonical overload: all other
   * {@code processBlock} variants delegate here.
   *
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @param blockAccessList the optional block access list
   * @return the block processing result
   */
  BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList);
}
