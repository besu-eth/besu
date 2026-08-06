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
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
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
   * Processes the block with an optional block access list.
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
      final Block block,
      final AbstractBlockProcessor.PreprocessingFunction preprocessingBlockFunction);

  /**
   * Processes the block with an optional block access list.
   *
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @param blockAccessList the optional block access list
   * @param preprocessingBlockFunction a preprocessing function for block execution
   * @return the block processing result
   */
  BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final AbstractBlockProcessor.PreprocessingFunction preprocessingBlockFunction);

  /**
   * Processes the block with an explicit operation tracer, bypassing the plugin-based import
   * tracer. Used by debug tooling (e.g. {@code debug_executionWitness}) that needs to observe EVM
   * execution without interfering with the normal import tracer.
   *
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @param blockAccessList the optional block access list
   * @param maybeTracer the tracer to use for this execution, replacing the plugin-based import
   *     tracer; when empty the plugin-based import tracer is resolved as usual
   * @return the block processing result
   */
  BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<BlockAwareOperationTracer> maybeTracer);

  /**
   * Processes the block with both a preprocessing function and an optional explicit operation
   * tracer. This is the canonical overload — all other {@code processBlock} variants delegate here.
   *
   * @param protocolContext the current context of the protocol
   * @param blockchain the blockchain to append the block to
   * @param worldState the world state to apply changes to
   * @param block the block to process
   * @param blockAccessList the optional block access list
   * @param preprocessingBlockFunction a preprocessing function for block execution
   * @param maybeTracer the tracer to use for this execution; when empty the plugin-based import
   *     tracer is resolved as usual
   * @return the block processing result
   */
  BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final AbstractBlockProcessor.PreprocessingFunction preprocessingBlockFunction,
      final Optional<BlockAwareOperationTracer> maybeTracer);

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
