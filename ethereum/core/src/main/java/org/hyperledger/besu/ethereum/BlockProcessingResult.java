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
package org.hyperledger.besu.ethereum;

import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Optional;

/** Contains the outputs of processing a block. */
public class BlockProcessingResult extends BlockValidationResult {

  private final Optional<BlockProcessingOutputs> yield;
  private final boolean isPartial;
  private Optional<Integer> nbParallelizedTransactions = Optional.empty();
  private final Optional<BlockAccessList> maybeGeneratedBlockAccessList;
  private final boolean worldStateUnavailable;

  /** A result indicating that processing failed. */
  public static final BlockProcessingResult FAILED = new BlockProcessingResult("processing failed");

  /**
   * Creates a result indicating that the parent world state was not available when attempting to
   * process the block. This is distinct from a bad block — the block itself may be valid; the node
   * simply lacks the required world state to validate it.
   *
   * @param errorMessage the error message
   * @return a result with {@link #isWorldStateUnavailable()} returning {@code true}
   */
  public static BlockProcessingResult worldStateUnavailable(final String errorMessage) {
    return new BlockProcessingResult(errorMessage, true);
  }

  private BlockProcessingResult(final String errorMessage, final boolean worldStateUnavailable) {
    super(errorMessage);
    this.isPartial = false;
    this.yield = Optional.empty();
    this.maybeGeneratedBlockAccessList = Optional.empty();
    this.worldStateUnavailable = worldStateUnavailable;
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   */
  public BlockProcessingResult(final Optional<BlockProcessingOutputs> yield) {
    this(yield, false);
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param nbParallelizedTransactions potential number of parallelized transactions during block
   *     processing
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield,
      final Optional<Integer> nbParallelizedTransactions) {
    this(yield, false);
    this.nbParallelizedTransactions = nbParallelizedTransactions;
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param isPartial whether the processing was incomplete
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield, final boolean isPartial) {
    this.yield = yield;
    this.isPartial = isPartial;
    this.maybeGeneratedBlockAccessList = Optional.empty();
    this.worldStateUnavailable = false;
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param errorMessage the error message if any
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield, final String errorMessage) {
    this(yield, errorMessage, false);
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param cause the cause of the error if any
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield, final Throwable cause) {
    super(cause.getLocalizedMessage(), cause);
    this.yield = yield;
    this.isPartial = false;
    this.maybeGeneratedBlockAccessList = Optional.empty();
    this.worldStateUnavailable = false;
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param errorMessage the error message if any
   * @param isPartial whether the processing was incomplete
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield,
      final String errorMessage,
      final boolean isPartial) {
    this(yield, errorMessage, isPartial, Optional.empty());
  }

  /**
   * A result indicating that processing was successful but incomplete.
   *
   * @param yield the outputs of processing a block
   * @param errorMessage the error message if any
   * @param isPartial whether the processing was incomplete
   * @param generatedBlockAccessList the generated block access list, if available
   */
  public BlockProcessingResult(
      final Optional<BlockProcessingOutputs> yield,
      final String errorMessage,
      final boolean isPartial,
      final Optional<BlockAccessList> generatedBlockAccessList) {
    super(errorMessage);
    this.yield = yield;
    this.isPartial = isPartial;
    this.maybeGeneratedBlockAccessList = generatedBlockAccessList;
    this.worldStateUnavailable = false;
  }

  /**
   * A result indicating that processing failed.
   *
   * @param errorMessage the error message
   */
  public BlockProcessingResult(final String errorMessage) {
    super(errorMessage);
    this.isPartial = false;
    this.yield = Optional.empty();
    this.maybeGeneratedBlockAccessList = Optional.empty();
    this.worldStateUnavailable = false;
  }

  /**
   * Gets the block processing outputs of the result.
   *
   * @return the block processing outputs of the result
   */
  public Optional<BlockProcessingOutputs> getYield() {
    return yield;
  }

  /**
   * Checks if the processing was incomplete.
   *
   * @return true if the processing was incomplete, false otherwise
   */
  public boolean isPartial() {
    return isPartial;
  }

  /**
   * Gets the transaction receipts of the result.
   *
   * @return the transaction receipts of the result
   */
  public List<TransactionReceipt> getReceipts() {
    return yield.map(BlockProcessingOutputs::getReceipts).orElse(List.of());
  }

  /**
   * Gets the requests of the result.
   *
   * @return the requests of the result
   */
  public Optional<List<Request>> getRequests() {
    return yield.flatMap(BlockProcessingOutputs::getRequests);
  }

  /**
   * Returns an optional that contains the number of parallelized transactions (if there is any)
   *
   * @return Optional of parallelized transactions during the block execution
   */
  public Optional<Integer> getNbParallelizedTransactions() {
    return nbParallelizedTransactions;
  }

  /**
   * Returns the generated block access list produced during processing, when available.
   *
   * @return the generated block access list
   */
  public Optional<BlockAccessList> getGeneratedBlockAccessList() {
    return maybeGeneratedBlockAccessList;
  }

  /**
   * Returns {@code true} if processing failed because the parent world state was not available.
   * Such failures should not be treated as bad blocks — the block may be valid once the world state
   * is repaired (e.g. via {@code debug_resyncWorldState}).
   *
   * @return {@code true} if the parent world state was unavailable
   */
  public boolean isWorldStateUnavailable() {
    return worldStateUnavailable;
  }
}
