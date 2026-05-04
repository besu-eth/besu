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
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

/** Contains the outputs of processing a block. */
public class BlockProcessingOutputs {

  private final MutableWorldState worldState;
  private final List<TransactionReceipt> receipts;
  private final Optional<List<Request>> maybeRequests;
  private final Optional<BlockAccessList> maybeBlockAccessList;
  private final long cumulativeBlockGasUsed;

  /**
   * Creates a new instance.
   *
   * @param worldState the world state after processing the block
   * @param receipts the receipts produced by processing the block
   */
  public BlockProcessingOutputs(
      final MutableWorldState worldState, final List<TransactionReceipt> receipts) {
    this(worldState, receipts, Optional.empty());
  }

  /**
   * Creates a new instance.
   *
   * @param worldState the world state after processing the block
   * @param receipts the receipts produced by processing the block
   * @param maybeRequests the requests produced by processing the block
   */
  public BlockProcessingOutputs(
      final MutableWorldState worldState,
      final List<TransactionReceipt> receipts,
      final Optional<List<Request>> maybeRequests) {
    this(worldState, receipts, maybeRequests, Optional.empty(), 0);
  }

  /**
   * Creates a new instance.
   *
   * @param worldState the world state after processing the block
   * @param receipts the receipts produced by processing the block
   * @param maybeRequests the requests produced by processing the block
   * @param blockAccessList the block-level access list produced by processing the block
   */
  public BlockProcessingOutputs(
      final MutableWorldState worldState,
      final List<TransactionReceipt> receipts,
      final Optional<List<Request>> maybeRequests,
      final Optional<BlockAccessList> blockAccessList) {
    this(worldState, receipts, maybeRequests, blockAccessList, 0);
  }

  /**
   * Creates a new instance.
   *
   * @param worldState the world state after processing the block
   * @param receipts the receipts produced by processing the block
   * @param maybeRequests the requests produced by processing the block
   * @param blockAccessList the block-level access list produced by processing the block
   * @param cumulativeBlockGasUsed the cumulative block gas used (pre-refund for EIP-7778)
   */
  public BlockProcessingOutputs(
      final MutableWorldState worldState,
      final List<TransactionReceipt> receipts,
      final Optional<List<Request>> maybeRequests,
      final Optional<BlockAccessList> blockAccessList,
      final long cumulativeBlockGasUsed) {
    this.worldState = worldState;
    this.receipts = receipts;
    this.maybeRequests = maybeRequests;
    this.maybeBlockAccessList = blockAccessList;
    this.cumulativeBlockGasUsed = cumulativeBlockGasUsed;
  }

  /**
   * Returns the world state after processing the block.
   *
   * @return the world state after processing the block
   */
  public MutableWorldState getWorldState() {
    return worldState;
  }

  /**
   * Returns the receipts produced by processing the block.
   *
   * @return the receipts produced by processing the block
   */
  public List<TransactionReceipt> getReceipts() {
    return receipts;
  }

  /**
   * Returns the requests produced by processing the block.
   *
   * @return the requests produced by processing the block
   */
  public Optional<List<Request>> getRequests() {
    return maybeRequests;
  }

  /**
   * Returns the block-level access list produced by processing the block.
   *
   * @return the block-level access list produced by processing the block
   */
  public Optional<BlockAccessList> getBlockAccessList() {
    return maybeBlockAccessList;
  }

  /**
   * Returns the cumulative block gas used. For EIP-7778 (Amsterdam+), this is the pre-refund gas
   * used for block gas limit enforcement. For earlier forks, this equals the receipt's
   * cumulativeGasUsed.
   *
   * @return the cumulative block gas used
   */
  public long getCumulativeBlockGasUsed() {
    return cumulativeBlockGasUsed;
  }
}
