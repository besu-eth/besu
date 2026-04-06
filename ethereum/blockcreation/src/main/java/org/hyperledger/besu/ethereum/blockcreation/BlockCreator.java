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
package org.hyperledger.besu.ethereum.blockcreation;

import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Optional;

public interface BlockCreator {
  class BlockCreationResult {
    private final Block block;
    private final TransactionSelectionResults transactionSelectionResults;
    private final BlockCreationTiming blockCreationTiming;
    private final Optional<BlockAccessList> blockAccessList;
    private final Optional<List<Request>> requests;

    public BlockCreationResult(
        final Block block,
        final TransactionSelectionResults transactionSelectionResults,
        final BlockCreationTiming timings,
        final Optional<BlockAccessList> blockAccessList) {
      this(block, transactionSelectionResults, timings, blockAccessList, Optional.empty());
    }

    public BlockCreationResult(
        final Block block,
        final TransactionSelectionResults transactionSelectionResults,
        final BlockCreationTiming timings,
        final Optional<BlockAccessList> blockAccessList,
        final Optional<List<Request>> requests) {
      this.block = block;
      this.transactionSelectionResults = transactionSelectionResults;
      this.blockCreationTiming = timings;
      this.blockAccessList = blockAccessList;
      this.requests = requests;
    }

    public Block getBlock() {
      return block;
    }

    public TransactionSelectionResults getTransactionSelectionResults() {
      return transactionSelectionResults;
    }

    public BlockCreationTiming getBlockCreationTimings() {
      return blockCreationTiming;
    }

    public Optional<BlockAccessList> getBlockAccessList() {
      return blockAccessList;
    }

    public Optional<List<Request>> getRequests() {
      return requests;
    }
  }

  BlockCreationResult createBlock(final long timestamp, final BlockHeader parentHeader);

  BlockCreationResult createEmptyWithdrawalsBlock(
      final long timestamp, final BlockHeader parentHeader);

  BlockCreationResult createBlock(
      final List<Transaction> transactions,
      final List<BlockHeader> ommers,
      final long timestamp,
      final BlockHeader parentHeader);

  BlockCreationResult createBlock(
      final Optional<List<Transaction>> maybeTransactions,
      final Optional<List<BlockHeader>> maybeOmmers,
      final long timestamp,
      final BlockHeader parentHeader);
}
