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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListItemSizeCheck;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rejects a transaction when applying its partial block access view would exceed the EIP-7928 item
 * budget for the block (same rule as block import in {@link
 * org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor}).
 */
public class BlockAccessListItemBudgetTransactionSelector extends AbstractTransactionSelector {

  private static final Logger LOG =
      LoggerFactory.getLogger(BlockAccessListItemBudgetTransactionSelector.class);

  private final Optional<BlockAccessList.BlockAccessListBuilder> maybeBlockAccessListBuilder;

  public BlockAccessListItemBudgetTransactionSelector(
      final BlockSelectionContext context,
      final Optional<BlockAccessList.BlockAccessListBuilder> maybeBlockAccessListBuilder) {
    super(context);
    this.maybeBlockAccessListBuilder = maybeBlockAccessListBuilder;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPreProcessing(
      final TransactionEvaluationContext evaluationContext) {
    return TransactionSelectionResult.SELECTED;
  }

  @Override
  public TransactionSelectionResult evaluateTransactionPostProcessing(
      final TransactionEvaluationContext evaluationContext,
      final TransactionProcessingResult processingResult) {
    if (maybeBlockAccessListBuilder.isEmpty()) {
      return TransactionSelectionResult.SELECTED;
    }
    final BlockAccessList.BlockAccessListBuilder mainBuilder = maybeBlockAccessListBuilder.get();
    final long itemCount;
    if (processingResult.getPartialBlockAccessView().isEmpty()) {
      itemCount = mainBuilder.eip7928ItemCount();
    } else {
      final BlockAccessList committedSnapshot = mainBuilder.build();
      final BlockAccessList.BlockAccessListBuilder probe = BlockAccessList.builder();
      probe.mergeFrom(committedSnapshot);
      probe.apply(processingResult.getPartialBlockAccessView().get());
      itemCount = probe.eip7928ItemCount();
    }
    final BlockAccessListItemSizeCheck itemSizeCheck =
        context
            .protocolSpec()
            .getBlockAccessListValidator()
            .validateExecutedBlockAccessListItemSize(
                itemCount, context.pendingBlockHeader(), context.protocolSpec());
    if (itemSizeCheck.isOverBudget()) {
      LOG.trace(
          "Transaction not selected: {}",
          itemSizeCheck.overBudgetError().orElseThrow().errorMessage());
      return TransactionSelectionResult.BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED;
    }
    return TransactionSelectionResult.SELECTED;
  }
}
