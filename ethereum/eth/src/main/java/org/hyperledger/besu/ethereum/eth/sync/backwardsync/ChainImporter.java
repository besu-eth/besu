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
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Imports one block of the backward sync: from its block access list and receipts when it has both,
 * else by executing it.
 */
final class ChainImporter {
  private static final Logger LOG = LoggerFactory.getLogger(ChainImporter.class);
  private static final long FALLBACK_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  enum Outcome {
    IMPORTED,
    BAD_BLOCK,
    WORLD_STATE_UNAVAILABLE,
    FAILED
  }

  record Result(Outcome outcome, boolean fromBal, String error) {}

  private final ProtocolContext context;
  private final ProtocolSchedule schedule;
  private final BackwardSyncMetrics metrics;
  private long lastFallbackWarn = System.nanoTime() - FALLBACK_WARN_INTERVAL_NANOS;

  ChainImporter(
      final ProtocolContext context,
      final ProtocolSchedule schedule,
      final BackwardSyncMetrics metrics) {
    this.context = context;
    this.schedule = schedule;
    this.metrics = metrics;
  }

  Result importBlock(
      final Block block,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<List<TransactionReceipt>> receipts) {
    final Hash hash = block.getHash();
    final MutableBlockchain blockchain = context.getBlockchain();
    final BadBlockManager badBlockManager = context.getBadBlockManager();
    if (badBlockManager.isBadBlock(hash)) {
      // a block that made it onto the chain cannot be bad, the entry is stale
      if (!blockchain.contains(hash)) {
        return new Result(Outcome.BAD_BLOCK, false, "known bad block");
      }
      badBlockManager.removeBadBlock(hash);
    }
    final BlockValidator validator =
        schedule.getByBlockHeader(block.getHeader()).getBlockValidator();
    if (blockAccessList.isPresent() && receipts.isPresent()) {
      LOG.atTrace().setMessage("Going to apply block {}").addArgument(block::toLogString).log();
      final BlockProcessingResult applied =
          validator.validateAndApplyBlockAccessList(
              context, block, blockAccessList.get(), receipts.get());
      if (applied.isSuccessful()) {
        blockchain.appendBlock(block, receipts.get(), blockAccessList);
        possiblyMoveHead(block);
        metrics.imported(true);
        return new Result(Outcome.IMPORTED, true, null);
      }
      metrics.balApplyFallback();
      warnFallback(block, applied.errorMessage.orElse("unknown error"));
    }

    LOG.atTrace().setMessage("Going to validate block {}").addArgument(block::toLogString).log();
    final BlockProcessingResult processed =
        validator.validateAndProcessBlock(
            context,
            block,
            HeaderValidationMode.FULL,
            HeaderValidationMode.NONE,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            true);
    if (processed.isSuccessful()) {
      LOG.atTrace()
          .setMessage("Block {} was validated, going to move the head")
          .addArgument(block::toLogString)
          .log();
      blockchain.appendBlock(
          block,
          processed.getYield().orElseThrow().getReceipts(),
          processed.getYield().orElseThrow().getBlockAccessList());
      possiblyMoveHead(block);
      logImportedBlockParallelization(
          block, processed.getNbParallelizedTransactions(), blockAccessList.isPresent());
      metrics.imported(false);
      return new Result(Outcome.IMPORTED, false, null);
    }
    final String error = processed.errorMessage.orElse("unknown error");
    if (processed.isWorldStateUnavailable()) {
      return new Result(Outcome.WORLD_STATE_UNAVAILABLE, false, error);
    }
    // the validator records the block only when the failure condemns the block itself
    if (badBlockManager.isBadBlock(hash)) {
      return new Result(Outcome.BAD_BLOCK, false, error);
    }
    return new Result(Outcome.FAILED, false, error);
  }

  private void warnFallback(final Block block, final String error) {
    final long now = System.nanoTime();
    final boolean warn = now - lastFallbackWarn >= FALLBACK_WARN_INTERVAL_NANOS;
    if (warn) {
      lastFallbackWarn = now;
    }
    LOG.atLevel(warn ? Level.WARN : Level.DEBUG)
        .setMessage("Block {} could not be applied from its block access list ({}), executing it")
        .addArgument(block::toLogString)
        .addArgument(error)
        .log();
  }

  private void possiblyMoveHead(final Block lastSavedBlock) {
    final MutableBlockchain blockchain = context.getBlockchain();

    if (blockchain.getChainHead().getHash().equals(lastSavedBlock.getHash())) {
      LOG.atDebug()
          .setMessage("Head is already properly set to {}")
          .addArgument(lastSavedBlock::toLogString)
          .log();
      return;
    }

    LOG.atDebug()
        .setMessage("Rewinding head to last saved block {}")
        .addArgument(lastSavedBlock::toLogString)
        .log();
    blockchain.rewindToBlock(lastSavedBlock.getHash());
  }

  private void logImportedBlockParallelization(
      final Block block,
      final Optional<Integer> nbParallelizedTransactions,
      final boolean balProvided) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    final int nbTransactions = block.getBody().getTransactions().size();
    final String balSuffix =
        block.getHeader().getBalHash().isPresent()
            ? (balProvided ? " | BAL provided" : " | BAL reconstructed")
            : "";
    if (nbParallelizedTransactions.isPresent() && nbTransactions > 0) {
      final double parallelizedTxPercentage =
          (double) (nbParallelizedTransactions.get() * 100) / nbTransactions;
      LOG.debug(
          String.format(
              "Backward sync imported #%,d (%s)| %4d tx (%5.1f%% parallel)%s",
              block.getHeader().getNumber(),
              block.getHash().toShortLogString(),
              nbTransactions,
              parallelizedTxPercentage,
              balSuffix));
    } else {
      LOG.debug(
          String.format(
              "Backward sync imported #%,d (%s)| %4d tx%s",
              block.getHeader().getNumber(),
              block.getHash().toShortLogString(),
              nbTransactions,
              balSuffix));
    }
  }
}
