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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last stage of the import pipeline: imports each block of a batch in order, finalized blocks from
 * their block access list when one was downloaded with their receipts, the others by executing
 * them, and makes each one the chain head.
 */
class ImportBlocksStep implements Consumer<BlockBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(ImportBlocksStep.class);
  private static final long LOG_INTERVAL_MILLIS = 10_000;

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final BalImporter balImporter;
  private final long finalizedNumber;
  private final long targetNumber;
  private final Consumer<BlockHeader> badBlockListener;
  private long fromBal;
  private long executed;
  private long lastLogMillis;

  /**
   * @param finalizedNumber the highest finalized block of the sync, -1 if none
   * @param targetNumber the head the sync imports up to, for the progress logs
   * @param badBlockListener told of a block that is invalid
   */
  ImportBlocksStep(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final BalImporter balImporter,
      final long finalizedNumber,
      final long targetNumber,
      final Consumer<BlockHeader> badBlockListener) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.balImporter = balImporter;
    this.finalizedNumber = finalizedNumber;
    this.targetNumber = targetNumber;
    this.badBlockListener = badBlockListener;
  }

  @Override
  public void accept(final BlockBatch batch) {
    for (final Block block : batch.blocks()) {
      importBlock(
          block,
          Optional.ofNullable(batch.blockAccessLists().get(block.getHash())),
          Optional.ofNullable(batch.receipts().get(block.getHash())));
    }
  }

  /** Imports a block whose parent is imported, or throws if it cannot. */
  void importBlock(
      final Block block,
      final Optional<BlockAccessList> bal,
      final Optional<List<TransactionReceipt>> receipts) {
    // executing it again can only repeat the failure that recorded it
    if (protocolContext.getBadBlockManager().isBadBlock(block.getHash())) {
      badBlockListener.accept(block.getHeader());
      throw new IllegalStateException("Block " + block.toLogString() + " is a known bad block");
    }
    final boolean importedFromBal =
        block.getHeader().getNumber() <= finalizedNumber
            && bal.isPresent()
            && receipts.isPresent()
            && balImporter.applyToWorldState(block, receipts.get(), bal.get());
    if (importedFromBal) {
      append(block, receipts.get(), bal);
      fromBal++;
    } else {
      execute(block, bal);
      executed++;
    }
    logProgress(block.getHeader().getNumber());
  }

  private void execute(final Block block, final Optional<BlockAccessList> bal) {
    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext,
                block,
                HeaderValidationMode.FULL,
                HeaderValidationMode.NONE,
                bal,
                InclusionListTransactions.EMPTY,
                true);
    if (result.isFailed()) {
      if (result.isWorldStateUnavailable()) {
        throw new IllegalStateException(
            "The parent world state of block "
                + block.toLogString()
                + " is unavailable, call debug_resyncWorldState to repair it");
      }
      // the validator records the block only when the failure condemns the block itself
      if (protocolContext.getBadBlockManager().isBadBlock(block.getHash())) {
        badBlockListener.accept(block.getHeader());
      }
      throw new IllegalStateException(
          "Cannot import block "
              + block.toLogString()
              + ": "
              + result.errorMessage.orElse("unknown reason"));
    }
    final BlockProcessingOutputs outputs = result.getYield().orElseThrow();
    append(block, outputs.getReceipts(), outputs.getBlockAccessList());
  }

  private void append(
      final Block block,
      final List<TransactionReceipt> receipts,
      final Optional<BlockAccessList> bal) {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    blockchain.appendBlock(block, receipts, bal);
    // a block whose parent is not the head, after a reorg, is not made the head when appended
    if (!blockchain.getChainHeadHash().equals(block.getHash())) {
      blockchain.rewindToBlock(block.getHash());
    }
  }

  private void logProgress(final long number) {
    final long now = System.currentTimeMillis();
    if (now - lastLogMillis >= LOG_INTERVAL_MILLIS || number == targetNumber) {
      lastLogMillis = now;
      LOG.info(
          String.format(
              "Backward sync imported #%,d of #%,d, %,d from their BAL, %,d executed",
              number, targetNumber, fromBal, executed));
    }
  }
}
