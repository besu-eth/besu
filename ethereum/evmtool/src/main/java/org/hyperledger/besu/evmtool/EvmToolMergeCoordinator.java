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
package org.hyperledger.besu.evmtool;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Minimal MergeMiningCoordinator for evmtool engine-test. Implements the core methods used by
 * AbstractEngineNewPayload and AbstractEngineForkchoiceUpdated without requiring the full merge
 * infrastructure (TransactionPool, BackwardSyncContext, etc.).
 */
public class EvmToolMergeCoordinator implements MergeMiningCoordinator {

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final EthScheduler ethScheduler;

  /**
   * Creates a coordinator for a single engine test's chain.
   *
   * @param protocolContext the context holding that test's blockchain and world state
   * @param protocolSchedule the schedule for the fixture's network
   * @param ethScheduler shared across tests, since a scheduler per test exhausts threads
   */
  public EvmToolMergeCoordinator(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final EthScheduler ethScheduler) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.ethScheduler = ethScheduler;
  }

  @Override
  public BlockProcessingResult rememberBlock(final Block block) {
    return rememberBlock(block, Optional.empty());
  }

  /**
   * Validates the block and, unlike production, makes it canonical straight away.
   *
   * <p>{@link org.hyperledger.besu.consensus.merge.blockcreation.MergeCoordinator} stores the block
   * non-canonically here and moves the head only in {@code updateForkChoice}. This fuses the two,
   * which is sound only because the fixtures import linearly: every VALID payload in {@code
   * blockchain_tests_engine} extends the previous VALID one, so there is never a stored block that
   * the following forkchoiceUpdated does not make head anyway. A fixture carrying a VALID payload
   * on a side chain would advance the head where a real node would not, and the {@code
   * lastblockhash} oracle would then be checking this harness's semantics rather than production's
   * — so mirror store-then-FCU here if such fixtures ever appear.
   */
  @Override
  public BlockProcessingResult rememberBlock(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    final var result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext,
                block,
                HeaderValidationMode.FULL,
                HeaderValidationMode.NONE,
                blockAccessList,
                false);
    result
        .getYield()
        .ifPresent(
            outputs -> {
              protocolContext
                  .getBlockchain()
                  .appendBlock(block, outputs.getReceipts(), outputs.getBlockAccessList());
              // Update world state head to the new block's state root
              protocolContext
                  .getWorldStateArchive()
                  .getWorldState(
                      org.hyperledger.besu.ethereum.trie.pathbased.common.provider
                          .WorldStateQueryParams.newBuilder()
                          .withBlockHeader(block.getHeader())
                          .withShouldWorldStateUpdateHead(true)
                          .build());
            });
    return result;
  }

  @Override
  public BlockProcessingResult validateBlock(final Block block) {
    // No engine method calls this — newPayload goes through rememberBlock — so rather than carry a
    // second copy of the validate-and-process call that nothing exercises, say so.
    throw new UnsupportedOperationException(
        "Block validation without import not used by engine-test");
  }

  @Override
  public ForkchoiceResult updateForkChoice(
      final BlockHeader newHead, final Hash finalizedBlockHash, final Hash safeBlockHash) {
    final var blockchain = protocolContext.getBlockchain();
    if (!blockchain.contains(newHead.getHash())) {
      return ForkchoiceResult.withFailure(
          ForkchoiceResult.Status.INVALID, "Block not found", Optional.empty());
    }
    // By hash, not by height: at a given height the canonical chain may hold a different block
    // than the one being made head, and rewinding by number would land on that one instead.
    if (!blockchain.getChainHeadHash().equals(newHead.getHash())
        && !blockchain.rewindToBlock(newHead.getHash())) {
      return ForkchoiceResult.withFailure(
          ForkchoiceResult.Status.INVALID,
          "Unable to rewind chain head to " + newHead.getHash(),
          Optional.empty());
    }
    if (!finalizedBlockHash.equals(Hash.ZERO)) {
      blockchain
          .getBlockHeader(finalizedBlockHash)
          .ifPresent(h -> blockchain.setFinalized(h.getHash()));
    }
    if (!safeBlockHash.equals(Hash.ZERO)) {
      blockchain.getBlockHeader(safeBlockHash).ifPresent(h -> blockchain.setSafeBlock(h.getHash()));
    }
    return ForkchoiceResult.withResult(
        blockchain.getBlockHeader(finalizedBlockHash), Optional.of(newHead));
  }

  @Override
  public Optional<Hash> getLatestValidAncestor(final Hash blockHash) {
    return protocolContext.getBlockchain().getBlockHeader(blockHash).map(BlockHeader::getHash);
  }

  @Override
  public Optional<Hash> getLatestValidAncestor(final BlockHeader blockHeader) {
    return getLatestValidAncestor(blockHeader.getParentHash());
  }

  @Override
  public boolean isDescendantOf(final BlockHeader ancestorBlock, final BlockHeader newBlock) {
    return true;
  }

  // Answered from the same bad-block cache production reads. validateAndProcessBlock populates it
  // on every rejection, so a fixture that re-sends an invalid payload gets the cached verdict and
  // latestValidHash rather than a fresh validation with a different message — which is what the
  // hive run these tasks reproduce would report.
  @Override
  public boolean isBadBlock(final Hash blockHash) {
    return protocolContext.getBadBlockManager().isBadBlock(blockHash);
  }

  @Override
  public Optional<Hash> getLatestValidHashOfBadBlock(final Hash blockHash) {
    return protocolContext.getBadBlockManager().getLatestValidHash(blockHash);
  }

  @Override
  public boolean isBackwardSyncing() {
    return false;
  }

  @Override
  public CompletableFuture<Void> appendNewPayloadToSync(final Block newPayload) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Optional<BlockHeader> getOrSyncHeadByHash(final Hash headHash, final Hash finalizedHash) {
    return protocolContext.getBlockchain().getBlockHeader(headHash);
  }

  @Override
  public boolean isMiningBeforeMerge() {
    return false;
  }

  @Override
  public PayloadIdentifier preparePayload(final PreparePayloadArgs payloadArgs) {
    throw new UnsupportedOperationException("Payload building not supported in evmtool");
  }

  @Override
  public void finalizeProposalById(final PayloadIdentifier payloadId) {}

  @Override
  public void awaitCurrentBuildCompletion(final PayloadIdentifier payloadId) {}

  @Override
  public EthScheduler getEthScheduler() {
    return ethScheduler;
  }

  // MiningCoordinator interface methods
  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void awaitStop() {}

  @Override
  public boolean enable() {
    return true;
  }

  @Override
  public boolean disable() {
    return true;
  }

  @Override
  public boolean isMining() {
    return false;
  }

  @Override
  public Wei getMinTransactionGasPrice() {
    return Wei.ZERO;
  }

  @Override
  public Wei getMinPriorityFeePerGas() {
    return Wei.ZERO;
  }

  @Override
  public Optional<Block> createBlock(
      final BlockHeader parentHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers) {
    return Optional.empty();
  }

  @Override
  public Optional<Block> createBlock(final BlockHeader parentHeader, final long timestamp) {
    return Optional.empty();
  }

  @Override
  public void changeTargetGasLimit(final Long targetGasLimit) {}

  @Override
  public boolean isAncestorOfFinalized(final BlockHeader candidateHeadBlockHeader) {
    // The harness tracks no finalized block, so no head is ever an ancestor of finalized.
    // Returning false lets forkchoiceUpdated proceed to set the head normally.
    return false;
  }

  @Override
  public OptionalLong computeReorgDepth(final BlockHeader newHead) {
    // The engine-test harness imports blocks sequentially and only advances the head via
    // forkchoiceUpdated to the block just imported, so there is never a reorg. Report depth 0
    // so the FCU MAX_REORG_DEPTH guard is never tripped.
    return OptionalLong.of(0L);
  }
}
