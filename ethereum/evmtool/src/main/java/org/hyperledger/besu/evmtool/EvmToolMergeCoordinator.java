/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.evmtool;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Minimal MergeMiningCoordinator for evmtool engine-test.
 * Implements the core methods used by AbstractEngineNewPayload and
 * AbstractEngineForkchoiceUpdated without requiring the full merge
 * infrastructure (TransactionPool, BackwardSyncContext, etc.).
 */
public class EvmToolMergeCoordinator implements MergeMiningCoordinator {

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final EthScheduler ethScheduler;

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

  @Override
  public BlockProcessingResult rememberBlock(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    final var result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                protocolContext, block, HeaderValidationMode.FULL, HeaderValidationMode.NONE,
                blockAccessList, false);
    result
        .getYield()
        .ifPresent(
            outputs -> {
              protocolContext.getBlockchain()
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
    return protocolSchedule
        .getByBlockHeader(block.getHeader())
        .getBlockValidator()
        .validateAndProcessBlock(
            protocolContext, block, HeaderValidationMode.FULL, HeaderValidationMode.NONE,
            Optional.empty(), false);
  }

  @Override
  public ForkchoiceResult updateForkChoice(
      final BlockHeader newHead, final Hash finalizedBlockHash, final Hash safeBlockHash) {
    final var blockchain = protocolContext.getBlockchain();
    if (!blockchain.contains(newHead.getHash())) {
      return ForkchoiceResult.withFailure(
          ForkchoiceResult.Status.INVALID, "Block not found", Optional.empty());
    }
    if (!blockchain.getChainHeadHash().equals(newHead.getHash())) {
      blockchain.rewindToBlock(newHead.getNumber());
    }
    if (!finalizedBlockHash.equals(Hash.ZERO)) {
      blockchain.getBlockHeader(finalizedBlockHash)
          .ifPresent(h -> blockchain.setFinalized(h.getHash()));
    }
    if (!safeBlockHash.equals(Hash.ZERO)) {
      blockchain.getBlockHeader(safeBlockHash)
          .ifPresent(h -> blockchain.setSafeBlock(h.getHash()));
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

  @Override
  public boolean isBadBlock(final Hash blockHash) {
    return false;
  }

  @Override
  public Optional<Hash> getLatestValidHashOfBadBlock(final Hash blockHash) {
    return Optional.empty();
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
  public PayloadIdentifier preparePayload(
      final BlockHeader parentHeader, final Long timestamp, final Bytes32 prevRandao,
      final Address feeRecipient, final Optional<List<Withdrawal>> withdrawals,
      final Optional<Bytes32> parentBeaconBlockRoot, final Optional<Long> slotNumber) {
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
  @Override public void start() {}
  @Override public void stop() {}
  @Override public void awaitStop() {}
  @Override public boolean enable() { return true; }
  @Override public boolean disable() { return true; }
  @Override public boolean isMining() { return false; }
  @Override public Wei getMinTransactionGasPrice() { return Wei.ZERO; }
  @Override public Wei getMinPriorityFeePerGas() { return Wei.ZERO; }
  @Override public Optional<Address> getCoinbase() { return Optional.empty(); }

  @Override
  public Optional<Block> createBlock(
      final BlockHeader parentHeader, final List<Transaction> transactions,
      final List<BlockHeader> ommers) {
    return Optional.empty();
  }

  @Override
  public Optional<Block> createBlock(final BlockHeader parentHeader, final long timestamp) {
    return Optional.empty();
  }

  @Override public void changeTargetGasLimit(final Long targetGasLimit) {}
}
