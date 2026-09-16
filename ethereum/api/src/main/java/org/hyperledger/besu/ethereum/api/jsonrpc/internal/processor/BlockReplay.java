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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer.TraceableState;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BlockReplay {

  private final ProtocolSchedule protocolSchedule;
  private final Blockchain blockchain;
  private final ProtocolContext protocolContext;

  public BlockReplay(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Blockchain blockchain) {
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.blockchain = blockchain;
  }

  public Optional<BlockTrace> block(
      final Block block, final TransactionAction<TransactionTrace> action) {
    return performActionWithBlock(
        block,
        (blk, blockchain, transactionProcessor, protocolSpec) -> {
          final Wei blobGasPrice =
              protocolSpec
                  .getFeeMarket()
                  .blobGasPricePerGas(
                      blockchain
                          .getBlockHeader(blk.getHeader().getParentHash())
                          .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                          .orElse(BlobGas.ZERO));

          final List<Transaction> transactions = blk.getBody().getTransactions();
          final List<TransactionTrace> transactionTraces = new ArrayList<>(transactions.size());
          for (int i = 0; i < transactions.size(); i++) {
            transactionTraces.add(
                action.performAction(
                    transactions.get(i), i, blk, blockchain, transactionProcessor, blobGasPrice));
          }
          return Optional.of(new BlockTrace(transactionTraces));
        });
  }

  public Optional<BlockTrace> block(
      final Hash blockHash, final TransactionAction<TransactionTrace> action) {
    return getBlock(blockHash).flatMap(block -> block(block, action));
  }

  public <T> Optional<T> beforeTransactionInBlock(
      final TraceableState mutableWorldState,
      final Hash blockHash,
      final Hash transactionHash,
      final TransactionAction<T> action) {
    return performActionWithBlock(
        blockHash,
        (block, blockchain, transactionProcessor, protocolSpec) -> {
          final BlockHeader header = block.getHeader();
          final BlockHashLookup blockHashLookup =
              protocolSpec.getPreExecutionProcessor().createBlockHashLookup(blockchain, header);
          final Wei blobGasPrice =
              protocolSpec
                  .getFeeMarket()
                  .blobGasPricePerGas(
                      blockchain
                          .getBlockHeader(header.getParentHash())
                          .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                          .orElse(BlobGas.ZERO));

          final List<Transaction> transactions = block.getBody().getTransactions();
          for (int i = 0; i < transactions.size(); i++) {
            final Transaction transaction = transactions.get(i);
            if (transaction.getHash().equals(transactionHash)) {
              return Optional.of(
                  action.performAction(
                      transaction, i, block, blockchain, transactionProcessor, blobGasPrice));
            } else {
              transactionProcessor.processTransaction(
                  mutableWorldState.updater(),
                  header,
                  transaction,
                  protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(header),
                  blockHashLookup,
                  TransactionValidationParams.blockReplay(),
                  blobGasPrice);
            }
          }
          return Optional.empty();
        });
  }

  public <T> Optional<T> afterTransactionInBlock(
      final TraceableState mutableWorldState,
      final Hash blockHash,
      final Hash transactionHash,
      final TransactionAction<T> action) {
    return beforeTransactionInBlock(
        mutableWorldState,
        blockHash,
        transactionHash,
        (transaction, transactionIndex, block, blockchain, transactionProcessor, blobGasPrice) -> {
          final BlockHeader blockHeader = block.getHeader();
          final ProtocolSpec spec = protocolSchedule.getByBlockHeader(blockHeader);
          transactionProcessor.processTransaction(
              mutableWorldState.updater(),
              blockHeader,
              transaction,
              spec.getMiningBeneficiaryCalculator().calculateBeneficiary(blockHeader),
              spec.getPreExecutionProcessor().createBlockHashLookup(blockchain, blockHeader),
              TransactionValidationParams.blockReplay(),
              blobGasPrice);
          return action.performAction(
              transaction, transactionIndex, block, blockchain, transactionProcessor, blobGasPrice);
        });
  }

  public <T> Optional<T> performActionWithBlock(final Hash blockHash, final BlockAction<T> action) {
    Optional<Block> maybeBlock = getBlock(blockHash);
    if (maybeBlock.isEmpty()) {
      maybeBlock = protocolContext.getBadBlockManager().getBadBlock(blockHash);
    }
    return maybeBlock.flatMap(block -> performActionWithBlock(block, action));
  }

  private <T> Optional<T> performActionWithBlock(final Block block, final BlockAction<T> action) {
    if (block.getHeader() == null) {
      return Optional.empty();
    }
    if (block.getBody() == null) {
      return Optional.empty();
    }
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();

    return action.perform(block, blockchain, transactionProcessor, protocolSpec);
  }

  private Optional<Block> getBlock(final Hash blockHash) {
    final BlockHeader blockHeader = blockchain.getBlockHeader(blockHash).orElse(null);
    if (blockHeader != null) {
      final BlockBody blockBody = blockchain.getBlockBody(blockHeader.getHash()).orElse(null);
      if (blockBody != null) {
        return Optional.of(new Block(blockHeader, blockBody));
      }
    }
    return Optional.empty();
  }

  public ProtocolSpec getProtocolSpec(final BlockHeader header) {
    return protocolSchedule.getByBlockHeader(header);
  }

  @FunctionalInterface
  public interface BlockAction<T> {
    Optional<T> perform(
        Block block,
        Blockchain blockchain,
        MainnetTransactionProcessor transactionProcessor,
        ProtocolSpec protocolSpec);
  }

  @FunctionalInterface
  public interface TransactionAction<T> {
    T performAction(
        Transaction transaction,
        int transactionIndex,
        Block block,
        Blockchain blockchain,
        MainnetTransactionProcessor transactionProcessor,
        Wei blobGasPrice);
  }
}
