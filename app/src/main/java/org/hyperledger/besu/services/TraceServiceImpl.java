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
package org.hyperledger.besu.services;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockTraceResult;
import org.hyperledger.besu.plugin.data.TransactionTraceResult;
import org.hyperledger.besu.plugin.services.TraceService;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Trace service implementation. */
@Unstable
public class TraceServiceImpl implements TraceService {
  private static final Logger LOG = LoggerFactory.getLogger(TraceServiceImpl.class);

  private final BlockchainQueries blockchainQueries;
  private final ProtocolSchedule protocolSchedule;

  /**
   * Instantiates a new TraceServiceImpl service.
   *
   * @param blockchainQueries the blockchainQueries
   * @param protocolSchedule the protocolSchedule
   */
  public TraceServiceImpl(
      final BlockchainQueries blockchainQueries, final ProtocolSchedule protocolSchedule) {
    this.blockchainQueries = blockchainQueries;
    this.protocolSchedule = protocolSchedule;
  }

  /**
   * Traces block
   *
   * @param blockNumber the block number to be traced
   * @param tracer an instance of OperationTracer
   */
  @Override
  public BlockTraceResult traceBlock(
      final long blockNumber, final BlockAwareOperationTracer tracer) {
    return traceBlock(blockchainQueries.getBlockchain().getBlockByNumber(blockNumber), tracer);
  }

  /**
   * Traces block
   *
   * @param hash the block hash to be traced
   * @param tracer an instance of OperationTracer
   */
  @Override
  public BlockTraceResult traceBlock(final Hash hash, final BlockAwareOperationTracer tracer) {
    return traceBlock(blockchainQueries.getBlockchain().getBlockByHash(hash), tracer);
  }

  private BlockTraceResult traceBlock(
      final Optional<Block> maybeBlock, final BlockAwareOperationTracer tracer) {
    checkArgument(tracer != null);
    if (maybeBlock.isEmpty()) {
      return BlockTraceResult.empty();
    }

    final Optional<List<TransactionProcessingResult>> results = trace(maybeBlock.get(), tracer);

    if (results.isEmpty()) {
      return BlockTraceResult.empty();
    }

    final BlockTraceResult.Builder builder = BlockTraceResult.builder();

    final List<TransactionProcessingResult> transactionProcessingResults = results.get();
    final List<Transaction> transactions = maybeBlock.get().getBody().getTransactions();
    for (int i = 0; i < transactionProcessingResults.size(); i++) {
      final TransactionProcessingResult transactionProcessingResult =
          transactionProcessingResults.get(i);
      final TransactionTraceResult transactionTraceResult =
          transactionProcessingResult.isInvalid()
              ? TransactionTraceResult.error(
                  transactions.get(i).getHash(),
                  transactionProcessingResult.getValidationResult().getErrorMessage())
              : TransactionTraceResult.success(transactions.get(i).getHash());

      builder.addTransactionTraceResult(transactionTraceResult);
    }

    return builder.build();
  }

  /**
   * Traces range of blocks
   *
   * @param fromBlockNumber the beginning of the range (inclusive)
   * @param toBlockNumber the end of the range (inclusive)
   * @param beforeTracing Function which performs an operation on a MutableWorldState before tracing
   * @param afterTracing Function which performs an operation on a MutableWorldState after tracing
   * @param tracer an instance of OperationTracer
   */
  @Override
  public void trace(
      final long fromBlockNumber,
      final long toBlockNumber,
      final Consumer<WorldUpdater> beforeTracing,
      final Consumer<WorldUpdater> afterTracing,
      final BlockAwareOperationTracer tracer) {
    checkArgument(tracer != null);
    LOG.debug("Tracing from block {} to block {}", fromBlockNumber, toBlockNumber);
    final Blockchain blockchain = blockchainQueries.getBlockchain();
    final List<Block> blocks =
        LongStream.rangeClosed(fromBlockNumber, toBlockNumber)
            .mapToObj(
                number ->
                    blockchain
                        .getBlockByNumber(number)
                        .orElseThrow(() -> new RuntimeException("Block not found " + number)))
            .toList();
    Tracer.processTracing(
        blockchainQueries,
        blocks.getFirst().getHash(),
        traceableState -> {
          final WorldUpdater worldStateUpdater = traceableState.updater();
          final ChainUpdater chainUpdater = new ChainUpdater(traceableState, worldStateUpdater);
          beforeTracing.accept(worldStateUpdater);
          final List<TransactionProcessingResult> results = new ArrayList<>();
          blocks.forEach(
              block ->
                  results.addAll(trace(blockchain, block, traceableState, chainUpdater, tracer)));
          afterTracing.accept(chainUpdater.getNextUpdater());
          return Optional.of(results);
        });
  }

  private Optional<List<TransactionProcessingResult>> trace(
      final Block block, final BlockAwareOperationTracer tracer) {
    LOG.debug("Tracing block {}", block.toLogString());
    final Blockchain blockchain = blockchainQueries.getBlockchain();

    final Optional<List<TransactionProcessingResult>> results =
        Tracer.processTracing(
            blockchainQueries,
            block.getHash(),
            traceableState ->
                Optional.of(
                    trace(
                        blockchain,
                        block,
                        traceableState,
                        new ChainUpdater(traceableState),
                        tracer)));

    return results;
  }

  private List<TransactionProcessingResult> trace(
      final Blockchain blockchain,
      final Block block,
      final MutableWorldState worldState,
      final ChainUpdater chainUpdater,
      final BlockAwareOperationTracer tracer) {
    final List<TransactionProcessingResult> results = new ArrayList<>();
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
    final MainnetTransactionProcessor transactionProcessor = protocolSpec.getTransactionProcessor();
    final BlockHeader header = block.getHeader();
    final Address miningBeneficiary =
        protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(block.getHeader());
    tracer.traceStartBlock(
        chainUpdater.getNextUpdater(), block.getHeader(), block.getBody(), miningBeneficiary);
    runPreExecutionProcessor(blockchain, block, worldState, tracer);
    block
        .getBody()
        .getTransactions()
        .forEach(
            transaction -> {
              final Optional<BlockHeader> maybeParentHeader =
                  blockchain.getBlockHeader(header.getParentHash());
              final Wei blobGasPrice =
                  protocolSpec
                      .getFeeMarket()
                      .blobGasPricePerGas(
                          maybeParentHeader
                              .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                              .orElse(BlobGas.ZERO));

              final TransactionProcessingResult result =
                  transactionProcessor.processTransaction(
                      chainUpdater.getNextUpdater(),
                      header,
                      transaction,
                      protocolSpec.getMiningBeneficiaryCalculator().calculateBeneficiary(header),
                      tracer,
                      protocolSpec
                          .getPreExecutionProcessor()
                          .createBlockHashLookup(blockchain, header),
                      blobGasPrice);

              results.add(result);
            });

    tracer.traceEndBlock(block.getHeader(), block.getBody());

    return results;
  }

  private void runPreExecutionProcessor(
      final Blockchain blockchain,
      final Block block,
      final MutableWorldState blockUpdater,
      final BlockAwareOperationTracer tracer) {
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
    BlockHashLookup blockHashLookup =
        protocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(blockchain, block.getHeader());
    final BlockProcessingContext blockProcessingContext =
        new BlockProcessingContext(
            block.getHeader(),
            blockUpdater,
            protocolSpec,
            blockHashLookup,
            tracer,
            Optional.empty());
    protocolSpec.getPreExecutionProcessor().process(blockProcessingContext, Optional.empty());
  }
}
