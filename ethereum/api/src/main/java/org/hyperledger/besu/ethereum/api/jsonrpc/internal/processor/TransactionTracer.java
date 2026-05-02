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

import static java.util.function.Predicate.isEqual;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.apache.tuweni.units.bigints.UInt256;

/** Used to produce debug traces of transactions */
public class TransactionTracer {

  public static final String TRACE_PATH = "traces";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BlockReplay blockReplay;

  public TransactionTracer(final BlockReplay blockReplay) {
    this.blockReplay = blockReplay;
  }

  public Optional<TransactionTrace> traceTransaction(
      final Tracer.TraceableState mutableWorldState,
      final Hash blockHash,
      final Hash transactionHash,
      final DebugOperationTracer tracer) {
    return blockReplay.beforeTransactionInBlock(
        mutableWorldState,
        blockHash,
        transactionHash,
        (transaction, header, blockchain, transactionProcessor, blobGasPrice) -> {
          final TransactionProcessingResult result =
              processTransaction(
                  header,
                  blockchain,
                  mutableWorldState.updater(),
                  transaction,
                  transactionProcessor,
                  tracer,
                  blobGasPrice);
          return new TransactionTrace(transaction, result, tracer.getTraceFrames());
        });
  }

  public List<String> traceTransactionToFile(
      final MutableWorldState mutableWorldState,
      final Hash blockHash,
      final Optional<TransactionTraceParams> transactionTraceParams,
      final Path traceDir) {

    final Optional<Hash> selectedHash =
        transactionTraceParams
            .map(TransactionTraceParams::getTransactionHash)
            .map(Hash::fromHexString);
    final OpCodeTracerConfig opCodeTracerConfig =
        OpCodeTracerConfigBuilder.create()
            .traceMemory(
                transactionTraceParams
                    .map(TransactionTraceParams::disableMemoryNullable)
                    .map(Boolean.FALSE::equals)
                    .orElse(true))
            .traceStack(
                transactionTraceParams
                    .map(TransactionTraceParams::disableStackNullable)
                    .map(Boolean.FALSE::equals)
                    .orElse(true))
            .traceStorage(
                transactionTraceParams
                    .map(TransactionTraceParams::disableStorageNullable)
                    .map(Boolean.FALSE::equals)
                    .orElse(false))
            .traceReturnData(true)
            .traceOpcodes(
                transactionTraceParams
                    .map(TransactionTraceParams::opcodes)
                    .orElse(Collections.emptySet()))
            .eip3155Strict(true)
            .build();

    if (!Files.isDirectory(traceDir) && !traceDir.toFile().mkdirs()) {
      throw new RuntimeException(
          String.format("Trace directory '%s' does not exist and could not be made.", traceDir));
    }

    return blockReplay
        .performActionWithBlock(
            blockHash,
            (body, header, blockchain, transactionProcessor, protocolSpec) -> {
              WorldUpdater stackedUpdater = mutableWorldState.updater().updater();
              final List<String> traces = new ArrayList<>();
              final Wei blobGasPrice =
                  protocolSpec
                      .getFeeMarket()
                      .blobGasPricePerGas(
                          blockchain
                              .getBlockHeader(header.getParentHash())
                              .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                              .orElse(BlobGas.ZERO));
              for (int i = 0; i < body.getTransactions().size(); i++) {
                stackedUpdater.markTransactionBoundary();
                final Transaction transaction = body.getTransactions().get(i);
                if (selectedHash.isEmpty()
                    || selectedHash.filter(isEqual(transaction.getHash())).isPresent()) {
                  final File traceFile = generateTraceFile(traceDir, blockHash, i, transaction);
                  try (PrintStream out = new PrintStream(new FileOutputStream(traceFile))) {
                    final Stopwatch timer = Stopwatch.createStarted();
                    final TransactionProcessingResult result =
                        processTransaction(
                            header,
                            blockchain,
                            stackedUpdater,
                            transaction,
                            transactionProcessor,
                            new StreamingOperationTracer(out, opCodeTracerConfig),
                            blobGasPrice);
                    out.println(
                        summaryTrace(
                            transaction, timer.stop().elapsed(TimeUnit.NANOSECONDS), result));
                    traces.add(traceFile.getPath());
                  } catch (FileNotFoundException e) {
                    throw new RuntimeException(
                        "Unable to create transaction trace : " + e.getMessage());
                  }
                } else {
                  processTransaction(
                      header,
                      blockchain,
                      stackedUpdater,
                      transaction,
                      transactionProcessor,
                      OperationTracer.NO_TRACING,
                      blobGasPrice);
                }
              }
              return Optional.of(traces);
            })
        .orElse(new ArrayList<>());
  }

  private File generateTraceFile(
      final Path traceDir,
      final Hash blockHash,
      final int indexTransaction,
      final Transaction transaction) {
    return traceDir
        .resolve(
            String.format(
                "block_%.10s-%d-%.10s-%s",
                blockHash.getBytes().toHexString(),
                indexTransaction,
                transaction.getHash().getBytes().toHexString(),
                System.currentTimeMillis()))
        .toFile();
  }

  private TransactionProcessingResult processTransaction(
      final BlockHeader header,
      final Blockchain blockchain,
      final WorldUpdater worldUpdater,
      final Transaction transaction,
      final MainnetTransactionProcessor transactionProcessor,
      final OperationTracer tracer,
      final Wei blobGasPrice) {
    return transactionProcessor.processTransaction(
        worldUpdater,
        header,
        transaction,
        header.getCoinbase(),
        tracer,
        blockReplay
            .getProtocolSpec(header)
            .getPreExecutionProcessor()
            .createBlockHashLookup(blockchain, header),
        ImmutableTransactionValidationParams.builder().isAllowFutureNonce(true).build(),
        blobGasPrice);
  }

  public static String summaryTrace(
      final Transaction transaction, final long timer, final TransactionProcessingResult result) {
    final ObjectNode summaryLine = OBJECT_MAPPER.createObjectNode();
    summaryLine.put("output", result.getOutput().toUnprefixedHexString());
    summaryLine.put(
        "gasUsed",
        StreamingOperationTracer.shortNumber(
            UInt256.valueOf(transaction.getGasLimit() - result.getGasRemaining())));
    summaryLine.put("time", timer);
    return summaryLine.toString();
  }
}
