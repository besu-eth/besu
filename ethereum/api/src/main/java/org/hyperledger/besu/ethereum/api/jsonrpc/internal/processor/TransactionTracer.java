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
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Used to produce debug traces of transactions */
public class TransactionTracer {

  public static final String TRACE_PATH = "traces";
  private static final Logger LOG = LoggerFactory.getLogger(TransactionTracer.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BlockReplay blockReplay;

  public TransactionTracer(final BlockReplay blockReplay) {
    this.blockReplay = blockReplay;
  }

  public Optional<TransactionTrace> traceTransaction(
      final Tracer.TraceableState mutableWorldState,
      final Hash blockHash,
      final Hash transactionHash,
      final OperationTracer tracer) {
    return blockReplay.beforeTransactionInBlock(
        mutableWorldState,
        blockHash,
        transactionHash,
        (transaction, transactionIndex, block, blockchain, transactionProcessor, blobGasPrice) -> {
          final TransactionProcessingResult result =
              processTransaction(
                  block.getHeader(),
                  blockchain,
                  mutableWorldState.updater(),
                  transaction,
                  transactionProcessor,
                  tracer,
                  blobGasPrice);
          return new TransactionTrace(
              transaction, result, tracer.getTraceFrames(), Optional.of(block), transactionIndex);
        });
  }

  public List<String> traceTransactionToFile(
      final MutableWorldState mutableWorldState,
      final Hash blockHash,
      final Optional<TransactionTraceParams> transactionTraceParams,
      final Path traceDir) {
    return traceTransactionToFile(
        mutableWorldState, blockHash, transactionTraceParams, traceDir, 0L);
  }

  /**
   * Traces the transactions of a block to files in {@code traceDir}.
   *
   * @param maxDirSize maximum total size in bytes of {@code traceDir}; 0 means unlimited. If a
   *     trace would push the directory past this size, the partially written file is deleted and a
   *     {@link TraceFilesDirSizeLimitExceededException} is thrown.
   */
  public List<String> traceTransactionToFile(
      final MutableWorldState mutableWorldState,
      final Hash blockHash,
      final Optional<TransactionTraceParams> transactionTraceParams,
      final Path traceDir,
      final long maxDirSize) {

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
            (block, blockchain, transactionProcessor, protocolSpec) -> {
              final BlockHeader header = block.getHeader();
              final BlockBody body = block.getBody();
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
                  final long remaining =
                      maxDirSize > 0
                          ? maxDirSize - directorySizeExcluding(traceDir, traceFile)
                          : Long.MAX_VALUE;
                  if (remaining <= 0) {
                    throw dirSizeLimitExceeded(traceDir, maxDirSize);
                  }
                  boolean completed = false;
                  try (PrintStream out =
                      new PrintStream(
                          new SizeLimitedOutputStream(
                              new FileOutputStream(traceFile), remaining))) {
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
                    if (out.checkError()) {
                      throw dirSizeLimitExceeded(traceDir, maxDirSize);
                    }
                    traces.add(traceFile.getPath());
                    completed = true;
                  } catch (FileNotFoundException e) {
                    throw new RuntimeException(
                        "Unable to create transaction trace : " + e.getMessage());
                  } finally {
                    if (!completed) {
                      deleteQuietly(traceFile);
                    }
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
                "block_%.10s-%d-%.10s",
                blockHash.getBytes().toHexString(),
                indexTransaction,
                transaction.getHash().getBytes().toHexString()))
        .toFile();
  }

  private static long directorySizeExcluding(final Path dir, final File excluded) {
    final File[] files = dir.toFile().listFiles();
    if (files == null) {
      return 0L;
    }
    long size = 0L;
    for (final File file : files) {
      if (file.isFile() && !file.equals(excluded)) {
        size += file.length();
      }
    }
    return size;
  }

  private static TraceFilesDirSizeLimitExceededException dirSizeLimitExceeded(
      final Path traceDir, final long maxDirSize) {
    return new TraceFilesDirSizeLimitExceededException(
        String.format(
            "Trace directory '%s' would exceed the configured limit of %d bytes",
            traceDir, maxDirSize));
  }

  private static void deleteQuietly(final File file) {
    try {
      Files.deleteIfExists(file.toPath());
    } catch (final IOException e) {
      LOG.warn("Unable to delete partial trace file {}", file, e);
    }
  }

  /** Output stream that fails once more than {@code limit} bytes have been written. */
  private static final class SizeLimitedOutputStream extends FilterOutputStream {
    private long remaining;

    SizeLimitedOutputStream(final OutputStream out, final long limit) {
      super(out);
      this.remaining = limit;
    }

    @Override
    public void write(final int b) throws IOException {
      reserve(1);
      out.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      reserve(len);
      out.write(b, off, len);
    }

    private void reserve(final int len) throws IOException {
      if (len > remaining) {
        remaining = -1;
        throw new IOException("Trace file size limit exceeded");
      }
      remaining -= len;
    }
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
