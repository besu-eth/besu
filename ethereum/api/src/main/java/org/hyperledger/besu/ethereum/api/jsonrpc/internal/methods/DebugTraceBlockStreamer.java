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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.DebugTraceTransactionResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.debug.TracerType;
import org.hyperledger.besu.ethereum.mainnet.ImmutableTransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.ethereum.vm.StreamingDebugOperationTracer;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class DebugTraceBlockStreamer {

  // Sized to stay below Netty's default high watermark (64 KB). This ensures each
  // flush does not overshoot the backpressure threshold in JsonResponseStreamer, allowing
  // the drain/resume cycle to regulate direct-memory usage smoothly.
  private static final int BUF_SIZE = 32 * 1024;

  private static final byte[] TX_OPEN = "{\"txHash\":\"".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TX_RESULT_STRUCT_OPEN =
      "\",\"result\":{\"structLogs\":[".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TX_GAS_PREFIX = "],\"gas\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TX_FAILED_TRUE_RV =
      ",\"failed\":true,\"returnValue\":\"".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TX_FAILED_FALSE_RV =
      ",\"failed\":false,\"returnValue\":\"".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] TX_CLOSE = "\"}}".getBytes(StandardCharsets.US_ASCII);

  private static final byte[] SL_PC = "{\"pc\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_OP = ",\"op\":\"".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_GAS = "\",\"gas\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_GAS_COST = ",\"gasCost\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_DEPTH = ",\"depth\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_REFUND = ",\"refund\":".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_STACK = ",\"stack\":[".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_MEMORY = ",\"memory\":[".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_STORAGE = ",\"storage\":{".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_REASON = ",\"reason\":\"".getBytes(StandardCharsets.US_ASCII);
  private static final byte[] SL_ERROR = ",\"error\":\"".getBytes(StandardCharsets.US_ASCII);

  private static final byte COMMA = ',';
  private static final byte QUOTE = '"';
  private static final byte COLON = ':';
  private static final byte OBJ_CLOSE = '}';
  private static final byte ARR_CLOSE = ']';

  private final Block block;
  private final TraceOptions traceOptions;
  private final ProtocolSchedule protocolSchedule;
  private final BlockchainQueries blockchainQueries;

  private final byte[] numBuf = new byte[20];
  private final byte[] writeBuf = new byte[BUF_SIZE];
  private final TreeMap<UInt256, UInt256> sortedStorage = new TreeMap<>();
  private final HashMap<String, byte[]> opcodeCache = new HashMap<>(256);

  private OutputStream rawOut;
  private int writePos;
  private boolean firstStructLog;
  private boolean firstTx;

  public DebugTraceBlockStreamer(
      final Block block,
      final TraceOptions traceOptions,
      final ProtocolSchedule protocolSchedule,
      final BlockchainQueries blockchainQueries) {
    this.block = block;
    this.traceOptions = traceOptions;
    this.protocolSchedule = protocolSchedule;
    this.blockchainQueries = blockchainQueries;
  }

  // ── unsynchronized buffer management ──────────────────────────────

  private void flushBuf() throws IOException {
    if (writePos > 0) {
      rawOut.write(writeBuf, 0, writePos);
      writePos = 0;
    }
  }

  private void writeByte(final int b) throws IOException {
    if (writePos >= BUF_SIZE) flushBuf();
    writeBuf[writePos++] = (byte) b;
  }

  private void writeBytes(final byte[] src) throws IOException {
    writeBytes(src, 0, src.length);
  }

  private void writeBytes(final byte[] src, final int off, final int len) throws IOException {
    if (len >= BUF_SIZE) {
      flushBuf();
      rawOut.write(src, off, len);
      return;
    }
    if (writePos + len > BUF_SIZE) flushBuf();
    System.arraycopy(src, off, writeBuf, writePos, len);
    writePos += len;
  }

  // ── public API ────────────────────────────────────────────────────

  public void streamTo(final OutputStream out, final ObjectMapper mapper) throws IOException {
    this.rawOut = out;
    this.writePos = 0;
    this.firstTx = true;

    try {
      writeByte('[');

      Tracer.processTracing(
          blockchainQueries,
          Optional.of(block.getHeader()),
          traceableState -> {
            final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
            final MainnetTransactionProcessor transactionProcessor =
                protocolSpec.getTransactionProcessor();
            final TraceBlock.ChainUpdater chainUpdater =
                new TraceBlock.ChainUpdater(traceableState);

            final BlockHeader header = block.getHeader();
            final Optional<BlockHeader> maybeParentHeader =
                blockchainQueries.getBlockchain().getBlockHeader(header.getParentHash());
            final Wei blobGasPrice =
                protocolSpec
                    .getFeeMarket()
                    .blobGasPricePerGas(
                        maybeParentHeader
                            .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                            .orElse(BlobGas.ZERO));
            final BlockHashLookup blockHashLookup =
                protocolSpec
                    .getPreExecutionProcessor()
                    .createBlockHashLookup(blockchainQueries.getBlockchain(), header);

            final boolean isOpcodeTracer = traceOptions.tracerType() == TracerType.OPCODE_TRACER;

            for (final Transaction transaction : block.getBody().getTransactions()) {
              if (isOpcodeTracer) {
                streamOpcodeTransaction(
                    transaction,
                    chainUpdater,
                    transactionProcessor,
                    header,
                    blobGasPrice,
                    blockHashLookup);
              } else {
                try {
                  final byte[] json =
                      mapper.writeValueAsBytes(
                          buildTransactionResult(
                              transaction,
                              chainUpdater,
                              transactionProcessor,
                              protocolSpec,
                              header,
                              blobGasPrice,
                              blockHashLookup));
                  if (!firstTx) writeByte(COMMA);
                  firstTx = false;
                  writeBytes(json);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              }
            }

            return Optional.of(Boolean.TRUE);
          });

      writeByte(']');
      flushBuf();
    } finally {
      this.rawOut = null;
    }
  }

  public List<Object> accumulateAll() {
    final List<Object> results = new ArrayList<>();
    Tracer.processTracing(
        blockchainQueries,
        Optional.of(block.getHeader()),
        traceableState -> {
          final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
          final MainnetTransactionProcessor transactionProcessor =
              protocolSpec.getTransactionProcessor();
          final TraceBlock.ChainUpdater chainUpdater = new TraceBlock.ChainUpdater(traceableState);

          final BlockHeader header = block.getHeader();
          final Optional<BlockHeader> maybeParentHeader =
              blockchainQueries.getBlockchain().getBlockHeader(header.getParentHash());
          final Wei blobGasPrice =
              protocolSpec
                  .getFeeMarket()
                  .blobGasPricePerGas(
                      maybeParentHeader
                          .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                          .orElse(BlobGas.ZERO));
          final BlockHashLookup blockHashLookup =
              protocolSpec
                  .getPreExecutionProcessor()
                  .createBlockHashLookup(blockchainQueries.getBlockchain(), header);

          for (final Transaction transaction : block.getBody().getTransactions()) {
            results.add(
                buildTransactionResult(
                    transaction,
                    chainUpdater,
                    transactionProcessor,
                    protocolSpec,
                    header,
                    blobGasPrice,
                    blockHashLookup));
          }

          return Optional.of(Boolean.TRUE);
        });
    return results;
  }

  // ── streaming opcode transaction ──────────────────────────────────

  private void streamOpcodeTransaction(
      final Transaction transaction,
      final TraceBlock.ChainUpdater chainUpdater,
      final MainnetTransactionProcessor transactionProcessor,
      final BlockHeader header,
      final Wei blobGasPrice,
      final BlockHashLookup blockHashLookup) {

    final StreamingDebugOperationTracer tracer =
        new StreamingDebugOperationTracer(
            traceOptions.opCodeTracerConfig(),
            true,
            (pc, opcode, gasRemaining, gasCost, depth, stack, frame, halt, revert) ->
                writeStructLog(
                    pc, opcode, gasRemaining, gasCost, depth, stack, frame, halt, revert));

    try {
      if (!firstTx) writeByte(COMMA);
      firstTx = false;

      writeBytes(TX_OPEN);
      writeAscii(transaction.getHash().toHexString());
      writeBytes(TX_RESULT_STRUCT_OPEN);

      firstStructLog = true;

      final TransactionProcessingResult result =
          transactionProcessor.processTransaction(
              chainUpdater.getNextUpdater(),
              header,
              transaction,
              header.getCoinbase(),
              tracer,
              blockHashLookup,
              ImmutableTransactionValidationParams.builder().build(),
              blobGasPrice,
              Optional.empty());

      writeBytes(TX_GAS_PREFIX);
      writeLong(transaction.getGasLimit() - result.getGasRemaining());
      writeBytes(result.isSuccessful() ? TX_FAILED_FALSE_RV : TX_FAILED_TRUE_RV);
      final Bytes output = result.getOutput();
      if (!output.isEmpty()) {
        writeAscii(output.toHexString());
      }
      writeBytes(TX_CLOSE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ── non-streaming fallback (flat tracer, etc.) ────────────────────

  private DebugTraceTransactionResult buildTransactionResult(
      final Transaction transaction,
      final TraceBlock.ChainUpdater chainUpdater,
      final MainnetTransactionProcessor transactionProcessor,
      final ProtocolSpec protocolSpec,
      final BlockHeader header,
      final Wei blobGasPrice,
      final BlockHashLookup blockHashLookup) {
    final DebugOperationTracer tracer =
        new DebugOperationTracer(traceOptions.opCodeTracerConfig(), true);

    final AccessLocationTracker accessListTracker =
        BlockAccessList.BlockAccessListBuilder.createTransactionAccessLocationTracker(0);

    final TransactionProcessingResult result =
        transactionProcessor.processTransaction(
            chainUpdater.getNextUpdater(),
            header,
            transaction,
            header.getCoinbase(),
            tracer,
            blockHashLookup,
            ImmutableTransactionValidationParams.builder().build(),
            blobGasPrice,
            Optional.of(accessListTracker));

    final TransactionTrace transactionTrace =
        new TransactionTrace(
            transaction,
            result,
            tracer.copyTraceFrames(),
            Optional.empty(),
            accessListTracker.getTouchedAccounts());
    tracer.reset();

    return DebugTraceTransactionStepFactory.create(traceOptions, protocolSpec)
        .apply(transactionTrace);
  }

  // ── struct log writer (hot path) ──────────────────────────────────

  private void writeStructLog(
      final int pc,
      final String opcode,
      final long gasRemaining,
      final long gasCost,
      final int depth,
      final Bytes[] stack,
      final MessageFrame frame,
      final ExceptionalHaltReason haltReason,
      final Bytes revertReason) {
    try {
      if (!firstStructLog) writeByte(COMMA);
      firstStructLog = false;

      writeBytes(SL_PC);
      writeInt(pc);

      writeBytes(SL_OP);
      writeBytes(opcodeToBytes(opcode));

      writeBytes(SL_GAS);
      writeLong(gasRemaining);

      writeBytes(SL_GAS_COST);
      writeLong(gasCost);

      writeBytes(SL_DEPTH);
      writeInt(depth + 1);

      final long refund = frame.getGasRefund();
      if (refund > 0) {
        writeBytes(SL_REFUND);
        writeLong(refund);
      }

      if (stack != null) {
        writeBytes(SL_STACK);
        for (int s = 0; s < stack.length; s++) {
          if (s > 0) writeByte(COMMA);
          writeByte(QUOTE);
          writeHex(stack[s].toArrayUnsafe(), true);
          writeByte(QUOTE);
        }
        writeByte(ARR_CLOSE);
      }

      if (traceOptions.opCodeTracerConfig().traceMemory() && frame.memoryWordSize() > 0) {
        writeBytes(SL_MEMORY);
        final int wordCount = frame.memoryWordSize();
        for (int i = 0; i < wordCount; i++) {
          if (i > 0) writeByte(COMMA);
          writeByte(QUOTE);
          writeHex(frame.readMutableMemory(i * 32L, 32).toArrayUnsafe(), false);
          writeByte(QUOTE);
        }
        writeByte(ARR_CLOSE);
      }

      if (traceOptions.opCodeTracerConfig().traceStorage()) {
        try {
          final MutableAccount account =
              frame.getWorldUpdater().getAccount(frame.getRecipientAddress());
          if (account != null) {
            final Map<UInt256, UInt256> updatedStorage = account.getUpdatedStorage();
            if (!updatedStorage.isEmpty()) {
              writeBytes(SL_STORAGE);
              sortedStorage.clear();
              sortedStorage.putAll(updatedStorage);
              boolean firstEntry = true;
              for (final Map.Entry<UInt256, UInt256> entry : sortedStorage.entrySet()) {
                if (!firstEntry) writeByte(COMMA);
                firstEntry = false;
                writeByte(QUOTE);
                writeHex(entry.getKey().toArrayUnsafe(), false);
                writeByte(QUOTE);
                writeByte(COLON);
                writeByte(QUOTE);
                writeHex(entry.getValue().toArrayUnsafe(), false);
                writeByte(QUOTE);
              }
              writeByte(OBJ_CLOSE);
            }
          }
        } catch (final ModificationNotAllowedException e) {
          // no storage to write
        }
      }

      if (revertReason != null) {
        writeBytes(SL_REASON);
        writeHex(revertReason.toArrayUnsafe(), true);
        writeByte(QUOTE);
      }

      if (haltReason != null) {
        writeBytes(SL_ERROR);
        writeBytes(haltReason.name().getBytes(StandardCharsets.US_ASCII));
        writeByte(QUOTE);
      }

      writeByte(OBJ_CLOSE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ── primitives ────────────────────────────────────────────────────

  /**
   * Encodes {@code bytes} as {@code 0x}-prefixed hex directly into {@link #writeBuf}, flushing
   * first if needed to guarantee room. For values whose hex exceeds the buffer size, writes in
   * chunks.
   */
  private void writeHex(final byte[] bytes, final boolean stripLeading) throws IOException {
    // encodeTo always writes the "0x" prefix (2 bytes) plus at least one hex digit,
    // even when the input is empty (producing "0x0" = 3 bytes).
    final int maxLen = Math.max(3, 2 + bytes.length * 2);
    if (maxLen <= BUF_SIZE) {
      if (writePos + maxLen > BUF_SIZE) flushBuf();
      writePos = HexWriter.encodeTo(bytes, bytes.length, writeBuf, writePos, stripLeading);
    } else {
      // Value too large for writeBuf — write prefix + hex in chunks via rawOut
      flushBuf();
      rawOut.write('0');
      rawOut.write('x');
      for (final byte b : bytes) {
        rawOut.write(HexWriter.HEX_PAIR[(b & 0xFF) << 1]);
        rawOut.write(HexWriter.HEX_PAIR[((b & 0xFF) << 1) + 1]);
      }
    }
  }

  private byte[] opcodeToBytes(final String opcode) {
    return opcodeCache.computeIfAbsent(opcode, s -> s.getBytes(StandardCharsets.US_ASCII));
  }

  private void writeLong(final long value) throws IOException {
    if (value >= 0 && value <= 9) {
      writeByte('0' + (int) value);
      return;
    }
    long remaining = value;
    boolean neg = false;
    if (remaining < 0) {
      neg = true;
      remaining = -remaining;
    }
    int pos = numBuf.length;
    while (remaining > 0) {
      numBuf[--pos] = (byte) ('0' + (int) (remaining % 10));
      remaining /= 10;
    }
    if (neg) {
      numBuf[--pos] = '-';
    }
    writeBytes(numBuf, pos, numBuf.length - pos);
  }

  private void writeInt(final int value) throws IOException {
    writeLong(value);
  }

  private void writeAscii(final String s) throws IOException {
    writeBytes(s.getBytes(StandardCharsets.US_ASCII));
  }
}
