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
package org.hyperledger.besu.chainimport;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.util.RawBlockIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tool for importing rlp-encoded block data from files. */
public class RlpBlockImporter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RlpBlockImporter.class);

  private final Semaphore blockBacklog = new Semaphore(2);

  private final ExecutorService validationExecutor = Executors.newCachedThreadPool();
  private final ExecutorService importExecutor = Executors.newSingleThreadExecutor();

  private long cumulativeGas;
  private long segmentGas;
  private final Stopwatch cumulativeTimer = Stopwatch.createUnstarted();
  private final Stopwatch segmentTimer = Stopwatch.createUnstarted();
  private static final long SEGMENT_SIZE = 1000;
  // lets the background work of one block settle before the next, as between blocks on a live node
  private static final long PAUSE_BETWEEN_BLOCKS_MS =
      Long.getLong("besu.bench.pauseBetweenBlocksMs", 0L);
  private static final boolean LOG_BLOCK_TIMES = Boolean.getBoolean("besu.bench.logBlockTimes");

  /** Default Constructor. */
  public RlpBlockImporter() {}

  /**
   * Imports blocks that are stored as concatenated RLP sections in the given file into Besu's block
   * storage.
   *
   * @param blocks Path to the file containing the blocks
   * @param besuController the BesuController that defines blockchain behavior
   * @param skipPowValidation Skip proof of work validation (correct mix hash and difficulty)
   * @return the import result
   * @throws IOException On Failure
   */
  public RlpBlockImporter.ImportResult importBlockchain(
      final Path blocks, final BesuController besuController, final boolean skipPowValidation)
      throws IOException {
    return importBlockchain(blocks, besuController, skipPowValidation, 0L, Long.MAX_VALUE);
  }

  /**
   * Import blockchain.
   *
   * @param blocks the blocks
   * @param besuController the besu controller
   * @param skipPowValidation the skip pow validation
   * @param startBlock the start block
   * @param endBlock the end block
   * @return the rlp block importer - import result
   * @throws IOException the io exception
   */
  public RlpBlockImporter.ImportResult importBlockchain(
      final Path blocks,
      final BesuController besuController,
      final boolean skipPowValidation,
      final long startBlock,
      final long endBlock)
      throws IOException {
    final ProtocolSchedule protocolSchedule = besuController.getProtocolSchedule();
    final ProtocolContext context = besuController.getProtocolContext();
    final MutableBlockchain blockchain = context.getBlockchain();
    int count = 0;
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
    try (final RawBlockIterator iterator = new RawBlockIterator(blocks, blockHeaderFunctions)) {
      BlockHeader previousHeader = null;
      CompletableFuture<Void> previousBlockFuture = null;
      final AtomicReference<Throwable> threadedException = new AtomicReference<>();
      while (iterator.hasNext()) {
        final Block block = iterator.next();
        final BlockHeader header = block.getHeader();
        final long blockNumber = header.getNumber();
        if (blockNumber == BlockHeader.GENESIS_BLOCK_NUMBER
            || blockNumber < startBlock
            || blockNumber >= endBlock) {
          continue;
        }
        if (blockchain.contains(header.getHash())) {
          continue;
        }
        if (previousHeader == null) {
          previousHeader = lookupPreviousHeader(blockchain, header);
        }
        final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
        final BlockHeader lastHeader = previousHeader;

        final CompletableFuture<Void> validationFuture =
            CompletableFuture.runAsync(
                () -> validateBlock(protocolSpec, context, lastHeader, header, skipPowValidation),
                validationExecutor);

        final CompletableFuture<Void> extractingFuture =
            CompletableFuture.runAsync(() -> extractSignatures(block));

        final CompletableFuture<Void> calculationFutures;
        if (previousBlockFuture == null) {
          calculationFutures = extractingFuture;
        } else {
          calculationFutures = CompletableFuture.allOf(extractingFuture, previousBlockFuture);
        }

        try {
          do {
            final Throwable t = (Exception) threadedException.get();
            if (t != null) {
              throw new RuntimeException("Error importing block " + header.getNumber(), t);
            }
          } while (!blockBacklog.tryAcquire(1, SECONDS));
        } catch (final InterruptedException e) {
          LOG.error("Interrupted adding to backlog.", e);
          break;
        }
        previousBlockFuture =
            validationFuture.runAfterBothAsync(
                calculationFutures,
                () ->
                    evaluateBlock(
                        context,
                        block,
                        header,
                        protocolSchedule.getByBlockHeader(header),
                        skipPowValidation),
                importExecutor);
        previousBlockFuture.exceptionally(
            exception -> {
              threadedException.set(exception);
              return null;
            });

        ++count;
        previousHeader = header;
      }
      if (previousBlockFuture != null) {
        previousBlockFuture.join();
      }
      logProgress(blockchain.getChainHeadBlockNumber());
      return new RlpBlockImporter.ImportResult(
          blockchain.getChainHead().getTotalDifficulty(), count);
    }
  }

  private void extractSignatures(final Block block) {
    final List<CompletableFuture<Void>> futures =
        new ArrayList<>(block.getBody().getTransactions().size());
    for (final Transaction tx : block.getBody().getTransactions()) {
      futures.add(CompletableFuture.runAsync(tx::getSender, validationExecutor));
    }
    for (final CompletableFuture<Void> future : futures) {
      future.join();
    }
  }

  private void validateBlock(
      final ProtocolSpec protocolSpec,
      final ProtocolContext context,
      final BlockHeader previousHeader,
      final BlockHeader header,
      final boolean skipPowValidation) {
    final BlockHeaderValidator blockHeaderValidator = protocolSpec.getBlockHeaderValidator();
    final boolean validHeader =
        blockHeaderValidator.validateHeader(
            header,
            previousHeader,
            context,
            skipPowValidation
                ? HeaderValidationMode.LIGHT_DETACHED_ONLY
                : HeaderValidationMode.DETACHED_ONLY);
    if (!validHeader) {
      throw new IllegalStateException("Invalid header at block number " + header.getNumber() + ".");
    }
  }

  private void evaluateBlock(
      final ProtocolContext context,
      final Block block,
      final BlockHeader header,
      final ProtocolSpec protocolSpec,
      final boolean skipPowValidation) {
    try {
      cumulativeTimer.start();
      segmentTimer.start();
      final BlockImporter blockImporter = protocolSpec.getBlockImporter();
      final long importStart = System.nanoTime();
      final BlockImportResult blockImported =
          blockImporter.importBlock(
              context,
              block,
              skipPowValidation
                  ? HeaderValidationMode.LIGHT_SKIP_DETACHED
                  : HeaderValidationMode.SKIP_DETACHED,
              skipPowValidation ? HeaderValidationMode.LIGHT : HeaderValidationMode.FULL);
      if (!blockImported.isImported()) {
        throw new IllegalStateException(
            "Invalid block at block number " + header.getNumber() + ".");
      }
      if (LOG_BLOCK_TIMES) {
        LOG.info(
            "Bench block {} gas {} txs {} import_us {}",
            header.getNumber(),
            header.getGasUsed(),
            block.getBody().getTransactions().size(),
            (System.nanoTime() - importStart) / 1000);
      }
    } finally {
      blockBacklog.release();
      cumulativeTimer.stop();
      segmentTimer.stop();
      final long thisGas = block.getHeader().getGasUsed();
      cumulativeGas += thisGas;
      segmentGas += thisGas;
      if (header.getNumber() % SEGMENT_SIZE == 0) {
        logProgress(header.getNumber());
      }
      pauseBetweenBlocks();
    }
  }

  private static void pauseBetweenBlocks() {
    if (PAUSE_BETWEEN_BLOCKS_MS > 0) {
      try {
        Thread.sleep(PAUSE_BETWEEN_BLOCKS_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void logProgress(final long blockNum) {
    final long elapseMicros = segmentTimer.elapsed(TimeUnit.MICROSECONDS);
    //noinspection PlaceholderCountMatchesArgumentCount
    LOG.info(
        "Import at block {} / {} gas {} micros / Mgps {} segment {} cumulative",
        blockNum,
        segmentGas,
        elapseMicros,
        segmentGas / (double) elapseMicros,
        cumulativeGas / (double) cumulativeTimer.elapsed(TimeUnit.MICROSECONDS));
    segmentGas = 0;
    segmentTimer.reset();
  }

  private BlockHeader lookupPreviousHeader(
      final MutableBlockchain blockchain, final BlockHeader header) {
    try {
      return blockchain
          .getBlockHeader(header.getParentHash())
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      String.format(
                          "Block %s does not connect to the existing chain. Current chain head %s",
                          header.getNumber(), blockchain.getChainHeadBlockNumber())));
    } catch (IllegalStateException e) {
      LOG.info("Block {} does not connect to the existing chain.", header.getNumber());
    }
    return null;
  }

  @Override
  public void close() {
    validationExecutor.shutdownNow();
    try {
      //noinspection ResultOfMethodCallIgnored
      validationExecutor.awaitTermination(5, SECONDS);
    } catch (final Exception e) {
      LOG.error("Error shutting down validatorExecutor.", e);
    }

    importExecutor.shutdownNow();
    try {
      //noinspection ResultOfMethodCallIgnored
      importExecutor.awaitTermination(5, SECONDS);
    } catch (final Exception e) {
      LOG.error("Error shutting down importExecutor", e);
    }
  }

  /** The Import result. */
  public static final class ImportResult {

    /** The difficulty. */
    public final Difficulty td;

    /** The Count. */
    final int count;

    /**
     * Instantiates a new Import result.
     *
     * @param td the td
     * @param count the count
     */
    ImportResult(final Difficulty td, final int count) {
      this.td = td;
      this.count = count;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("td", td).add("count", count).toString();
    }
  }
}
