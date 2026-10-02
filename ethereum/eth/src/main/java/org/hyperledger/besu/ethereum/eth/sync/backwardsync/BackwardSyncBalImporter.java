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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block access lists over eth/71 and imports backward-sync blocks. Blocks are processed
 * in windows of {@link #BAL_REQUEST_WINDOW}; the BALs of the next window are fetched while the
 * current one is executing, so network latency is hidden behind block execution. Downloads are
 * best-effort: each request goes to a single peer without retries and the import waits at most
 * {@link #BAL_WAIT_TIMEOUT} for a window, missing BALs being reconstructed during execution.
 */
public class BackwardSyncBalImporter {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncBalImporter.class);
  static final int BAL_REQUEST_WINDOW = 16;
  static final Duration BAL_WAIT_TIMEOUT = Duration.ofSeconds(2);

  private final BackwardSyncContext context;

  public BackwardSyncBalImporter(final BackwardSyncContext context) {
    this.context = context;
  }

  /**
   * Loads a locally stored BAL when the header advertises one. Skips storage lookups for pre-BAL
   * headers.
   */
  public Optional<BlockAccessList> lookupStoredBal(final BlockHeader header) {
    if (header.getBalHash().isEmpty()) {
      return Optional.empty();
    }
    return context.getProtocolContext().getBlockchain().getBlockAccessList(header.getHash());
  }

  /**
   * Starts downloading the BALs of the first window of {@code headers}, so it can run concurrently
   * with the bodies download. Pass the result to {@link #importBlocks(List, CompletableFuture)}, or
   * cancel it if the blocks are not imported.
   */
  public CompletableFuture<Map<Hash, BlockAccessList>> prefetchFirstWindow(
      final List<BlockHeader> headers) {
    return fetchBalsAsync(
        headers.stream()
            .sorted(Comparator.comparingLong(BlockHeader::getNumber))
            .limit(BAL_REQUEST_WINDOW)
            .toList());
  }

  /**
   * Imports {@code blocks} in order, using {@code firstWindowBals} for the first window and
   * prefetching the BALs of each following window while the previous one executes. BAL downloads
   * still pending when the import stops are cancelled.
   */
  public void importBlocks(
      final List<Block> blocks,
      final CompletableFuture<Map<Hash, BlockAccessList>> firstWindowBals) {
    CompletableFuture<Map<Hash, BlockAccessList>> currentBals = firstWindowBals;
    CompletableFuture<Map<Hash, BlockAccessList>> nextBals = currentBals;
    try {
      if (blocks.isEmpty()) {
        context.halveBatchSize();
        LOG.debug("No blocks to save, reducing batch size to {}", context.getBatchSize());
        return;
      }

      for (int start = 0; start < blocks.size(); start += BAL_REQUEST_WINDOW) {
        final int end = Math.min(start + BAL_REQUEST_WINDOW, blocks.size());
        nextBals =
            end < blocks.size()
                ? fetchBalsAsync(windowHeaders(blocks, end))
                : CompletableFuture.completedFuture(Map.of());
        final Map<Hash, BlockAccessList> bals = awaitBals(currentBals);
        for (int i = start; i < end; i++) {
          final Block block = blocks.get(i);
          if (!saveBlock(block, Optional.ofNullable(bals.get(block.getHash())))) {
            return;
          }
        }
        currentBals = nextBals;
      }

      if (blocks.size() == context.getBatchSize()) {
        context.resetBatchSize();
      }
    } finally {
      // no-op for completed downloads, stops the ones abandoned by an early return or a failure
      currentBals.cancel(false);
      nextBals.cancel(false);
    }
  }

  /**
   * Headers of the window of blocks starting at {@code start}, including the ones without a BAL
   * hash: {@link #fetchBalsAsync(List)} filters them out.
   */
  private static List<BlockHeader> windowHeaders(final List<Block> blocks, final int start) {
    return blocks.subList(start, Math.min(start + BAL_REQUEST_WINDOW, blocks.size())).stream()
        .map(Block::getHeader)
        .toList();
  }

  private CompletableFuture<Map<Hash, BlockAccessList>> fetchBalsAsync(
      final List<BlockHeader> headers) {
    final List<BlockHeader> balHeaders =
        headers.stream().filter(header -> header.getBalHash().isPresent()).toList();
    if (balHeaders.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }
    // the fetch stops between requests once the returned future is completed or cancelled
    final CompletableFuture<Map<Hash, BlockAccessList>> bals = new CompletableFuture<>();
    context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              bals.complete(fetchBals(balHeaders, bals::isDone));
              return CompletableFuture.completedFuture(null);
            })
        .whenComplete(
            (result, throwable) -> {
              if (throwable != null) {
                bals.completeExceptionally(throwable);
              }
            });
    return bals;
  }

  private static Map<Hash, BlockAccessList> awaitBals(
      final CompletableFuture<Map<Hash, BlockAccessList>> bals) {
    try {
      return bals.get(BAL_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      bals.cancel(false);
      LOG.debug("BAL download timed out after {}, continuing without", BAL_WAIT_TIMEOUT);
      return Map.of();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      bals.cancel(false);
      return Map.of();
    } catch (final ExecutionException | RuntimeException e) {
      LOG.atDebug()
          .setMessage("BAL download failed ({}), continuing without")
          .addArgument(e::toString)
          .log();
      return Map.of();
    }
  }

  /**
   * Downloads the BALs of {@code headers}. A soft-limited (partial) response is followed by a
   * request for the remaining headers; stops as soon as a request fails or returns nothing, or once
   * {@code abandoned} is true. Each request is a single attempt against one peer, so an unhelpful
   * peer costs a single request per window.
   */
  private Map<Hash, BlockAccessList> fetchBals(
      final List<BlockHeader> headers, final BooleanSupplier abandoned) {
    final Map<Hash, BlockAccessList> fetched = new HashMap<>();
    int offset = 0;
    while (offset < headers.size() && !abandoned.getAsBoolean()) {
      final List<BlockHeader> remaining = headers.subList(offset, headers.size());
      final List<Optional<BlockAccessList>> downloaded = requestBals(remaining);
      if (downloaded.isEmpty()) {
        break;
      }
      final int count = Math.min(remaining.size(), downloaded.size());
      for (int i = 0; i < count; i++) {
        final int index = i;
        downloaded.get(i).ifPresent(bal -> fetched.put(remaining.get(index).getHash(), bal));
      }
      offset += count;
    }
    LOG.atDebug()
        .setMessage("Downloaded {}/{} BAL(s) for blocks {}->{}")
        .addArgument(fetched::size)
        .addArgument(headers::size)
        .addArgument(() -> headers.getFirst().getNumber())
        .addArgument(() -> headers.getLast().getNumber())
        .log();
    return fetched;
  }

  private List<Optional<BlockAccessList>> requestBals(final List<BlockHeader> headers) {
    try {
      final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> result =
          context
              .getEthContext()
              .getPeerTaskExecutor()
              .execute(new GetBlockAccessListsFromPeerTask(headers, true));
      if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
          || result.result().isEmpty()) {
        LOG.atDebug()
            .setMessage("BAL download unsuccessful ({}), continuing without")
            .addArgument(result::responseCode)
            .log();
        return List.of();
      }
      return result.result().get();
    } catch (final RuntimeException e) {
      LOG.atDebug()
          .setMessage("BAL download failed ({}), continuing without")
          .addArgument(e::toString)
          .log();
      return List.of();
    }
  }

  private boolean saveBlock(final Block block, final Optional<BlockAccessList> bal) {
    final Optional<BlockHeader> parent =
        context
            .getProtocolContext()
            .getBlockchain()
            .getBlockHeader(block.getHeader().getParentHash());
    if (parent.isEmpty()) {
      context.halveBatchSize();
      LOG.atDebug()
          .setMessage("Parent block {} not found, while saving block {}, reducing batch size to {}")
          .addArgument(block.getHeader().getParentHash())
          .addArgument(block::toLogString)
          .addArgument(context::getBatchSize)
          .log();
      return false;
    }
    context.saveBlock(block, bal);
    return true;
  }
}
