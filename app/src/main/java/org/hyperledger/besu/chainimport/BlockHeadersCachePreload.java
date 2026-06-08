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
package org.hyperledger.besu.chainimport;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriority;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriorityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for preloading block headers from the blockchain into cache.
 *
 * <p>This class preloads a specified number of recent block headers by processing them in chunks to
 * avoid memory exhaustion while maximizing concurrency. It uses a semaphore-based backpressure
 * mechanism to control resource usage.
 */
public class BlockHeadersCachePreload {

  private static final Logger LOG = LoggerFactory.getLogger(BlockHeadersCachePreload.class);
  private final Blockchain blockchain;
  private final EthScheduler ethScheduler;
  private final int numberOfBlockHeadersToCache;

  /**
   * Creates a new block headers cache preloader.
   *
   * @param blockchain the blockchain to retrieve block headers from disk
   * @param ethScheduler the scheduler for executing preload tasks
   * @param numberOfBlockHeadersToCache the number of recent block headers to cache
   */
  public BlockHeadersCachePreload(
      final Blockchain blockchain,
      final EthScheduler ethScheduler,
      final int numberOfBlockHeadersToCache) {
    this.blockchain = blockchain;
    this.ethScheduler = ethScheduler;
    this.numberOfBlockHeadersToCache = numberOfBlockHeadersToCache;
  }

  /**
   * Preloads block headers into the cache asynchronously.
   *
   * @return a CompletableFuture that completes when all block headers have been successfully loaded
   *     into cache, or completes exceptionally if the process fails. The future returns null on
   *     successful completion.
   */
  public CompletableFuture<Void> preloadCache() {
    final BlockHeader chainHead = blockchain.getChainHeadHeader();
    final long chainHeadNumber = chainHead.getNumber();
    final long lastBlockToCache = Math.max(0, chainHeadNumber - numberOfBlockHeadersToCache);
    final int maxConcurrent = Runtime.getRuntime().availableProcessors() * 2;
    final int chunkSize = maxConcurrent * 10; // Process in reasonable chunks

    return processChunksSequentially(
        chainHeadNumber - 1, lastBlockToCache, chunkSize, maxConcurrent);
  }

  /**
   * Processes block headers in sequential chunks to maintain constant memory usage.
   *
   * @param startBlock the highest block number to process in this and subsequent chunks
   * @param endBlock the lowest block number to process (exclusive, will not be processed)
   * @param chunkSize the maximum number of blocks to process in each chunk
   * @param maxConcurrent the maximum number of concurrent operations within each chunk
   * @return a CompletableFuture that completes when all chunks have been processed
   */
  private CompletableFuture<Void> processChunksSequentially(
      final long startBlock, final long endBlock, final int chunkSize, final int maxConcurrent) {
    if (startBlock <= endBlock) {
      return CompletableFuture.completedFuture(null);
    }

    long chunkEnd = Math.max(endBlock + 1, startBlock - chunkSize + 1);

    return processChunk(startBlock, chunkEnd, maxConcurrent)
        .thenCompose(
            v -> processChunksSequentially(chunkEnd - 1, endBlock, chunkSize, maxConcurrent));
  }

  /**
   * Processes a single chunk of block headers with controlled concurrency.
   *
   * @param startBlock the highest block number to process (inclusive)
   * @param endBlock the lowest block number to process (inclusive)
   * @param maxConcurrent the maximum number of concurrent block header retrievals
   * @return a CompletableFuture that completes when all block headers in the chunk have been
   *     processed (successfully or with failures logged)
   */
  private CompletableFuture<Void> processChunk(
      final long startBlock, final long endBlock, final int maxConcurrent) {
    final Semaphore semaphore = new Semaphore(maxConcurrent);
    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (long blockNumber = startBlock; blockNumber >= endBlock; blockNumber--) {
      final long currentBlockNumber = blockNumber;

      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        futures.forEach(future -> future.cancel(false));
        break;
      }

      CompletableFuture<Void> future =
          ethScheduler
              .scheduleServiceTask(
                  () -> {
                    try {
                      StorageReadPriorityContext.withPriority(
                          StorageReadPriority.LOW,
                          "block-header-preload",
                          () -> blockchain.getBlockHeader(currentBlockNumber));
                    } catch (Exception e) {
                      LOG.warn(
                          "Failed to preload block header {}: {}",
                          currentBlockNumber,
                          e.getMessage());
                    } finally {
                      semaphore.release();
                    }
                  })
              .orTimeout(30, TimeUnit.SECONDS)
              .exceptionally(
                  throwable -> {
                    if (throwable instanceof TimeoutException) {
                      LOG.warn("Timeout preloading block header {}", currentBlockNumber);
                    }
                    return null;
                  });

      futures.add(future);
    }

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
  }
}
