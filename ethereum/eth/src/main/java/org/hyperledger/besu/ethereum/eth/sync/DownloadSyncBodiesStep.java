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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block bodies for a batch of headers in forward order.
 *
 * <p>Each invocation of {@link #apply(List)} accepts a list of {@link BlockHeader}s whose bodies
 * are needed and returns a future that resolves to the corresponding {@link SyncBlock}s in the same
 * order. Bodies are fetched from peers using {@link PeerTaskExecutor} and accumulated across
 * multiple peer requests until all bodies in the batch have been received.
 *
 * <h2>Retry mechanism</h2>
 *
 * <p>The download is resilient to transient peer failures. When a peer request does not succeed,
 * the behavior depends on the response code:
 *
 * <ul>
 *   <li><b>SUCCESS</b> – the returned bodies are appended to the accumulated list and the
 *       corresponding headers are removed from the remaining set. If the batch is still incomplete,
 *       the loop immediately issues another request (possibly to a different peer) for the
 *       remaining headers.
 *   <li><b>INTERNAL_SERVER_ERROR</b> – a fatal, non-retriable condition. The returned future is
 *       failed immediately with a {@link RuntimeException} and no further attempts are made.
 *   <li><b>Any other code</b> (e.g. {@code NO_PEER_AVAILABLE}, {@code PEER_DISCONNECTED}) – a
 *       transient condition. The next attempt is scheduled after {@link #RETRY_DELAY} via {@link
 *       EthScheduler#scheduleFutureTask}. There is no upper bound on the number of retries; the
 *       process continues until either all bodies are received or the overall {@link
 *       #timeoutDuration} elapses.
 * </ul>
 *
 * <p>The total time budget is enforced by {@link CompletableFuture#orTimeout} applied in {@link
 * #apply(List)}. When the deadline is exceeded a {@link java.util.concurrent.TimeoutException} is
 * raised, which is logged at TRACE level before propagating to the caller.
 *
 * <p>Partial progress is preserved across retries: bodies already downloaded are stored in a shared
 * list and the matching headers are removed from the pending set, so a retry after a partial
 * success does not re-request bodies that were already received.
 */
public class DownloadSyncBodiesStep
    implements Function<List<BlockHeader>, CompletableFuture<List<SyncBlock>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadSyncBodiesStep.class);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(1);
  private static final AtomicInteger taskSequence = new AtomicInteger(0);

  private final ProtocolSchedule protocolSchedule;
  private final EthScheduler ethScheduler;
  private final PeerTaskExecutor peerTaskExecutor;
  private final Duration timeoutDuration;

  /**
   * Creates a new DownloadSyncBodiesStep.
   *
   * @param protocolSchedule the protocol schedule
   * @param ethContext the eth context
   * @param timeoutDuration the maximum time to wait including all retries
   */
  public DownloadSyncBodiesStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final Duration timeoutDuration) {
    this.protocolSchedule = protocolSchedule;
    this.ethScheduler = ethContext.getScheduler();
    this.peerTaskExecutor = ethContext.getPeerTaskExecutor();
    this.timeoutDuration = timeoutDuration;
  }

  /**
   * Initiates the download of block bodies for the given headers.
   *
   * <p>The returned future completes with one {@link SyncBlock} per header, in the same order as
   * the input list, once all bodies have been retrieved, or fails if a fatal error occurs or the
   * {@link #timeoutDuration} is exceeded.
   *
   * @param blockHeaders the headers whose bodies must be downloaded
   * @return a future that resolves to the downloaded {@link SyncBlock}s in the same order as the
   *     input headers
   */
  @Override
  public CompletableFuture<List<SyncBlock>> apply(final List<BlockHeader> blockHeaders) {
    final int currTaskId = taskSequence.incrementAndGet();
    final List<SyncBlock> syncBlocks = new ArrayList<>(blockHeaders.size());
    final List<BlockHeader> remainingHeaders = new ArrayList<>(blockHeaders);
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    return ethScheduler
        .scheduleServiceTask(
            () -> getSyncBodies(currTaskId, 0, remainingHeaders, syncBlocks, cancelled))
        .orTimeout(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS)
        .whenComplete(
            (unused, throwable) -> {
              if (throwable instanceof TimeoutException) {
                cancelled.set(true);
                LOG.trace(
                    "[{}] Timed out after {} ms while downloading bodies for {} blocks",
                    currTaskId,
                    timeoutDuration.toMillis(),
                    blockHeaders.size());
              }
            });
  }

  /**
   * Core download loop that accumulates bodies for a single batch, retrying on transient peer
   * failures.
   *
   * <p>On each iteration the method issues a {@link GetSyncBlockBodiesFromPeerTask} for all headers
   * still present in {@code remainingHeaders}. When bodies are returned successfully, they are
   * appended to {@code downloadedSyncBlocks} and the corresponding entries are removed from the
   * front of {@code remainingHeaders}. The loop continues synchronously as long as peer requests
   * succeed, exiting only when {@code remainingHeaders} is empty.
   *
   * <p>When a peer request fails transiently, the method returns a future that fires after {@link
   * #RETRY_DELAY} and then calls itself recursively via {@link EthScheduler#scheduleServiceTask}.
   * The recursion carries the current {@code remainingHeaders} and {@code downloadedSyncBlocks}
   * lists forward, so no progress is lost between retries. A fatal {@link
   * PeerTaskExecutorResponseCode#INTERNAL_SERVER_ERROR} skips the retry and fails the future
   * immediately.
   *
   * @param currTaskId monotonically increasing identifier used in log messages to correlate entries
   *     belonging to the same logical download task
   * @param prevIterations number of iterations completed before this invocation (0 on the first
   *     call; preserved across recursive retry calls so that log messages show the true attempt
   *     count)
   * @param remainingHeaders mutable list of headers whose bodies have not yet been received;
   *     entries are removed from the front as bodies arrive
   * @param downloadedSyncBlocks mutable list that accumulates successfully downloaded blocks;
   *     shared across the synchronous loop iterations and across recursive retry invocations
   * @param cancelled flag set to {@code true} by the timeout handler in {@link #apply(List)} when
   *     the overall deadline has elapsed; the loop checks this flag before each iteration and exits
   *     early if it is set, preventing further peer requests after a timeout
   * @return a future that resolves to {@code downloadedSyncBlocks} once all bodies have been
   *     fetched, or fails on a fatal peer error or timeout
   */
  private CompletableFuture<List<SyncBlock>> getSyncBodies(
      final int currTaskId,
      final int prevIterations,
      final List<BlockHeader> remainingHeaders,
      final List<SyncBlock> downloadedSyncBlocks,
      final AtomicBoolean cancelled) {

    final int headerCount = remainingHeaders.size();
    int iteration = prevIterations;
    while (!cancelled.get() && !remainingHeaders.isEmpty()) {
      ++iteration;

      LOG.atTrace()
          .setMessage("[{}:{}] Requesting {} block bodies")
          .addArgument(currTaskId)
          .addArgument(iteration)
          .addArgument(remainingHeaders::size)
          .log();

      final GetSyncBlockBodiesFromPeerTask task =
          new GetSyncBlockBodiesFromPeerTask(remainingHeaders, protocolSchedule);
      final PeerTaskExecutorResult<List<SyncBlock>> result = peerTaskExecutor.execute(task);
      final PeerTaskExecutorResponseCode responseCode = result.responseCode();

      if (responseCode == PeerTaskExecutorResponseCode.SUCCESS) {
        final List<SyncBlock> received =
            result
                .result()
                .filter(blocks -> !blocks.isEmpty())
                .orElseThrow(() -> new IllegalStateException("Empty response not expected"));
        LOG.atTrace()
            .setMessage("[{}:{}] Received {} block bodies (requested {})")
            .addArgument(currTaskId)
            .addArgument(iteration)
            .addArgument(received::size)
            .addArgument(remainingHeaders::size)
            .log();

        downloadedSyncBlocks.addAll(received);
        remainingHeaders.subList(0, received.size()).clear();
      } else {
        LOG.atTrace()
            .setMessage("[{}:{}] Failed with {} to retrieve {} block bodies")
            .addArgument(currTaskId)
            .addArgument(iteration)
            .addArgument(responseCode)
            .addArgument(remainingHeaders::size)
            .log();

        if (responseCode == PeerTaskExecutorResponseCode.INTERNAL_SERVER_ERROR) {
          return CompletableFuture.failedFuture(
              new RuntimeException("Failed to download bodies for " + headerCount + " blocks"));
        }

        LOG.trace("[{}:{}] Waiting for {} before retrying", currTaskId, iteration, RETRY_DELAY);
        final int passIterations = iteration;
        return ethScheduler.scheduleFutureTask(
            () ->
                ethScheduler.scheduleServiceTask(
                    () ->
                        getSyncBodies(
                            currTaskId,
                            passIterations,
                            remainingHeaders,
                            downloadedSyncBlocks,
                            cancelled)),
            RETRY_DELAY);
      }
    }

    LOG.atTrace()
        .setMessage("[{}:{}] Downloaded {} block bodies")
        .addArgument(currTaskId)
        .addArgument(iteration)
        .addArgument(downloadedSyncBlocks::size)
        .log();

    return CompletableFuture.completedFuture(downloadedSyncBlocks);
  }
}
