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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block headers in reverse direction (backward from a pivot block toward genesis).
 * Returns headers in reverse order: {@code [n, n-1, n-2, ...]}.
 *
 * <p>Each invocation of {@link #apply(Long)} downloads a contiguous batch of headers ending at the
 * given start block number and stopping no earlier than {@code trustAnchorBlockNumber}. Headers are
 * fetched from peers using {@link PeerTaskExecutor} and accumulated across multiple peer requests
 * until the full batch is complete.
 *
 * <h2>Retry mechanism</h2>
 *
 * <p>The download is resilient to transient peer failures. When a peer request does not succeed,
 * the behavior depends on the response code:
 *
 * <ul>
 *   <li><b>SUCCESS</b> – the returned headers are appended to the accumulated list. If the batch is
 *       still incomplete, the loop immediately issues another request (possibly to a different
 *       peer) for the remaining headers.
 *   <li><b>INTERNAL_SERVER_ERROR</b> – a fatal, non-retriable condition. The returned future is
 *       failed immediately with a {@link RuntimeException} and no further attempts are made.
 *   <li><b>Any other code</b> (e.g. {@code NO_PEER_AVAILABLE}, {@code PEER_DISCONNECTED}) – a
 *       transient condition. The next attempt is scheduled after {@link #RETRY_DELAY} via {@link
 *       EthScheduler#scheduleFutureTask}. There is no upper bound on the number of retries; the
 *       process continues until either all headers are received or the overall {@link
 *       #timeoutDuration} elapses.
 * </ul>
 *
 * <p>The total time budget is enforced by {@link CompletableFuture#orTimeout} applied in {@link
 * #apply(Long)}. When the deadline is exceeded a {@link java.util.concurrent.TimeoutException} is
 * raised, which is logged at TRACE level before propagating to the caller.
 *
 * <p>Partial progress is preserved across retries: headers already downloaded are stored in a
 * shared list and subsequent requests only ask for the remaining headers, so a retry after a
 * partial success does not re-download headers that were already received.
 */
public class DownloadBackwardHeadersStep
    implements Function<Long, CompletableFuture<List<BlockHeader>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadBackwardHeadersStep.class);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(1);
  private static final AtomicInteger taskId = new AtomicInteger(0);

  private final ProtocolSchedule protocolSchedule;
  private final EthScheduler ethScheduler;
  private final PeerTaskExecutor peerTaskExecutor;
  private final int headerRequestSize;
  private final long trustAnchorBlockNumber;
  private final Duration timeoutDuration;

  /**
   * Creates a new DownloadBackwardHeadersStep.
   *
   * @param protocolSchedule the protocol schedule
   * @param ethContext the eth context
   * @param headerRequestSize the number of headers to request per batch
   * @param trustAnchorBlockNumber the lowest header that we want to download
   * @param timeoutDuration the maximum time to wait including all retries
   */
  public DownloadBackwardHeadersStep(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final int headerRequestSize,
      final long trustAnchorBlockNumber,
      final Duration timeoutDuration) {
    if (headerRequestSize < 1) throw new IllegalArgumentException("headerRequestSize must be >= 1");
    this.protocolSchedule = protocolSchedule;
    this.ethScheduler = ethContext.getScheduler();
    this.peerTaskExecutor = ethContext.getPeerTaskExecutor();
    this.headerRequestSize = headerRequestSize;
    this.trustAnchorBlockNumber = trustAnchorBlockNumber;
    this.timeoutDuration = timeoutDuration;
  }

  /**
   * Initiates the download of a backward batch of block headers.
   *
   * <p>The batch starts at {@code startBlockNumber} and contains at most {@link #headerRequestSize}
   * headers, capped so that the download does not go below {@link #trustAnchorBlockNumber}. The
   * returned future completes with the full list of headers in reverse order once all headers in
   * the batch have been retrieved, or fails if a fatal error occurs or the {@link #timeoutDuration}
   * is exceeded.
   *
   * @param startBlockNumber the block number of the first (highest) header to download
   * @return a future that resolves to the downloaded headers in reverse order (highest to lowest)
   * @throws IllegalStateException if {@code startBlockNumber} is not strictly greater than {@link
   *     #trustAnchorBlockNumber} (i.e. there are no headers left to download)
   */
  @Override
  public CompletableFuture<List<BlockHeader>> apply(final Long startBlockNumber) {
    final long remainingHeaders = startBlockNumber - trustAnchorBlockNumber;
    final int headersToRequest = (int) Math.min(headerRequestSize, remainingHeaders);
    if (headersToRequest < 1) {
      throw new IllegalStateException(
          "Number of headers to request is less than 1:" + headersToRequest);
    }

    final int currentTaskId = taskId.getAndIncrement();
    final List<BlockHeader> downloadedHeaders = new ArrayList<>(headersToRequest);
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    final CompletableFuture<List<BlockHeader>> result =
        ethScheduler
            .scheduleServiceTask(
                () ->
                    downloadAllHeaders(
                        currentTaskId,
                        0,
                        startBlockNumber,
                        headersToRequest,
                        downloadedHeaders,
                        cancelled))
            .orTimeout(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete(
                (unused, throwable) -> {
                  if (throwable instanceof TimeoutException) {
                    cancelled.set(true);
                    LOG.trace(
                        "[{}] Timed out after {} ms while downloading {} backward headers from block {}",
                        currentTaskId,
                        timeoutDuration.toMillis(),
                        headersToRequest,
                        startBlockNumber);
                  }
                });
    // Stop the retry chain if the result itself is cancelled externally,
    // e.g. by AsyncOperationProcessor.abort() when the pipeline is aborted.
    // The whenComplete above is attached to the inner P1 future; cancelling
    // the returned result (P3) does not complete P1, so we need a separate
    // callback on the returned future to catch that case.
    result.whenComplete(
        (unused, throwable) -> {
          if (throwable instanceof CancellationException) {
            cancelled.set(true);
          }
        });
    return result;
  }

  /**
   * Core download loop that accumulates headers for a single batch, retrying on transient peer
   * failures.
   *
   * <p>On each iteration the method asks peers for the remaining headers in the batch (i.e. {@code
   * headersToRequest - downloadedHeaders.size()} headers starting from {@code startBlockNumber -
   * downloadedHeaders.size()}). The loop continues synchronously as long as peer requests succeed,
   * exiting only when the batch is complete.
   *
   * <p>When a peer request fails transiently, the method returns a future that fires after {@link
   * #RETRY_DELAY} and then calls itself recursively via {@link EthScheduler#scheduleServiceTask}.
   * The recursion carries the current {@code downloadedHeaders} list and iteration count forward,
   * so no progress is lost between retries. A fatal {@link
   * PeerTaskExecutorResponseCode#INTERNAL_SERVER_ERROR} skips the retry and fails the future
   * immediately.
   *
   * <p>When consecutive headers from different peer responses are joined, the method verifies that
   * the hash of the first header of the new response matches the parent hash recorded in the last
   * already-downloaded header, ensuring chain continuity.
   *
   * @param currTaskId monotonically increasing identifier used in log messages to correlate entries
   *     belonging to the same logical download task
   * @param prevIterations number of iterations completed before this invocation (0 on the first
   *     call; preserved across recursive retry calls so that log messages show the true attempt
   *     count)
   * @param startBlockNumber block number of the first (highest) header in the batch
   * @param headersToRequest total number of headers the batch must contain
   * @param downloadedHeaders mutable list that accumulates successfully downloaded headers; shared
   *     across the synchronous loop iterations and across recursive retry invocations
   * @param cancelled flag set to {@code true} by the timeout handler in {@link #apply(Long)} when
   *     the overall deadline has elapsed; the loop checks this flag before each iteration and exits
   *     early if it is set, preventing further peer requests after a timeout
   * @return a future that resolves to {@code downloadedHeaders} once all {@code headersToRequest}
   *     headers have been fetched, or fails on a fatal peer error or timeout
   */
  private CompletableFuture<List<BlockHeader>> downloadAllHeaders(
      final int currTaskId,
      final int prevIterations,
      final Long startBlockNumber,
      final int headersToRequest,
      final List<BlockHeader> downloadedHeaders,
      final AtomicBoolean cancelled) {

    int iteration = prevIterations;
    while (!cancelled.get() && downloadedHeaders.size() < headersToRequest) {
      ++iteration;

      final long requestStartBlockNumber = startBlockNumber - downloadedHeaders.size();
      final int requestMaxHeaders = headersToRequest - downloadedHeaders.size();

      LOG.trace(
          "[{}:{}] Backward downloading {} headers starting from block {}",
          currTaskId,
          iteration,
          requestMaxHeaders,
          requestStartBlockNumber);

      final GetHeadersFromPeerTask task =
          new GetHeadersFromPeerTask(
              requestStartBlockNumber,
              requestMaxHeaders,
              0,
              GetHeadersFromPeerTask.Direction.REVERSE,
              protocolSchedule);

      final PeerTaskExecutorResult<List<BlockHeader>> result = peerTaskExecutor.execute(task);

      final PeerTaskExecutorResponseCode responseCode = result.responseCode();

      if (responseCode == PeerTaskExecutorResponseCode.SUCCESS) {
        final List<BlockHeader> resultBlockHeaders = result.result().orElseGet(List::of);
        if (!downloadedHeaders.isEmpty() // check the parent hash and block hash match
            && !resultBlockHeaders.isEmpty()
            && !resultBlockHeaders
                .getFirst()
                .getHash()
                .getBytes()
                .equals(downloadedHeaders.getLast().getParentHash().getBytes())) {
          throw new IllegalStateException("Parent hash of last header does not match first header");
        }
        downloadedHeaders.addAll(resultBlockHeaders);
        LOG.trace(
            "[{}:{}] Successfully received {} headers starting from block {}",
            currTaskId,
            iteration,
            requestMaxHeaders,
            requestStartBlockNumber);
      } else {
        LOG.trace(
            "[{}:{}] Failed with {} to retrieve {} headers starting from block {}",
            currTaskId,
            iteration,
            responseCode,
            requestMaxHeaders,
            requestStartBlockNumber);
        if (responseCode == PeerTaskExecutorResponseCode.INTERNAL_SERVER_ERROR) {
          return CompletableFuture.failedFuture(
              new RuntimeException(
                  "Failed to download "
                      + headersToRequest
                      + " headers starting from block "
                      + startBlockNumber));
        } else {

          LOG.trace("[{}:{}] Waiting for {} before retrying", currTaskId, iteration, RETRY_DELAY);
          final int passIterations = iteration;
          return ethScheduler.scheduleFutureTask(
              () ->
                  ethScheduler.scheduleServiceTask(
                      () ->
                          downloadAllHeaders(
                              currTaskId,
                              passIterations,
                              startBlockNumber,
                              headersToRequest,
                              downloadedHeaders,
                              cancelled)),
              RETRY_DELAY);
        }
      }
    }
    LOG.atTrace()
        .setMessage("[{}:{}] Downloaded {} headers: blocks {} to {}")
        .addArgument(currTaskId)
        .addArgument(iteration)
        .addArgument(downloadedHeaders::size)
        .addArgument(downloadedHeaders.getFirst()::getNumber)
        .addArgument(downloadedHeaders.getLast()::getNumber)
        .log();

    return CompletableFuture.completedFuture(downloadedHeaders);
  }
}
