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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads and stores the headers between a trusted header and the floor of the chain (genesis or
 * a checkpoint). Segments are fetched by number in parallel, then checked top down: the highest
 * header of each segment must be the parent of the lowest one above. A segment from another chain
 * is fetched again by hash, which can only return the right one.
 */
public final class HeaderBackfill {

  private static final Logger LOG = LoggerFactory.getLogger(HeaderBackfill.class);

  private final RequestSender sender;
  private final ProtocolSchedule protocolSchedule;
  private final Consumer<List<BlockHeader>> store;
  private final HeaderFetcher fetcher;
  private final BlockHeader floor;
  private final BlockHeader top;
  // the blocks whose headers are downloaded, from the highest down
  private final long highestToStore;
  private final long lowestToStore;
  private final int headersPerRequest;
  private final int parallelism;
  private final MetricsSystem metricsSystem;
  // the requests by number in flight, which a stop cancels as the scheduler retries them until then
  private final Set<CompletableFuture<?>> requests = ConcurrentHashMap.newKeySet();
  // the top, then the lowest header stored
  private volatile BlockHeader lowestTrusted;
  private volatile boolean stopped;
  private volatile Instant started = Instant.now();
  // guarded by this
  private boolean ran;
  private Pipeline<Segment> pipeline;

  /**
   * Creates a backfill.
   *
   * @param store writes verified headers to the blockchain
   * @param floor the highest header below which nothing is needed, already stored and trusted
   * @param top the trusted header the download starts below, already stored
   */
  public HeaderBackfill(
      final RequestSender sender,
      final ProtocolSchedule protocolSchedule,
      final Consumer<List<BlockHeader>> store,
      final HeaderFetcher fetcher,
      final BlockHeader floor,
      final BlockHeader top,
      final int headersPerRequest,
      final int parallelism,
      final MetricsSystem metricsSystem) {
    checkArgument(headersPerRequest > 0, "headersPerRequest must be positive");
    checkArgument(parallelism > 0, "parallelism must be positive");
    checkArgument(top.getNumber() >= floor.getNumber(), "the top is below the floor");
    this.sender = sender;
    this.protocolSchedule = protocolSchedule;
    this.store = store;
    this.fetcher = fetcher;
    this.floor = floor;
    this.top = top;
    this.highestToStore = top.getNumber() - 1;
    this.lowestToStore = floor.getNumber() + 1;
    this.headersPerRequest = headersPerRequest;
    this.parallelism = parallelism;
    this.metricsSystem = metricsSystem;
    this.lowestTrusted = top;
  }

  /** Returns the number of the lowest header stored so far, the top before any. */
  public long lowestStored() {
    return lowestTrusted.getNumber();
  }

  /** Returns the number of headers stored so far. */
  public long done() {
    return top.getNumber() - lowestStored();
  }

  /** Returns the time since the download started. */
  public Duration elapsed() {
    return Duration.between(started, Instant.now());
  }

  /** Returns the number of headers to store, from below the top down to above the floor. */
  public long total() {
    return Math.max(0, highestToStore - lowestToStore + 1);
  }

  /** Downloads every header in between and checks the chain reaches the floor; runs once. */
  public CompletableFuture<Void> run(final ExecutorService executor) {
    return start(executor)
        .thenRun(
            () -> {
              if (!reachesFloor()) {
                throw new WrongChainException(
                    "the chain of the trusted head does not lead to block "
                        + floor.getNumber()
                        + " "
                        + floor.getHash()
                        + ", the consensus client follows another chain");
              }
              LOG.info(
                  "SNAP2 headers complete in {} | down to {}",
                  ProgressFormat.duration(elapsed()),
                  ProgressFormat.block(lowestToStore));
            });
  }

  // the lowest trusted header is the floor itself, or its child
  private boolean reachesFloor() {
    if (lowestTrusted.getNumber() == floor.getNumber()) {
      return lowestTrusted.getHash().equals(floor.getHash());
    }
    return lowestTrusted.getParentHash().equals(floor.getHash());
  }

  // under the lock, so that a stop comes first or finds the pipeline started
  private synchronized CompletableFuture<Void> start(final ExecutorService executor) {
    checkState(!ran, "a header download runs once");
    ran = true;
    started = Instant.now();
    if (stopped) {
      return CompletableFuture.failedFuture(cancellation());
    }
    if (total() == 0) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info(
        "SNAP2 headers started | {} headers, {} down to {}",
        ProgressFormat.compact(total()),
        ProgressFormat.block(highestToStore),
        ProgressFormat.block(lowestToStore));
    final Pipeline<Segment> created = createPipeline();
    final CompletableFuture<Void> finished = created.start(executor);
    // kept once started, as a pipeline that failed to start cannot be aborted
    pipeline = created;
    return finished;
  }

  private Pipeline<Segment> createPipeline() {
    return PipelineBuilder.createPipelineFrom(
            "headerSegments",
            new Segments(highestToStore, lowestToStore, headersPerRequest),
            parallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "snap2_header_pipeline_processed_total",
                "Header segments processed by the SNAP2 sync",
                "step",
                "action"),
            false,
            "snap2-headers")
        .thenProcessAsyncOrdered("downloadHeaders", this::download, parallelism)
        .andFinishWith("storeHeaders", this::verifyAndStore);
  }

  /** Stops the download; no header is stored afterwards. */
  public void stop() {
    final Pipeline<Segment> running;
    synchronized (this) {
      stopped = true;
      running = pipeline;
    }
    // aborted outside the lock: the pipeline runs the callbacks of its future under its own lock
    if (running != null) {
      running.abort();
    }
    // the pipeline cancels the downloads, not the requests they wait on
    requests.forEach(request -> request.cancel(false));
  }

  // best effort: the headers of the segment, highest first and linked, or none if a request fails
  private CompletableFuture<Fetched> download(final Segment segment) {
    return byNumber(segment)
        .thenCompose(headers -> completeByHash(headers, segment.low()))
        .thenApply(headers -> new Fetched(segment, headers))
        .exceptionally(
            error -> {
              LOG.debug(
                  "Header segment {} to {} failed: {}",
                  segment.high(),
                  segment.low(),
                  error.getMessage());
              return Fetched.failed(segment);
            });
  }

  // the headers of the segment by number
  private CompletableFuture<List<BlockHeader>> byNumber(final Segment segment) {
    return track(
            sender.submitWithRetries(
                new PeerTaskRequest<>(
                    () ->
                        new GetHeadersFromPeerTask(
                            segment.high(),
                            segment.size(),
                            0,
                            Direction.REVERSE,
                            0,
                            protocolSchedule),
                    Priority.HEADERS,
                    "headers",
                    segment.high())))
        .thenApply(RequestSender.Response::value);
  }

  // added before the check, so that a concurrent stop either cancels it or is seen here
  private <T> CompletableFuture<T> track(final CompletableFuture<T> request) {
    requests.add(request);
    request.whenComplete((result, error) -> requests.remove(request));
    if (stopped) {
      request.cancel(false);
    }
    return request;
  }

  // a short answer goes on down to low by hash from its last header, which keeps it linked
  private CompletableFuture<List<BlockHeader>> completeByHash(
      final List<BlockHeader> headers, final long low) {
    final BlockHeader last = headers.getLast();
    if (last.getNumber() == low) {
      return CompletableFuture.completedFuture(headers);
    }
    // the chain down starts with the last header, already in the answer
    return descendingFrom(last, low)
        .thenApply(below -> Stream.concat(headers.stream(), below.stream().skip(1)).toList());
  }

  // the chain from this header down to low, highest first
  private CompletableFuture<List<BlockHeader>> descendingFrom(
      final BlockHeader header, final long low) {
    return fetcher.chainDown(header, low, Priority.HEADERS).thenApply(List::reversed);
  }

  // the lock only keeps a stopped download from storing, the refetch runs outside it
  private void verifyAndStore(final Fetched fetched) {
    final List<BlockHeader> headers = trusted(fetched);
    synchronized (this) {
      throwIfStopped();
      store.accept(headers);
    }
    lowestTrusted = headers.getLast();
  }

  // the fetched headers when they continue the trusted chain, else that chain fetched by hash
  private List<BlockHeader> trusted(final Fetched fetched) {
    final Hash expected = lowestTrusted.getParentHash();
    if (fetched.startsWith(expected)) {
      return fetched.headers();
    }
    // no refetch once stopped
    throwIfStopped();
    LOG.debug(
        "Header segment {} to {} is missing or on another chain, fetching it by hash",
        fetched.segment().high(),
        fetched.segment().low());
    return chainByHash(expected, fetched.segment().low());
  }

  private void throwIfStopped() {
    if (stopped) {
      throw cancellation();
    }
  }

  private static CancellationException cancellation() {
    return new CancellationException("header download stopped");
  }

  // the trusted chain from the header with this hash down to low, highest first
  private List<BlockHeader> chainByHash(final Hash hash, final long low) {
    final CompletableFuture<List<BlockHeader>> chain =
        fetcher.byHash(hash, Priority.HEADERS).thenCompose(header -> descendingFrom(header, low));
    // get rather than join, which the interrupt of a stop does not end
    try {
      return chain.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw cancellation();
    } catch (final ExecutionException e) {
      throw new CompletionException(e.getCause());
    }
  }

  /**
   * Headers fetched for a segment.
   *
   * @param segment the segment
   * @param headers the headers, highest first, each the parent of the one before, or none
   */
  private record Fetched(Segment segment, List<BlockHeader> headers) {

    // none, as the download of the segment failed
    private static Fetched failed(final Segment segment) {
      return new Fetched(segment, List.of());
    }

    // the highest header has this hash
    private boolean startsWith(final Hash hash) {
      return !headers.isEmpty() && headers.getFirst().getHash().equals(hash);
    }
  }

  /**
   * A segment of block numbers, both included.
   *
   * @param high the highest block
   * @param low the lowest block
   */
  private record Segment(long high, long low) {

    private int size() {
      return (int) (high - low + 1);
    }
  }

  private static final class Segments implements Iterator<Segment> {
    private final long low;
    private final int size;
    private long next;

    private Segments(final long high, final long low, final int size) {
      this.next = high;
      this.low = low;
      this.size = size;
    }

    @Override
    public boolean hasNext() {
      return next >= low;
    }

    @Override
    public Segment next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Segment segment = new Segment(next, Math.max(low, next - size + 1));
      next = segment.low() - 1;
      return segment;
    }
  }
}
