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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.NO_PEER;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.STOPPED;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the bodies of a batch and the block access lists of the blocks that have one, which
 * their execution uses, in requests of a few blocks that the request scheduler spreads over the
 * peers. Only the bodies are required: the block access lists are waited for at most {@link
 * #OPTIONAL_DOWNLOAD_TIMEOUT}, and a block without one is executed all the same.
 */
class DownloadBlockDataStep implements Function<List<BlockHeader>, CompletableFuture<BlockBatch>> {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadBlockDataStep.class);

  // how long a batch waits, once it has its bodies, for what it can be imported without
  private static final Duration OPTIONAL_DOWNLOAD_TIMEOUT = Duration.ofSeconds(10);

  // the requests at most for the block access lists of a few blocks
  private static final int OPTIONAL_ATTEMPTS = 4;

  private static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);

  private final RequestSender sender;
  private final EthScheduler scheduler;
  private final ProtocolSchedule protocolSchedule;
  private final int requestSize;
  private final Duration bodiesTimeout;
  private final long firstBlock;

  /**
   * @param requestSize the blocks of one request
   * @param bodiesTimeout how long the bodies of a batch may take before the sync fails
   * @param firstBlock the first block of the import, whose batch is asked ahead of the others
   */
  DownloadBlockDataStep(
      final RequestSender sender,
      final EthScheduler scheduler,
      final ProtocolSchedule protocolSchedule,
      final int requestSize,
      final Duration bodiesTimeout,
      final long firstBlock) {
    this.sender = sender;
    this.scheduler = scheduler;
    this.protocolSchedule = protocolSchedule;
    this.requestSize = requestSize;
    this.bodiesTimeout = bodiesTimeout;
    this.firstBlock = firstBlock;
  }

  @Override
  public CompletableFuture<BlockBatch> apply(final List<BlockHeader> headers) {
    return new BatchDownload(headers).start();
  }

  /** The downloads of one batch. */
  private final class BatchDownload {

    private final List<BlockHeader> headers;
    // the import waits for the first batch, the others are downloaded ahead of it
    private final Priority priority;
    private final Map<Hash, BlockAccessList> bals = new ConcurrentHashMap<>();
    // the requests not answered yet, cancelled when the batch stops waiting
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();
    private final long startMillis = System.currentTimeMillis();
    private volatile boolean abandoned;

    BatchDownload(final List<BlockHeader> headers) {
      this.headers = headers;
      this.priority =
          headers.getFirst().getNumber() == firstBlock ? Priority.HEADERS : Priority.BODIES;
    }

    CompletableFuture<BlockBatch> start() {
      // queued first, as the batch cannot be imported without them
      final CompletableFuture<List<SyncBlock>> bodies = downloadBodies();
      // the block access lists only need the headers, so they come next to the bodies
      final List<BlockHeader> withBal =
          headers.stream().filter(header -> header.getBalHash().isPresent()).toList();
      final CompletableFuture<Void> balsDone =
          inRequests(withBal, chunk -> requestBals(chunk, new HashSet<>(), OPTIONAL_ATTEMPTS));
      bodies.whenComplete(
          (blocks, error) -> {
            if (error != null) {
              abandon();
            }
          });
      return bodies.thenCompose(
          blocks ->
              waitAtMost(balsDone).thenApplyAsync(ignored -> batch(blocks), sender.workers()));
    }

    private CompletableFuture<List<SyncBlock>> downloadBodies() {
      final List<CompletableFuture<List<SyncBlock>>> chunks =
          Lists.partition(headers, requestSize).stream().map(this::requestBodies).toList();
      final CompletableFuture<List<SyncBlock>> bodies =
          CompletableFuture.allOf(chunks.toArray(CompletableFuture[]::new))
              .thenApply(
                  ignored -> chunks.stream().flatMap(chunk -> chunk.join().stream()).toList());
      scheduler.failAfterTimeout(bodies, bodiesTimeout);
      return bodies;
    }

    // a peer may send the first bodies only, the rest is asked again
    private CompletableFuture<List<SyncBlock>> requestBodies(final List<BlockHeader> chunk) {
      return track(
              sender.submitWithRetries(
                  new PeerTaskRequest<>(
                      () -> new GetSyncBlockBodiesFromPeerTask(chunk, protocolSchedule),
                      priority,
                      "bodies")))
          .thenCompose(
              response -> {
                final List<SyncBlock> received = response.value();
                if (received.size() >= chunk.size()) {
                  return CompletableFuture.completedFuture(received);
                }
                return requestBodies(chunk.subList(received.size(), chunk.size()))
                    .thenApply(rest -> Stream.concat(received.stream(), rest.stream()).toList());
              });
    }

    // asks again for the lists a response left out, never a peer that failed or lacked one
    private CompletableFuture<Void> requestBals(
        final List<BlockHeader> chunk, final Set<EthPeer> excluded, final int attempts) {
      return track(
              sender.submit(
                  new PeerTaskRequest<>(
                      () -> new GetBlockAccessListsFromPeerTask(chunk),
                      priority,
                      "block_access_lists"),
                  excluded))
          .handle(
              (response, error) -> {
                final boolean retry;
                if (error == null) {
                  storeBals(chunk, response, excluded);
                  retry = true;
                } else {
                  final Optional<RequestFailedException> failure =
                      RequestFailedException.from(error);
                  failure.flatMap(RequestFailedException::peer).ifPresent(excluded::add);
                  // no peer left to ask, or the batch stopped waiting
                  retry =
                      failure
                          .map(f -> f.reason() != NO_PEER && f.reason() != STOPPED)
                          .orElse(false);
                }
                final List<BlockHeader> missing =
                    chunk.stream().filter(header -> !bals.containsKey(header.getHash())).toList();
                return missing.isEmpty() || attempts <= 1 || abandoned || !retry
                    ? DONE
                    : requestBals(missing, excluded, attempts - 1);
              })
          .thenCompose(Function.identity());
    }

    private void storeBals(
        final List<BlockHeader> chunk,
        final RequestSender.Response<List<Optional<BlockAccessList>>> response,
        final Set<EthPeer> excluded) {
      final List<Optional<BlockAccessList>> lists = response.value();
      for (int i = 0; i < lists.size(); i++) {
        final Optional<BlockAccessList> list = lists.get(i);
        if (list.isPresent()) {
          bals.put(chunk.get(i).getHash(), list.get());
        } else {
          // a peer without a list will not have it a moment later
          excluded.add(response.peer());
        }
      }
    }

    private <T> CompletableFuture<Void> inRequests(
        final List<T> items, final Function<List<T>, CompletableFuture<Void>> request) {
      return CompletableFuture.allOf(
          Lists.partition(items, requestSize).stream()
              .map(request)
              .toArray(CompletableFuture[]::new));
    }

    // completes once the downloads ended or the batch stopped waiting for them
    private CompletableFuture<Void> waitAtMost(final CompletableFuture<Void> downloads) {
      final CompletableFuture<Void> wait = new CompletableFuture<>();
      downloads.whenComplete((ignored, error) -> wait.complete(null));
      scheduler.failAfterTimeout(wait, OPTIONAL_DOWNLOAD_TIMEOUT);
      return wait.handle(
          (ignored, timeout) -> {
            abandon();
            return null;
          });
    }

    private <T> CompletableFuture<T> track(final CompletableFuture<T> request) {
      pending.add(request);
      request.whenComplete((response, error) -> pending.remove(request));
      // added before the check, so that a concurrent abandon either cancels it or is seen here
      if (abandoned) {
        request.cancel(false);
      }
      return request;
    }

    private void abandon() {
      abandoned = true;
      pending.forEach(request -> request.cancel(false));
    }

    private BlockBatch batch(final List<SyncBlock> blocks) {
      LOG.atDebug()
          .setMessage("Backward sync downloaded #{} to #{} in {} ms: {} block access lists")
          .addArgument(() -> headers.getFirst().getNumber())
          .addArgument(() -> headers.getLast().getNumber())
          .addArgument(() -> System.currentTimeMillis() - startMillis)
          .addArgument(bals::size)
          .log();
      return new BlockBatch(decodeBlocks(blocks), Map.copyOf(bals));
    }
  }

  private static List<Block> decodeBlocks(final List<SyncBlock> syncBlocks) {
    return syncBlocks.stream()
        .map(block -> new Block(block.getHeader(), block.getBody().getBodySupplier().get()))
        .toList();
  }
}
