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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Extends the pending chain to a trusted head: the headers between the head and the highest header
 * already linked to the local chain are downloaded and stored in parallel, and a head on a fork
 * below that point is followed down by hash to the block it shares with the chain.
 */
final class HeaderSync {

  /**
   * A trusted head linked to the chain.
   *
   * @param top the trusted head
   * @param sharedNumber the number of the highest block the head's chain shares with the local or
   *     pending chain
   * @param replacing the headers of the head's chain above the shared block that the download did
   *     not store, ascending
   */
  record Link(BlockHeader top, long sharedNumber, List<BlockHeader> replacing) {}

  /**
   * The progress of the running header download.
   *
   * @param done the headers stored
   * @param total the headers to store
   */
  record Progress(long done, long total) {}

  private final RequestSender sender;
  private final ProtocolSchedule schedule;
  private final PendingChain chain;
  private final Blockchain blockchain;
  private final BadBlockManager badBlocks;
  private final HeaderFetcher fetcher;
  private final ExecutorService headerExecutor;
  private final BackwardSyncConfiguration config;
  private final MetricsSystem metrics;
  // guarded by this
  private CompletableFuture<Link> running;
  private HeaderBackfill backfill;
  private CompletableFuture<?> walkRequest;

  HeaderSync(
      final RequestSender sender,
      final ProtocolSchedule schedule,
      final PendingChain chain,
      final Blockchain blockchain,
      final BadBlockManager badBlocks,
      final HeaderFetcher fetcher,
      final ExecutorService headerExecutor,
      final BackwardSyncConfiguration config,
      final MetricsSystem metrics) {
    this.sender = sender;
    this.schedule = schedule;
    this.chain = chain;
    this.blockchain = blockchain;
    this.badBlocks = badBlocks;
    this.fetcher = fetcher;
    this.headerExecutor = headerExecutor;
    this.config = config;
    this.metrics = metrics;
  }

  /**
   * Downloads the headers that link the trusted head to the chain. Only one download runs at a
   * time. The future fails with a {@link BadHeaderException} when a header of the chain is a known
   * bad block, with a {@link WrongChainException} when the chain forks below the finalized block,
   * and is cancelled by {@link #stop()}.
   */
  CompletableFuture<Link> extendTo(final BlockHeader top) {
    final CompletableFuture<Link> result = new CompletableFuture<>();
    synchronized (this) {
      checkState(running == null || running.isDone(), "a header download is already running");
      running = result;
      backfill = null;
      walkRequest = null;
    }
    guarded(result, () -> start(top, result));
    return result;
  }

  /** Returns the progress of the running header download, else zero of zero. */
  synchronized Progress progress() {
    if (backfill == null || running == null || running.isDone()) {
      return new Progress(0, 0);
    }
    return new Progress(backfill.done(), backfill.total());
  }

  /** Stops the running header download and the walk down a fork. */
  void stop() {
    final CompletableFuture<Link> result;
    final HeaderBackfill stopping;
    final CompletableFuture<?> request;
    synchronized (this) {
      result = running;
      stopping = backfill;
      request = walkRequest;
    }
    if (result != null) {
      result.cancel(false);
    }
    if (stopping != null) {
      stopping.stop();
    }
    if (request != null) {
      request.cancel(false);
    }
  }

  private void start(final BlockHeader top, final CompletableFuture<Link> result) {
    if (badBlocks.isBadBlock(top.getHash())) {
      result.completeExceptionally(new BadHeaderException(top));
      return;
    }
    final BlockHeader floor = floor();
    if (top.getHash().equals(floor.getHash())) {
      result.complete(new Link(top, top.getNumber(), List.of()));
      return;
    }
    if (top.getNumber() <= floor.getNumber()) {
      walk(top, result, top, new ArrayList<>(List.of(top)));
      return;
    }
    // the importer never reads above the linked top, which these writes do not touch
    chain.store(List.of(top));
    final HeaderBackfill created =
        new HeaderBackfill(
            sender,
            schedule,
            this::storeChecked,
            fetcher,
            floor,
            top,
            config.headersPerRequest(),
            config.headerParallelism(),
            metrics,
            "Backward sync",
            "backward_sync");
    synchronized (this) {
      backfill = created;
    }
    created
        .run(headerExecutor)
        .whenComplete(
            (unused, error) ->
                guarded(
                    result,
                    () -> {
                      if (error == null) {
                        result.complete(new Link(top, floor.getNumber(), List.of()));
                        return;
                      }
                      if (cause(error, WrongChainException.class) == null) {
                        result.completeExceptionally(failure(error));
                        return;
                      }
                      final BlockHeader lowest =
                          chain
                              .header(created.lowestStored())
                              .orElseThrow(
                                  () ->
                                      new IllegalStateException(
                                          "header " + created.lowestStored() + " is not stored"));
                      walk(top, result, lowest, new ArrayList<>());
                    }));
  }

  // the header the download goes down to: the linked top while the chain is ahead of the local head
  private BlockHeader floor() {
    final BlockHeader head = blockchain.getChainHeadHeader();
    final OptionalLong linkedTop = chain.linkedTop();
    if (linkedTop.isPresent() && linkedTop.getAsLong() > head.getNumber()) {
      return chain.header(linkedTop.getAsLong()).orElse(head);
    }
    return head;
  }

  private void storeChecked(final List<BlockHeader> headers) {
    for (final BlockHeader header : headers) {
      if (badBlocks.isBadBlock(header.getHash())) {
        throw new BadHeaderException(header);
      }
    }
    chain.store(headers);
  }

  // follows the chain of the head down by hash until it meets the local or the pending chain
  private void walk(
      final BlockHeader top,
      final CompletableFuture<Link> result,
      final BlockHeader from,
      final List<BlockHeader> descending) {
    if (result.isDone()) {
      return;
    }
    final long forkFloor =
        blockchain
            .getFinalized()
            .flatMap(blockchain::getBlockHeader)
            .map(BlockHeader::getNumber)
            .orElse(0L);
    if (from.getNumber() <= forkFloor) {
      result.completeExceptionally(
          new WrongChainException(
              "head "
                  + top.toLogString()
                  + " does not descend from the local chain above the finalized block"));
      return;
    }
    final long lowest = Math.max(forkFloor, from.getNumber() - config.headersPerRequest());
    final CompletableFuture<List<BlockHeader>> request =
        fetcher.chainDown(from, lowest, Priority.HEADERS);
    synchronized (this) {
      walkRequest = request;
    }
    request.whenComplete(
        (headers, error) ->
            guarded(
                result,
                () -> {
                  if (error != null) {
                    result.completeExceptionally(failure(error));
                    return;
                  }
                  // ascending and ending with `from`, which is already handled
                  for (int i = headers.size() - 2; i >= 0; i--) {
                    final BlockHeader header = headers.get(i);
                    if (badBlocks.isBadBlock(header.getHash())) {
                      result.completeExceptionally(new BadHeaderException(header));
                      return;
                    }
                    if (isShared(header)) {
                      final List<BlockHeader> replacing = new ArrayList<>(descending);
                      Collections.reverse(replacing);
                      result.complete(new Link(top, header.getNumber(), replacing));
                      return;
                    }
                    descending.add(header);
                  }
                  walk(top, result, headers.getFirst(), descending);
                }));
  }

  // on the local chain, or on the pending chain above the local head
  private boolean isShared(final BlockHeader header) {
    final long number = header.getNumber();
    if (number <= blockchain.getChainHeadBlockNumber()) {
      return blockchain.getBlockHashByNumber(number).filter(header.getHash()::equals).isPresent();
    }
    final OptionalLong linkedTop = chain.linkedTop();
    return linkedTop.isPresent()
        && number <= linkedTop.getAsLong()
        && chain
            .header(number)
            .filter(pending -> pending.getHash().equals(header.getHash()))
            .isPresent();
  }

  // a bug fails the download rather than leaving it pending
  private static void guarded(final CompletableFuture<Link> result, final Runnable step) {
    try {
      step.run();
    } catch (final RuntimeException e) {
      result.completeExceptionally(failure(e));
    }
  }

  // the failure the download is known by, the one an exception wraps if it is one of ours
  private static Throwable failure(final Throwable error) {
    final Throwable bad = cause(error, BadHeaderException.class);
    if (bad != null) {
      return bad;
    }
    final Throwable wrong = cause(error, WrongChainException.class);
    if (wrong != null) {
      return wrong;
    }
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static Throwable cause(final Throwable error, final Class<? extends Throwable> type) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return current;
      }
    }
    return null;
  }
}
