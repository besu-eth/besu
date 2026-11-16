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
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BlockDataFetcher.ImportData;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.HeaderSync.Link;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.HeaderSync.Progress;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * The backward sync loop: given the head and the finalized block the consensus client names, it
 * links the pending chain of headers to the local chain, keeps the data of the blocks ahead of the
 * import cursor downloaded, and imports the blocks one after the other. Every method runs on the
 * single loop thread, except the two gauge readers.
 */
final class BackwardSync {
  private static final Logger LOG = LoggerFactory.getLogger(BackwardSync.class);
  private static final double BYTES_PER_MIB = 1024 * 1024;

  /** Receives what happens to the sync, on the loop thread. */
  interface Listener {
    /** A head was named: there is work to do, or still. */
    void syncing();

    /** The named head is imported. */
    void completed();

    /** The sync stopped on this head until the consensus client names one again. */
    void failed(BackwardSyncException e);

    /**
     * A block of the chain is bad.
     *
     * @param bad the header of the bad block
     * @param blocks the descendants whose body is known, ordered towards the head
     * @param headers the descendants only known by header, ordered towards the head
     */
    void badChain(BlockHeader bad, List<Block> blocks, List<BlockHeader> headers);
  }

  private final ProtocolContext context;
  private final MutableBlockchain blockchain;
  private final PendingChain chain;
  private final TrustedPayloads payloads;
  private final HeaderSync headerSync;
  private final HeaderFetcher headerFetcher;
  private final BlockDataFetcher fetcher;
  private final ChainImporter importer;
  private final ScheduledExecutorService loop;
  private final LongSupplier nanoTime;
  private final BackwardSyncConfiguration config;
  private final Listener listener;

  private boolean ready;
  private boolean failed;
  private Hash targetHash;
  private BlockHeader targetHeader;
  private Hash finalizedHash;
  // -1 until the finalized block is known
  private long finalizedNumber = -1;
  private CompletableFuture<Link> headerJob;
  private CompletableFuture<BlockHeader> topLookup;
  private Hash lookupWarnedFor;
  // the number of the next block to import
  private long cursor;
  private boolean importQueued;
  private int importAttempts;
  private Hash lastUnavailableWorldState;

  private boolean sessionActive;
  private long sessionStartNanos;
  private long sessionStartHead;
  private long sessionImported;
  private long sessionFromBal;
  private long lastLogNanos;
  private long lastLogImported;

  // read by the metric gauges from other threads
  private volatile long targetBlockNumber;

  BackwardSync(
      final ProtocolContext context,
      final PendingChain chain,
      final TrustedPayloads payloads,
      final HeaderSync headerSync,
      final HeaderFetcher headerFetcher,
      final BlockDataFetcher fetcher,
      final ChainImporter importer,
      final ScheduledExecutorService loop,
      final LongSupplier nanoTime,
      final BackwardSyncConfiguration config,
      final BackwardSyncMetrics metrics,
      final Listener listener) {
    this.context = context;
    this.blockchain = context.getBlockchain();
    this.chain = chain;
    this.payloads = payloads;
    this.headerSync = headerSync;
    this.headerFetcher = headerFetcher;
    this.fetcher = fetcher;
    this.importer = importer;
    this.loop = loop;
    this.nanoTime = nanoTime;
    this.config = config;
    this.listener = listener;
  }

  /**
   * Keeps the pending chain of a previous run when it continues the local chain, drops it else.
   * Nothing is imported until a head is named.
   */
  void initialize() {
    final long head = blockchain.getChainHeadBlockNumber();
    final OptionalLong linkedTop = chain.linkedTop();
    if (linkedTop.isPresent()) {
      // the headers above were being downloaded when the node stopped
      chain.removeAbove(linkedTop.getAsLong());
      final boolean continuesLocalChain =
          chain
              .header(head + 1)
              .map(header -> header.getParentHash().equals(blockchain.getChainHeadHash()))
              .orElse(false);
      if (continuesLocalChain) {
        chain.removeUpTo(head);
      } else {
        chain.clear();
      }
    } else {
      chain.clear();
    }
    cursor = head + 1;
    final long intervalMillis = config.progressInterval().toMillis();
    loop.scheduleWithFixedDelay(
        this::logProgress, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  void onReadiness(final boolean isReady) {
    if (isReady) {
      ready = true;
      resume();
    } else {
      ready = false;
      cancelWork();
    }
  }

  void onNewHead(final Hash head, final Hash finalized) {
    if (!finalized.equals(Hash.ZERO)) {
      finalizedHash = finalized;
      resolveFinalized();
    }
    if (failed) {
      failed = false;
      importAttempts = 0;
    }
    startSession();
    listener.syncing();
    if (!head.equals(targetHash)) {
      targetHash = head;
      importAttempts = 0;
      setTargetHeader(
          blockchain
              .getBlockHeader(head)
              .or(() -> chain.header(head))
              .or(() -> payloads.block(head).map(Block::getHeader))
              .orElse(null));
    }
    resume();
  }

  void onNewPayload(final Block block, final Optional<BlockAccessList> blockAccessList) {
    payloads.add(block, blockAccessList);
    onNewHead(block.getHash(), Hash.ZERO);
  }

  void stop() {
    ready = false;
    cancelWork();
  }

  /** Returns the headers waiting to be imported, 0 when idle. Callable from any thread. */
  long pendingHeaders() {
    final OptionalLong linkedTop = chain.linkedTop();
    return linkedTop.isPresent()
        ? Math.max(0, linkedTop.getAsLong() - blockchain.getChainHeadBlockNumber())
        : 0;
  }

  /** Returns the number of the block the sync aims at, 0 when idle. Callable from any thread. */
  long targetBlock() {
    return targetBlockNumber;
  }

  void scheduleImport() {
    if (!importQueued) {
      importQueued = true;
      loop.execute(this::importNext);
    }
  }

  private void cancelWork() {
    headerSync.stop();
    headerJob = null;
    fetcher.reset();
    if (topLookup != null) {
      topLookup.cancel(false);
      topLookup = null;
    }
  }

  private void startSession() {
    if (sessionActive) {
      return;
    }
    sessionActive = true;
    sessionStartNanos = nanoTime.getAsLong();
    sessionStartHead = blockchain.getChainHeadBlockNumber();
    sessionImported = 0;
    sessionFromBal = 0;
    lastLogNanos = sessionStartNanos;
    lastLogImported = 0;
  }

  private void setTargetHeader(final BlockHeader header) {
    targetHeader = header;
    targetBlockNumber = header == null ? 0 : header.getNumber();
  }

  private void resolveFinalized() {
    if (finalizedHash == null) {
      return;
    }
    blockchain
        .getBlockHeader(finalizedHash)
        .or(() -> chain.header(finalizedHash))
        .ifPresent(header -> finalizedNumber = header.getNumber());
  }

  private OptionalLong balApplyUpTo() {
    return config.balApplyEnabled() && finalizedNumber >= 0
        ? OptionalLong.of(finalizedNumber)
        : OptionalLong.empty();
  }

  private void resume() {
    if (!ready || targetHash == null) {
      return;
    }
    if (targetHeader == null) {
      lookUpTarget();
      return;
    }
    if (headerJob == null && !blockchain.contains(targetHash)) {
      final OptionalLong linkedTop = chain.linkedTop();
      final long number = targetHeader.getNumber();
      final boolean linked =
          linkedTop.isPresent()
              && number <= linkedTop.getAsLong()
              && chain.header(targetHash).isPresent();
      if (!linked) {
        extendTo(targetHeader);
      } else if (number < linkedTop.getAsLong()) {
        // the consensus client moved its head back: nothing is imported past it
        chain.removeAbove(number);
        fetcher.resetAbove(number);
      }
    }
    scheduleImport();
  }

  private void lookUpTarget() {
    if (topLookup != null) {
      return;
    }
    final Hash hash = targetHash;
    final CompletableFuture<BlockHeader> lookup = headerFetcher.byHash(hash, Priority.PIVOT);
    topLookup = lookup;
    lookup.whenCompleteAsync(
        (header, error) -> {
          if (lookup != topLookup) {
            return;
          }
          topLookup = null;
          if (error != null) {
            if (!hash.equals(lookupWarnedFor)) {
              lookupWarnedFor = hash;
              LOG.warn(
                  "Backward sync could not look up the head {}: {}",
                  hash.toShortLogString(),
                  rootCause(error).getMessage());
            }
            loop.schedule(this::resume, config.retryDelay().toNanos(), TimeUnit.NANOSECONDS);
          } else {
            if (hash.equals(targetHash)) {
              setTargetHeader(header);
            }
            resume();
          }
        },
        loop);
  }

  private void extendTo(final BlockHeader top) {
    final CompletableFuture<Link> job = headerSync.extendTo(top);
    headerJob = job;
    job.whenCompleteAsync(
        (link, error) -> {
          if (job != headerJob) {
            return;
          }
          headerJob = null;
          if (error == null) {
            onLinked(link);
          } else {
            onLinkFailed(rootCause(error));
          }
        },
        loop);
  }

  private void onLinked(final Link link) {
    final long topNumber = link.top().getNumber();
    chain.store(link.replacing());
    chain.removeAbove(topNumber);
    chain.setLinkedTop(topNumber);
    if (link.sharedNumber() + 1 < cursor) {
      // a reorganization: the parent world state is rolled back by the world state archive
      cursor = link.sharedNumber() + 1;
      fetcher.resetAbove(link.sharedNumber());
    }
    resolveFinalized();
    if (!link.top().getHash().equals(targetHash)) {
      resume();
    }
    scheduleImport();
  }

  private void onLinkFailed(final Throwable error) {
    if (error instanceof BadHeaderException bad) {
      emitBadChainEvent(bad.header());
      discardUnlinkedHeaders();
      fail("Header " + bad.header().toLogString() + " is invalid or a known bad block");
    } else if (error instanceof WrongChainException) {
      LOG.warn("Backward sync could not link the head to the local chain: {}", error.getMessage());
      discardUnlinkedHeaders();
      fail(error.getMessage());
    } else if (error instanceof CancellationException) {
      LOG.debug("Backward sync header download cancelled");
    } else {
      LOG.debug("Backward sync header download failed, retrying", error);
      loop.schedule(this::resume, config.retryDelay().toNanos(), TimeUnit.NANOSECONDS);
    }
  }

  private void discardUnlinkedHeaders() {
    final OptionalLong linkedTop = chain.linkedTop();
    if (linkedTop.isPresent()) {
      chain.removeAbove(linkedTop.getAsLong());
    } else {
      chain.clear();
    }
  }

  private void importNext() {
    importQueued = false;
    if (!ready || failed || targetHash == null) {
      return;
    }
    try {
      importOne();
    } catch (final RuntimeException e) {
      LOG.error("Backward sync failed to import block #{}", cursor, e);
      fail("Cannot import block #" + cursor + " because of " + e);
    }
  }

  private void importOne() {
    if (headerJob == null && blockchain.contains(targetHash)) {
      complete();
      return;
    }
    final OptionalLong linkedTop = chain.linkedTop();
    if (linkedTop.isEmpty() || cursor > linkedTop.getAsLong()) {
      return;
    }
    fetcher.fill(cursor, linkedTop.getAsLong(), balApplyUpTo());
    final Optional<BlockHeader> maybeHeader = chain.header(cursor);
    if (maybeHeader.isEmpty()) {
      chain.clear();
      fetcher.reset();
      cursor = blockchain.getChainHeadBlockNumber() + 1;
      resume();
      return;
    }
    final BlockHeader header = maybeHeader.get();
    if (blockchain.getBlockHashByNumber(cursor).filter(header.getHash()::equals).isPresent()) {
      // another path imported it
      fetcher.release(cursor);
      cursor++;
      scheduleImport();
      return;
    }
    if (blockchain
        .getBlockHashByNumber(cursor - 1)
        .filter(header.getParentHash()::equals)
        .isEmpty()) {
      chain.clear();
      fail("the local chain head moved off the backward sync chain");
      return;
    }
    final Optional<ImportData> data = fetcher.take(cursor, balApplyUpTo());
    if (data.isEmpty()) {
      fetcher
          .nanosUntilExpiry(cursor)
          .ifPresent(nanos -> loop.schedule(this::scheduleImport, nanos, TimeUnit.NANOSECONDS));
      return;
    }
    final ChainImporter.Result result =
        importer.importBlock(
            data.get().block(), data.get().blockAccessList(), data.get().receipts());
    switch (result.outcome()) {
      case IMPORTED -> onImported(result.fromBal());
      case BAD_BLOCK -> {
        emitBadChainEvent(header);
        chain.removeAbove(cursor - 1);
        fail("Block " + header.toLogString() + " is invalid");
      }
      case WORLD_STATE_UNAVAILABLE -> onWorldStateUnavailable(header);
      case FAILED -> {
        LOG.debug("Backward sync failed to import {}: {}", header.toLogString(), result.error());
        if (++importAttempts < config.maxImportAttempts()) {
          loop.schedule(this::scheduleImport, config.retryDelay().toNanos(), TimeUnit.NANOSECONDS);
        } else {
          fail("Cannot save block " + header.toLogString() + " because of " + result.error());
        }
      }
    }
  }

  private void onImported(final boolean fromBal) {
    final long imported = cursor;
    fetcher.release(imported);
    payloads.removeUpTo(imported);
    cursor++;
    importAttempts = 0;
    sessionImported++;
    if (fromBal) {
      sessionFromBal++;
    }
    if (sessionImported % config.pendingPruneInterval() == 0) {
      chain.removeUpTo(imported);
    }
    scheduleImport();
  }

  private void onWorldStateUnavailable(final BlockHeader header) {
    // every new head hits the same block again, warn once per block
    final boolean firstAttemptAtBlock = !header.getHash().equals(lastUnavailableWorldState);
    lastUnavailableWorldState = header.getHash();
    LOG.atLevel(firstAttemptAtBlock ? Level.WARN : Level.DEBUG)
        .setMessage(
            "Backward sync halted: parent world state is unavailable while validating block {}. "
                + "This may indicate snap sync completed with an incomplete world state. "
                + "Call debug_resyncWorldState to repair the world state and resume syncing.")
        .addArgument(header::toLogString)
        .log();
    fail(
        "Parent world state unavailable for block "
            + header.toLogString()
            + " backward sync halted. Run debug_resyncWorldState to recover.");
  }

  /**
   * Tells the listener about a bad block and its descendants on the pending chain that are not
   * marked as bad yet. The total is capped, descendants already marked do not count, so marking a
   * chain longer than the cap makes progress on every call.
   */
  private void emitBadChainEvent(final BlockHeader bad) {
    final BadBlockManager badBlockManager = context.getBadBlockManager();
    final List<Block> blocks = new ArrayList<>();
    final List<BlockHeader> headers = new ArrayList<>();
    Hash parentHash = bad.getHash();
    long number = bad.getNumber() + 1;
    while (blocks.size() + headers.size() < BadBlockManager.MAX_BAD_CHAIN_SIZE) {
      final Hash expectedParent = parentHash;
      final Optional<BlockHeader> descendant =
          chain.header(number).filter(header -> header.getParentHash().equals(expectedParent));
      if (descendant.isEmpty()) {
        break;
      }
      final BlockHeader header = descendant.get();
      if (!badBlockManager.isBadBlock(header.getHash())) {
        // the bad block manager keeps no more descendant bodies than this, the rest of a long
        // chain is collected as headers so its bodies are never loaded
        final Optional<Block> block =
            blocks.size() < BadBlockManager.MAX_BAD_DESCENDANT_BODIES
                ? knownBlock(header)
                : Optional.empty();
        if (block.isPresent()) {
          blocks.add(block.get());
        } else {
          headers.add(header);
        }
      }
      parentHash = header.getHash();
      number++;
    }
    listener.badChain(bad, blocks, headers);
  }

  private Optional<Block> knownBlock(final BlockHeader header) {
    return payloads
        .block(header.getHash())
        .or(
            () ->
                fetcher
                    .take(header.getNumber(), OptionalLong.empty())
                    .map(ImportData::block)
                    .filter(block -> block.getHash().equals(header.getHash())));
  }

  private void fail(final String message) {
    failed = true;
    cancelWork();
    listener.failed(new BackwardSyncException(message));
  }

  private void complete() {
    chain.clear();
    payloads.clear();
    fetcher.reset();
    cursor = blockchain.getChainHeadBlockNumber() + 1;
    LOG.info(
        "Backward sync completed | imported {} blocks in {} | head {}",
        ProgressFormat.count(sessionImported),
        ProgressFormat.duration(Duration.ofNanos(nanoTime.getAsLong() - sessionStartNanos)),
        ProgressFormat.block(blockchain.getChainHeadBlockNumber()));
    targetHash = null;
    setTargetHeader(null);
    sessionActive = false;
    listener.completed();
  }

  private void logProgress() {
    if (!ready || failed || targetHash == null || blockchain.contains(targetHash)) {
      return;
    }
    final OptionalLong linkedTop = chain.linkedTop();
    if (headerJob != null && (linkedTop.isEmpty() || cursor > linkedTop.getAsLong())) {
      final Progress progress = headerSync.progress();
      LOG.info(
          "Backward sync | headers {} of {}",
          ProgressFormat.count(progress.done()),
          ProgressFormat.count(progress.total()));
      return;
    }
    if (linkedTop.isEmpty()) {
      return;
    }
    final long now = nanoTime.getAsLong();
    final double seconds = (now - lastLogNanos) / 1e9;
    final double rate = seconds > 0 ? (sessionImported - lastLogImported) / seconds : 0;
    lastLogNanos = now;
    lastLogImported = sessionImported;
    final long total = Math.max(0, linkedTop.getAsLong() - sessionStartHead);
    LOG.info(
        "Backward sync | imported {} of {} blocks ({}) | head {} | {} blk/s | {} from BALs "
            + "| window {} blocks, {}",
        ProgressFormat.count(sessionImported),
        ProgressFormat.count(total),
        ProgressFormat.percent(sessionImported, total),
        ProgressFormat.block(blockchain.getChainHeadBlockNumber()),
        String.format(Locale.ROOT, "%.1f", rate),
        ProgressFormat.percent(sessionFromBal, sessionImported),
        fetcher.blocks(),
        String.format(Locale.ROOT, "%.1f MiB", fetcher.bytes() / BYTES_PER_MIB));
  }

  private static Throwable rootCause(final Throwable error) {
    return error instanceof CompletionException && error.getCause() != null
        ? error.getCause()
        : error;
  }
}
