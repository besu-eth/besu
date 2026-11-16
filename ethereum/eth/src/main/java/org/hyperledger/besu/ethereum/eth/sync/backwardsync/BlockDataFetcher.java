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
import org.hyperledger.besu.ethereum.chain.ChainPrunerConfiguration;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * Downloads the block data the importer needs next into a window above its cursor: bodies, block
 * access lists and, for the blocks imported without executing them, receipts. The window is bounded
 * in bytes, in blocks and in requests in flight, except for the block at the cursor, whose missing
 * data is always requested, as the importer waits for it. All methods run on the loop thread, which
 * every response is handed back to.
 */
final class BlockDataFetcher {

  // what one response is expected to weigh, which every request in flight reserves in the window
  private static final long RESPONSE_BYTES = 2L << 20;
  private static final double EWMA_ALPHA = 0.2;
  private static final double INITIAL_BODY_BYTES = 64 << 10;
  private static final double INITIAL_BAL_BYTES = 96 << 10;
  private static final double INITIAL_RECEIPTS_BYTES = 64 << 10;

  /**
   * The data of a block, as far as it is available.
   *
   * @param block the block
   * @param blockAccessList its block access list, if available
   * @param receipts its receipts, if they were needed and available
   */
  record ImportData(
      Block block,
      Optional<BlockAccessList> blockAccessList,
      Optional<List<TransactionReceipt>> receipts) {}

  private enum BalState {
    UNKNOWN,
    REQUESTED,
    READY,
    UNAVAILABLE
  }

  private enum ReceiptsState {
    NOT_NEEDED,
    NEEDED,
    REQUESTED,
    READY,
    UNAVAILABLE
  }

  private enum Kind {
    BODIES,
    BAL,
    RECEIPTS
  }

  // a request in flight, and the entries that wait for it
  private static final class Inflight {
    private final Kind kind;
    private final List<Entry> entries;
    private CompletableFuture<?> request;
    private boolean cancelled;

    private Inflight(final Kind kind, final List<Entry> entries) {
      this.kind = kind;
      this.entries = entries;
    }
  }

  private static final class Entry {
    private final BlockHeader header;
    private Block block;
    private Inflight bodyRequest;
    private BalState bal = BalState.UNKNOWN;
    private BlockAccessList balList;
    private long balRequestedAt;
    private Inflight balRequest;
    // the peers that answered that they have no list of this block
    private final Set<EthPeer> noBalFrom = new HashSet<>();
    private ReceiptsState receipts = ReceiptsState.NOT_NEEDED;
    private long receiptsRequestedAt;
    private Inflight receiptsRequest;
    private List<SyncTransactionReceipt> partialReceipts = List.of();
    private List<TransactionReceipt> receiptList;
    private long partialReceiptsBytes;
    private long bytes;

    private Entry(final BlockHeader header) {
      this.header = header;
    }

    private long number() {
      return header.getNumber();
    }
  }

  private final RequestSender sender;
  private final EthPeers ethPeers;
  private final ProtocolSchedule schedule;
  private final PendingChain chain;
  private final TrustedPayloads payloads;
  private final ScheduledExecutorService loop;
  private final LongSupplier nanoTime;
  private final BackwardSyncConfiguration config;
  private final Runnable arrived;
  private final SyncTransactionReceiptEncoder receiptEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());
  private final NavigableMap<Long, Entry> entries = new TreeMap<>();
  private final Set<Inflight> requests = new HashSet<>();
  private long bytes;
  private double averageBodyBytes = INITIAL_BODY_BYTES;
  private double averageBalBytes = INITIAL_BAL_BYTES;
  private double averageReceiptsBytes = INITIAL_RECEIPTS_BYTES;
  // whether a peer serves block access lists, known once per fill
  private Boolean balServer;

  /**
   * Creates a fetcher.
   *
   * @param loop the loop thread, which runs every response and its retries
   * @param arrived runs on the loop after any response
   */
  BlockDataFetcher(
      final RequestSender sender,
      final EthPeers ethPeers,
      final ProtocolSchedule schedule,
      final PendingChain chain,
      final TrustedPayloads payloads,
      final ScheduledExecutorService loop,
      final LongSupplier nanoTime,
      final BackwardSyncConfiguration config,
      final Runnable arrived) {
    this.sender = sender;
    this.ethPeers = ethPeers;
    this.schedule = schedule;
    this.chain = chain;
    this.payloads = payloads;
    this.loop = loop;
    this.nanoTime = nanoTime;
    this.config = config;
    this.arrived = arrived;
  }

  /**
   * Requests the missing data of the blocks from the cursor up, as far as the window allows.
   *
   * @param cursor the number of the next block to import
   * @param linkedTop the number of the highest block of the pending chain
   * @param balApplyUpTo the highest block imported from its block access list, if any
   */
  void fill(final long cursor, final long linkedTop, final OptionalLong balApplyUpTo) {
    balServer = null;
    final long end = Math.min(linkedTop, cursor + config.maxWindowBlocks() - 1);
    final int maxInFlight = Math.max(4, sender.capacity(Protocol.ETH) * 5 / 4);
    final List<Entry> bodies = new ArrayList<>();
    final List<Entry> lists = new ArrayList<>();
    final List<Entry> receipts = new ArrayList<>();
    for (long number = cursor; number <= end; number++) {
      if (number > cursor && windowFull(bodies, lists, receipts, maxInFlight)) {
        break;
      }
      Entry entry = entries.get(number);
      if (entry == null) {
        final Optional<BlockHeader> header = chain.header(number);
        if (header.isEmpty()) {
          break;
        }
        entry = new Entry(header.get());
        entries.put(number, entry);
      }
      planBody(entry, bodies);
      planBlockAccessList(entry, linkedTop, lists);
      planReceipts(entry, balApplyUpTo, receipts);
    }
    requestBodies(bodies);
    requestBlockAccessLists(lists);
    requestReceipts(receipts);
  }

  /**
   * Returns the data to import the block with, or nothing while the block is missing or the data
   * that may still arrive is awaited. A block access list or receipts that did not arrive within
   * the wait of their request are given up on, for every block of that request at once.
   */
  Optional<ImportData> take(final long number, final OptionalLong balApplyUpTo) {
    final Entry entry = entries.get(number);
    if (entry == null || entry.block == null) {
      return Optional.empty();
    }
    final long now = nanoTime.getAsLong();
    if (entry.bal == BalState.UNKNOWN) {
      return Optional.empty();
    }
    if (entry.bal == BalState.REQUESTED) {
      if (!expired(entry.balRequestedAt, now)) {
        return Optional.empty();
      }
      entry.bal = BalState.UNAVAILABLE;
      entry.balRequest = null;
    }
    final boolean applyFromList =
        entry.bal == BalState.READY
            && balApplyUpTo.isPresent()
            && number <= balApplyUpTo.getAsLong();
    if (applyFromList) {
      if (entry.receipts == ReceiptsState.REQUESTED && expired(entry.receiptsRequestedAt, now)) {
        entry.receipts = ReceiptsState.UNAVAILABLE;
        entry.receiptsRequest = null;
      }
      // the next fill requests receipts a block that became eligible still needs
      if (entry.receipts == ReceiptsState.NOT_NEEDED
          || entry.receipts == ReceiptsState.NEEDED
          || entry.receipts == ReceiptsState.REQUESTED) {
        return Optional.empty();
      }
    }
    return Optional.of(
        new ImportData(
            entry.block,
            entry.bal == BalState.READY ? Optional.of(entry.balList) : Optional.empty(),
            applyFromList && entry.receipts == ReceiptsState.READY
                ? Optional.of(entry.receiptList)
                : Optional.empty()));
  }

  /** Returns the time until the optional data of the block that is awaited is given up on. */
  OptionalLong nanosUntilExpiry(final long number) {
    final Entry entry = entries.get(number);
    if (entry == null) {
      return OptionalLong.empty();
    }
    final long requestedAt;
    if (entry.bal == BalState.REQUESTED) {
      requestedAt = entry.balRequestedAt;
    } else if (entry.receipts == ReceiptsState.REQUESTED) {
      requestedAt = entry.receiptsRequestedAt;
    } else {
      return OptionalLong.empty();
    }
    return OptionalLong.of(
        Math.max(0, config.optionalDataWait().toNanos() - (nanoTime.getAsLong() - requestedAt)));
  }

  /** Forgets the block, which is imported. */
  void release(final long number) {
    final Entry entry = entries.remove(number);
    if (entry != null) {
      bytes -= entry.bytes;
    }
  }

  /** Forgets the blocks above the number and cancels the requests that include any of them. */
  void resetAbove(final long number) {
    for (final Inflight inflight : List.copyOf(requests)) {
      if (inflight.entries.stream().anyMatch(entry -> entry.number() > number)) {
        cancel(inflight);
      }
    }
    final NavigableMap<Long, Entry> above = entries.tailMap(number, false);
    above.values().forEach(entry -> bytes -= entry.bytes);
    above.clear();
  }

  /** Forgets every block and cancels every request. */
  void reset() {
    List.copyOf(requests).forEach(this::cancel);
    entries.clear();
    bytes = 0;
  }

  /** Returns the number of blocks in the window. */
  int blocks() {
    return entries.size();
  }

  /** Returns the bytes held by the window. */
  long bytes() {
    return bytes;
  }

  // the window holds what it may, counting what the open batches and the requests are to bring
  private boolean windowFull(
      final List<Entry> bodies,
      final List<Entry> lists,
      final List<Entry> receipts,
      final int maxInFlight) {
    final int reserved =
        requests.size()
            + (bodies.isEmpty() ? 0 : 1)
            + (lists.isEmpty() ? 0 : 1)
            + (receipts.isEmpty() ? 0 : 1);
    return bytes + reserved * RESPONSE_BYTES >= config.windowBytes() || reserved >= maxInFlight;
  }

  // a block joins the open batch if it follows the last block of it, else the batch is sent
  private void planBody(final Entry entry, final List<Entry> batch) {
    if (entry.block == null && entry.bodyRequest == null) {
      final Optional<Block> known = payloads.block(entry.header.getHash());
      if (known.isPresent()) {
        entry.block = known.get();
        account(entry, entry.block.getSize());
      } else {
        batch.add(entry);
        if (batch.size() >= batchSize(averageBodyBytes, 8, 128)) {
          requestBodies(batch);
        }
        return;
      }
    }
    requestBodies(batch);
  }

  private void planBlockAccessList(
      final Entry entry, final long linkedTop, final List<Entry> batch) {
    if (entry.bal == BalState.UNKNOWN && !resolveBlockAccessList(entry, linkedTop)) {
      batch.add(entry);
      if (batch.size() >= batchSize(averageBalBytes, 4, 128)) {
        requestBlockAccessLists(batch);
      }
      return;
    }
    requestBlockAccessLists(batch);
  }

  // returns whether the state of the list is known without asking a peer
  private boolean resolveBlockAccessList(final Entry entry, final long linkedTop) {
    final Hash hash = entry.header.getHash();
    if (entry.header.getBalHash().isEmpty()) {
      entry.bal = BalState.UNAVAILABLE;
      return true;
    }
    final Optional<BlockAccessList> known = payloads.blockAccessList(hash);
    if (known.isPresent()) {
      entry.bal = BalState.READY;
      entry.balList = known.get();
      account(entry, size(known.get()));
      return true;
    }
    // beyond the retention of the peers, or nobody to ask: do not wait for the scheduler to give up
    if (entry.number() + ChainPrunerConfiguration.HISTORY_PRUNE_BLOCKS < linkedTop
        || !hasBlockAccessListServer()) {
      entry.bal = BalState.UNAVAILABLE;
      return true;
    }
    return false;
  }

  private boolean hasBlockAccessListServer() {
    if (balServer == null) {
      balServer =
          ethPeers
              .streamAvailablePeers()
              .filter(EthPeerImmutableAttributes::isFullyValidated)
              .anyMatch(
                  peer ->
                      peer.ethPeer().getAgreedCapabilities().stream()
                          .anyMatch(EthProtocol::isEth71Compatible));
    }
    return balServer;
  }

  private void planReceipts(
      final Entry entry, final OptionalLong balApplyUpTo, final List<Entry> batch) {
    if (entry.receipts == ReceiptsState.NOT_NEEDED
        && entry.bal == BalState.READY
        && entry.block != null
        && balApplyUpTo.isPresent()
        && entry.number() <= balApplyUpTo.getAsLong()) {
      if (entry.header.getReceiptsRoot().equals(Hash.EMPTY_TRIE_HASH)) {
        entry.receipts = ReceiptsState.READY;
        entry.receiptList = List.of();
      } else {
        entry.receipts = ReceiptsState.NEEDED;
      }
    }
    if (entry.receipts == ReceiptsState.NEEDED) {
      // the answer completes the block that holds receipts first
      if (!entry.partialReceipts.isEmpty()) {
        requestReceipts(batch);
      }
      batch.add(entry);
      if (batch.size() >= batchSize(averageReceiptsBytes, 4, 256)) {
        requestReceipts(batch);
      }
      return;
    }
    requestReceipts(batch);
  }

  private void requestBodies(final List<Entry> batch) {
    if (batch.isEmpty()) {
      return;
    }
    final List<Entry> blocks = List.copyOf(batch);
    batch.clear();
    final List<BlockHeader> headers = blocks.stream().map(entry -> entry.header).toList();
    final Inflight inflight = new Inflight(Kind.BODIES, blocks);
    final CompletableFuture<RequestSender.Response<List<Block>>> request =
        sender.submitWithRetries(
            new PeerTaskRequest<>(
                () -> new GetBodiesFromPeerTask(headers, schedule), Priority.BODIES, "bodies"),
            config.requestAttempts());
    track(inflight, request);
    blocks.forEach(entry -> entry.bodyRequest = inflight);
    request
        .thenApplyAsync(this::recoverSenders, sender.workers())
        .whenCompleteAsync((response, error) -> onBodies(inflight, response, error), loop);
  }

  // signature recovery is cached in the transactions, so it is done here rather than on the loop
  private RequestSender.Response<List<Block>> recoverSenders(
      final RequestSender.Response<List<Block>> response) {
    for (final Block block : response.value()) {
      for (final Transaction transaction : block.getBody().getTransactions()) {
        try {
          transaction.getSender();
        } catch (final RuntimeException e) {
          // the import rejects the block, which is not for this download to decide
          return response;
        }
      }
    }
    return response;
  }

  private void onBodies(
      final Inflight inflight,
      final RequestSender.Response<List<Block>> response,
      final Throwable error) {
    if (!finish(inflight)) {
      return;
    }
    if (error != null) {
      inflight.entries.stream()
          .filter(entry -> owns(entry, inflight))
          .forEach(entry -> entry.bodyRequest = null);
      retryLater(error);
      return;
    }
    final List<Block> blocks = response.value();
    long received = 0;
    for (int i = 0; i < blocks.size(); i++) {
      received += blocks.get(i).getSize();
    }
    averageBodyBytes = average(averageBodyBytes, received, blocks.size());
    for (int i = 0; i < inflight.entries.size(); i++) {
      final Entry entry = inflight.entries.get(i);
      if (!owns(entry, inflight)) {
        continue;
      }
      entry.bodyRequest = null;
      if (i < blocks.size()) {
        entry.block = blocks.get(i);
        account(entry, entry.block.getSize());
      }
    }
    arrived.run();
  }

  private void requestBlockAccessLists(final List<Entry> batch) {
    if (batch.isEmpty()) {
      return;
    }
    final List<Entry> blocks = List.copyOf(batch);
    batch.clear();
    final List<BlockHeader> headers = blocks.stream().map(entry -> entry.header).toList();
    final Set<EthPeer> excluded = new HashSet<>();
    blocks.forEach(entry -> excluded.addAll(entry.noBalFrom));
    final Inflight inflight = new Inflight(Kind.BAL, blocks);
    final CompletableFuture<RequestSender.Response<List<Optional<BlockAccessList>>>> request =
        sender.submit(
            new PeerTaskRequest<>(
                () -> new GetBlockAccessListsFromPeerTask(headers, true),
                Priority.BODIES,
                "block_access_lists"),
            excluded);
    track(inflight, request);
    final long now = nanoTime.getAsLong();
    for (final Entry entry : blocks) {
      entry.bal = BalState.REQUESTED;
      entry.balRequestedAt = now;
      entry.balRequest = inflight;
    }
    request.whenCompleteAsync(
        (response, error) -> onBlockAccessLists(inflight, response, error), loop);
  }

  private void onBlockAccessLists(
      final Inflight inflight,
      final RequestSender.Response<List<Optional<BlockAccessList>>> response,
      final Throwable error) {
    if (!finish(inflight)) {
      return;
    }
    if (error != null) {
      onBlockAccessListsFailed(inflight, error);
      return;
    }
    final List<Optional<BlockAccessList>> lists = response.value();
    long received = 0;
    int served = 0;
    for (int i = 0; i < inflight.entries.size(); i++) {
      final Entry entry = inflight.entries.get(i);
      final Optional<BlockAccessList> list = i < lists.size() ? lists.get(i) : null;
      if (list != null && list.isPresent()) {
        received += size(list.get());
        served++;
      }
      if (!owns(entry, inflight)) {
        continue;
      }
      entry.balRequest = null;
      if (list == null) {
        entry.bal = BalState.UNKNOWN;
      } else if (list.isPresent()) {
        entry.bal = BalState.READY;
        entry.balList = list.get();
        account(entry, size(list.get()));
      } else {
        markNoList(entry, Optional.ofNullable(response.peer()));
      }
    }
    averageBalBytes = average(averageBalBytes, received, served);
    arrived.run();
  }

  private void onBlockAccessListsFailed(final Inflight inflight, final Throwable error) {
    final Optional<RequestFailedException> failure = RequestFailedException.from(error);
    final Optional<Reason> reason = failure.map(RequestFailedException::reason);
    boolean retry = false;
    for (final Entry entry : inflight.entries) {
      if (!owns(entry, inflight)) {
        continue;
      }
      entry.balRequest = null;
      if (reason.orElse(null) == Reason.EMPTY) {
        markNoList(entry, failure.orElseThrow().peer());
      } else if (reason.orElse(null) == Reason.NO_PEER) {
        entry.bal = BalState.UNAVAILABLE;
      } else {
        entry.bal = BalState.UNKNOWN;
        retry = true;
      }
    }
    if (reason.orElse(null) == Reason.EMPTY) {
      arrived.run();
    } else if (retry) {
      retryLater(error);
    }
  }

  // a peer without the list is not asked for it again, and a few of them make it unavailable
  private void markNoList(final Entry entry, final Optional<EthPeer> peer) {
    peer.ifPresent(entry.noBalFrom::add);
    entry.bal =
        entry.noBalFrom.size() >= config.balPeerAttempts()
            ? BalState.UNAVAILABLE
            : BalState.UNKNOWN;
  }

  private void requestReceipts(final List<Entry> batch) {
    if (batch.isEmpty()) {
      return;
    }
    final List<Entry> blocks = List.copyOf(batch);
    batch.clear();
    final List<SyncBlock> syncBlocks =
        blocks.stream().map(entry -> SyncBlock.of(entry.block, schedule)).toList();
    final GetSyncReceiptsFromPeerTask.Request content =
        new GetSyncReceiptsFromPeerTask.Request(syncBlocks, blocks.getFirst().partialReceipts);
    final Inflight inflight = new Inflight(Kind.RECEIPTS, blocks);
    final CompletableFuture<RequestSender.Response<GetSyncReceiptsFromPeerTask.Response>> request =
        sender.submitWithRetries(
            new PeerTaskRequest<>(
                () -> new GetSyncReceiptsFromPeerTask(content, schedule, receiptEncoder),
                Priority.BODIES,
                "receipts"),
            config.requestAttempts());
    track(inflight, request);
    final long now = nanoTime.getAsLong();
    for (final Entry entry : blocks) {
      entry.receipts = ReceiptsState.REQUESTED;
      entry.receiptsRequestedAt = now;
      entry.receiptsRequest = inflight;
    }
    request.whenCompleteAsync(
        (response, error) -> onReceipts(inflight, syncBlocks, response, error), loop);
  }

  private void onReceipts(
      final Inflight inflight,
      final List<SyncBlock> syncBlocks,
      final RequestSender.Response<GetSyncReceiptsFromPeerTask.Response> response,
      final Throwable error) {
    if (!finish(inflight)) {
      return;
    }
    if (error != null) {
      // the sender retried it
      for (final Entry entry : inflight.entries) {
        if (owns(entry, inflight)) {
          entry.receiptsRequest = null;
          entry.receipts = ReceiptsState.UNAVAILABLE;
        }
      }
      arrived.run();
      return;
    }
    final GetSyncReceiptsFromPeerTask.Response answer = response.value();
    long received = 0;
    for (int i = 0; i < inflight.entries.size(); i++) {
      final Entry entry = inflight.entries.get(i);
      final boolean complete = i < answer.completeCount();
      final List<SyncTransactionReceipt> receipts =
          complete ? answer.completeReceiptsByBlock().get(syncBlocks.get(i)) : null;
      if (complete) {
        received += rlpSize(receipts);
      }
      if (!owns(entry, inflight)) {
        continue;
      }
      entry.receiptsRequest = null;
      if (complete) {
        entry.receiptList =
            receipts.stream().map(TransactionReceiptDecoder::decodeSyncReceipt).toList();
        entry.receipts = ReceiptsState.READY;
        setPartialReceipts(entry, List.of());
        account(entry, rlpSize(receipts));
      } else {
        entry.receipts = ReceiptsState.NEEDED;
        if (i == answer.completeCount()) {
          setPartialReceipts(entry, answer.lastBlockPartialReceipts());
        }
      }
    }
    averageReceiptsBytes = average(averageReceiptsBytes, received, answer.completeCount());
    arrived.run();
  }

  private void setPartialReceipts(final Entry entry, final List<SyncTransactionReceipt> partial) {
    final long partialBytes = rlpSize(partial);
    account(entry, partialBytes - entry.partialReceiptsBytes);
    entry.partialReceipts = partial;
    entry.partialReceiptsBytes = partialBytes;
  }

  private void track(final Inflight inflight, final CompletableFuture<?> request) {
    inflight.request = request;
    requests.add(inflight);
  }

  // returns whether the response is still wanted: a request cancelled is already forgotten
  private boolean finish(final Inflight inflight) {
    if (inflight.cancelled) {
      return false;
    }
    requests.remove(inflight);
    return true;
  }

  // the entry was not released or replaced since the request, nor given up on
  private boolean owns(final Entry entry, final Inflight inflight) {
    if (entries.get(entry.number()) != entry) {
      return false;
    }
    return switch (inflight.kind) {
      case BODIES -> entry.bodyRequest == inflight;
      case BAL -> entry.balRequest == inflight;
      case RECEIPTS -> entry.receiptsRequest == inflight;
    };
  }

  // the entries of the request can be requested again
  private void cancel(final Inflight inflight) {
    inflight.cancelled = true;
    requests.remove(inflight);
    inflight.request.cancel(false);
    for (final Entry entry : inflight.entries) {
      switch (inflight.kind) {
        case BODIES -> {
          if (entry.bodyRequest == inflight) {
            entry.bodyRequest = null;
          }
        }
        case BAL -> {
          if (entry.balRequest == inflight) {
            entry.balRequest = null;
            entry.bal = BalState.UNKNOWN;
          }
        }
        case RECEIPTS -> {
          if (entry.receiptsRequest == inflight) {
            entry.receiptsRequest = null;
            entry.receipts = ReceiptsState.NEEDED;
          }
        }
      }
    }
  }

  private void retryLater(final Throwable error) {
    final boolean stopped =
        RequestFailedException.from(error)
            .filter(failure -> failure.reason() == Reason.STOPPED)
            .isPresent();
    if (!stopped) {
      loop.schedule(arrived, config.retryDelay().toNanos(), TimeUnit.NANOSECONDS);
    }
  }

  private boolean expired(final long requestedAt, final long now) {
    return now - requestedAt >= config.optionalDataWait().toNanos();
  }

  private void account(final Entry entry, final long delta) {
    entry.bytes += delta;
    bytes += delta;
  }

  private static int batchSize(final double averageBytes, final int min, final int max) {
    return Math.clamp((long) (RESPONSE_BYTES / Math.max(1, averageBytes)), min, max);
  }

  private static double average(final double average, final long bytes, final int count) {
    if (count == 0) {
      return average;
    }
    return average + EWMA_ALPHA * ((double) bytes / count - average);
  }

  private static long size(final BlockAccessList list) {
    return list.rawRlp().map(Bytes::size).orElse(0);
  }

  private static long rlpSize(final List<SyncTransactionReceipt> receipts) {
    return receipts.stream().mapToLong(receipt -> receipt.getRlpBytes().size()).sum();
  }
}
