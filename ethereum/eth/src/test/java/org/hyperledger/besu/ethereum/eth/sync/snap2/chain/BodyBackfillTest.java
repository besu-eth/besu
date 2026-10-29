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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.diskFailure;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.notCommitted;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetPaginatedReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill.Settings;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

import com.google.common.collect.ImmutableList;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BodyBackfillTest {

  private static final int HEAD = 60;
  private static final int RANGE_BLOCKS = 8;
  // far more steps or requests than a back-fill of these tests takes
  private static final int BUDGET = 10_000;

  private final BlockDataGenerator gen = new BlockDataGenerator(5);
  private final List<Block> blocks = new ArrayList<>();
  private final Map<Hash, Block> blocksByHash = new HashMap<>();
  private final Map<Hash, List<TransactionReceipt>> receiptsByHash = new HashMap<>();
  private final FakeSender sender = new FakeSender();
  private final List<Long> pauses = new ArrayList<>();
  private MutableBlockchain blockchain;

  @Test
  void storesTheMissingBodiesAndReceiptsHighestFirst() {
    blockchain = chain(51);
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(1, HEAD);
    assertThat(blockchain.getBodiesFrom()).contains(1L);
    assertThat(backfill.lowestStored()).isEqualTo(1);
    assertThat(backfill.blocksRemaining()).isZero();
    assertThat(backfill.blocksStored()).isEqualTo(50);
    // ranges of eight blocks, from the highest missing one down
    assertThat(sender.askedRanges).containsExactly(43L, 35L, 27L, 19L, 11L, 3L, 1L);
  }

  @Test
  void asksNothingWhileTheNodeIsBehindTheHead() {
    blockchain = chain(51);
    final AtomicInteger checks = new AtomicInteger();
    final List<Integer> requestsWhileBehind = new ArrayList<>();
    final BooleanSupplier inSync =
        () -> {
          final boolean behind = checks.incrementAndGet() <= 3;
          if (behind) {
            requestsWhileBehind.add(sender.requests);
          }
          return !behind;
        };
    final BodyBackfill backfill = backfill(Long.MAX_VALUE, inSync, head -> 1);

    runToEnd(backfill);

    assertThat(requestsWhileBehind).containsExactly(0, 0, 0);
    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(1, HEAD);
  }

  @Test
  void resumesFromTheBlockRecordedOnStop() {
    blockchain = chain(51);
    final BodyBackfill stopped = backfill();
    // stopped once the bodies of its third range arrived, while their receipts download
    sender.beforeReceipts =
        first -> {
          if (first == 27) {
            stopped.stop();
          }
        };

    runToEnd(stopped);

    assertThat(stopped.done()).isCompletedExceptionally();
    assertThat(blockchain.getBodiesFrom()).contains(35L);
    assertNoBodies(27, 34);

    sender.beforeReceipts = first -> {};
    sender.askedRanges.clear();
    final BodyBackfill resumed = backfill();
    runToEnd(resumed);

    assertThat(resumed.done()).isCompleted();
    assertThat(sender.askedRanges).startsWith(27L);
    assertBodiesStored(1, HEAD);
  }

  @Test
  void shutsTheWorkersDownWhenStartedAfterAStop() {
    blockchain = chain(51);
    final BodyBackfill backfill = backfill();
    final QueuedExecutor workers = new QueuedExecutor();

    backfill.stop();
    backfill.start(workers);

    assertThat(workers.isShutdown()).isTrue();
    assertThat(backfill.done()).isCompletedExceptionally();
  }

  @Test
  void asksNothingOnceStopped() {
    blockchain = chain(51);
    final BodyBackfill backfill = backfill();
    final List<Long> receiptsAsked = new ArrayList<>();
    // stopped while the bodies of its first range download
    sender.beforeBodies = first -> backfill.stop();
    sender.beforeReceipts = receiptsAsked::add;

    runToEnd(backfill);

    assertThat(receiptsAsked).isEmpty();
  }

  @Test
  void stopsAtTheLowestBlockNeeded() {
    blockchain = chain(51);
    final BodyBackfill backfill = backfill(head -> head - 29);

    runToEnd(backfill);

    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(31, HEAD);
    assertNoBodies(1, 30);
    assertThat(blockchain.getBodiesFrom()).contains(31L);
  }

  @Test
  void hasNothingToDoWhenEveryNeededBodyIsStored() {
    blockchain = chain(1);
    final BodyBackfill backfill = backfill();
    final QueuedExecutor workers = new QueuedExecutor();

    backfill.start(workers);

    assertThat(backfill.done()).isCompleted();
    assertThat(workers.isShutdown()).isTrue();
  }

  @Test
  void asksARangeAgainAfterAFailedRequest() {
    blockchain = chain(51);
    // the first answer holds no body
    sender.failNextBodies = true;
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(sender.askedRanges).startsWith(43L, 43L, 35L);
    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(1, HEAD);
  }

  @Test
  void dropsTheBlocksThatFallBelowTheFloorWhileTheyDownload() {
    blockchain = chain(51);
    final AtomicLong floor = new AtomicLong(1);
    // a pruner raises the floor to 38 while the second range, 35 to 42, downloads
    sender.beforeBodies =
        first -> {
          if (first == 35) {
            floor.set(38);
          }
        };
    final BodyBackfill backfill = backfill(head -> floor.get());

    runToEnd(backfill);

    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(38, HEAD);
    assertNoBodies(35, 37);
    assertThat(blockchain.getBodiesFrom()).contains(38L);
  }

  @Test
  void recordsTheLowestBlockAfterEveryRange() {
    blockchain = chain(51);
    final List<Long> recorded = new ArrayList<>();
    sender.beforeBodies = first -> recorded.add(blockchain.getBodiesFrom().orElseThrow());
    final BodyBackfill backfill = backfill(0, () -> true, head -> 1);

    runToEnd(backfill);

    // a node that stops at any time downloads one range again at most
    assertThat(recorded).containsExactly(51L, 43L, 35L, 27L, 19L, 11L, 3L);
  }

  @Test
  void dropsARangeAReorgReplacedAndAsksItAgain() {
    blockchain = chain(51);
    final Block fork =
        gen.block(
            new BlockOptions()
                .setBlockNumber(50)
                .setParentHash(blocks.get(49).getHash())
                .setDifficulty(Difficulty.ZERO)
                .hasOmmers(false)
                .hasTransactions(false)
                .setReceiptsRoot(Hash.EMPTY_TRIE_HASH));
    blocksByHash.put(fork.getHash(), fork);
    final AtomicBoolean reorged = new AtomicBoolean();
    // block 50 leaves the canonical chain while the range that ends with it downloads
    sender.beforeBodies =
        first -> {
          if (first == 43 && reorged.compareAndSet(false, true)) {
            blockchain.storeBlockHeaders(List.of(fork.getHeader()), Difficulty.ONE);
          }
        };
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(sender.askedRanges).startsWith(43L, 43L, 35L);
    assertThat(blockchain.getBlockBody(fork.getHash())).contains(fork.getBody());
    assertThat(blockchain.getBlockBody(blocks.get(50).getHash())).isEmpty();
    assertBodiesStored(1, 49);
  }

  @Test
  void failsOnAMissingHeader() {
    // the headers from block 21 only, as left by a sync that stopped early
    blockchain = chain(51, 21);
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(backfill.done())
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .withMessageContaining("no header stored for block 19");
    assertBodiesStored(27, HEAD);
    assertThat(blockchain.getBodiesFrom()).contains(27L);
  }

  @Test
  void failsWhenItCannotRecordWhatItStored() {
    blockchain = spy(chain(51));
    doThrow(new IllegalStateException("disk full")).when(blockchain).unsafeSetBodiesFrom(anyLong());
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(backfill.done())
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .withMessageContaining("disk full");
  }

  @Test
  void recordsWhatItStoredAgainWhenTheRecordIsNotCommitted() {
    blockchain = spy(chain(51));
    // the transaction of the record fails its conflict check once
    doThrow(notCommitted()).doCallRealMethod().when(blockchain).unsafeSetBodiesFrom(anyLong());
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(backfill.done()).isCompleted();
    assertThat(blockchain.getBodiesFrom()).contains(1L);
    assertThat(pauses).containsExactly(20L);
  }

  @Test
  void endsOnAStorageFailure() {
    blockchain = spy(chain(51));
    final StorageException failure = diskFailure();
    doThrow(failure).when(blockchain).unsafeStoreSyncBodiesAndReceipts(anyList(), anyBoolean());
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    // rather than asking the same range again and again
    assertThat(backfill.done())
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .havingCause()
        .isSameAs(failure);
  }

  @Test
  void endsWhenItsWorkerIsInterrupted() {
    blockchain = chain(51);
    sender.leaveNextUnanswered = true;
    final BodyBackfill backfill = backfill();

    runToEnd(backfill);

    assertThat(backfill.done()).isCompletedExceptionally();
    // nobody waits for the request anymore, so the scheduler may drop it
    assertThat(sender.unanswered).isCancelled();
  }

  @Test
  void completesTheReceiptsOfABlockCutAcrossPages() {
    blockchain = chain(51);
    // an eth/70 peer answers two receipts at most, of blocks that have three
    sender.speakEth70(2);
    final BodyBackfill backfill = backfill(head -> 46);

    runToEnd(backfill);

    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(46, HEAD);
  }

  @Test
  void asksARangeAgainAfterAPageThatCompletesNothing() {
    blockchain = chain(51);
    sender.speakEth70(2);
    sender.stallNextPage = true;
    final BodyBackfill backfill = backfill(head -> 46);

    runToEnd(backfill);

    assertThat(sender.askedRanges).containsExactly(46L, 46L);
    assertThat(backfill.done()).isCompleted();
    assertBodiesStored(46, HEAD);
  }

  // down to block 1, in sync, and the lowest block recorded once the back-fill ends
  private BodyBackfill backfill() {
    return backfill(head -> 1);
  }

  private BodyBackfill backfill(final LongUnaryOperator lowestNeeded) {
    return backfill(Long.MAX_VALUE, () -> true, lowestNeeded);
  }

  // one worker, ranges of eight blocks, no wait, and the lowest block recorded every so many bytes
  private BodyBackfill backfill(
      final long recordBytes, final BooleanSupplier inSync, final LongUnaryOperator lowestNeeded) {
    return new BodyBackfill(
        blockchain,
        ProtocolScheduleFixture.MAINNET,
        sender,
        bounded(inSync),
        blockchain.getBodiesFrom().orElseThrow(),
        lowestNeeded,
        new Settings(
            "test", 1, recordBytes, Duration.ZERO, Duration.ZERO, RANGE_BLOCKS, false, true),
        new NoOpMetricsSystem(),
        pauses::add);
  }

  // each step asks whether the node is in sync: a back-fill that never ends fails, not hangs
  private static BooleanSupplier bounded(final BooleanSupplier inSync) {
    final AtomicInteger steps = new AtomicInteger();
    return () -> {
      if (steps.incrementAndGet() > BUDGET) {
        throw new AssertionError("the back-fill never ends");
      }
      return inSync.getAsBoolean();
    };
  }

  // runs the back-fill to its end, its worker on the test thread
  private static void runToEnd(final BodyBackfill backfill) {
    final QueuedExecutor workers = new QueuedExecutor();
    backfill.start(workers);
    workers.runAll();
  }

  private MutableBlockchain chain(final long bodiesFrom) {
    return chain(bodiesFrom, 1);
  }

  // headers headersFrom to HEAD stored, the head set, and the bodies and receipts from bodiesFrom
  private MutableBlockchain chain(final long bodiesFrom, final long headersFrom) {
    final Block genesis = gen.genesisBlock();
    blocks.add(genesis);
    final MutableBlockchain stored =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis);
    Block parent = genesis;
    for (long number = 1; number <= HEAD; number++) {
      final List<Transaction> transactions =
          List.of(gen.transaction(), gen.transaction(), gen.transaction());
      final List<TransactionReceipt> receipts =
          transactions.stream().map(transaction -> gen.receipt()).toList();
      parent =
          gen.block(
              new BlockOptions()
                  .setBlockNumber(number)
                  .setParentHash(parent.getHash())
                  .setDifficulty(Difficulty.ZERO)
                  .hasOmmers(false)
                  .addTransaction(transactions)
                  .setReceiptsRoot(BodyValidation.receiptsRoot(receipts)));
      blocks.add(parent);
      blocksByHash.put(parent.getHash(), parent);
      receiptsByHash.put(parent.getHash(), receipts);
    }
    stored.storeBlockHeaders(
        blocks.subList((int) headersFrom, blocks.size()).stream().map(Block::getHeader).toList(),
        Difficulty.ONE);
    for (long number = bodiesFrom; number <= HEAD; number++) {
      final Block block = blocks.get((int) number);
      stored.unsafeImportBlock(block, receiptsByHash.get(block.getHash()), Optional.empty());
    }
    stored.unsafeSetChainHead(blocks.get(HEAD).getHeader(), Difficulty.ONE);
    stored.unsafeSetBodiesFrom(bodiesFrom);
    return stored;
  }

  private long number(final Hash hash) {
    return blocksByHash.get(hash).getHeader().getNumber();
  }

  private void assertBodiesStored(final long from, final long to) {
    for (long number = from; number <= to; number++) {
      final Block block = blocks.get((int) number);
      assertThat(blockchain.getBlockBody(block.getHash())).contains(block.getBody());
      assertThat(blockchain.getTxReceipts(block.getHash()))
          .contains(receiptsByHash.get(block.getHash()));
    }
  }

  private void assertNoBodies(final long from, final long to) {
    for (long number = from; number <= to; number++) {
      assertThat(blockchain.getBlockBody(blocks.get((int) number).getHash())).isEmpty();
    }
  }

  private static EthPeer peer(final EthProtocolVersion version) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(version.getCapability()));
    when(peer.chainState()).thenReturn(new ChainState());
    return peer;
  }

  /** Answers the requests on the calling thread from the chain, through a single peer. */
  private final class FakeSender implements RequestSender {
    private EthProtocolVersion version = EthProtocolVersion.V68;
    private EthPeer peer = peer(version);
    // the first block of each range whose bodies were asked, in request order
    private final List<Long> askedRanges = new ArrayList<>();
    private int requests;
    private boolean failNextBodies;
    // the receipts an eth/70 peer answers at most
    private int pageSize;
    private boolean stallNextPage;
    // the next request is left unanswered and its worker interrupted, as by its owner
    private boolean leaveNextUnanswered;
    private CompletableFuture<?> unanswered;
    // called with the first block of each request, before it is answered
    private Consumer<Long> beforeBodies = first -> {};
    private Consumer<Long> beforeReceipts = first -> {};

    // an eth/70 peer, which pages the receipts it answers
    private void speakEth70(final int receiptsPerPage) {
      version = EthProtocolVersion.V70;
      peer = peer(version);
      pageSize = receiptsPerPage;
    }

    @Override
    public <T> CompletableFuture<Response<T>> submit(
        final SyncRequest<T> request, final Set<EthPeer> excluded) {
      if (++requests > BUDGET) {
        throw new AssertionError("the back-fill asks without end");
      }
      if (leaveNextUnanswered) {
        leaveNextUnanswered = false;
        Thread.currentThread().interrupt();
        final CompletableFuture<Response<T>> pending = new CompletableFuture<>();
        unanswered = pending;
        return pending;
      }
      // an empty or invalid answer fails the request, as with the scheduler
      try {
        return CompletableFuture.completedFuture(
            new Response<>(request.decode(answer(request.message(peer)), peer), peer));
      } catch (final RuntimeException e) {
        return CompletableFuture.failedFuture(e);
      }
    }

    // the answer of a peer that has the whole chain
    private MessageData answer(final MessageData message) {
      return switch (message.getCode()) {
        case EthProtocolMessages.GET_BLOCK_BODIES -> bodies(message);
        case EthProtocolMessages.GET_RECEIPTS ->
            version == EthProtocolVersion.V70 ? page(message) : receipts(message);
        default ->
            throw new IllegalArgumentException("unexpected eth message " + message.getCode());
      };
    }

    private MessageData bodies(final MessageData message) {
      final List<Hash> hashes =
          ImmutableList.copyOf(GetBlockBodiesMessage.readFrom(message).hashes());
      final long first = number(hashes.getFirst());
      askedRanges.add(first);
      beforeBodies.accept(first);
      if (failNextBodies) {
        failNextBodies = false;
        return BlockBodiesMessage.create(List.of());
      }
      return BlockBodiesMessage.create(
          hashes.stream().map(hash -> blocksByHash.get(hash).getBody()).toList());
    }

    // every receipt of the blocks asked, as an eth/68 peer sends them
    private MessageData receipts(final MessageData message) {
      final List<Hash> hashes =
          ImmutableList.copyOf(GetReceiptsMessage.readFrom(message).blockHashes());
      beforeReceipts.accept(number(hashes.getFirst()));
      return ReceiptsMessage.createUnsafe(
          encoded(hashes.stream().map(receiptsByHash::get).toList()));
    }

    // the receipts from the first one asked, cut after a page, as an eth/70 peer sends them
    private MessageData page(final MessageData message) {
      final GetPaginatedReceiptsMessage request = GetPaginatedReceiptsMessage.readFrom(message);
      final List<Hash> hashes = ImmutableList.copyOf(request.blockHashes());
      beforeReceipts.accept(number(hashes.getFirst()));
      final boolean restOfCutBlock = request.firstBlockReceiptIndex() > 0;
      if (stallNextPage && restOfCutBlock) {
        // none of the rest comes
        stallNextPage = false;
        return pageMessage(List.of(List.of()), true);
      }
      final List<List<TransactionReceipt>> page = new ArrayList<>();
      int left = pageSize;
      for (int i = 0; i < hashes.size() && left > 0; i++) {
        final List<TransactionReceipt> all = receiptsByHash.get(hashes.get(i));
        final List<TransactionReceipt> rest =
            i == 0 ? all.subList(request.firstBlockReceiptIndex(), all.size()) : all;
        final int taken = Math.min(left, rest.size());
        page.add(rest.subList(0, taken));
        if (taken < rest.size()) {
          return pageMessage(page, true);
        }
        left -= taken;
      }
      return pageMessage(page, false);
    }

    private static MessageData pageMessage(
        final List<List<TransactionReceipt>> page, final boolean lastBlockIncomplete) {
      final BytesValueRLPOutput flag = new BytesValueRLPOutput();
      flag.writeLongScalar(lastBlockIncomplete ? 1 : 0);
      return new RawMessage(
          EthProtocolMessages.RECEIPTS, Bytes.concatenate(flag.encoded(), encoded(page)));
    }

    private static Bytes encoded(final List<List<TransactionReceipt>> receiptsByBlock) {
      final BytesValueRLPOutput out = new BytesValueRLPOutput();
      out.startList();
      for (final List<TransactionReceipt> receipts : receiptsByBlock) {
        out.startList();
        receipts.forEach(
            receipt ->
                TransactionReceiptEncoder.writeTo(
                    receipt,
                    out,
                    TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
        out.endList();
      }
      out.endList();
      return out.encoded();
    }

    @Override
    public <T> CompletableFuture<Response<T>> submitWithRetries(
        final SyncRequest<T> request, final int attempts) {
      return submit(request);
    }

    @Override
    public int peerCount(final Protocol protocol) {
      return 1;
    }

    @Override
    public int capacity(final Protocol protocol) {
      return 1;
    }

    @Override
    public Executor workers() {
      return Runnable::run;
    }
  }
}
