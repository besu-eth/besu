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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetPaginatedReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BlockDataFetcher.ImportData;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class BlockDataFetcherTest {

  private static final long BODY_SIZE = 64 << 10;
  private static final long WINDOW_BYTES = 5L << 20;
  private static final Duration OPTIONAL_DATA_WAIT = Duration.ofSeconds(10);

  private final ManualScheduledExecutor loop = new ManualScheduledExecutor();
  private final FakeSender sender = new FakeSender();
  private final EthPeers ethPeers = mock(EthPeers.class);
  // the peers that serve block access lists
  private final List<EthPeer> servingPeers = new ArrayList<>();
  private final PendingChain pending =
      new PendingChain(
          new InMemoryKeyValueStorage(),
          new InMemoryKeyValueStorage(),
          new InMemoryKeyValueStorage(),
          new MainnetBlockHeaderFunctions());
  private final TrustedPayloads payloads = new TrustedPayloads(Long.MAX_VALUE);
  private final AtomicInteger arrived = new AtomicInteger();
  private final EthPeer peerA = peer(EthProtocol.LATEST);
  private final EthPeer peerB = peer(EthProtocol.LATEST);
  private final EthPeer peerC = peer(EthProtocol.LATEST);
  private List<BlockHeader> headers;
  private BlockDataFetcher fetcher;

  @Test
  void requestsTheBodiesAscendingFromTheCursor() {
    open(40, false, WINDOW_BYTES * 100);

    fetcher.fill(5, 40, OptionalLong.empty());

    final List<FakeSender.Sent> requests = sender.requests("bodies");
    assertThat(requests).hasSize(2);
    // up to 32 blocks of the initial size estimate, then the rest
    assertThat(numbers(requests.get(0))).containsExactlyElementsOf(range(5, 36));
    assertThat(numbers(requests.get(1))).containsExactlyElementsOf(range(37, 40));
    assertThat(fetcher.blocks()).isEqualTo(36);
  }

  @Test
  void staysWithinTheWindowBytes() {
    open(1000, false, WINDOW_BYTES);

    for (int round = 0; round < 40; round++) {
      fetcher.fill(1, 1000, OptionalLong.empty());
      answerAllBodies();
      // the blocks a request is sent for are admitted by the check before them
      assertThat(fetcher.bytes()).isLessThanOrEqualTo(WINDOW_BYTES + BODY_SIZE);
    }

    // the window is used up to its limit, and requests stopped there
    assertThat(fetcher.bytes()).isGreaterThanOrEqualTo(WINDOW_BYTES);
    final int requested = sender.requests("bodies").size();
    fetcher.fill(1, 1000, OptionalLong.empty());
    assertThat(sender.requests("bodies")).hasSize(requested);
  }

  @Test
  void neverHoldsBackTheBlockAtTheCursor() {
    // a window that is full from the start
    open(50, false, 1);

    fetcher.fill(1, 50, OptionalLong.empty());

    assertThat(sender.requests("bodies")).hasSize(1);
    assertThat(numbers(sender.requests("bodies").getFirst())).containsExactly(1L);
    answerAllBodies();

    fetcher.release(1);
    fetcher.fill(2, 50, OptionalLong.empty());

    assertThat(numbers(sender.requests("bodies").getLast())).containsExactly(2L);
  }

  @Test
  void requestsAgainTheBodiesAPeerLeftOut() {
    open(10, false, WINDOW_BYTES * 100);
    fetcher.fill(1, 10, OptionalLong.empty());
    final FakeSender.Sent first = sender.requests("bodies").getFirst();
    assertThat(numbers(first)).containsExactlyElementsOf(range(1, 10));

    // the peer answers the first four blocks only
    first.answer(blocks(first, 4), peerA);
    loop.runPending();

    assertThat(arrived.get()).isEqualTo(1);
    assertThat(fetcher.take(4, OptionalLong.empty())).isPresent();
    assertThat(fetcher.take(5, OptionalLong.empty())).isEmpty();
    fetcher.fill(1, 10, OptionalLong.empty());
    assertThat(sender.requests("bodies")).hasSize(2);
    assertThat(numbers(sender.requests("bodies").getLast()))
        .containsExactlyElementsOf(range(5, 10));
  }

  @Test
  void asksOtherPeersForEmptyBlockAccessListsAndGivesUpAfterThreeAttempts() {
    open(3, true, WINDOW_BYTES);
    servingPeers.add(peerA);
    fetcher.fill(1, 3, OptionalLong.empty());
    answerAllBodies();
    assertThat(sender.requests("block_access_lists")).hasSize(1);

    final List<EthPeer> answering = List.of(peerA, peerB, peerC);
    for (int attempt = 0; attempt < 3; attempt++) {
      final List<FakeSender.Sent> lists = sender.requests("block_access_lists");
      final FakeSender.Sent request = lists.getLast();
      assertThat(numbers(request)).containsExactly(1L, 2L, 3L);
      // the peers that answered before are left out
      assertThat(request.excluded())
          .containsExactlyInAnyOrderElementsOf(answering.subList(0, attempt));
      // nothing to import while the list may still come from another peer
      assertThat(fetcher.take(1, OptionalLong.empty())).isEmpty();
      request.answer(
          List.of(Optional.<BlockAccessList>empty(), Optional.empty(), Optional.empty()),
          answering.get(attempt));
      loop.runPending();
      fetcher.fill(1, 3, OptionalLong.empty());
    }

    // given up: three peers had none, so the blocks import without it
    assertThat(sender.requests("block_access_lists")).hasSize(3);
    for (long number = 1; number <= 3; number++) {
      assertThat(fetcher.take(number, OptionalLong.empty()))
          .hasValueSatisfying(data -> assertThat(data.blockAccessList()).isEmpty());
    }
  }

  @Test
  void keepsTheBlockAccessListsThatArrive() {
    open(3, true, WINDOW_BYTES);
    servingPeers.add(peerA);
    fetcher.fill(1, 3, OptionalLong.empty());
    answerAllBodies();
    final BlockAccessList list = blockAccessList();

    // the peer has the lists of the first two blocks only
    sender
        .requests("block_access_lists")
        .getFirst()
        .answer(List.of(Optional.of(list), Optional.of(list), Optional.empty()), peerA);
    loop.runPending();

    assertThat(fetcher.take(2, OptionalLong.empty()))
        .hasValueSatisfying(data -> assertThat(data.blockAccessList()).contains(list));
    // the third is asked to the other peers
    assertThat(fetcher.take(3, OptionalLong.empty())).isEmpty();
    fetcher.fill(1, 3, OptionalLong.empty());
    final FakeSender.Sent next = sender.requests("block_access_lists").getLast();
    assertThat(numbers(next)).containsExactly(3L);
    assertThat(next.excluded()).containsExactly(peerA);
  }

  @Test
  void asksForNoBlockAccessListWhenNoPeerServesThem() {
    open(3, true, WINDOW_BYTES);
    // a peer without eth/71 cannot serve them
    servingPeers.add(peer(Capability.create(EthProtocol.NAME, 70)));

    fetcher.fill(1, 3, OptionalLong.empty());
    answerAllBodies();

    assertThat(sender.requests("block_access_lists")).isEmpty();
    for (long number = 1; number <= 3; number++) {
      assertThat(fetcher.take(number, OptionalLong.empty()))
          .hasValueSatisfying(data -> assertThat(data.blockAccessList()).isEmpty());
    }
  }

  @Test
  void requestsReceiptsOnlyForTheBlocksTheBalApplyLimitCovers() {
    open(4, true, WINDOW_BYTES);
    servingPeers.add(peerA);
    final OptionalLong applyUpTo = OptionalLong.of(2);
    fetcher.fill(1, 4, applyUpTo);
    answerAllBodies();
    final BlockAccessList list = blockAccessList();
    sender
        .requests("block_access_lists")
        .getFirst()
        .answer(
            List.of(Optional.of(list), Optional.of(list), Optional.of(list), Optional.of(list)),
            peerA);
    loop.runPending();
    assertThat(sender.requests("receipts")).isEmpty();

    fetcher.fill(1, 4, applyUpTo);

    final List<FakeSender.Sent> receipts = sender.requests("receipts");
    assertThat(receipts).hasSize(1);
    assertThat(numbers(receipts.getFirst())).containsExactly(1L, 2L);
    // the importer waits for the receipts of a block it applies from its list
    assertThat(fetcher.take(1, applyUpTo)).isEmpty();
    assertThat(fetcher.take(3, applyUpTo))
        .hasValueSatisfying(
            data -> {
              assertThat(data.blockAccessList()).contains(list);
              assertThat(data.receipts()).isEmpty();
            });

    receipts.getFirst().answer(completeReceipts(1, 2), peerA);
    loop.runPending();

    assertThat(fetcher.take(1, applyUpTo))
        .hasValueSatisfying(data -> assertThat(data.receipts()).contains(List.of()));
    assertThat(fetcher.take(2, applyUpTo))
        .hasValueSatisfying(data -> assertThat(data.receipts()).contains(List.of()));
  }

  @Test
  void resumesAPartialReceiptsList() {
    open(2, true, WINDOW_BYTES);
    servingPeers.add(peerA);
    final OptionalLong applyUpTo = OptionalLong.of(2);
    fetcher.fill(1, 2, applyUpTo);
    answerAllBodies();
    final BlockAccessList list = blockAccessList();
    sender
        .requests("block_access_lists")
        .getFirst()
        .answer(List.of(Optional.of(list), Optional.of(list)), peerA);
    loop.runPending();
    fetcher.fill(1, 2, applyUpTo);
    final FakeSender.Sent first = sender.requests("receipts").getFirst();
    assertThat(first.firstReceiptIndex()).isZero();

    // the peer sends one receipt of the first block, which does not complete it
    first.answer(
        new GetSyncReceiptsFromPeerTask.Response(
            Map.of(), List.of(new SyncTransactionReceipt(Bytes.of(1, 2, 3)))),
        peerA);
    loop.runPending();
    assertThat(fetcher.take(1, applyUpTo)).isEmpty();
    fetcher.fill(1, 2, applyUpTo);

    // the next request starts after the receipt received, and holds the first block first
    final List<FakeSender.Sent> receipts = sender.requests("receipts");
    assertThat(receipts).hasSize(2);
    assertThat(receipts.getLast().firstReceiptIndex()).isEqualTo(1);
    assertThat(numbers(receipts.getLast())).containsExactly(1L, 2L);
    receipts.getLast().answer(completeReceipts(1, 2), peerA);
    loop.runPending();
    assertThat(fetcher.take(1, applyUpTo))
        .hasValueSatisfying(data -> assertThat(data.receipts()).isPresent());
  }

  @Test
  void neverRequestsABlockOfThePayloadCache() {
    // room for the bodies and the lists of both sides of the cached block
    open(5, true, WINDOW_BYTES * 100);
    sender.capacity = 4;
    servingPeers.add(peerA);
    final BlockAccessList list = blockAccessList();
    final Block cached = new Block(headers.get(3), BlockBody.empty());
    payloads.add(cached, Optional.of(list));

    fetcher.fill(1, 5, OptionalLong.empty());

    // the cached block splits the bodies, its list is not requested either
    final List<FakeSender.Sent> bodies = sender.requests("bodies");
    assertThat(bodies).hasSize(2);
    assertThat(numbers(bodies.get(0))).containsExactly(1L, 2L);
    assertThat(numbers(bodies.get(1))).containsExactly(4L, 5L);
    // the cached block splits the lists as well, its own is known
    final List<FakeSender.Sent> lists = sender.requests("block_access_lists");
    assertThat(lists).hasSize(2);
    assertThat(numbers(lists.get(0))).containsExactly(1L, 2L);
    assertThat(numbers(lists.get(1))).containsExactly(4L, 5L);
    assertThat(fetcher.take(3, OptionalLong.empty()))
        .hasValue(new ImportData(cached, Optional.of(list), Optional.empty()));
  }

  @Test
  void aStuckBlockAccessListRequestExpiresOnceForAllItsBlocks() {
    open(3, true, WINDOW_BYTES);
    servingPeers.add(peerA);
    fetcher.fill(1, 3, OptionalLong.empty());
    answerAllBodies();

    // the request of the lists is never answered
    assertThat(fetcher.take(1, OptionalLong.empty())).isEmpty();
    loop.advance(Duration.ofSeconds(4));
    assertThat(fetcher.take(1, OptionalLong.empty())).isEmpty();
    assertThat(fetcher.nanosUntilExpiry(1)).hasValue(Duration.ofSeconds(6).toNanos());

    loop.advance(Duration.ofSeconds(6));

    // every block of the request goes on without its list at once, none waits again
    assertThat(fetcher.nanosUntilExpiry(2)).hasValue(0);
    for (long number = 1; number <= 3; number++) {
      assertThat(fetcher.take(number, OptionalLong.empty()))
          .hasValueSatisfying(data -> assertThat(data.blockAccessList()).isEmpty());
    }
    assertThat(fetcher.nanosUntilExpiry(1)).isEmpty();
  }

  @Test
  void forgetsTheBlocksAboveANumberAndCancelsTheirRequests() {
    open(10, false, WINDOW_BYTES * 100);
    fetcher.fill(1, 10, OptionalLong.empty());
    final FakeSender.Sent request = sender.requests("bodies").getFirst();
    assertThat(fetcher.blocks()).isEqualTo(10);

    fetcher.resetAbove(4);

    assertThat(request.future().isCancelled()).isTrue();
    assertThat(fetcher.blocks()).isEqualTo(4);
    fetcher.fill(1, 4, OptionalLong.empty());
    // the blocks that stay are requested again
    assertThat(sender.requests("bodies")).hasSize(2);
    assertThat(numbers(sender.requests("bodies").getLast())).containsExactlyElementsOf(range(1, 4));
  }

  @Test
  void releasesTheBytesOfAnImportedBlock() {
    open(3, false, WINDOW_BYTES);
    fetcher.fill(1, 3, OptionalLong.empty());
    answerAllBodies();
    assertThat(fetcher.bytes()).isEqualTo(3 * BODY_SIZE);

    fetcher.release(1);

    assertThat(fetcher.blocks()).isEqualTo(2);
    assertThat(fetcher.bytes()).isEqualTo(2 * BODY_SIZE);
    fetcher.reset();
    assertThat(fetcher.blocks()).isZero();
    assertThat(fetcher.bytes()).isZero();
  }

  private void open(final int blocks, final boolean withLists, final long windowBytes) {
    headers = new ArrayList<>();
    headers.add(new BlockHeaderTestFixture().number(0).buildHeader());
    for (int number = 1; number <= blocks; number++) {
      headers.add(header(headers.getLast(), withLists));
    }
    pending.store(headers.subList(1, headers.size()));
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(
            invocation ->
                servingPeers.stream()
                    .map(
                        peer ->
                            new EthPeerImmutableAttributes(
                                UInt256.ZERO,
                                true,
                                0,
                                0,
                                0,
                                0,
                                false,
                                true,
                                false,
                                true,
                                false,
                                peer)));
    fetcher =
        new BlockDataFetcher(
            sender,
            ethPeers,
            ProtocolScheduleFixture.MAINNET,
            pending,
            payloads,
            loop,
            loop::nanoTime,
            new BackwardSyncConfiguration(
                64,
                16,
                windowBytes,
                8192,
                OPTIONAL_DATA_WAIT,
                3,
                10,
                Duration.ofSeconds(5),
                3,
                1024,
                64L << 20,
                Duration.ofSeconds(30),
                true),
            arrived::incrementAndGet);
  }

  private static EthPeer peer(final Capability capability) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(capability));
    return peer;
  }

  // a block of the proof-of-stake chain, with a block access list hash and receipts if asked
  private static BlockHeader header(final BlockHeader parent, final boolean withList) {
    final long number = parent.getNumber() + 1;
    final BlockHeaderTestFixture fixture =
        new BlockHeaderTestFixture()
            .number(number)
            .parentHash(parent.getHash())
            .timestamp(number * 12)
            .gasLimit(30_000_000);
    if (withList) {
      fixture
          .baseFeePerGas(Wei.of(7))
          .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
          .blobGasUsed(0L)
          .excessBlobGas(BlobGas.ZERO)
          .parentBeaconBlockRoot(Optional.of(Bytes32.ZERO))
          .requestsHash(Hash.EMPTY_REQUESTS_HASH)
          .balHash(Hash.hash(Bytes.ofUnsignedLong(number)))
          .receiptsRoot(Hash.hash(Bytes.ofUnsignedLong(number + 1_000)));
    }
    return fixture.buildHeader();
  }

  private static BlockAccessList blockAccessList() {
    return new BlockAccessList(List.of(), Bytes.of(1, 2, 3));
  }

  private static List<Long> range(final long from, final long to) {
    return LongStream.rangeClosed(from, to).boxed().toList();
  }

  // the block numbers a request is for
  private List<Long> numbers(final FakeSender.Sent request) {
    return request.hashes().stream()
        .map(hash -> headers.stream().filter(header -> header.getHash().equals(hash)).findFirst())
        .map(header -> header.orElseThrow().getNumber())
        .toList();
  }

  // the first blocks a body request is for, as a peer serves them
  private List<Block> blocks(final FakeSender.Sent request, final int count) {
    return numbers(request).stream()
        .limit(count)
        .map(
            number -> new Block(headers.get(number.intValue()), BlockBody.empty(), (int) BODY_SIZE))
        .toList();
  }

  private void answerAllBodies() {
    for (final FakeSender.Sent request : sender.requests("bodies")) {
      if (!request.answered()) {
        request.answer(blocks(request, request.hashes().size()), peerA);
      }
    }
    loop.runPending();
  }

  // the receipts of every block asked, none of them having any transaction
  private GetSyncReceiptsFromPeerTask.Response completeReceipts(final long from, final long to) {
    final Map<SyncBlock, List<SyncTransactionReceipt>> byBlock = new HashMap<>();
    for (long number = from; number <= to; number++) {
      final Block block = new Block(headers.get((int) number), BlockBody.empty());
      byBlock.put(SyncBlock.of(block, ProtocolScheduleFixture.MAINNET), List.of());
    }
    return new GetSyncReceiptsFromPeerTask.Response(byBlock, List.of());
  }

  /** Records the requests, which the test answers when it says. */
  private static final class FakeSender implements RequestSender {
    // the requests the peers take at once
    private int capacity = 1;

    private final EthPeer requester = mock(EthPeer.class);
    private final List<Sent> sent = new ArrayList<>();

    private FakeSender() {
      when(requester.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
    }

    private List<Sent> requests(final String name) {
      return sent.stream().filter(request -> request.name().equals(name)).toList();
    }

    @Override
    public <T> CompletableFuture<Response<T>> submit(
        final SyncRequest<T> request, final Set<EthPeer> excluded) {
      final MessageData message = request.message(requester);
      final CompletableFuture<Response<T>> future = new CompletableFuture<>();
      final Sent entry =
          switch (request.name()) {
            case "bodies" ->
                new Sent(
                    request.name(),
                    hashes(GetBlockBodiesMessage.readFrom(message).hashes()),
                    excluded,
                    0,
                    future);
            case "block_access_lists" ->
                new Sent(
                    request.name(),
                    hashes(GetBlockAccessListsMessage.readFrom(message).blockHashes()),
                    excluded,
                    0,
                    future);
            default -> {
              final GetPaginatedReceiptsMessage receipts =
                  GetPaginatedReceiptsMessage.readFrom(message);
              yield new Sent(
                  request.name(),
                  hashes(receipts.blockHashes()),
                  excluded,
                  receipts.firstBlockReceiptIndex(),
                  future);
            }
          };
      sent.add(entry);
      return future;
    }

    @Override
    public <T> CompletableFuture<Response<T>> submitWithRetries(
        final SyncRequest<T> request, final int attempts) {
      return submit(request, Set.of());
    }

    @Override
    public int peerCount(final Protocol protocol) {
      return 1;
    }

    @Override
    public int capacity(final Protocol protocol) {
      return capacity;
    }

    @Override
    public Executor workers() {
      return Runnable::run;
    }

    private static List<Hash> hashes(final Iterable<Hash> hashes) {
      final List<Hash> list = new ArrayList<>();
      hashes.forEach(list::add);
      return list;
    }

    /** A request sent, and the future its sender answers. */
    private static final class Sent {
      private final String name;
      private final List<Hash> hashes;
      private final Set<EthPeer> excluded;
      private final long firstReceiptIndex;
      private final CompletableFuture<?> future;
      private boolean answered;

      private Sent(
          final String name,
          final List<Hash> hashes,
          final Set<EthPeer> excluded,
          final long firstReceiptIndex,
          final CompletableFuture<?> future) {
        this.name = name;
        this.hashes = hashes;
        this.excluded = excluded;
        this.firstReceiptIndex = firstReceiptIndex;
        this.future = future;
      }

      private String name() {
        return name;
      }

      private List<Hash> hashes() {
        return hashes;
      }

      private Set<EthPeer> excluded() {
        return excluded;
      }

      private long firstReceiptIndex() {
        return firstReceiptIndex;
      }

      private CompletableFuture<?> future() {
        return future;
      }

      private boolean answered() {
        return answered;
      }

      @SuppressWarnings("unchecked")
      private void answer(final Object value, final EthPeer peer) {
        answered = true;
        ((CompletableFuture<Response<Object>>) future).complete(new Response<>(value, peer));
      }
    }
  }
}
