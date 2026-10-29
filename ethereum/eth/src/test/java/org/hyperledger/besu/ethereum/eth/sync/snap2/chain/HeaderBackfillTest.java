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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class HeaderBackfillTest {

  private static final int HEADERS_PER_REQUEST = 10;
  // more than the segments, so the pipeline buffers hold them all and its stages run in turn
  private static final int PARALLELISM = 16;

  private final List<BlockHeader> chain = chain(101);
  private final FakeSender sender = new FakeSender();
  private final QueuedExecutor executor = new QueuedExecutor();
  // each list the back-fill handed to its store
  private final List<List<BlockHeader>> batches = new ArrayList<>();

  @Test
  void storesTheHeadersDownToTheFloor() {
    sender.serve(chain);
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(100));

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThat(done).isCompleted();
    assertThat(backfill.lowestStored()).isEqualTo(1);
    assertThat(backfill.done()).isEqualTo(backfill.total()).isEqualTo(99);
    // top down, each segment highest first
    assertThat(stored()).containsExactlyElementsOf(chain.subList(1, 100).reversed());
  }

  @Test
  void runsOnce() {
    final HeaderBackfill backfill = backfill(chain.get(10), chain.get(10));
    backfill.run(executor);

    assertThatThrownBy(() -> backfill.run(executor)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void followsTheTrustedHeaderWhenPeersServeAnotherChain() {
    // the peers serve by number a fork from block 31, the top stays on the trusted chain
    sender.serve(extend(chain.subList(0, 31), 60, 1));
    sender.know(chain);
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(60));

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThat(done).isCompleted();
    assertThat(stored()).containsExactlyElementsOf(chain.subList(1, 60).reversed());
  }

  @Test
  void completesShortAnswersWithinTheirSegment() {
    // peers answer four headers at most, so each segment goes on down by hash
    sender.answerAtMost(4);
    sender.serve(chain);
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(60));

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThat(done).isCompleted();
    assertThat(stored()).containsExactlyElementsOf(chain.subList(1, 60).reversed());
    // each segment stored whole, not completed by the next one
    assertThat(batches).extracting(List::size).containsExactly(10, 10, 10, 10, 10, 9);
  }

  @Test
  void fetchesByHashASegmentThePeersDoNotServeByNumber() {
    // blocks 20 to 29 are known by hash only, so their request by number fails
    sender.serve(chain.subList(0, 20));
    sender.serve(chain.subList(30, 101));
    sender.know(chain);
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(60));

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThat(done).isCompleted();
    assertThat(stored()).containsExactlyElementsOf(chain.subList(1, 60).reversed());
  }

  @Test
  void failsWhenTheHeadersDoNotLeadToTheFloor() {
    sender.serve(chain);
    final BlockHeader otherFloor = new BlockHeaderTestFixture().number(5).buildHeader();
    final HeaderBackfill backfill = backfill(otherFloor, chain.get(20));

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThatThrownBy(done::join)
        .hasCauseInstanceOf(WrongChainException.class)
        .hasMessageContaining("does not lead to block 5");
  }

  @Test
  void storesNothingOnceStopped() {
    sender.serve(chain);
    final List<GetBlockHeadersMessage> asked = new ArrayList<>();
    sender.beforeAnswer = asked::add;
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(100));
    backfill.stop();

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThatThrownBy(done::join).hasCauseInstanceOf(CancellationException.class);
    assertThat(asked).isEmpty();
    assertThat(batches).isEmpty();
  }

  @Test
  void storesNothingOnceStoppedWhileASegmentIsFetchedAgain() {
    sender.serve(chain);
    // a peer serves another block 29 by number, so the segment from 29 down is fetched by hash
    sender.serve(List.of(child(chain.get(28), 1)));
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(60));
    sender.beforeAnswer =
        message -> {
          if (message.hash().isPresent()) {
            backfill.stop();
          }
        };

    final CompletableFuture<Void> done = runToEnd(backfill);

    assertThatThrownBy(done::join).hasCauseInstanceOf(CancellationException.class);
    // the segments above the one fetched again
    assertThat(stored()).containsExactlyElementsOf(chain.subList(30, 60).reversed());
  }

  @Test
  void checksTheTopItselfWhenItIsTheFloor() {
    final CompletableFuture<Void> same = backfill(chain.get(10), chain.get(10)).run(executor);
    final CompletableFuture<Void> other =
        backfill(chain.get(10), child(chain.get(9), 1)).run(executor);

    assertThat(same).isCompleted();
    assertThatThrownBy(other::join).hasCauseInstanceOf(WrongChainException.class);
  }

  @Test
  void cancelsItsRequestsByNumberOnStop() {
    sender.serve(chain);
    sender.leaveUnanswered();
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(100));
    // stopped while it sends its third request, the first two in flight
    sender.beforeAnswer =
        message -> {
          if (sender.unanswered.size() == 2) {
            backfill.stop();
          }
        };

    runToEnd(backfill);

    // or the scheduler would retry them for nobody
    assertThat(sender.unanswered).hasSize(3).allMatch(CompletableFuture::isCancelled);
  }

  @Test
  void stopsWithoutItsLockWhileThePipelineRunsItsCallbacks() {
    sender.serve(chain);
    final HeaderBackfill backfill = backfill(chain.getFirst(), chain.get(100));
    sender.beforeAnswer = message -> backfill.stop();
    final CompletableFuture<Void> done = backfill.run(executor);
    // else a stop from a callback deadlocks with another stop holding our lock
    final List<Boolean> locked = new ArrayList<>();
    done.whenComplete((unused, error) -> locked.add(Thread.holdsLock(backfill)));

    executor.runAll();

    assertThat(locked).containsExactly(false);
  }

  private HeaderBackfill backfill(final BlockHeader floor, final BlockHeader top) {
    return new HeaderBackfill(
        sender,
        ProtocolScheduleFixture.MAINNET,
        batches::add,
        new HeaderFetcher(
            sender, ProtocolScheduleFixture.MAINNET, Optional::empty, HEADERS_PER_REQUEST),
        floor,
        top,
        HEADERS_PER_REQUEST,
        PARALLELISM,
        new NoOpMetricsSystem());
  }

  // runs the back-fill to its end, its pipeline stages in turn on the test thread
  private CompletableFuture<Void> runToEnd(final HeaderBackfill backfill) {
    final CompletableFuture<Void> done = backfill.run(executor);
    executor.runAll();
    return done;
  }

  // every header stored, in the order stored
  private List<BlockHeader> stored() {
    return batches.stream().flatMap(List::stream).toList();
  }

  // headers 0 to count - 1, each the parent of the next
  private static List<BlockHeader> chain(final int count) {
    return extend(List.of(new BlockHeaderTestFixture().number(0).buildHeader()), count - 1, 0);
  }

  // the base, then a chain of its own up to the highest block
  private static List<BlockHeader> extend(
      final List<BlockHeader> base, final long highest, final int fork) {
    final List<BlockHeader> headers = new ArrayList<>(base);
    while (headers.getLast().getNumber() < highest) {
      headers.add(child(headers.getLast(), fork));
    }
    return headers;
  }

  // the next header, stamped apart for each fork
  private static BlockHeader child(final BlockHeader parent, final int fork) {
    final long number = parent.getNumber() + 1;
    return new BlockHeaderTestFixture()
        .number(number)
        .parentHash(parent.getHash())
        .timestamp(1_000L * fork + number)
        .buildHeader();
  }

  /**
   * Answers header requests on the calling thread: by number from the chain it serves, by hash from
   * every header it knows.
   */
  private static final class FakeSender implements RequestSender {
    private final EthPeer peer = mock(EthPeer.class);
    private final Map<Long, BlockHeader> served = new HashMap<>();
    private final Map<Hash, BlockHeader> known = new HashMap<>();
    private int maxAnswer = Integer.MAX_VALUE;
    // called with each request before it is answered
    private Consumer<GetBlockHeadersMessage> beforeAnswer = message -> {};
    // the requests left unanswered, once it stops answering
    private final List<CompletableFuture<?>> unanswered = new ArrayList<>();
    private boolean answers = true;

    private FakeSender() {
      when(peer.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
      // header tasks report the height they see to the peer
      when(peer.chainState()).thenReturn(new ChainState());
    }

    // served by number, and known by hash
    private void serve(final List<BlockHeader> headers) {
      headers.forEach(header -> served.put(header.getNumber(), header));
      know(headers);
    }

    // known by hash only
    private void know(final List<BlockHeader> headers) {
      headers.forEach(header -> known.put(header.getHash(), header));
    }

    // answers at most this many headers, as a peer may
    private void answerAtMost(final int headers) {
      maxAnswer = headers;
    }

    private void leaveUnanswered() {
      answers = false;
    }

    @Override
    public <T> CompletableFuture<Response<T>> submit(
        final SyncRequest<T> request, final Set<EthPeer> excluded) {
      final GetBlockHeadersMessage message = GetBlockHeadersMessage.readFrom(request.message(peer));
      beforeAnswer.accept(message);
      if (!answers) {
        final CompletableFuture<Response<T>> pending = new CompletableFuture<>();
        unanswered.add(pending);
        return pending;
      }
      // a reverse request, from the header asked for down its own chain
      BlockHeader header =
          message
              .hash()
              .map(known::get)
              .orElseGet(() -> served.get(message.blockNumber().getAsLong()));
      final int limit = Math.min(message.maxHeaders(), maxAnswer);
      final List<BlockHeader> headers = new ArrayList<>();
      while (header != null && headers.size() < limit) {
        headers.add(header);
        header = known.get(header.getParentHash());
      }
      // an invalid answer fails the request, as with the real sender
      try {
        return CompletableFuture.completedFuture(
            new Response<>(request.decode(BlockHeadersMessage.create(headers), peer), peer));
      } catch (final RuntimeException e) {
        return CompletableFuture.failedFuture(e);
      }
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
