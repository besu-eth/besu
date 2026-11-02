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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Requests going through real peers, request managers and response streams. */
@Timeout(30)
class RequestSchedulerTest {

  private static final Duration TIMEOUT = Duration.ofMillis(500);

  private MutableBlockchain blockchain;
  private ProtocolSchedule protocolSchedule;
  private EthProtocolManager ethProtocolManager;
  private EthPeers ethPeers;
  private RespondingEthPeer peer;
  private TestLoop loop;
  private RequestScheduler scheduler;
  // a clock that only moves when a test moves it
  private final AtomicLong clock = new AtomicLong();

  @BeforeEach
  void setUp() {
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    setup.importFirstBlocks(10);
    blockchain = setup.getBlockchain();
    protocolSchedule = setup.getProtocolSchedule();
    ethProtocolManager = EthProtocolManagerTestBuilder.builder().setBlockchain(blockchain).build();
    ethPeers = ethProtocolManager.ethContext().getEthPeers();
    peer = newPeer();
    scheduler = newScheduler(TIMEOUT, 4, System::nanoTime);
  }

  @AfterEach
  void tearDown() {
    scheduler.stop();
  }

  @Test
  void deliversTheResponseOfAPeer() throws Exception {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(headers(1, 5));

    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !response.isDone());

    assertThat(response.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void absorbsAResponseThatArrivesAfterTheTimeout() throws Exception {
    // the request is abandoned one timeout after it timed out: held, it waits for the test
    loop.hold();
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> late =
        scheduler.submit(headers(1, 2));
    await().until(() -> loop.heldCount() == 1);
    loop.release();
    assertFailure(() -> late.get(5, TimeUnit.SECONDS), Reason.TIMEOUT);

    // the late response is absorbed, not counted as the answer to an unknown request
    final int score = peer.getEthPeer().getReputation().getScore();
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));
    assertThat(peer.getEthPeer().getReputation().getScore()).isEqualTo(score);

    // and the peer keeps serving
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());

    assertThat(next.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L, 4L);
    assertThat(peer.getEthPeer().isDisconnected()).isFalse();
  }

  @Test
  void givesBackTheSlotOfARequestNeverAnswered() throws Exception {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> lost =
        scheduler.submit(headers(1, 2));
    assertFailure(() -> lost.get(5, TimeUnit.SECONDS), Reason.TIMEOUT);
    // the peer drops it without ever answering
    peer.respond((capability, ethPeer, message) -> Optional.empty());

    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());

    assertThat(next.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L, 4L);
  }

  @Test
  void sendsTheMostUrgentRequestFirst() {
    // two requests take the two slots the peer starts with
    scheduler.submit(headers(1, 1));
    scheduler.submit(headers(2, 1));
    await().until(() -> peer.streamPendingOutgoingRequests().count() == 2);
    scheduler.submit(headers(3, 1));
    scheduler.submit(headers(4, 1, Priority.PIVOT));

    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));

    await().until(() -> peer.streamPendingOutgoingRequests().count() == 2);
    final GetBlockHeadersMessage next =
        GetBlockHeadersMessage.readFrom(
            peer.peekNextOutgoingRequest().orElseThrow().unwrapMessageData().getValue());
    assertThat(next.blockNumber()).hasValue(4L);
  }

  @Test
  void keepsAskingAPeerThatHadNothingForAnEarlierRequest() throws Exception {
    // the clock stands still: a back-off would hold the peer for ever
    newScheduler(TIMEOUT, 4, clock::get);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> pruned =
        scheduler.submit(headers(1, 2));
    await().until(peer::hasOutstandingRequests);
    peer.respond(
        (capability, ethPeer, message) -> Optional.of(BlockHeadersMessage.create(List.of())));
    assertFailure(() -> pruned.get(5, TimeUnit.SECONDS), Reason.EMPTY);

    // the empty answer is recorded before the next request is dispatched, and asked to the peer
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    await().until(() -> nextRequestedHeader().equals(OptionalLong.of(3)));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());
    assertThat(next.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L, 4L);
  }

  @Test
  void neverSendsARequestToAnExcludedPeer() {
    // a clock that never reaches the queue timeout: the request fails at once
    newScheduler(TIMEOUT, 4, clock::get);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(headers(1, 2), Set.of(peer.getEthPeer()));

    assertFailure(() -> response.get(10, TimeUnit.SECONDS), Reason.NO_PEER);
    assertThat(peer.hasOutstandingRequests()).isFalse();
  }

  @Test
  void failsARequestOnceItsAttemptsRunOut() {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submitWithRetries(headers(1, 2), 2);

    // the peer never answers: each attempt goes to it, the one left out only waits a pause
    assertFailure(() -> response.get(10, TimeUnit.SECONDS), Reason.TIMEOUT);
    assertThat(peer.streamPendingOutgoingRequests()).hasSize(2);
  }

  @Test
  void retriesWithTheOnlyPeerWithoutWaitingForTheQueueTimeout() throws Exception {
    // a queue timeout of a minute
    newScheduler(Duration.ofSeconds(10), 4, System::nanoTime);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submitWithRetries(headers(1, 2), 5);
    await().until(peer::hasOutstandingRequests);
    peer.respond(
        (capability, ethPeer, message) -> Optional.of(BlockHeadersMessage.create(List.of())));

    // the only peer had nothing: the retry goes back to it after a pause
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> nextRequestedHeader().equals(OptionalLong.of(1)));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !response.isDone());
    assertThat(response.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(1L, 2L);
  }

  @Test
  void keepsARequestThatOnlyWaitsForABusyPeer() throws Exception {
    newScheduler(TIMEOUT, 4, clock::get);
    loop.hold();
    scheduler.submit(headers(1, 1));
    scheduler.submit(headers(2, 1));
    await().until(() -> peer.streamPendingOutgoingRequests().count() == 2);
    final CountDownLatch queued = new CountDownLatch(1);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> waiting =
        scheduler.submit(
            new Forwarding<>(headers(3, 1)) {
              @Override
              public Protocol protocolFor(final EthPeer ethPeer) {
                queued.countDown();
                return super.protocolFor(ethPeer);
              }
            });
    assertThat(queued.await(5, TimeUnit.SECONDS)).isTrue();

    // far past the queue timeout, a later request shows a dispatch saw the waiting one
    clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
    final CountDownLatch dispatched = new CountDownLatch(1);
    scheduler.submit(
        new Forwarding<>(headers(4, 1)) {
          @Override
          public Protocol protocolFor(final EthPeer ethPeer) {
            dispatched.countDown();
            return super.protocolFor(ethPeer);
          }

          @Override
          public boolean canServe(final EthPeer ethPeer) {
            return false;
          }
        });
    assertThat(dispatched.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(waiting).isNotDone();

    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !waiting.isDone());
    assertThat(waiting.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L);
  }

  @Test
  void ranksAPeerThatAnswersSlowlyAboveOneThatTimedOut() throws Exception {
    newScheduler(TIMEOUT, 4, clock::get);
    final RespondingEthPeer other = newPeer();

    // two small answers that take 200 ms each
    for (int i = 0; i < 2; i++) {
      final CompletableFuture<RequestSender.Response<List<BlockHeader>>> slow =
          scheduler.submit(headers(1, 2), Set.of(other.getEthPeer()));
      await().until(peer::hasOutstandingRequests);
      clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(200));
      peer.respond(RespondingEthPeer.blockchainResponder(blockchain));
      slow.get(5, TimeUnit.SECONDS);
    }

    // a timeout of the other peer, whose late answer gives its slot back
    loop.hold();
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> lost =
        scheduler.submit(headers(1, 2), Set.of(peer.getEthPeer()));
    await().until(() -> loop.heldCount() == 1);
    loop.release();
    assertFailure(() -> lost.get(5, TimeUnit.SECONDS), Reason.TIMEOUT);
    other.respond(RespondingEthPeer.blockchainResponder(blockchain));

    // once its back-off is over, the peer that answered is still preferred
    clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
    scheduler.submit(headers(3, 2));
    await().until(() -> peer.hasOutstandingRequests() || other.hasOutstandingRequests());
    assertThat(peer.hasOutstandingRequests()).isTrue();
    assertThat(other.hasOutstandingRequests()).isFalse();
  }

  @Test
  void keepsTheRankOfAPeerWhoseAnswerFailsALocalCheck() throws Exception {
    newScheduler(TIMEOUT, 4, clock::get);
    final RespondingEthPeer other = newPeer();
    // the peer answers in 100 ms, the other in a second
    answer(peer, other, 100);
    answer(other, peer, 1_000);

    // an answer of the peer that fails our check, after 5 s
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> failed =
        scheduler.submit(
            new Forwarding<>(headers(1, 2)) {
              @Override
              public List<BlockHeader> decode(final MessageData message, final EthPeer ethPeer) {
                throw new IllegalArgumentException("local check");
              }
            },
            Set.of(other.getEthPeer()));
    await().until(peer::hasOutstandingRequests);
    clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));
    assertFailure(() -> failed.get(5, TimeUnit.SECONDS), Reason.INVALID);

    // the peer stays the faster one
    scheduler.submit(headers(3, 2));
    await().until(() -> peer.hasOutstandingRequests() || other.hasOutstandingRequests());
    assertThat(peer.hasOutstandingRequests()).isTrue();
    assertThat(other.hasOutstandingRequests()).isFalse();
  }

  @Test
  void backsOffOnceForTheTimeoutsOfRequestsSentTogether() throws Exception {
    newScheduler(TIMEOUT, 4, clock::get);
    loop.hold();
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> first =
        scheduler.submit(headers(1, 1));
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> second =
        scheduler.submit(headers(2, 1));
    await().until(() -> loop.heldCount() == 2);

    // both time out a second after they went out, and their late answers give the slots back
    clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
    loop.release();
    assertFailure(() -> first.get(5, TimeUnit.SECONDS), Reason.TIMEOUT);
    assertFailure(() -> second.get(5, TimeUnit.SECONDS), Reason.TIMEOUT);
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));

    // a single back-off of a second, not one per timeout
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_500));
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 1));
    await().until(peer::hasOutstandingRequests);
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));
    assertThat(next.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L);
  }

  @Test
  void failsARequestWhoseCodeThrowsWithoutHoldingTheOthers() throws Exception {
    final IllegalStateException bug = new IllegalStateException("bug");
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> broken =
        scheduler.submit(
            new Forwarding<>(headers(1, 2)) {
              @Override
              public boolean canServe(final EthPeer ethPeer) {
                throw bug;
              }
            });

    assertThatThrownBy(() -> broken.get(5, TimeUnit.SECONDS)).hasCause(bug);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());
    assertThat(next.get(5, TimeUnit.SECONDS).value()).hasSize(2);
  }

  @Test
  void failsARequestWhoseMessageCannotBeBuiltWithoutBackingOffThePeer() throws Exception {
    // the clock stands still: a back-off would hold the peer for ever
    newScheduler(TIMEOUT, 4, clock::get);
    final IllegalStateException bug = new IllegalStateException("bug");
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> broken =
        scheduler.submitWithRetries(
            new Forwarding<>(headers(1, 2)) {
              @Override
              public MessageData message(final EthPeer ethPeer) {
                throw bug;
              }
            });

    // not retried: it would fail on any peer
    assertThatThrownBy(() -> broken.get(5, TimeUnit.SECONDS)).hasCause(bug);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());
    assertThat(next.get(5, TimeUnit.SECONDS).value()).hasSize(2);
  }

  @Test
  void keepsAPeerWhoseAnswerFailsALocalCheck() {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(
            new Forwarding<>(headers(1, 2)) {
              @Override
              public List<BlockHeader> decode(final MessageData message, final EthPeer ethPeer) {
                throw new IllegalArgumentException("local check");
              }
            });
    await().until(peer::hasOutstandingRequests);
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));

    assertFailure(() -> response.get(5, TimeUnit.SECONDS), Reason.INVALID);
    assertThat(peer.getEthPeer().isDisconnected()).isFalse();
  }

  @Test
  void disconnectsAPeerThatSendsAMalformedAnswer() {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(
            new Forwarding<>(headers(1, 2)) {
              @Override
              public List<BlockHeader> decode(final MessageData message, final EthPeer ethPeer) {
                throw new RLPException("malformed");
              }
            });
    await().until(peer::hasOutstandingRequests);
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));

    assertFailure(() -> response.get(5, TimeUnit.SECONDS), Reason.INVALID);
    assertThat(peer.getEthPeer().isDisconnected()).isTrue();
  }

  @Test
  void queuesTheBookkeepingOfAnAnswerBeforeTheCallerContinues() throws Exception {
    // a single slot, held until the answer is recorded
    newScheduler(TIMEOUT, 1, System::nanoTime);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> first =
        scheduler.submit(headers(1, 2));
    // the continuation runs on the worker and blocks it until the next request goes out
    final CompletableFuture<Boolean> nextSent =
        first.thenApply(
            response -> {
              scheduler.submit(headers(3, 2));
              try {
                await()
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> nextRequestedHeader().equals(OptionalLong.of(3)));
                return true;
              } catch (final RuntimeException timedOut) {
                return false;
              }
            });
    await().until(peer::hasOutstandingRequests);
    peer.respond(RespondingEthPeer.blockchainResponder(blockchain));

    assertThat(nextSent.get(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void failsARequestSubmittedWhileTheLoopShutsDown() {
    // what a concurrent stop leaves to a submit: the loop shut down, the stop not seen yet
    loop.shutdownNow();

    assertFailure(() -> scheduler.submit(headers(1, 1)).get(5, TimeUnit.SECONDS), Reason.STOPPED);
    assertFailure(
        () -> scheduler.submitWithRetries(headers(1, 1)).get(5, TimeUnit.SECONDS), Reason.STOPPED);
  }

  @Test
  void failsARetryStoppedDuringItsPause() {
    loop.hold();
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submitWithRetries(headers(1, 2), 5);
    await().until(() -> loop.heldCount() == 1);
    // the timeout, then the only peer is left out: the retry waits a pause
    loop.release();
    await().until(() -> loop.heldCount() == 2);

    scheduler.stop();

    assertFailure(() -> response.get(5, TimeUnit.SECONDS), Reason.STOPPED);
  }

  @Test
  void closesTheStreamsOfUnansweredRequestsWhenStopped() {
    loop.hold();
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(headers(1, 2));
    await().until(peer::hasOutstandingRequests);
    assertThat(peer.getEthPeer().outstandingRequests()).isEqualTo(1);

    scheduler.stop();

    assertFailure(() -> response.get(5, TimeUnit.SECONDS), Reason.STOPPED);
    assertThat(peer.getEthPeer().outstandingRequests()).isZero();
  }

  @Test
  void runsTheWorkQueuedOnItsWorkersAfterItStopped() throws Exception {
    final CountDownLatch release = new CountDownLatch(1);
    for (int i = 0; i < 2; i++) {
      CompletableFuture.runAsync(
          () -> {
            try {
              release.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          },
          scheduler.workers());
    }
    final CompletableFuture<Integer> queued =
        CompletableFuture.supplyAsync(() -> 1, scheduler.workers());

    scheduler.stop();
    release.countDown();

    assertThat(queued.get(5, TimeUnit.SECONDS)).isEqualTo(1);
  }

  private static void assertFailure(final ThrowingCallable call, final Reason reason) {
    assertThatThrownBy(call)
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOfSatisfying(
            RequestFailedException.class,
            failure -> assertThat(failure.reason()).isEqualTo(reason));
  }

  // replaces the scheduler of the test
  private RequestScheduler newScheduler(
      final Duration timeout, final int maxEthRequests, final LongSupplier nanoTime) {
    if (scheduler != null) {
      scheduler.stop();
    }
    loop = new TestLoop();
    scheduler =
        new RequestScheduler(
            ethPeers, new NoOpMetricsSystem(), timeout, 4, maxEthRequests, 2, loop, nanoTime);
    return scheduler;
  }

  private RespondingEthPeer newPeer() {
    return RespondingEthPeer.builder()
        .ethProtocolManager(ethProtocolManager)
        .chainHeadHash(blockchain.getChainHeadHash())
        .totalDifficulty(blockchain.getChainHead().getTotalDifficulty())
        .estimatedHeight(blockchain.getChainHeadBlockNumber())
        .build();
  }

  // a request the peer answers after the given time, the other peer left out
  private void answer(
      final RespondingEthPeer answering, final RespondingEthPeer leftOut, final long millis)
      throws Exception {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(headers(1, 2), Set.of(leftOut.getEthPeer()));
    await().until(answering::hasOutstandingRequests);
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    answering.respond(RespondingEthPeer.blockchainResponder(blockchain));
    response.get(5, TimeUnit.SECONDS);
  }

  private OptionalLong nextRequestedHeader() {
    return peer.peekNextOutgoingRequest()
        .map(
            request ->
                GetBlockHeadersMessage.readFrom(request.unwrapMessageData().getValue())
                    .blockNumber())
        .orElse(OptionalLong.empty());
  }

  private PeerTaskRequest<List<BlockHeader>> headers(final long first, final int count) {
    return headers(first, count, Priority.HEADERS);
  }

  private PeerTaskRequest<List<BlockHeader>> headers(
      final long first, final int count, final Priority priority) {
    return new PeerTaskRequest<>(
        () -> new GetHeadersFromPeerTask(first, count, 0, Direction.FORWARD, protocolSchedule),
        priority,
        "headers");
  }

  /** A request that does what another does, unless a test overrides it. */
  private static class Forwarding<T> implements SyncRequest<T> {
    private final SyncRequest<T> delegate;

    private Forwarding(final SyncRequest<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Protocol protocol() {
      return delegate.protocol();
    }

    @Override
    public Protocol protocolFor(final EthPeer peer) {
      return delegate.protocolFor(peer);
    }

    @Override
    public Priority priority() {
      return delegate.priority();
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public MessageData message(final EthPeer peer) {
      return delegate.message(peer);
    }

    @Override
    public T decode(final MessageData response, final EthPeer peer) {
      return delegate.decode(response, peer);
    }

    @Override
    public boolean canServe(final EthPeer peer) {
      return delegate.canServe(peer);
    }
  }
}
