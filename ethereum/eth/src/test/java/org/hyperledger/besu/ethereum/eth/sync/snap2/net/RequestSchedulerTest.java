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
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Requests going through real peers, request managers and response streams. */
@Timeout(30)
class RequestSchedulerTest {

  private MutableBlockchain blockchain;
  private ProtocolSchedule protocolSchedule;
  private RespondingEthPeer peer;
  private RequestScheduler scheduler;

  @BeforeEach
  void setUp() {
    final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    setup.importFirstBlocks(10);
    blockchain = setup.getBlockchain();
    protocolSchedule = setup.getProtocolSchedule();
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder().setBlockchain(blockchain).build();
    peer =
        RespondingEthPeer.builder()
            .ethProtocolManager(ethProtocolManager)
            .chainHeadHash(blockchain.getChainHeadHash())
            .totalDifficulty(blockchain.getChainHead().getTotalDifficulty())
            .estimatedHeight(blockchain.getChainHeadBlockNumber())
            .build();
    scheduler =
        new RequestScheduler(
            ethProtocolManager.ethContext().getEthPeers(),
            new NoOpMetricsSystem(),
            Duration.ofMillis(500),
            4,
            4,
            2);
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
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> late =
        scheduler.submit(headers(1, 2));
    assertThatThrownBy(() -> late.get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RequestFailedException.class);

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
    assertThatThrownBy(() -> lost.get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RequestFailedException.class);
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
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> pruned =
        scheduler.submit(headers(1, 2));
    await().until(peer::hasOutstandingRequests);
    peer.respond(
        (capability, ethPeer, message) -> Optional.of(BlockHeadersMessage.create(List.of())));
    assertThatThrownBy(() -> pruned.get(5, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOfSatisfying(
            RequestFailedException.class,
            failure -> assertThat(failure.reason()).isEqualTo(RequestFailedException.Reason.EMPTY));

    // once the empty answer is recorded, the peer is asked again right away, not after a back-off
    Thread.sleep(200);
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> next =
        scheduler.submit(headers(3, 2));
    // a back-off would hold it for a second
    await()
        .atMost(Duration.ofMillis(500))
        .until(() -> nextRequestedHeader().equals(OptionalLong.of(3)));
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !next.isDone());
    assertThat(next.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(3L, 4L);
  }

  @Test
  void neverSendsARequestToAnExcludedPeer() {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submit(headers(1, 2), Set.of(peer.getEthPeer()));

    assertThatThrownBy(() -> response.get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOfSatisfying(
            RequestFailedException.class,
            failure ->
                assertThat(failure.reason()).isEqualTo(RequestFailedException.Reason.NO_PEER));
    assertThat(peer.hasOutstandingRequests()).isFalse();
  }

  @Test
  void retriesARequestUntilAPeerAnswersIt() throws Exception {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submitWithRetries(headers(1, 2), 5);

    // the first attempt times out, then the peer is tried again once no other peer is left
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> peer.streamPendingOutgoingRequests().count() == 2);
    peer.respondWhileOtherThreadsWork(
        RespondingEthPeer.blockchainResponder(blockchain), () -> !response.isDone());

    assertThat(response.get(5, TimeUnit.SECONDS).value())
        .extracting(BlockHeader::getNumber)
        .containsExactly(1L, 2L);
  }

  @Test
  void failsARequestOnceItsAttemptsRunOut() {
    final CompletableFuture<RequestSender.Response<List<BlockHeader>>> response =
        scheduler.submitWithRetries(headers(1, 2), 2);

    // the peer never answers: a timeout, then no peer left to try
    assertThatThrownBy(() -> response.get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .cause()
        .isInstanceOfSatisfying(
            RequestFailedException.class,
            failure ->
                assertThat(failure.reason()).isEqualTo(RequestFailedException.Reason.NO_PEER));
    assertThat(peer.streamPendingOutgoingRequests()).hasSize(1);
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
}
