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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseCallback;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.TrieNodeRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateRepairBudget;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MissingTrieNodeFetcherTest {

  private static final Bytes NODE = Bytes.fromHexString("0xc22001");
  private static final Hash NODE_HASH = Hash.hash(NODE);
  private static final Hash STATE_ROOT = Hash.EMPTY_TRIE_HASH;
  private final EthContext context = mock(EthContext.class);
  private final EthPeers peers = mock(EthPeers.class);
  private final EthPeer peer = mock(EthPeer.class);
  private final ResponseStream responseStream = mock(ResponseStream.class);
  private final WorldStateRepairBudget budget = WorldStateRepairBudget.defaults();
  private final MissingTrieNodeFetcher fetcher =
      new MissingTrieNodeFetcher(context, Duration.ofMillis(500));

  @BeforeEach
  void setUp() throws Exception {
    when(context.getEthPeers()).thenReturn(peers);
    when(peers.streamAvailablePeers()).thenAnswer(invocation -> Stream.of(attributes(peer)));
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocolVersion.V1.getCapability()));
    when(peer.getSnapTrieNode(any(), any())).thenReturn(responseStream);
  }

  @Test
  void returnsHashVerifiedAccountNodeAndClosesResponseStream() throws Exception {
    answerWith(TrieNodesMessage.create(List.of(NODE)).wrapMessageData(BigInteger.ONE));

    assertThat(fetch(Optional.empty())).contains(NODE);
    // Nibble location [1, 2, 3] has the compact encoding 0x1123.
    verify(peer).getSnapTrieNode(STATE_ROOT, List.of(List.of(Bytes.fromHexString("0x1123"))));
    verify(responseStream).close();
  }

  @Test
  void requestsStorageNodeWithAccountHashAndCompactLocation() throws Exception {
    answerWith(TrieNodesMessage.create(List.of(NODE)).wrapMessageData(BigInteger.ONE));

    assertThat(fetch(Optional.of(Hash.ZERO))).contains(NODE);
    verify(peer)
        .getSnapTrieNode(
            STATE_ROOT, List.of(List.of(Hash.ZERO.getBytes(), Bytes.fromHexString("0x1123"))));
    verify(responseStream).close();
  }

  @Test
  void rejectsWrongHashAndChargesReceivedBytes() {
    final MessageData invalid =
        TrieNodesMessage.create(List.of(Bytes.of(1))).wrapMessageData(BigInteger.ONE);
    answerWith(invalid);

    assertThat(fetch(Optional.empty())).isEmpty();
    assertReceivedBytesCharged(invalid.getSize());
    verify(responseStream).close();
  }

  @Test
  void rejectsMalformedResponseWithoutThrowing() {
    final MessageData malformed = new RawMessage(SnapV1.TRIE_NODES, Bytes.of(1, 2, 3));
    answerWith(malformed);

    assertThat(fetch(Optional.empty())).isEmpty();
    assertReceivedBytesCharged(malformed.getSize());
    verify(responseStream).close();
  }

  @Test
  void rejectsEmptyResponse() {
    answerWith(TrieNodesMessage.create(List.of()).wrapMessageData(BigInteger.ONE));

    assertThat(fetch(Optional.empty())).isEmpty();
    verify(responseStream).close();
  }

  @Test
  void rejectsMultipleNodesEvenWhenOneMatches() {
    final MessageData surplus =
        TrieNodesMessage.create(List.of(NODE, NODE)).wrapMessageData(BigInteger.ONE);
    answerWith(surplus);

    assertThat(fetch(Optional.empty())).isEmpty();
    assertReceivedBytesCharged(surplus.getSize());
    verify(responseStream).close();
  }

  @Test
  void rejectsOversizedResponseBeforeDecodingAndExhaustsBudget() {
    final MessageData oversized = mock(MessageData.class);
    when(oversized.getSize()).thenReturn(8 * 1024 * 1024 + 1);
    answerWith(oversized);

    assertThat(fetch(Optional.empty())).isEmpty();
    assertThat(budget.isExhausted()).isTrue();
    verify(oversized, never()).getData();
    verify(responseStream).close();
  }

  @Test
  void skipsSnap2OnlyPeer() throws Exception {
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocolVersion.V2.getCapability()));

    assertThat(fetch(Optional.empty())).isEmpty();
    verify(peer, never()).getSnapTrieNode(any(), any());
  }

  @Test
  void closesStreamWhenBudgetIsCancelledDuringRequest() {
    when(responseStream.then(any()))
        .thenAnswer(
            invocation -> {
              budget.cancel();
              return responseStream;
            });

    assertThat(fetch(Optional.empty())).isEmpty();
    verify(responseStream).close();
  }

  @Test
  void doesNotRequestWhenAlreadyCancelled() throws Exception {
    budget.cancel();

    assertThat(fetch(Optional.empty())).isEmpty();
    verify(peer, never()).getSnapTrieNode(any(), any());
  }

  @Test
  void cancellationWaitsForTheInProgressResponseCallbackToDrain() throws Exception {
    final CountDownLatch processing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final MessageData message = mock(MessageData.class);
    final AtomicReference<CompletableFuture<Void>> callbackWork = new AtomicReference<>();
    when(message.getSize())
        .thenAnswer(
            invocation -> {
              processing.countDown();
              assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
              return 1;
            });
    when(responseStream.then(any()))
        .thenAnswer(
            invocation -> {
              final ResponseCallback callback = invocation.getArgument(0);
              callbackWork.set(
                  CompletableFuture.runAsync(() -> callback.exec(false, message, peer)));
              return responseStream;
            });
    final CompletableFuture<Optional<Bytes>> fetched =
        CompletableFuture.supplyAsync(() -> fetch(Optional.empty()));
    try {
      assertThat(processing.await(5, TimeUnit.SECONDS)).isTrue();
      budget.cancel();

      assertThatThrownBy(() -> fetched.get(100, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);
    } finally {
      release.countDown();
      fetched.get(5, TimeUnit.SECONDS);
      if (callbackWork.get() != null) {
        callbackWork.get().get(5, TimeUnit.SECONDS);
      }
    }
    verify(responseStream).close();
  }

  @Test
  void closesStreamAndPreservesInterruption() {
    when(responseStream.then(any()))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return responseStream;
            });
    try {
      assertThat(fetch(Optional.empty())).isEmpty();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      verify(responseStream).close();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void closesSilentStreamAndUsesSecondPeerWithinTotalTimeout() throws Exception {
    final EthPeer silentPeer = mock(EthPeer.class);
    final ResponseStream silentStream = mock(ResponseStream.class);
    when(silentPeer.getAgreedCapabilities())
        .thenReturn(Set.of(SnapProtocolVersion.V1.getCapability()));
    when(silentPeer.getSnapTrieNode(any(), any())).thenReturn(silentStream);
    when(peers.streamAvailablePeers())
        .thenAnswer(invocation -> Stream.of(attributes(silentPeer), attributes(peer)));
    answerWith(TrieNodesMessage.create(List.of(NODE)).wrapMessageData(BigInteger.ONE));

    assertThat(fetch(Optional.empty())).contains(NODE);
    verify(silentStream).close();
    verify(responseStream).close();
  }

  @Test
  void closesSilentStreamWhenTotalTimeoutExpires() {
    assertThat(
            assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () ->
                    new MissingTrieNodeFetcher(context, Duration.ofMillis(20))
                        .fetch(request(Optional.empty()), budget)))
        .isEmpty();
    verify(responseStream).close();
  }

  @Test
  void neverRequestsMoreThanTwoPeers() throws Exception {
    final EthPeer secondPeer = mock(EthPeer.class);
    final ResponseStream secondStream = mock(ResponseStream.class);
    final EthPeer thirdPeer = mock(EthPeer.class);
    when(secondPeer.getAgreedCapabilities())
        .thenReturn(Set.of(SnapProtocolVersion.V1.getCapability()));
    when(secondPeer.getSnapTrieNode(any(), any())).thenReturn(secondStream);
    when(peers.streamAvailablePeers())
        .thenAnswer(
            invocation ->
                Stream.of(attributes(peer), attributes(secondPeer), attributes(thirdPeer)));
    final MessageData empty = TrieNodesMessage.create(List.of()).wrapMessageData(BigInteger.ONE);
    answerWith(empty);
    answerWith(secondStream, secondPeer, empty);

    assertThat(fetch(Optional.empty())).isEmpty();
    verify(responseStream).close();
    verify(secondStream).close();
    verify(thirdPeer, never()).getSnapTrieNode(any(), any());
  }

  @Test
  void sharesReceivedByteBudgetAcrossInvalidAndValidPeerResponses() throws Exception {
    final EthPeer secondPeer = mock(EthPeer.class);
    final ResponseStream secondStream = mock(ResponseStream.class);
    when(secondPeer.getAgreedCapabilities())
        .thenReturn(Set.of(SnapProtocolVersion.V1.getCapability()));
    when(secondPeer.getSnapTrieNode(any(), any())).thenReturn(secondStream);
    when(peers.streamAvailablePeers())
        .thenAnswer(invocation -> Stream.of(attributes(peer), attributes(secondPeer)));
    final MessageData invalid = new RawMessage(SnapV1.TRIE_NODES, Bytes.of(1, 2, 3));
    final MessageData valid =
        TrieNodesMessage.create(List.of(NODE)).wrapMessageData(BigInteger.ONE);
    assertThat(
            budget.consumeReceivedBytes(8L * 1024 * 1024 - invalid.getSize() - valid.getSize() + 1))
        .isTrue();
    answerWith(invalid);
    answerWith(secondStream, secondPeer, valid);

    assertThat(fetch(Optional.empty())).isEmpty();
    assertThat(budget.isExhausted()).isTrue();
    verify(responseStream).close();
    verify(secondStream).close();
  }

  private void answerWith(final MessageData message) {
    answerWith(responseStream, peer, message);
  }

  private void answerWith(
      final ResponseStream stream, final EthPeer sender, final MessageData message) {
    when(stream.then(any()))
        .thenAnswer(
            invocation -> {
              invocation.<ResponseCallback>getArgument(0).exec(false, message, sender);
              return stream;
            });
  }

  private Optional<Bytes> fetch(final Optional<Hash> accountHash) {
    return fetcher.fetch(request(accountHash), budget);
  }

  private static TrieNodeRequest request(final Optional<Hash> accountHash) {
    return new TrieNodeRequest(STATE_ROOT, accountHash, Bytes.of(1, 2, 3), NODE_HASH);
  }

  private void assertReceivedBytesCharged(final long bytes) {
    assertThat(budget.consumeReceivedBytes(8L * 1024 * 1024 - bytes)).isTrue();
    assertThat(budget.consumeReceivedBytes(1)).isFalse();
  }

  private static EthPeerImmutableAttributes attributes(final EthPeer peer) {
    return new EthPeerImmutableAttributes(
        UInt256.ZERO, false, 0, 0, 0, 0, false, true, false, true, false, peer);
  }
}
