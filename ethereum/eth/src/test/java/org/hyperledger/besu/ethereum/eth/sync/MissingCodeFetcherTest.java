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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseCallback;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MissingCodeFetcherTest {

  private static final Bytes CODE = Bytes.fromHexString("0x600160005500");
  private static final Hash CODE_HASH = Hash.hash(CODE);

  private final EthContext ethContext = mock(EthContext.class);
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final Blockchain blockchain = mock(Blockchain.class);
  private final EthPeer peer = mock(EthPeer.class);
  private final ResponseStream stream = mock(ResponseStream.class);
  private final MissingCodeFetcher fetcher =
      new MissingCodeFetcher(ethContext, blockchain, Duration.ofMillis(100));

  @BeforeEach
  void setUp() throws Exception {
    final BlockHeader head = mock(BlockHeader.class);
    when(head.getStateRoot()).thenReturn(Hash.ZERO);
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(ethPeers.streamAvailablePeers()).thenAnswer(invocation -> Stream.of(attributes(peer)));
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocol.SNAP1));
    when(peer.getSnapBytecode(any(), any())).thenReturn(stream);
  }

  @Test
  void returnsTheCodeOfThePeerAndClosesTheStream() {
    answerWith(CODE);

    assertThat(fetcher.fetch(CODE_HASH)).contains(CODE);
    verify(stream).close();
  }

  @Test
  void closesTheStreamOfASilentPeerAndAsksTheNextOne() throws Exception {
    final EthPeer silentPeer = mock(EthPeer.class);
    final ResponseStream silentStream = mock(ResponseStream.class);
    when(silentPeer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocol.SNAP1));
    when(silentPeer.getSnapBytecode(any(), any())).thenReturn(silentStream);
    when(silentStream.then(any())).thenReturn(silentStream);
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(invocation -> Stream.of(attributes(silentPeer), attributes(peer)));
    answerWith(CODE);

    assertThat(fetcher.fetch(CODE_HASH)).contains(CODE);
    verify(silentStream).close();
    verify(stream).close();
  }

  @Test
  void asksTheNextPeerRightAwayOnAMalformedResponse() throws Exception {
    final MissingCodeFetcher patientFetcher =
        new MissingCodeFetcher(ethContext, blockchain, Duration.ofSeconds(30));
    final EthPeer malformedPeer = mock(EthPeer.class);
    final ResponseStream malformedStream = mock(ResponseStream.class);
    when(malformedPeer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocol.SNAP1));
    when(malformedPeer.getSnapBytecode(any(), any())).thenReturn(malformedStream);
    // the response is delivered later on another thread, as by the network
    when(malformedStream.then(any()))
        .thenAnswer(
            invocation -> {
              final ResponseCallback callback = invocation.getArgument(0);
              CompletableFuture.runAsync(
                  () ->
                      callback.exec(
                          false,
                          new RawMessage(SnapV1.BYTECODES, Bytes.of(1, 2, 3)),
                          malformedPeer));
              return malformedStream;
            });
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(invocation -> Stream.of(attributes(malformedPeer), attributes(peer)));
    answerWith(CODE);

    assertThat(
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> patientFetcher.fetch(CODE_HASH)))
        .contains(CODE);
    verify(malformedStream).close();
  }

  @Test
  void stopsAskingThePeersOnceInterrupted() throws Exception {
    final EthPeer interruptedPeer = mock(EthPeer.class);
    final ResponseStream interruptedStream = mock(ResponseStream.class);
    when(interruptedPeer.getAgreedCapabilities()).thenReturn(Set.of(SnapProtocol.SNAP1));
    when(interruptedPeer.getSnapBytecode(any(), any())).thenReturn(interruptedStream);
    // the caller is interrupted while it waits for the first peer
    when(interruptedStream.then(any()))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return interruptedStream;
            });
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(invocation -> Stream.of(attributes(interruptedPeer), attributes(peer)));
    answerWith(CODE);

    try {
      assertThat(fetcher.fetch(CODE_HASH)).isEmpty();
      verify(interruptedStream).close();
      verify(peer, never()).getSnapBytecode(any(), any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void rejectsACodeOfAnotherHash() {
    answerWith(Bytes.of(1));

    assertThat(fetcher.fetch(CODE_HASH)).isEmpty();
    verify(stream).close();
  }

  @Test
  void asksOnlyThePeersAgreeingOnSnap() throws Exception {
    when(peer.getAgreedCapabilities()).thenReturn(Set.<Capability>of(EthProtocol.LATEST));

    assertThat(fetcher.fetch(CODE_HASH)).isEmpty();
    verify(peer, never()).getSnapBytecode(any(), any());
  }

  private void answerWith(final Bytes code) {
    when(stream.then(any()))
        .thenAnswer(
            invocation -> {
              invocation
                  .<ResponseCallback>getArgument(0)
                  .exec(
                      false,
                      ByteCodesMessage.create(List.of(code)).wrapMessageData(BigInteger.ONE),
                      peer);
              return stream;
            });
  }

  private static EthPeerImmutableAttributes attributes(final EthPeer peer) {
    return new EthPeerImmutableAttributes(
        UInt256.ZERO, false, 0, 0, 0, 0, false, true, false, true, false, peer);
  }
}
