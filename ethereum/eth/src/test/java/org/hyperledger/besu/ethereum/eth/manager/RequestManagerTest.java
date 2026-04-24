/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.testutil.TestClock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class RequestManagerTest {

  private final AtomicLong requestIdCounter = new AtomicLong(1);

  @Test
  public void dispatchesMessagesReceivedAfterRegisteringCallback() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final AtomicInteger sendCount = new AtomicInteger(0);
    final RequestManager.RequestSender sender = __ -> sendCount.incrementAndGet();
    final List<MessageData> receivedMessages = new ArrayList<>();
    final AtomicInteger closedCount = new AtomicInteger(0);
    final RequestManager.ResponseCallback responseHandler =
        (closed, msg, p) -> {
          if (closed) {
            closedCount.incrementAndGet();
          } else {
            receivedMessages.add(msg);
          }
        };

    // Send request
    final RequestManager.ResponseStream stream =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(sendCount.get()).isEqualTo(1);
    stream.then(responseHandler);

    // Dispatch message
    final EthMessage mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);

    // Response handler should get message
    assertThat(receivedMessages).hasSize(1);
    assertResponseCorrect(receivedMessages.get(0), mockMessage);
    assertThat(closedCount.get()).isEqualTo(1);
  }

  @Test
  public void dispatchesMessagesReceivedBeforeRegisteringCallback() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final AtomicInteger sendCount = new AtomicInteger(0);
    final RequestManager.RequestSender sender = __ -> sendCount.incrementAndGet();
    final List<MessageData> receivedMessages = new ArrayList<>();
    final AtomicInteger closedCount = new AtomicInteger(0);
    final RequestManager.ResponseCallback responseHandler =
        (closed, msg, p) -> {
          if (closed) {
            closedCount.incrementAndGet();
          } else {
            receivedMessages.add(msg);
          }
        };

    // Send request
    final RequestManager.ResponseStream stream =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(sendCount.get()).isEqualTo(1);

    // Dispatch message
    final EthMessage mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);

    // Response handler should get message
    stream.then(responseHandler);
    assertThat(receivedMessages).hasSize(1);
    assertResponseCorrect(receivedMessages.get(0), mockMessage);
    assertThat(closedCount.get()).isEqualTo(1);
  }

  @Test
  public void dispatchesMessagesReceivedBeforeAndAfterRegisteringCallback() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final AtomicInteger sendCount = new AtomicInteger(0);
    final RequestManager.RequestSender sender = __ -> sendCount.incrementAndGet();
    final List<MessageData> receivedMessages = new ArrayList<>();
    final AtomicInteger closedCount = new AtomicInteger(0);
    final RequestManager.ResponseCallback responseHandler =
        (closed, msg, p) -> {
          if (closed) {
            closedCount.incrementAndGet();
          } else {
            receivedMessages.add(msg);
          }
        };

    // Send 2 requests so we can receive 2 messages before closing
    final RequestManager.ResponseStream stream =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(sendCount.get()).isEqualTo(1);
    requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(sendCount.get()).isEqualTo(2);

    // Dispatch first message
    EthMessage mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);

    // Response handler should get messages sent before it is registered
    stream.then(responseHandler);
    assertThat(receivedMessages).hasSize(1);
    assertResponseCorrect(receivedMessages.get(0), mockMessage);
    assertThat(closedCount.get()).isZero();

    // Dispatch second message
    mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);

    // Response handler should get messages sent after it is registered
    assertThat(receivedMessages).hasSize(1);
    assertResponseCorrect(receivedMessages.get(0), mockMessage);
    assertThat(closedCount.get()).isEqualTo(1);
  }

  @Test
  public void dispatchesMessagesToSingleStreamIfRequestId() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final AtomicInteger sendCount = new AtomicInteger(0);
    final RequestManager.RequestSender sender = __ -> sendCount.incrementAndGet();

    final List<MessageData> receivedMessagesA = new ArrayList<>();
    final AtomicInteger closedCountA = new AtomicInteger(0);
    final RequestManager.ResponseCallback responseHandlerA =
        (closed, msg, p) -> {
          if (closed) {
            closedCountA.incrementAndGet();
          } else {
            receivedMessagesA.add(msg);
          }
        };
    final List<MessageData> receivedMessagesB = new ArrayList<>();
    final AtomicInteger closedCountB = new AtomicInteger(0);
    final RequestManager.ResponseCallback responseHandlerB =
        (closed, msg, p) -> {
          if (closed) {
            closedCountB.incrementAndGet();
          } else {
            receivedMessagesB.add(msg);
          }
        };

    // Send request
    final RequestManager.ResponseStream streamA =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    final RequestManager.ResponseStream streamB =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(sendCount.get()).isEqualTo(2);

    streamA.then(responseHandlerA);
    streamB.then(responseHandlerB);

    // Dispatch message
    final EthMessage mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);

    // Only handler A or B should get message
    assertThat(receivedMessagesA.size() + receivedMessagesB.size()).isEqualTo(1);
  }

  private EthMessage mockMessage(final EthPeer peer) {
    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    final long requestId = requestIdCounter.getAndIncrement();
    rlpOutput.writeLongScalar(requestId);
    rlpOutput.writeBytes(Bytes.EMPTY);
    rlpOutput.endList();
    return new EthMessage(peer, new RawMessage(1, rlpOutput.encoded()));
  }

  private void assertResponseCorrect(final MessageData response, final EthMessage mockMessage) {
    assertThat(response).isEqualTo(mockMessage.getData().unwrapMessageData().getValue());
  }

  private EthPeer createPeer() {
    final Set<Capability> caps = new HashSet<>(Collections.singletonList(EthProtocol.LATEST));
    final PeerConnection peerConnection = new MockPeerConnection(caps);
    final Consumer<EthPeer> onPeerReady = (peer) -> {};
    return new EthPeer(
        peerConnection,
        onPeerReady,
        Collections.emptyList(),
        EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
        TestClock.fixed(),
        Collections.emptyList(),
        Bytes.random(64));
  }

  @Test
  public void recordsUselessResponseWhenRequestIdDoesNotMatchOutstandingRequest() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    // Dispatch responses with request IDs that were never issued - each should record a useless
    // response. After USELESS_RESPONSE_THRESHOLD useless responses the peer should be disconnected.
    for (int i = 0; i < PeerReputation.USELESS_RESPONSE_THRESHOLD; i++) {
      requestManager.dispatchResponse(mockMessage(peer));
    }

    assertThat(peer.isDisconnected()).isTrue();
  }

  @Test
  public void disconnectsPeerOnBadMessage() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    requestManager
        .dispatchRequest(
            messageData -> RLP.input(messageData.getData()).nextSize(),
            new RawMessage(0x01, Bytes.EMPTY))
        .then(
            (closed, msg, p) -> {
              if (!closed) {
                RLP.input(msg.getData()).skipNext();
              }
            });
    final EthMessage mockMessage =
        new EthMessage(peer, new RawMessage(1, Bytes.of(0x81, 0x82, 0x83, 0x84)));

    requestManager.dispatchResponse(mockMessage);
    assertThat(peer.isDisconnected()).isTrue();
  }

  @Test
  public void closingStreamWithoutResponseReleasesOutstandingRequest() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final RequestManager.RequestSender sender = __ -> {};
    // Send a request - outstanding should go to 1
    final RequestManager.ResponseStream stream =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(requestManager.outstandingRequests()).isEqualTo(1);

    // Close the stream without dispatching a response (simulates timeout)
    stream.close();

    // Outstanding requests should be released back to 0
    assertThat(requestManager.outstandingRequests()).isEqualTo(0);
  }

  @Test
  public void closingStreamAfterResponseDoesNotDoubleDecrement() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final RequestManager.RequestSender sender = __ -> {};
    // Send a request
    final RequestManager.ResponseStream stream =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(requestManager.outstandingRequests()).isEqualTo(1);

    // Dispatch response - outstanding should go to 0
    final EthMessage mockMessage = mockMessage(peer);
    requestManager.dispatchResponse(mockMessage);
    assertThat(requestManager.outstandingRequests()).isEqualTo(0);

    // Close the stream after response was received - should not go negative
    stream.close();
    assertThat(requestManager.outstandingRequests()).isEqualTo(0);
  }

  @Test
  public void multipleTimedOutRequestsAllReleaseCapacity() throws Exception {
    final EthPeer peer = createPeer();
    final RequestManager requestManager = new RequestManager(peer, EthProtocol.NAME);

    final RequestManager.RequestSender sender = __ -> {};
    // Send 3 requests
    final RequestManager.ResponseStream stream1 =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    final RequestManager.ResponseStream stream2 =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    final RequestManager.ResponseStream stream3 =
        requestManager.dispatchRequest(sender, new RawMessage(0x01, Bytes.EMPTY));
    assertThat(requestManager.outstandingRequests()).isEqualTo(3);

    // Close all streams without responses (simulates 3 timeouts)
    stream1.close();
    assertThat(requestManager.outstandingRequests()).isEqualTo(2);
    stream2.close();
    assertThat(requestManager.outstandingRequests()).isEqualTo(1);
    stream3.close();
    assertThat(requestManager.outstandingRequests()).isEqualTo(0);
  }
}
