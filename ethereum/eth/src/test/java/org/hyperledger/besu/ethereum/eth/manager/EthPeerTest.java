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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hyperledger.besu.ethereum.eth.core.Utils.serializeReceiptsList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.EthPeerTestUtil;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.NodeDataMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.PingMessage;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;
import org.hyperledger.besu.testutil.TestClock;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class EthPeerTest {
  private static final BlockDataGenerator gen = new BlockDataGenerator();
  private final TestClock clock = new TestClock();
  private static final Bytes NODE_ID = Bytes.random(64);
  private static final Bytes NODE_ID_ZERO =
      Bytes.fromHexString(
          "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010");

  @Test
  public void getHeadersStream() throws PeerNotConnected {
    final ResponseStreamSupplier getStream =
        (peer) -> peer.getHeadersByHash(gen.hash(), 5, 0, false);
    final MessageData targetMessage =
        BlockHeadersMessage.create(asList(gen.header(), gen.header()));
    final MessageData otherMessage = BlockBodiesMessage.create(asList(gen.body(), gen.body()));

    messageStream(getStream, targetMessage, otherMessage);
  }

  @Test
  public void getBodiesStream() throws PeerNotConnected {
    final ResponseStreamSupplier getStream =
        (peer) -> peer.getBodies(asList(gen.hash(), gen.hash()));
    final MessageData targetMessage = BlockBodiesMessage.create(asList(gen.body(), gen.body()));
    final MessageData otherMessage = BlockHeadersMessage.create(asList(gen.header(), gen.header()));

    messageStream(getStream, targetMessage, otherMessage);
  }

  @Test
  public void getNodeDataStream() throws PeerNotConnected {
    final ResponseStreamSupplier getStream =
        (peer) -> peer.getNodeData(asList(gen.hash(), gen.hash()));
    final MessageData targetMessage = NodeDataMessage.create(singletonList(gen.bytesValue()));
    final MessageData otherMessage = BlockHeadersMessage.create(asList(gen.header(), gen.header()));

    messageStream(getStream, targetMessage, otherMessage);
  }

  @Test
  public void shouldHaveAvailableCapacityUntilOutstandingRequestLimitIsReached()
      throws PeerNotConnected {
    final EthPeer peer = createPeer();
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(0);

    peer.getBodies(asList(gen.hash(), gen.hash()));
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(1);

    peer.getHeadersByHash(gen.hash(), 4, 1, false);
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(2);

    peer.getNodeData(asList(gen.hash(), gen.hash()));
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(3);

    peer.getHeadersByHash(gen.hash(), 4, 1, false);
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(4);

    peer.getHeadersByNumber(1, 1, 1, false);
    assertThat(peer.hasAvailableRequestCapacity()).isFalse();
    assertThat(peer.outstandingRequests()).isEqualTo(5);

    peer.dispatch(
        new EthMessage(
            peer,
            BlockBodiesMessage.create(emptyList())
                .wrapMessageData(java.math.BigInteger.valueOf(1))));
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();
    assertThat(peer.outstandingRequests()).isEqualTo(4);
  }

  @Test
  public void shouldTrackLastRequestTime() throws PeerNotConnected {
    final EthPeer peer = createPeer();

    clock.stepMillis(10_000);
    peer.getBodies(asList(gen.hash(), gen.hash()));
    assertThat(peer.getLastRequestTimestamp()).isEqualTo(clock.millis());

    clock.stepMillis(10_000);
    peer.getNodeData(asList(gen.hash(), gen.hash()));
    assertThat(peer.getLastRequestTimestamp()).isEqualTo(clock.millis());

    clock.stepMillis(10_000);
    peer.getHeadersByHash(gen.hash(), 4, 1, false);
    assertThat(peer.getLastRequestTimestamp()).isEqualTo(clock.millis());

    clock.stepMillis(10_000);
    peer.getHeadersByNumber(1, 1, 1, false);
    assertThat(peer.getLastRequestTimestamp()).isEqualTo(clock.millis());
  }

  @Test
  public void closeStreamsOnPeerDisconnect() throws PeerNotConnected {
    final EthPeer peer = createPeer();
    // Setup headers stream
    final AtomicInteger headersClosedCount = new AtomicInteger(0);
    peer.getHeadersByHash(gen.hash(), 5, 0, false)
        .then(
            (closed, msg, p) -> {
              if (closed) {
                headersClosedCount.incrementAndGet();
              }
            });
    // Bodies stream
    final AtomicInteger bodiesClosedCount = new AtomicInteger(0);
    peer.getBodies(asList(gen.hash(), gen.hash()))
        .then(
            (closed, msg, p) -> {
              if (closed) {
                bodiesClosedCount.incrementAndGet();
              }
            });
    // NodeData stream
    final AtomicInteger nodeDataClosedCount = new AtomicInteger(0);
    peer.getNodeData(asList(gen.hash(), gen.hash()))
        .then(
            (closed, msg, p) -> {
              if (closed) {
                nodeDataClosedCount.incrementAndGet();
              }
            });

    // Sanity check
    assertThat(headersClosedCount.get()).isEqualTo(0);
    assertThat(bodiesClosedCount.get()).isEqualTo(0);
    assertThat(nodeDataClosedCount.get()).isEqualTo(0);

    // Disconnect and check
    peer.handleDisconnect();
    assertThat(headersClosedCount.get()).isEqualTo(1);
    assertThat(bodiesClosedCount.get()).isEqualTo(1);
    assertThat(nodeDataClosedCount.get()).isEqualTo(1);
  }

  @Test
  public void listenForMultipleStreams() throws PeerNotConnected {
    // Setup peer and messages
    final EthPeer peer = createPeer();
    final EthMessage headersMessage =
        new EthMessage(
            peer,
            BlockHeadersMessage.create(asList(gen.header(), gen.header()))
                .wrapMessageData(BigInteger.ONE));
    final EthMessage bodiesMessage =
        new EthMessage(
            peer,
            BlockBodiesMessage.create(asList(gen.body(), gen.body()))
                .wrapMessageData(BigInteger.ONE));
    final EthMessage otherMessage =
        new EthMessage(
            peer,
            ReceiptsMessage.createUnsafe(
                    serializeReceiptsList(
                        singletonList(gen.receipts(gen.block())),
                        TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION))
                .wrapMessageData(BigInteger.ONE));

    // Set up stream for headers
    final AtomicInteger headersMessageCount = new AtomicInteger(0);
    final AtomicInteger headersClosedCount = new AtomicInteger(0);
    peer.getHeadersByHash(gen.hash(), 5, 0, false)
        .then(
            (closed, msg, p) -> {
              if (closed) {
                headersClosedCount.incrementAndGet();
              } else {
                headersMessageCount.incrementAndGet();
                assertThat(msg.getCode()).isEqualTo(headersMessage.getData().getCode());
              }
            });
    // Set up stream for bodies
    final AtomicInteger bodiesMessageCount = new AtomicInteger(0);
    final AtomicInteger bodiesClosedCount = new AtomicInteger(0);
    peer.getBodies(asList(gen.hash(), gen.hash()))
        .then(
            (closed, msg, p) -> {
              if (closed) {
                bodiesClosedCount.incrementAndGet();
              } else {
                bodiesMessageCount.incrementAndGet();
                assertThat(msg.getCode()).isEqualTo(bodiesMessage.getData().getCode());
              }
            });

    // Dispatch some messages and check expectations
    peer.dispatch(headersMessage);
    assertThat(headersMessageCount.get()).isEqualTo(1);
    assertThat(headersClosedCount.get()).isEqualTo(1);
    assertThat(bodiesMessageCount.get()).isEqualTo(0);
    assertThat(bodiesClosedCount.get()).isEqualTo(0);

    peer.dispatch(bodiesMessage);
    assertThat(headersMessageCount.get()).isEqualTo(1);
    assertThat(headersClosedCount.get()).isEqualTo(1);
    assertThat(bodiesMessageCount.get()).isEqualTo(1);
    assertThat(bodiesClosedCount.get()).isEqualTo(1);

    peer.dispatch(otherMessage);
    assertThat(headersMessageCount.get()).isEqualTo(1);
    assertThat(headersClosedCount.get()).isEqualTo(1);
    assertThat(bodiesMessageCount.get()).isEqualTo(1);
    assertThat(bodiesClosedCount.get()).isEqualTo(1);

    // Dispatch again after close and check that nothing fires
    peer.dispatch(headersMessage);
    peer.dispatch(bodiesMessage);
    peer.dispatch(otherMessage);
    assertThat(headersMessageCount.get()).isEqualTo(1);
    assertThat(headersClosedCount.get()).isEqualTo(1);
    assertThat(bodiesMessageCount.get()).isEqualTo(1);
    assertThat(bodiesClosedCount.get()).isEqualTo(1);
  }

  @Test
  public void isFullyValidated_noPeerValidators() {
    final EthPeer peer = createPeer();
    assertThat(peer.isFullyValidated()).isTrue();
  }

  @Test
  public void isFullyValidated_singleValidator_notValidated() {
    final PeerValidator validator = mock(PeerValidator.class);
    final EthPeer peer = createPeer(validator);

    assertThat(peer.isFullyValidated()).isFalse();
  }

  @Test
  public void isFullyValidated_singleValidator_validated() {
    final PeerValidator validator = mock(PeerValidator.class);
    final EthPeer peer = createPeer(validator);
    peer.markValidated(validator);

    assertThat(peer.isFullyValidated()).isTrue();
  }

  @Test
  public void isFullyValidated_multipleValidators_unvalidated() {
    final List<PeerValidator> validators =
        Stream.generate(() -> mock(PeerValidator.class)).limit(2).collect(Collectors.toList());

    final EthPeer peer = createPeer(validators);

    assertThat(peer.isFullyValidated()).isFalse();
  }

  @Test
  public void isFullyValidated_multipleValidators_partiallyValidated() {
    final List<PeerValidator> validators =
        Stream.generate(() -> mock(PeerValidator.class)).limit(2).collect(Collectors.toList());

    final EthPeer peer = createPeer(validators);
    peer.markValidated(validators.get(0));

    assertThat(peer.isFullyValidated()).isFalse();
  }

  @Test
  public void isFullyValidated_multipleValidators_fullyValidated() {
    final List<PeerValidator> validators =
        Stream.generate(() -> mock(PeerValidator.class)).limit(2).collect(Collectors.toList());

    final EthPeer peer = createPeer(validators);
    validators.forEach(peer::markValidated);

    assertThat(peer.isFullyValidated()).isTrue();
  }

  @Test
  public void message_permissioning_any_false_permission_preventsMessageFromSendingToPeer()
      throws PeerNotConnected {
    NodeMessagePermissioningProvider trueProvider = mock(NodeMessagePermissioningProvider.class);
    NodeMessagePermissioningProvider falseProvider = mock(NodeMessagePermissioningProvider.class);
    when(trueProvider.isMessagePermitted(any(), anyInt())).thenReturn(true);
    when(falseProvider.isMessagePermitted(any(), anyInt())).thenReturn(false);

    // use failOnSend callback
    final EthPeer peer =
        createPeer(Collections.emptyList(), List.of(falseProvider, trueProvider), getFailOnSend());
    peer.send(PingMessage.get());
  }

  @Test
  public void compareTo_withSameNodeId() {
    final EthPeer peer1 = createPeerWithPeerInfo(NODE_ID);
    final EthPeer peer2 = createPeerWithPeerInfo(NODE_ID);
    assertThat(peer1.compareTo(peer2)).isEqualTo(0);
    assertThat(peer2.compareTo(peer1)).isEqualTo(0);
  }

  @Test
  public void compareTo_withDifferentNodeId() {
    final EthPeer peer1 = createPeerWithPeerInfo(NODE_ID);
    final EthPeer peer2 = createPeerWithPeerInfo(NODE_ID_ZERO);
    assertThat(peer1.compareTo(peer2)).isEqualTo(1);
    assertThat(peer2.compareTo(peer1)).isEqualTo(-1);
  }

  @Test
  public void recordUsefulResponse() {
    final EthPeer peer = createPeer();
    peer.recordUselessResponse("bodies");
    final EthPeer peer2 = createPeer();
    peer2.recordUselessResponse("bodies");
    peer.recordUsefulResponse();
    assertThat(peer.getReputation().compareTo(peer2.getReputation())).isGreaterThan(0);
  }

  private void messageStream(
      final ResponseStreamSupplier getStream,
      final MessageData targetMessage,
      final MessageData otherMessage)
      throws PeerNotConnected {
    // Setup peer and ask for stream
    final EthPeer peer = createPeer();
    final AtomicInteger messageCount = new AtomicInteger(0);
    int requestIdCounter = 1;
    final AtomicInteger closedCount = new AtomicInteger(0);
    final int targetCode = targetMessage.getCode();
    final RequestManager.ResponseCallback responseHandler =
        (closed, msg, p) -> {
          if (closed) {
            closedCount.incrementAndGet();
          } else {
            messageCount.incrementAndGet();
            assertThat(msg.getCode()).isEqualTo(targetCode);
          }
        };

    // Set up 1 stream
    getStream.get(peer).then(responseHandler);

    EthMessage targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter++)));
    // Dispatch message and check that stream processes messages
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(1);
    assertThat(closedCount.get()).isEqualTo(1);

    targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter++)));

    // Check that no new messages are delivered
    getStream.get(peer);
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(1);
    assertThat(closedCount.get()).isEqualTo(1);

    // Set up 2 streams
    getStream.get(peer).then(responseHandler);
    getStream.get(peer).then(responseHandler);

    // Reset counters
    messageCount.set(0);
    closedCount.set(0);

    targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter++)));

    // Dispatch message and check that stream processes messages
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(1);
    assertThat(closedCount.get()).isEqualTo(0);

    // Dispatch unrelated message and check that it is not process
    EthMessage otherEthMessage =
        new EthMessage(peer, otherMessage.wrapMessageData(BigInteger.valueOf(999)));
    peer.dispatch(otherEthMessage);
    assertThat(messageCount.get()).isEqualTo(1);
    assertThat(closedCount.get()).isEqualTo(0);

    targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter++)));
    // Dispatch last outstanding message and check that streams are closed
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(2);
    assertThat(closedCount.get()).isEqualTo(2);

    targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter++)));
    // Check that no new messages are delivered
    getStream.get(peer);
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(2);
    assertThat(closedCount.get()).isEqualTo(2);

    targetEthMessage =
        new EthMessage(peer, targetMessage.wrapMessageData(BigInteger.valueOf(requestIdCounter)));
    // Open stream, then close it and check no messages are processed
    final RequestManager.ResponseStream stream = getStream.get(peer).then(responseHandler);
    // Reset counters
    messageCount.set(0);
    closedCount.set(0);
    stream.close();
    getStream.get(peer);
    peer.dispatch(targetEthMessage);
    assertThat(messageCount.get()).isEqualTo(0);
    assertThat(closedCount.get()).isEqualTo(1);
  }

  private EthPeer createPeer() {
    return createPeer(Collections.emptyList(), Collections.emptyList());
  }

  private EthPeer createPeer(final PeerValidator... peerValidators) {
    return createPeer(Arrays.asList(peerValidators), Collections.emptyList());
  }

  private EthPeer createPeer(final List<PeerValidator> peerValidators) {
    return createPeer(peerValidators, Collections.emptyList());
  }

  private EthPeer createPeerWithPeerInfo(final Bytes nodeId) {
    final PeerConnection peerConnection = mock(PeerConnection.class);
    // Use a non-eth protocol name to ensure that EthPeer with sub-protocols such as Istanbul
    // that extend the sub-protocol work correctly
    PeerInfo peerInfo = new PeerInfo(1, "clientId", Collections.emptyList(), 30303, nodeId);
    when(peerConnection.getPeerInfo()).thenReturn(peerInfo);
    when(peerConnection.getPeer()).thenReturn(EthPeerTestUtil.createPeer(peerInfo.getNodeId()));

    final Consumer<EthPeer> onPeerReady = (peer) -> {};
    return new EthPeer(
        peerConnection,
        onPeerReady,
        Collections.emptyList(),
        EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
        clock,
        Collections.emptyList(),
        Bytes.random(64));
  }

  private MockPeerConnection.PeerSendHandler getFailOnSend() {
    return (cap, message, conn) -> {
      fail("should not call send");
    };
  }

  private MockPeerConnection.PeerSendHandler getNoOpSend() {
    return (cap, msg, conn) -> {};
  }

  private EthPeer createPeer(
      final List<PeerValidator> peerValidators,
      final List<NodeMessagePermissioningProvider> permissioningProviders) {
    return createPeer(peerValidators, permissioningProviders, getNoOpSend());
  }

  private EthPeer createPeer(
      final List<PeerValidator> peerValidators,
      final List<NodeMessagePermissioningProvider> permissioningProviders,
      final MockPeerConnection.PeerSendHandler onSend) {

    final PeerConnection peerConnection = getPeerConnection(onSend);
    final Consumer<EthPeer> onPeerReady = (peer) -> {};
    // Use a non-eth protocol name to ensure that EthPeer with sub-protocols such as Istanbul
    // that extend the sub-protocol work correctly
    return new EthPeer(
        peerConnection,
        onPeerReady,
        peerValidators,
        EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
        clock,
        permissioningProviders,
        Bytes.random(64));
  }

  private PeerConnection getPeerConnection(final MockPeerConnection.PeerSendHandler onSend) {
    // Use a non-eth protocol name to ensure that EthPeer with sub-protocols such as Istanbul
    // that extend the sub-protocol work correctly
    final Set<Capability> caps =
        new HashSet<>(Collections.singletonList(Capability.create("foo", 68)));

    return new MockPeerConnection(caps, onSend);
  }

  @FunctionalInterface
  interface ResponseStreamSupplier {
    RequestManager.ResponseStream get(EthPeer peer) throws PeerNotConnected;
  }
}
