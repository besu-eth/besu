/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.p2p.network;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.MessageCallback;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class NetworkRunnerTest {

  private NetworkRunner networkRunner;
  private P2PNetwork network;
  private LabelledMetric<Counter> inboundMessageCounter;
  private LabelledMetric<Counter> inboundBytesCounter;
  private Counter messageCounter;
  private Counter bytesCounter;
  private SubProtocol subProtocol;
  private ProtocolManager protocolManager;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    network = mock(P2PNetwork.class);
    subProtocol = mock(SubProtocol.class);
    protocolManager = mock(ProtocolManager.class);

    inboundMessageCounter = mock(LabelledMetric.class);
    inboundBytesCounter = mock(LabelledMetric.class);
    messageCounter = mock(Counter.class);
    bytesCounter = mock(Counter.class);

    MetricsSystem metricsSystem = mock(MetricsSystem.class);
    when(metricsSystem.createLabelledCounter(
            any(), eq("p2p_messages_inbound"), any(), any(), any(), any()))
        .thenReturn(inboundMessageCounter);
    when(metricsSystem.createLabelledCounter(
            any(), eq("p2p_bytes_inbound"), any(), any(), any(), any()))
        .thenReturn(inboundBytesCounter);

    when(inboundMessageCounter.labels(anyString(), anyString(), anyString()))
        .thenReturn(messageCounter);
    when(inboundBytesCounter.labels(anyString(), anyString(), anyString()))
        .thenReturn(bytesCounter);

    // Setup subProtocol to return "eth" as its name so it can be looked up
    when(subProtocol.getName()).thenReturn("eth");

    // Setup network mocks to allow start() to complete
    when(network.isListening()).thenReturn(true);
    when(network.getRlpxAgent()).thenReturn(Optional.empty());

    BiFunction<Peer, Boolean, Optional<DisconnectMessage.DisconnectReason>>
        peerConnectionGatekeeper = (peer, incoming) -> Optional.empty();

    NetworkRunner.NetworkBuilder networkBuilder = caps -> network;

    networkRunner =
        NetworkRunner.builder()
            .protocolManagers(List.of(protocolManager))
            .subProtocols(subProtocol)
            .network(networkBuilder)
            .metricsSystem(metricsSystem)
            .peerConnectionGatekeeper(peerConnectionGatekeeper)
            .build();
  }

  @Test
  public void shouldIncrementInboundBytesCounterWhenProcessingMessage() {
    // Setup
    Capability capability = Capability.create("eth", 68);
    int messageCode = 1;
    int messageSize = 1024;

    when(subProtocol.getName()).thenReturn("eth");
    when(subProtocol.isValidMessageCode(anyInt(), eq(messageCode))).thenReturn(true);
    when(subProtocol.messageName(anyInt(), eq(messageCode))).thenReturn("Status");
    when(protocolManager.getSupportedCapabilities()).thenReturn(List.of(capability));

    // Start network runner to register handlers
    networkRunner.start();

    // Capture the message handler that was registered
    ArgumentCaptor<MessageCallback> handlerCaptor = ArgumentCaptor.forClass(MessageCallback.class);
    verify(network).subscribe(eq(capability), handlerCaptor.capture());

    MessageCallback handler = handlerCaptor.getValue();

    // Create test message
    PeerConnection peerConnection = mock(PeerConnection.class);
    MessageData messageData = mock(MessageData.class);
    when(messageData.getSize()).thenReturn(messageSize);
    when(messageData.getCode()).thenReturn(messageCode);

    Message message = new DefaultMessage(peerConnection, messageData);

    // Process message through the handler
    handler.onMessage(capability, message);

    // Verify bytes counter was incremented with correct labels and size
    verify(inboundBytesCounter).labels(eq(capability.toString()), eq("Status"), eq("1"));
    verify(bytesCounter).inc(messageSize);
  }

  @Test
  public void shouldIncrementInboundBytesCounterForMultipleMessages() {
    // Setup
    Capability capability = Capability.create("eth", 68);
    int messageCode1 = 1;
    int messageCode2 = 2;
    int messageSize1 = 512;
    int messageSize2 = 2048;

    when(subProtocol.getName()).thenReturn("eth");
    when(subProtocol.isValidMessageCode(anyInt(), anyInt())).thenReturn(true);
    when(subProtocol.messageName(anyInt(), eq(messageCode1))).thenReturn("Status");
    when(subProtocol.messageName(anyInt(), eq(messageCode2))).thenReturn("GetBlockHeaders");
    when(protocolManager.getSupportedCapabilities()).thenReturn(List.of(capability));

    // Start network runner to register handlers
    networkRunner.start();

    // Capture the message handler
    ArgumentCaptor<MessageCallback> handlerCaptor = ArgumentCaptor.forClass(MessageCallback.class);
    verify(network).subscribe(eq(capability), handlerCaptor.capture());

    MessageCallback handler = handlerCaptor.getValue();

    // Create first test message
    PeerConnection peerConnection = mock(PeerConnection.class);
    MessageData messageData1 = mock(MessageData.class);
    when(messageData1.getSize()).thenReturn(messageSize1);
    when(messageData1.getCode()).thenReturn(messageCode1);
    Message message1 = new DefaultMessage(peerConnection, messageData1);

    // Create second test message
    MessageData messageData2 = mock(MessageData.class);
    when(messageData2.getSize()).thenReturn(messageSize2);
    when(messageData2.getCode()).thenReturn(messageCode2);
    Message message2 = new DefaultMessage(peerConnection, messageData2);

    // Process both messages
    handler.onMessage(capability, message1);
    handler.onMessage(capability, message2);

    // Verify bytes counter was incremented for both messages
    verify(bytesCounter).inc(messageSize1);
    verify(bytesCounter).inc(messageSize2);
    verify(bytesCounter, times(2)).inc(anyLong());
  }

  @Test
  public void shouldUseCorrectLabelsForInboundBytesCounter() {
    // Setup
    Capability capability = Capability.create("eth", 68);
    int messageCode = 5;
    int messageSize = 256;
    String messageName = "NewBlock";

    when(subProtocol.getName()).thenReturn("eth");
    when(subProtocol.isValidMessageCode(anyInt(), eq(messageCode))).thenReturn(true);
    when(subProtocol.messageName(anyInt(), eq(messageCode))).thenReturn(messageName);
    when(protocolManager.getSupportedCapabilities()).thenReturn(List.of(capability));

    // Start network runner to register handlers
    networkRunner.start();

    // Capture the message handler
    ArgumentCaptor<MessageCallback> handlerCaptor = ArgumentCaptor.forClass(MessageCallback.class);
    verify(network).subscribe(eq(capability), handlerCaptor.capture());

    MessageCallback handler = handlerCaptor.getValue();

    // Create test message
    PeerConnection peerConnection = mock(PeerConnection.class);
    MessageData messageData = mock(MessageData.class);
    when(messageData.getSize()).thenReturn(messageSize);
    when(messageData.getCode()).thenReturn(messageCode);
    Message message = new DefaultMessage(peerConnection, messageData);

    // Process message
    handler.onMessage(capability, message);

    // Verify correct labels were used (protocol, message name, code)
    verify(inboundBytesCounter)
        .labels(eq(capability.toString()), eq(messageName), eq(String.valueOf(messageCode)));
    verify(bytesCounter).inc(messageSize);
  }
}
