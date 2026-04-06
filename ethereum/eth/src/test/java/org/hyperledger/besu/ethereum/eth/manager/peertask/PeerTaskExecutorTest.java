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
package org.hyperledger.besu.ethereum.eth.manager.peertask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class PeerTaskExecutorTest {
  private @Mock PeerSelector peerSelector;
  private @Mock PeerTaskRequestSender requestSender;
  private @Mock PeerTask<Object> peerTask;
  private @Mock SubProtocol subprotocol;
  private @Mock MessageData requestMessageData;
  private @Mock MessageData responseMessageData;
  private @Mock EthPeer ethPeer;
  private AutoCloseable mockCloser;

  private PeerTaskExecutor peerTaskExecutor;

  @BeforeEach
  public void beforeTest() {
    mockCloser = MockitoAnnotations.openMocks(this);
    peerTaskExecutor = new PeerTaskExecutor(peerSelector, requestSender, new NoOpMetricsSystem());
    when(ethPeer.getAgreedCapabilities()).thenReturn(Set.of(EthProtocol.LATEST));
  }

  @AfterEach
  public void afterTest() throws Exception {
    mockCloser.close();
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndSuccessfulFlow()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {

    Object responseObject = new Object();

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any()))
        .thenReturn(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).recordUsefulResponse();

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertSame(responseObject, result.result().get());
    assertEquals(PeerTaskExecutorResponseCode.SUCCESS, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndPeerShouldBeDisconnected()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {

    Object responseObject = new Object();

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any()))
        .thenReturn(PeerTaskValidationResponse.NON_SEQUENTIAL_HEADERS_RETURNED);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer)
        .disconnect(DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL_NON_SEQUENTIAL_HEADERS);

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertSame(responseObject, result.result().get());
    assertEquals(PeerTaskExecutorResponseCode.INVALID_RESPONSE, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndPeerSuppliedMalformedRlp()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any()))
        .thenThrow(new MalformedRlpFromPeerException(new Exception(), Bytes.EMPTY));

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);

    assertNotNull(result);
    assertFalse(result.result().isPresent());
    assertEquals(PeerTaskExecutorResponseCode.INVALID_RESPONSE, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndPartialSuccessfulFlow()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {

    Object responseObject = new Object();

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any())).thenReturn(PeerTaskValidationResponse.NO_RESULTS_RETURNED);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertEquals(PeerTaskExecutorResponseCode.INVALID_RESPONSE, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithRetriesAndSuccessfulFlowAfterFirstFailure()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {
    Object responseObject = new Object();
    int requestMessageDataCode = 123;
    String protocolName = "snap";

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(2);

    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn(protocolName);
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenThrow(new TimeoutException())
        .thenReturn(responseMessageData);
    when(requestMessageData.getCode()).thenReturn(requestMessageDataCode);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any()))
        .thenReturn(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).recordRequestTimeout(protocolName, requestMessageDataCode);
    verify(ethPeer).recordUsefulResponse();

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertSame(responseObject, result.result().get());
    assertEquals(PeerTaskExecutorResponseCode.SUCCESS, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndPeerNotConnected()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException {

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenThrow(new PeerConnection.PeerNotConnected(""));

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    assertNotNull(result);
    assertTrue(result.result().isEmpty());
    assertEquals(PeerTaskExecutorResponseCode.PEER_DISCONNECTED, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndTimeoutException()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException {
    int requestMessageDataCode = 123;
    String protocolName = "snap";

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn(protocolName);
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenThrow(new TimeoutException());
    when(requestMessageData.getCode()).thenReturn(requestMessageDataCode);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).recordRequestTimeout(protocolName, requestMessageDataCode);

    assertNotNull(result);
    assertTrue(result.result().isEmpty());
    assertEquals(PeerTaskExecutorResponseCode.TIMEOUT, result.responseCode());
  }

  @Test
  public void testExecuteAgainstPeerWithNoRetriesAndInvalidResponseMessage()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenThrow(new InvalidPeerTaskResponseException());

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).recordUselessResponse(null);

    assertNotNull(result);
    assertTrue(result.result().isEmpty());
    assertEquals(PeerTaskExecutorResponseCode.INVALID_RESPONSE, result.responseCode());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithNoRetriesAndSuccessFlow()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {
    Object responseObject = new Object();

    when(peerSelector.getPeer(any(Predicate.class))).thenReturn(Optional.of(ethPeer));

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithOtherPeer()).thenReturn(0);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn("subprotocol");
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any()))
        .thenReturn(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.executeAgainstPeer(peerTask, ethPeer);

    verify(ethPeer).recordUsefulResponse();

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertSame(responseObject, result.result().get());
    assertEquals(PeerTaskExecutorResponseCode.SUCCESS, result.responseCode());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithPeerSwitchingAndSuccessFlow()
      throws PeerConnection.PeerNotConnected,
          ExecutionException,
          InterruptedException,
          TimeoutException,
          InvalidPeerTaskResponseException,
          MalformedRlpFromPeerException {
    Object responseObject = new Object();
    int requestMessageDataCode = 123;
    String protocolName = "snap";
    EthPeer peer2 = Mockito.mock(EthPeer.class);

    when(peerSelector.getPeer(any(Predicate.class)))
        .thenReturn(Optional.of(ethPeer))
        .thenReturn(Optional.of(peer2));

    when(peerTask.getRequestMessage(any())).thenReturn(requestMessageData);
    when(peerTask.getRetriesWithOtherPeer()).thenReturn(2);
    when(peerTask.getRetriesWithSamePeer()).thenReturn(0);
    when(peerTask.getSubProtocol()).thenReturn(subprotocol);
    when(subprotocol.getName()).thenReturn(protocolName);
    when(requestSender.sendRequest(subprotocol, requestMessageData, ethPeer))
        .thenThrow(new TimeoutException());
    when(requestMessageData.getCode()).thenReturn(requestMessageDataCode);
    when(requestSender.sendRequest(subprotocol, requestMessageData, peer2))
        .thenReturn(responseMessageData);
    when(peerTask.processResponse(any(), any())).thenReturn(responseObject);
    when(peerTask.validateResult(any()))
        .thenReturn(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD);

    PeerTaskExecutorResult<Object> result = peerTaskExecutor.execute(peerTask);

    verify(ethPeer).recordRequestTimeout(protocolName, requestMessageDataCode);
    verify(peer2).recordUsefulResponse();

    assertNotNull(result);
    assertTrue(result.result().isPresent());
    assertSame(responseObject, result.result().get());
    assertEquals(PeerTaskExecutorResponseCode.SUCCESS, result.responseCode());
  }
}
