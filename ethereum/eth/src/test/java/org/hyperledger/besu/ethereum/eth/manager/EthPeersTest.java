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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.NoAvailablePeersException;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.PeerDisconnectedException;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.sync.ChainHeadTracker;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class EthPeersTest {

  private EthProtocolManager ethProtocolManager;
  private EthPeers ethPeers;
  private final PeerRequest peerRequest = mock(PeerRequest.class);
  private final RequestManager.ResponseStream responseStream =
      mock(RequestManager.ResponseStream.class);

  @BeforeEach
  public void setup() throws Exception {
    when(peerRequest.sendRequest(any())).thenReturn(responseStream);
    ethProtocolManager = EthProtocolManagerTestBuilder.builder().build();
    ethPeers = ethProtocolManager.ethContext().getEthPeers();
    final ChainHeadTracker mock = mock(ChainHeadTracker.class);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(mock.getBestHeaderFromPeer(any()))
        .thenReturn(CompletableFuture.completedFuture(blockHeader));
    ethPeers.setChainHeadTracker(mock);
  }

  @Test
  public void comparesPeersWithHeightAndTd() {
    // Set peerA with better height, lower td
    final EthPeerImmutableAttributes peerA =
        EthPeerImmutableAttributes.from(
            EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(50), 20)
                .getEthPeer());
    final EthPeerImmutableAttributes peerB =
        EthPeerImmutableAttributes.from(
            EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(100), 10)
                .getEthPeer());

    assertThat(EthPeers.CHAIN_HEIGHT.compare(peerA, peerB)).isGreaterThan(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY.compare(peerA, peerB)).isLessThan(0);

    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerA, peerB)).isLessThan(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerB, peerA)).isGreaterThan(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerA, peerA)).isEqualTo(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerB, peerB)).isEqualTo(0);

    assertThat(ethProtocolManager.ethContext().getEthPeers().bestPeer()).contains(peerB.ethPeer());
    assertThat(ethProtocolManager.ethContext().getEthPeers().bestPeerWithHeightEstimate())
        .contains(peerB.ethPeer());
  }

  @Test
  public void comparesPeersWithTdAndNoHeight() {
    final EthPeerImmutableAttributes peerA =
        EthPeerImmutableAttributes.from(
            EthProtocolManagerTestUtil.createPeer(
                    ethProtocolManager, Difficulty.of(100), OptionalLong.empty())
                .getEthPeer());
    final EthPeerImmutableAttributes peerB =
        EthPeerImmutableAttributes.from(
            EthProtocolManagerTestUtil.createPeer(
                    ethProtocolManager, Difficulty.of(50), OptionalLong.empty())
                .getEthPeer());

    // Sanity check
    assertThat(peerA.estimatedChainHeight()).isEqualTo(0);
    assertThat(peerB.estimatedChainHeight()).isEqualTo(0);

    assertThat(EthPeers.CHAIN_HEIGHT.compare(peerA, peerB)).isEqualTo(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY.compare(peerA, peerB)).isGreaterThan(0);

    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerA, peerB)).isGreaterThan(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerB, peerA)).isLessThan(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerA, peerA)).isEqualTo(0);
    assertThat(EthPeers.TOTAL_DIFFICULTY_THEN_HEIGHT.compare(peerB, peerB)).isEqualTo(0);

    assertThat(ethProtocolManager.ethContext().getEthPeers().bestPeer()).contains(peerA.ethPeer());
    assertThat(ethProtocolManager.ethContext().getEthPeers().bestPeerWithHeightEstimate())
        .isEmpty();
  }

  @Test
  public void shouldExecutePeerRequestImmediatelyWhenPeerIsAvailable() throws Exception {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    when(peerRequest.isEthPeerSuitable(EthPeerImmutableAttributes.from(peer.getEthPeer())))
        .thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.empty());

    verify(peerRequest).sendRequest(peer.getEthPeer());
    assertRequestSuccessful(pendingRequest);
  }

  @Test
  public void shouldUseLeastBusyPeerForRequest() throws Exception {
    final RespondingEthPeer idlePeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final RespondingEthPeer workingPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    useRequestSlot(workingPeer.getEthPeer());

    when(peerRequest.isEthPeerSuitable(any())).thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.empty());

    verify(peerRequest).sendRequest(idlePeer.getEthPeer());
    assertRequestSuccessful(pendingRequest);
  }

  @Test
  public void shouldUseLeastRecentlyUsedPeerWhenBothHaveSameNumberOfOutstandingRequests()
      throws Exception {
    final RespondingEthPeer mostRecentlyUsedPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final RespondingEthPeer leastRecentlyUsedPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    useRequestSlot(mostRecentlyUsedPeer.getEthPeer());
    freeUpCapacity(mostRecentlyUsedPeer.getEthPeer());

    assertThat(leastRecentlyUsedPeer.getEthPeer().outstandingRequests())
        .isEqualTo(mostRecentlyUsedPeer.getEthPeer().outstandingRequests());

    when(peerRequest.isEthPeerSuitable(any())).thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.empty());

    verify(peerRequest).sendRequest(leastRecentlyUsedPeer.getEthPeer());
    assertRequestSuccessful(pendingRequest);
  }

  @Test
  public void shouldFailWithNoAvailablePeersWhenNoPeersConnected() {
    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.empty());

    verifyNoInteractions(peerRequest);
    assertRequestFailure(pendingRequest, NoAvailablePeersException.class);
  }

  @Test
  public void shouldFailWhenNoPeerWithSufficientHeight() {
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 100);
    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 200, Optional.empty());

    verifyNoInteractions(peerRequest);
    assertRequestFailure(pendingRequest, NoAvailablePeersException.class);
  }

  @Test
  public void shouldFailWhenAllPeersWithSufficientHeightHaveDisconnected() throws Exception {
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 100);
    final RespondingEthPeer suitablePeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    useAllAvailableCapacity(suitablePeer.getEthPeer());

    when(peerRequest.isEthPeerSuitable(EthPeerImmutableAttributes.from(suitablePeer.getEthPeer())))
        .thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 200, Optional.empty());

    verify(peerRequest, times(0)).sendRequest(suitablePeer.getEthPeer());

    assertNotDone(pendingRequest);

    suitablePeer.disconnect(DisconnectReason.TOO_MANY_PEERS);
    assertRequestFailure(pendingRequest, NoAvailablePeersException.class);
  }

  @Test
  public void shouldFailWithPeerNotConnectedIfPeerRequestThrows() throws Exception {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    when(peerRequest.sendRequest(peer.getEthPeer())).thenThrow(new PeerNotConnected("Oh dear"));
    when(peerRequest.isEthPeerSuitable(any())).thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 100, Optional.empty());

    assertRequestFailure(pendingRequest, PeerDisconnectedException.class);
  }

  @Test
  public void shouldDelayExecutionUntilPeerHasCapacity() throws Exception {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    useAllAvailableCapacity(peer.getEthPeer());

    when(peerRequest.isEthPeerSuitable(any())).thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 100, Optional.empty());
    verify(peerRequest, times(0)).sendRequest(peer.getEthPeer());

    freeUpCapacity(peer.getEthPeer());

    verify(peerRequest).sendRequest(peer.getEthPeer());
    assertRequestSuccessful(pendingRequest);
  }

  @Test
  public void shouldDelayExecutionUntilPeerWithSufficientHeightHasCapacity() throws Exception {
    // Create a peer that has available capacity but not the required height
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 10);

    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    when(peerRequest.isEthPeerSuitable(Mockito.any()))
        .thenAnswer(
            (invocationOnMock) -> {
              EthPeerImmutableAttributes ethPeer =
                  invocationOnMock.getArgument(0, EthPeerImmutableAttributes.class);
              return ethPeer.ethPeer().equals(peer.getEthPeer());
            });
    useAllAvailableCapacity(peer.getEthPeer());

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 100, Optional.empty());
    verify(peerRequest, times(0)).sendRequest(peer.getEthPeer());

    freeUpCapacity(peer.getEthPeer());

    verify(peerRequest).sendRequest(peer.getEthPeer());
    assertRequestSuccessful(pendingRequest);
  }

  @Test
  public void shouldNotExecuteAbortedRequest() throws Exception {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    useAllAvailableCapacity(peer.getEthPeer());

    when(peerRequest.isEthPeerSuitable(EthPeerImmutableAttributes.from(peer.getEthPeer())))
        .thenReturn(true);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 100, Optional.empty());
    verify(peerRequest, times(0)).sendRequest(peer.getEthPeer());

    pendingRequest.abort();

    freeUpCapacity(peer.getEthPeer());

    verify(peerRequest, times(0)).sendRequest(peer.getEthPeer());
    assertRequestFailure(pendingRequest, CancellationException.class);
  }

  // We had a bug where if a peer was busy when it was disconnected, pending peer requests that were
  // *explicitly* assigned to that peer would never be attempted and thus never completed
  @Test
  public void shouldFailRequestWithBusyDisconnectedAssignedPeer() throws Exception {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final EthPeer ethPeer = peer.getEthPeer();
    useAllAvailableCapacity(ethPeer);

    final PendingPeerRequest pendingRequest =
        ethPeers.executePeerRequest(peerRequest, 100, Optional.of(ethPeer));

    ethPeer.disconnect(DisconnectReason.UNKNOWN);
    ethPeers.registerDisconnect(ethPeer.getConnection());

    assertRequestFailure(pendingRequest, CancellationException.class);
  }

  @Test
  public void shouldNotFailWhenAttemptExecutionDisconnectSamePeer() throws PeerNotConnected {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final EthPeer ethPeer = spy(peer.getEthPeer());

    // Force request to be added to pending request list
    when(ethPeer.hasAvailableRequestCapacity()).thenReturn(false);

    final PendingPeerRequest pendingPeerRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.of(ethPeer));

    // Force Request Attempt to cause the peer to disconnect
    when(ethPeer.hasAvailableRequestCapacity())
        .thenAnswer(
            (Answer<Boolean>)
                invocation -> {
                  // Force Disconnect only on the first execution
                  if (!peer.getPeerConnection().isDisconnected()) {
                    peer.disconnect(DisconnectReason.UNKNOWN); // Force Peer to disconnect
                  }
                  return true;
                });

    // Sent Pending Requests
    ethPeers.reattemptPendingPeerRequests();

    // Request should be aborted.
    assertRequestFailure(pendingPeerRequest, CancellationException.class);

    // Mock works
    assertThat(peer.getEthPeer().isDisconnected()).isTrue(); // peer is disconnected
  }

  @Test
  public void shouldNotFailWhenAttemptExecutionDisconnectAnotherPeer() throws PeerNotConnected {
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final EthPeer ethPeer = spy(peer.getEthPeer());

    // Force request to be added to pending request list
    when(ethPeer.hasAvailableRequestCapacity()).thenReturn(false);

    final PendingPeerRequest pendingPeerRequest =
        ethPeers.executePeerRequest(peerRequest, 10, Optional.of(ethPeer));

    final RespondingEthPeer peerToDisconnect =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    // Force Request Attempt to cause the peer to disconnect
    when(ethPeer.hasAvailableRequestCapacity())
        .thenAnswer(
            (Answer<Boolean>)
                invocation -> {
                  // Force Disconnect only on the first execution
                  if (!peerToDisconnect.getPeerConnection().isDisconnected()) {
                    peerToDisconnect.disconnect(
                        DisconnectReason.UNKNOWN); // Force Peer to disconnect
                  }
                  return true;
                });

    // Sent Pending Requests
    ethPeers.reattemptPendingPeerRequests();

    // Request Should Execute
    assertRequestSuccessful(pendingPeerRequest);

    // Mock works
    assertThat(peerToDisconnect.getEthPeer().isDisconnected()).isTrue(); // peer is disconnected
  }

  @Test
  public void toString_hasExpectedInfo() {
    assertThat(ethPeers.toString()).isEqualTo("0 EthPeers {}");

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(50), 20)
            .getEthPeer();
    ethPeers.registerNewConnection(peerA.getConnection(), Collections.emptyList());
    assertThat(ethPeers.toString()).contains("1 EthPeers {");
    assertThat(ethPeers.toString()).contains(peerA.getLoggableId());
  }

  @Test
  public void snapServersPreferredWhileSyncing() {

    ethPeers.snapServerPeersNeeded(true);

    while (ethPeers.peerCount() < ethPeers.getMaxPeers()) {
      final EthPeer ethPeer =
          EthProtocolManagerTestUtil.createPeer(
                  ethProtocolManager, Difficulty.of(50), 20, false, false)
              .getEthPeer();
      assertThat(ethPeers.addPeerToEthPeers(ethPeer)).isTrue();
    }

    final EthPeer nonSnapServingPeer =
        EthProtocolManagerTestUtil.createPeer(
                ethProtocolManager, Difficulty.of(50), 20, false, false)
            .getEthPeer();

    assertThat(ethPeers.addPeerToEthPeers(nonSnapServingPeer)).isFalse();
    assertThat(nonSnapServingPeer.getConnection().isDisconnected()).isTrue();

    final EthPeer snapServingPeer =
        EthProtocolManagerTestUtil.createPeer(
                ethProtocolManager, Difficulty.of(50), 20, true, false)
            .getEthPeer();

    assertThat(ethPeers.addPeerToEthPeers(snapServingPeer)).isTrue();
    assertThat(ethPeers.peerCount()).isEqualTo(ethPeers.getMaxPeers());
  }

  @Test
  public void snapServersNotPreferredWhenInSync() {

    ethPeers.snapServerPeersNeeded(false);

    while (ethPeers.peerCount() < ethPeers.getMaxPeers()) {
      final EthPeer ethPeer =
          EthProtocolManagerTestUtil.createPeer(
                  ethProtocolManager, Difficulty.of(50), 20, false, false)
              .getEthPeer();
      assertThat(ethPeers.addPeerToEthPeers(ethPeer)).isTrue();
    }

    final EthPeer snapServingPeer =
        EthProtocolManagerTestUtil.createPeer(
                ethProtocolManager, Difficulty.of(50), 20, true, false)
            .getEthPeer();

    assertThat(ethPeers.addPeerToEthPeers(snapServingPeer)).isFalse();
    assertThat(snapServingPeer.getConnection().isDisconnected()).isTrue();
    assertThat(ethPeers.peerCount()).isEqualTo(ethPeers.getMaxPeers());
  }

  private void freeUpCapacity(final EthPeer ethPeer) {
    MessageData message = BlockBodiesMessage.create(emptyList());
    ethPeers.dispatchMessage(
        ethPeer, new EthMessage(ethPeer, message.wrapMessageData(BigInteger.ONE)));
    assertThat(ethPeer.hasAvailableRequestCapacity()).isTrue();
  }

  private void useAllAvailableCapacity(final EthPeer peer) throws PeerNotConnected {
    while (peer.hasAvailableRequestCapacity()) {
      useRequestSlot(peer);
    }
    assertThat(peer.hasAvailableRequestCapacity()).isFalse();
  }

  private void useRequestSlot(final EthPeer peer) throws PeerNotConnected {
    peer.getBodies(singletonList(Hash.ZERO));
  }

  @SuppressWarnings("unchecked")
  private void assertRequestSuccessful(final PendingPeerRequest pendingRequest) {
    final Consumer<RequestManager.ResponseStream> onSuccess = mock(Consumer.class);
    pendingRequest.then(onSuccess, error -> fail("Request should have executed", error));
    verify(onSuccess).accept(any());
  }

  @SuppressWarnings("unchecked")
  private void assertRequestFailure(
      final PendingPeerRequest pendingRequest, final Class<? extends Throwable> reason) {
    final Consumer<Throwable> errorHandler = mock(Consumer.class);
    pendingRequest.then(responseStream -> fail("Should not have performed request"), errorHandler);

    verify(errorHandler).accept(any(reason));
  }

  @SuppressWarnings("unchecked")
  private void assertNotDone(final PendingPeerRequest pendingRequest) {
    final Consumer<RequestManager.ResponseStream> onSuccess = mock(Consumer.class);
    final Consumer<Throwable> onError = mock(Consumer.class);
    pendingRequest.then(onSuccess, onError);

    verifyNoInteractions(onSuccess);
    verifyNoInteractions(onError);
  }
}
