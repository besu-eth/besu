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
package org.hyperledger.besu.ethereum.p2p.discovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class CompositePeerDiscoveryAgentTest {

  private final SharedDiscoveryTransport transport = mock(SharedDiscoveryTransport.class);

  @Test
  public void constructor_rejectsBothAgentsNull() {
    assertThat(catchThrowable(() -> new CompositePeerDiscoveryAgent(null, null, transport)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void start_wiresHandlersOnBothAgentsBeforeTransportStarts() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(true);
    when(transport.start()).thenReturn(new CompletableFuture<>());
    when(agentV4.start(anyInt())).thenReturn(new CompletableFuture<>());
    when(agentV5.start(anyInt())).thenReturn(new CompletableFuture<>());

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);
    composite.start(30303);

    final InOrder inOrder = inOrder(agentV4, agentV5, transport);
    inOrder.verify(agentV4).prepareHandlers();
    inOrder.verify(agentV5).prepareHandlers();
    inOrder.verify(transport).start();
  }

  @Test
  public void start_returnsV5PortWhenBothAgentsPresent() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(true);
    when(transport.start()).thenReturn(CompletableFuture.completedFuture(null));
    when(agentV4.start(30303)).thenReturn(CompletableFuture.completedFuture(30303));
    when(agentV5.start(30303)).thenReturn(CompletableFuture.completedFuture(9999));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.start(30303)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(9999);
  }

  @Test
  public void start_v4Only_doesNotTouchV5() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(true);
    when(transport.start()).thenReturn(CompletableFuture.completedFuture(null));
    when(agentV4.start(30303)).thenReturn(CompletableFuture.completedFuture(30303));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, null, transport);

    assertThat(composite.start(30303)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(30303);
    verify(agentV4).prepareHandlers();
  }

  @Test
  public void start_rollsBackOnTransportStartFailure() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(true);
    when(transport.start())
        .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("bind failed")));
    when(transport.stop()).thenReturn(CompletableFuture.completedFuture(null));
    when(agentV4.stop()).thenReturn(CompletableFuture.completedFuture(null));
    when(agentV5.stop()).thenReturn(CompletableFuture.completedFuture(null));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.start(30303)).failsWithin(Duration.ofSeconds(5));
    // Rollback stops whatever was wired, even though neither sub-agent's start() ever ran.
    verify(agentV4, times(1)).stop();
    verify(agentV5, times(1)).stop();
    verify(transport, times(1)).stop();
  }

  @Test
  public void start_secondCallFailsWithoutRestarting() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(true);
    when(transport.start()).thenReturn(CompletableFuture.completedFuture(null));
    when(agentV4.start(30303)).thenReturn(CompletableFuture.completedFuture(30303));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, null, transport);
    composite.start(30303).join();

    assertThat(composite.start(30303)).failsWithin(Duration.ofSeconds(5));
    verify(agentV4, times(1)).start(30303);
  }

  @Test
  public void stop_stopsBothAgentsThenTransport_evenIfAnAgentStopFails() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.stop()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
    when(agentV5.stop()).thenReturn(CompletableFuture.completedFuture(null));
    when(transport.stop()).thenReturn(CompletableFuture.completedFuture(null));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    composite.stop().join();

    verify(agentV4).stop();
    verify(agentV5).stop();
    // The failed V4 stop must not prevent the transport (and its event loop) from shutting down.
    verify(transport).stop();
  }

  @Test
  public void stop_isIdempotent() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    when(agentV4.stop()).thenReturn(CompletableFuture.completedFuture(null));
    when(transport.stop()).thenReturn(CompletableFuture.completedFuture(null));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, null, transport);

    composite.stop().join();
    composite.stop().join();

    verify(agentV4, times(1)).stop();
    verify(transport, times(1)).stop();
  }

  @Test
  public void streamDiscoveredPeers_prefersV5WhenBothAgentsKnowSamePeer() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);

    final Bytes sharedId = Bytes.fromHexString("0x1234");
    final DiscoveryPeer fromV4 = mock(DiscoveryPeer.class, RETURNS_DEEP_STUBS);
    when(fromV4.getId()).thenReturn(sharedId);
    final DiscoveryPeer fromV5 = mock(DiscoveryPeer.class, RETURNS_DEEP_STUBS);
    when(fromV5.getId()).thenReturn(sharedId);

    doReturn(Stream.of(fromV4)).when(agentV4).streamDiscoveredPeers();
    doReturn(Stream.of(fromV5)).when(agentV5).streamDiscoveredPeers();

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    final List<DiscoveryPeer> result =
        composite
            .streamDiscoveredPeers()
            .map(DiscoveryPeer.class::cast)
            .collect(Collectors.toList());

    assertThat(result).containsExactly(fromV5);
  }

  @Test
  public void getPeer_fallsBackToV4WhenV5DoesNotKnowThePeer() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    final PeerId peerId = mock(PeerId.class);
    final Peer v4Peer = mock(Peer.class);

    when(agentV5.getPeer(peerId)).thenReturn(Optional.empty());
    when(agentV4.getPeer(peerId)).thenReturn(Optional.of(v4Peer));

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.getPeer(peerId)).contains(v4Peer);
  }

  @Test
  public void checkForkId_bothAgentsPresent_delegatesToV4AndSkipsV5() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    final DiscoveryPeer peer = mock(DiscoveryPeer.class);
    when(agentV4.checkForkId(peer)).thenReturn(true);
    when(agentV5.checkForkId(peer)).thenReturn(false);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.checkForkId(peer)).isTrue();
    verify(agentV5, never()).checkForkId(peer);
  }

  @Test
  public void checkForkId_v4Only_delegatesToV4() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final DiscoveryPeer peer = mock(DiscoveryPeer.class);
    when(agentV4.checkForkId(peer)).thenReturn(true);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, null, transport);

    assertThat(composite.checkForkId(peer)).isTrue();
  }

  @Test
  public void checkForkId_v5Only_delegatesToV5() {
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    final DiscoveryPeer peer = mock(DiscoveryPeer.class);
    when(agentV5.checkForkId(peer)).thenReturn(false);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(null, agentV5, transport);

    assertThat(composite.checkForkId(peer)).isFalse();
  }

  @Test
  public void isEnabled_trueIfEitherAgentEnabled() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(false);
    when(agentV5.isEnabled()).thenReturn(true);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.isEnabled()).isTrue();
  }

  @Test
  public void isStopped_trueOnlyIfBothAgentsStopped() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    final PeerDiscoveryAgent agentV5 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isStopped()).thenReturn(true);
    when(agentV5.isStopped()).thenReturn(false);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);

    assertThat(composite.isStopped()).isFalse();

    when(agentV5.isStopped()).thenReturn(true);
    assertThat(composite.isStopped()).isTrue();
  }

  @Test
  public void start_disabledComposite_neverTouchesTransport() {
    final PeerDiscoveryAgent agentV4 = mock(PeerDiscoveryAgent.class);
    when(agentV4.isEnabled()).thenReturn(false);

    final CompositePeerDiscoveryAgent composite =
        new CompositePeerDiscoveryAgent(agentV4, null, transport);

    assertThat(composite.start(30303)).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(0);
    verify(transport, never()).start();
    verify(agentV4, never()).prepareHandlers();
  }
}
