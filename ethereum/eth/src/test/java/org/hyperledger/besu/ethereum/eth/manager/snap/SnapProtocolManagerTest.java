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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.DefaultMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SnapProtocolManagerTest {

  @Mock private WorldStateStorageCoordinator worldStateStorageCoordinator;
  @Mock private SnapSyncConfiguration snapConfig;
  @Mock private EthPeers ethPeers;
  @Mock private EthMessages snapMessages;
  @Mock private ProtocolContext protocolContext;
  @Mock private Synchronizer synchronizer;
  @Mock private EthScheduler ethScheduler;
  @Mock private EthPeer ethPeer;
  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();

  private SnapProtocolManager snapProtocolManager;

  @BeforeEach
  void setUp() {
    when(snapConfig.isSnapServerEnabled()).thenReturn(false);
    snapProtocolManager = createSnapProtocolManager();
  }

  @Test
  void advertisesOnlySnap1WhenSnap2IsDisabled() {
    when(snapConfig.isSnap2Enabled()).thenReturn(false);

    snapProtocolManager = createSnapProtocolManager();

    assertThat(snapProtocolManager.getSupportedCapabilities()).containsExactly(SnapProtocol.SNAP1);
  }

  @Test
  void advertisesSnap2WhenSnap2IsEnabled() {
    when(snapConfig.isSnap2Enabled()).thenReturn(true);

    snapProtocolManager = createSnapProtocolManager();

    assertThat(snapProtocolManager.getSupportedCapabilities())
        .containsExactly(SnapProtocol.SNAP1, SnapProtocol.SNAP2);
  }

  @Test
  void disconnectsPeerOnDecompressionFailure() {
    final MockPeerConnection peerConnection =
        new MockPeerConnection(
            new HashSet<>(Collections.singletonList(SnapProtocol.SNAP1)), (cap, msg, conn) -> {});
    when(ethPeers.peer(peerConnection)).thenReturn(ethPeer);
    when(ethPeer.validateReceivedMessage(any(), any())).thenReturn(true);

    // Create a RawMessage with invalid compressed data that will throw FramingException
    final RawMessage badMessage = new RawMessage(0x00, new byte[] {0x01, 0x02, 0x03});
    snapProtocolManager.processMessage(
        SnapProtocol.SNAP1, new DefaultMessage(peerConnection, badMessage));

    assertThat(peerConnection.isDisconnected()).isFalse();
    // ethPeer (mock) receives the disconnect call
    verify(ethPeer).disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
  }

  @Test
  void limitsConcurrentSnapRequestsPerPeer() {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(2);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    stubPendingServiceTasks();

    for (int i = 0; i < 5; i++) {
      snapProtocolManager.processMessage(
          SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, i));
    }

    // Only the first 2 requests are scheduled; the remaining 3 are dropped once the per-peer cap
    // (reproducing the unbounded fan-out this test guards against, absent the cap) is reached.
    verify(ethScheduler, times(2)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void perPeerCapDoesNotBlockADistinctPeer() {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(2);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnectionA = snapPeerConnection();
    stubPeer(ethPeer, peerConnectionA);
    final EthPeer ethPeerB = mock(EthPeer.class);
    final MockPeerConnection peerConnectionB = snapPeerConnection();
    stubPeer(ethPeerB, peerConnectionB);
    stubPendingServiceTasks();

    for (int i = 0; i < 2; i++) {
      snapProtocolManager.processMessage(
          SnapProtocol.SNAP1, getTrieNodesMessage(peerConnectionA, i));
      snapProtocolManager.processMessage(
          SnapProtocol.SNAP1, getTrieNodesMessage(peerConnectionB, i));
    }

    // Both peers stay within their own per-peer cap, so all 4 requests are scheduled.
    verify(ethScheduler, times(4)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void limitsConcurrentSnapRequestsGlobally() {
    when(snapConfig.getMaxConcurrentSnapRequestsGlobal()).thenReturn(3);
    snapProtocolManager = createSnapProtocolManager();
    stubPendingServiceTasks();

    // 3 distinct peers, each individually under any per-peer cap, sending 2 requests each.
    for (int peerIndex = 0; peerIndex < 3; peerIndex++) {
      final EthPeer peer = mock(EthPeer.class);
      final MockPeerConnection peerConnection = snapPeerConnection();
      stubPeer(peer, peerConnection);
      for (int i = 0; i < 2; i++) {
        snapProtocolManager.processMessage(
            SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, i));
      }
    }

    // Only the first 3 requests (across all peers) are scheduled once the global cap is reached.
    verify(ethScheduler, times(3)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void releasesSlotWhenScheduledTaskCompletes() {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    final List<CompletableFuture<Void>> scheduledTasks = new ArrayList<>();
    when(ethScheduler.scheduleServiceTask(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              final CompletableFuture<Void> future = new CompletableFuture<>();
              scheduledTasks.add(future);
              return future;
            });

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 0));
    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 1));
    // Second request dropped: the peer's single slot is still held by the first, in-flight task.
    verify(ethScheduler, times(1)).scheduleServiceTask(any(Runnable.class));

    scheduledTasks.get(0).complete(null);

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 2));
    // Completing the first task freed the slot, so the third request is now accepted.
    verify(ethScheduler, times(2)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void releasesSlotWhenSchedulingThrowsSynchronously() {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    // Simulates CompletableFuture.runAsync throwing RejectedExecutionException synchronously,
    // e.g. because the services executor is shutting down, before scheduleServiceTask ever
    // returns a future to attach the release-on-complete handler to.
    when(ethScheduler.scheduleServiceTask(any(Runnable.class)))
        .thenThrow(new RejectedExecutionException("executor is shutting down"))
        .thenAnswer(invocation -> new CompletableFuture<Void>());

    assertThatThrownBy(
            () ->
                snapProtocolManager.processMessage(
                    SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 0)))
        .isInstanceOf(RejectedExecutionException.class);

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 1));
    // If the failed first attempt had leaked its reserved slot, this second request would be
    // dropped (only 1 call to scheduleServiceTask) instead of being scheduled (2 calls).
    verify(ethScheduler, times(2)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void clearsPeerSlotsOnDisconnectPreventingALeak() {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    stubPendingServiceTasks();

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 0));
    verify(ethScheduler, times(1)).scheduleServiceTask(any(Runnable.class));

    // The peer disconnects before its in-flight task ever completes.
    snapProtocolManager.handleDisconnect(
        peerConnection, DisconnectReason.TCP_SUBSYSTEM_ERROR, false);

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 1));
    // Disconnect cleanup freed the slot rather than leaking it, so this request is accepted.
    verify(ethScheduler, times(2)).scheduleServiceTask(any(Runnable.class));
  }

  @Test
  void respondsEmptyWhenPerPeerCapReached() throws PeerConnection.PeerNotConnected {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    stubPendingServiceTasks();

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 0));
    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 1));

    // The first request holds the peer's only slot; the second is rejected but still answered
    // rather than left to time out.
    verify(ethScheduler, times(1)).scheduleServiceTask(any(Runnable.class));
    final ArgumentCaptor<MessageData> sent = ArgumentCaptor.forClass(MessageData.class);
    verify(ethPeer).send(sent.capture(), eq(SnapProtocol.NAME));
    assertThat(sent.getValue().getCode()).isEqualTo(SnapV1.TRIE_NODES);
    assertThat(TrieNodesMessage.readFrom(sent.getValue()).nodes(true)).isEmpty();
    assertThat(sent.getValue().unwrapMessageData().getKey()).isEqualTo(BigInteger.ONE);
  }

  @Test
  void respondsEmptyWhenGlobalCapReached() throws PeerConnection.PeerNotConnected {
    when(snapConfig.getMaxConcurrentSnapRequestsGlobal()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();
    stubPendingServiceTasks();

    final MockPeerConnection peerConnectionA = snapPeerConnection();
    stubPeer(ethPeer, peerConnectionA);
    final EthPeer ethPeerB = mock(EthPeer.class);
    final MockPeerConnection peerConnectionB = snapPeerConnection();
    stubPeer(ethPeerB, peerConnectionB);

    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnectionA, 0));
    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnectionB, 7));

    // The global cap is already spent on peer A's request, so peer B's is rejected but answered.
    verify(ethScheduler, times(1)).scheduleServiceTask(any(Runnable.class));
    final ArgumentCaptor<MessageData> sent = ArgumentCaptor.forClass(MessageData.class);
    verify(ethPeerB).send(sent.capture(), eq(SnapProtocol.NAME));
    assertThat(sent.getValue().unwrapMessageData().getKey()).isEqualTo(BigInteger.valueOf(7));
    verify(ethPeer, never()).send(any(), any());
  }

  @Test
  void disconnectsPeerWhenRejectedRequestIsMalformed() throws PeerConnection.PeerNotConnected {
    when(snapConfig.getMaxConcurrentSnapRequestsPerPeer()).thenReturn(1);
    snapProtocolManager = createSnapProtocolManager();

    final MockPeerConnection peerConnection = snapPeerConnection();
    stubPeer(ethPeer, peerConnection);
    stubPendingServiceTasks();

    // First request holds the peer's only slot.
    snapProtocolManager.processMessage(SnapProtocol.SNAP1, getTrieNodesMessage(peerConnection, 0));

    // Second request is over cap, and its body isn't a valid RLP list, so decoding a request id
    // for the empty reply fails.
    final MessageData malformed = new RawMessage(SnapV1.GET_TRIE_NODES, Bytes.of(0x01));
    snapProtocolManager.processMessage(
        SnapProtocol.SNAP1, new DefaultMessage(peerConnection, malformed));

    verify(ethPeer).disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    verify(ethPeer, never()).send(any(), any());
  }

  private void stubPeer(final EthPeer peer, final PeerConnection connection) {
    when(ethPeers.peer(connection)).thenReturn(peer);
    when(peer.getConnection()).thenReturn(connection);
    when(peer.validateReceivedMessage(any(), any())).thenReturn(true);
  }

  private void stubPendingServiceTasks() {
    when(ethScheduler.scheduleServiceTask(any(Runnable.class)))
        .thenAnswer(invocation -> new CompletableFuture<Void>());
  }

  private MockPeerConnection snapPeerConnection() {
    return new MockPeerConnection(
        new HashSet<>(Collections.singletonList(SnapProtocol.SNAP1)), (cap, msg, conn) -> {});
  }

  private Message getTrieNodesMessage(final PeerConnection peerConnection, final int requestId) {
    final MessageData data =
        GetTrieNodesMessage.create(Hash.ZERO, List.of(List.of(Bytes.EMPTY)))
            .wrapMessageData(BigInteger.valueOf(requestId));
    return new DefaultMessage(peerConnection, data);
  }

  private SnapProtocolManager createSnapProtocolManager() {
    return new SnapProtocolManager(
        worldStateStorageCoordinator,
        snapConfig,
        ethPeers,
        snapMessages,
        ethScheduler,
        protocolContext,
        synchronizer,
        metricsSystem);
  }
}
