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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocolVersion;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.MockPeerConnection;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Which peers take which requests, with peers whose capabilities and load the test sets. */
@Timeout(30)
class RequestSchedulerPeersTest {

  private final EthPeers ethPeers = mock(EthPeers.class);
  private final List<EthPeer> connected = new CopyOnWriteArrayList<>();
  private final Map<EthPeer, List<MessageData>> sent = new ConcurrentHashMap<>();
  private RequestScheduler scheduler;

  @BeforeEach
  void setUp() {
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(
            invocation ->
                connected.stream()
                    .map(EthPeerImmutableAttributes::from)
                    .filter(peer -> !peer.isDisconnected()));
    scheduler =
        new RequestScheduler(ethPeers, new NoOpMetricsSystem(), Duration.ofSeconds(10), 4, 4, 1);
  }

  @AfterEach
  void tearDown() {
    scheduler.stop();
  }

  @Test
  void sendsTheSnap2OnlyRequestsToSnap2Peers() {
    final EthPeer snap1 =
        connect(List.of(), EthProtocol.LATEST, SnapProtocolVersion.V1.getCapability());
    final EthPeer snap2 =
        connect(List.of(), EthProtocol.LATEST, SnapProtocolVersion.V2.getCapability());
    assertThat(scheduler.peerCount(Protocol.SNAP)).isEqualTo(2);
    assertThat(scheduler.peerCount(Protocol.SNAP2)).isEqualTo(1);

    // the snap/2 peer takes two, the third waits for it
    for (int i = 0; i < 3; i++) {
      scheduler.submit(blockAccessLists());
    }
    await().until(() -> sent(snap2).size() == 2);
    // a range request, dispatched after the waiting one, goes to the snap/1 peer
    scheduler.submit(accountRange());
    await().until(() -> !sent(snap1).isEmpty());

    assertThat(sent(snap1))
        .extracting(MessageData::getCode)
        .containsExactly(SnapV2.GET_ACCOUNT_RANGE);
    assertThat(sent(snap2))
        .extracting(MessageData::getCode)
        .containsExactly(SnapV2.GET_BLOCK_ACCESS_LISTS, SnapV2.GET_BLOCK_ACCESS_LISTS);
  }

  @Test
  void leavesARequestOfEachPeerToTheOtherDownloads() {
    final EthPeer peer = connect(List.of(), EthProtocol.LATEST);
    final List<ResponseStream> others = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      others.add(send(peer, GetBlockHeadersMessage.create(1, 1, 0, false)));
    }

    scheduler.submit(headers());
    scheduler.submit(headers());

    // one more request would leave the peer no room for the other downloads
    await().until(() -> scheduler.capacity(Protocol.ETH) == 1);
    assertThat(sent(peer)).hasSize(4);
    assertThat(peer.hasAvailableRequestCapacity()).isTrue();

    // once another download got its answer, the scheduler takes the room it left
    others.getFirst().close();
    await().until(() -> sent(peer).size() == 5);
  }

  @Test
  void countsOnlyTheFullyValidatedPeers() {
    final EthPeer validated =
        connect(List.of(), EthProtocol.LATEST, SnapProtocolVersion.V2.getCapability());
    final EthPeer pending =
        connect(
            List.of(mock(PeerValidator.class)),
            EthProtocol.LATEST,
            SnapProtocolVersion.V2.getCapability());
    validated.chainState().updateHeightEstimate(10);
    pending.chainState().updateHeightEstimate(1_000);

    assertThat(scheduler.peerCount(Protocol.ETH)).isEqualTo(1);
    assertThat(scheduler.peerCount(Protocol.SNAP2)).isEqualTo(1);
    assertThat(scheduler.bestPeerHeight()).isEqualTo(10);
    assertThat(scheduler.capacity(Protocol.SNAP)).isEqualTo(2);
  }

  @Test
  void ignoresThePeersWithoutAKnownHeight() {
    connectAt(1_000);
    connect(List.of(), EthProtocol.LATEST);

    assertThat(scheduler.bestPeerHeight()).isEqualTo(1_000);
  }

  @Test
  void takesTheLowerHeightOfTwoPeers() {
    connectAt(900);
    connectAt(1_000);

    assertThat(scheduler.bestPeerHeight()).isEqualTo(900);
  }

  @Test
  void knowsNoHeightWithoutPeers() {
    assertThat(scheduler.bestPeerHeight()).isZero();
  }

  @Test
  void prefersThePeersAtTheBestHeight() {
    runTheLoopOnTheTestThread();
    final EthPeer behind = connectAt(900);
    final EthPeer first = connectAt(1_000);
    final EthPeer second = connectAt(1_000);
    assertThat(scheduler.bestPeerHeight()).isEqualTo(1_000);

    // the two peers at the tip take the lookups as long as they have free slots
    for (int i = 0; i < 4; i++) {
      scheduler.submit(pivotHeader(scheduler.bestPeerHeight()));
    }

    assertThat(sent(first)).hasSize(2);
    assertThat(sent(second)).hasSize(2);
    assertThat(sent(behind)).isEmpty();
  }

  @Test
  void ignoresAHeightNoOtherPeerReaches() {
    runTheLoopOnTheTestThread();
    final EthPeer first = connectAt(1_000);
    final EthPeer second = connectAt(1_000);
    final EthPeer outlier = connectAt(1_000_000_000);
    // the highest height two peers reach
    assertThat(scheduler.bestPeerHeight()).isEqualTo(1_000);

    // no peer looks behind: the lookups spread over the three
    for (int i = 0; i < 3; i++) {
      scheduler.submit(pivotHeader(scheduler.bestPeerHeight()));
    }

    assertThat(sent(first)).hasSize(1);
    assertThat(sent(second)).hasSize(1);
    assertThat(sent(outlier)).hasSize(1);
  }

  // replaces the scheduler with one whose loop runs on the test thread and skips delayed tasks
  private void runTheLoopOnTheTestThread() {
    final ScheduledExecutorService loop = mock(ScheduledExecutorService.class);
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(loop)
        .execute(any());
    when(loop.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(invocation -> mock(ScheduledFuture.class));
    scheduler.stop();
    scheduler =
        new RequestScheduler(
            ethPeers, new NoOpMetricsSystem(), Duration.ofSeconds(10), 4, 4, 1, loop, () -> 0);
  }

  // a validated eth peer that announced the given height
  private EthPeer connectAt(final long height) {
    final EthPeer peer = connect(List.of(), EthProtocol.LATEST);
    peer.chainState().updateHeightEstimate(height);
    return peer;
  }

  private EthPeer connect(final List<PeerValidator> validators, final Capability... capabilities) {
    final List<MessageData> messages = new CopyOnWriteArrayList<>();
    final EthPeer peer =
        new EthPeer(
            new MockPeerConnection(
                Set.of(capabilities), (capability, message, connection) -> messages.add(message)),
            ethPeer -> {},
            validators,
            10 * 1024 * 1024,
            Clock.systemUTC(),
            List.of(),
            Bytes.random(64));
    sent.put(peer, messages);
    connected.add(peer);
    return peer;
  }

  private List<MessageData> sent(final EthPeer peer) {
    return sent.get(peer);
  }

  private static ResponseStream send(final EthPeer peer, final MessageData message) {
    try {
      return peer.send(message);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static SyncRequest<Void> blockAccessLists() {
    return new TestRequest(
        Protocol.SNAP2,
        Priority.PIVOT,
        SnapMessages.getBlockAccessLists(List.of(Hash.ZERO), 1_000));
  }

  private static SyncRequest<Void> accountRange() {
    return new TestRequest(
        Protocol.SNAP,
        Priority.STATE,
        SnapMessages.getAccountRange(Bytes32.ZERO, Bytes32.ZERO, Bytes32.ZERO, 1_000));
  }

  private static SyncRequest<Void> headers() {
    return new TestRequest(
        Protocol.ETH, Priority.HEADERS, GetBlockHeadersMessage.create(1, 1, 0, false));
  }

  // the header of a new head, asked first to the peers that reach the given height
  private static SyncRequest<Void> pivotHeader(final long minimumPeerHeight) {
    return new TestRequest(
        Protocol.ETH,
        Priority.PIVOT,
        GetBlockHeadersMessage.create(1, 1, 0, false),
        minimumPeerHeight);
  }

  /** A request whose answers do not matter. */
  private record TestRequest(
      Protocol protocol, Priority priority, MessageData request, long minimumPeerHeight)
      implements SyncRequest<Void> {

    private TestRequest(
        final Protocol protocol, final Priority priority, final MessageData request) {
      this(protocol, priority, request, 0);
    }

    @Override
    public String name() {
      return "test";
    }

    @Override
    public MessageData message(final EthPeer peer) {
      return request;
    }

    @Override
    public Void decode(final MessageData response, final EthPeer peer) {
      return null;
    }
  }
}
