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
package org.hyperledger.besu.ethereum.p2p.discovery.discv5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.ethereum.beacon.discovery.MutableDiscoverySystem;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.ethereum.beacon.discovery.storage.BucketStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PeerDiscoveryAgentV5Test {

  @Mock private RlpxAgent rlpxAgent;
  @Mock private ForkIdManager forkIdManager;
  @Mock private NodeRecordManager nodeRecordManager;
  @Mock private MutableDiscoverySystem mockSystem;
  @Mock private DiscoveryPeerV4 localPeer;
  @Mock private NodeRecord localNodeRecord;

  private NetworkingConfiguration config;
  private PeerDiscoveryAgentV5 agent;

  @BeforeEach
  void setUp() {
    config =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(
                DiscoveryConfiguration.create()
                    .setEnabled(true)
                    .setAdvertisedHost("127.0.0.1")
                    .setBindHost("0.0.0.0")
                    .setBindPort(0))
            .build();

    // Set up the nodeRecordManager mock chain for initializeLocalNodeRecord
    lenient().when(rlpxAgent.getIpv6ListeningPort()).thenReturn(Optional.empty());
    lenient().when(nodeRecordManager.getLocalNode()).thenReturn(Optional.of(localPeer));
    lenient().when(localPeer.getNodeRecord()).thenReturn(Optional.of(localNodeRecord));

    // Set up mock system to return localNodeRecord with UDP address
    lenient().when(mockSystem.getLocalNodeRecord()).thenReturn(localNodeRecord);
    lenient()
        .when(localNodeRecord.getUdpAddress())
        .thenReturn(Optional.of(new InetSocketAddress(InetAddress.getLoopbackAddress(), 30303)));

    // Default stubs for discoveryTick() — the scheduler fires immediately on successful start,
    // so these must be present to avoid NPEs from unstubbed Mockito returns.
    lenient().when(rlpxAgent.getConnectionCount()).thenReturn(0);
    lenient().when(rlpxAgent.getMaxPeers()).thenReturn(25);
    lenient()
        .when(mockSystem.searchForNewPeers())
        .thenReturn(CompletableFuture.completedFuture(List.of()));

    agent =
        new PeerDiscoveryAgentV5(
            config,
            PeerPermissions.NOOP,
            forkIdManager,
            nodeRecordManager,
            rlpxAgent,
            new NoOpMetricsSystem(),
            false,
            (nodeRecord, listener) -> mockSystem);
  }

  @AfterEach
  void tearDown() {
    agent.stop();
  }

  @Test
  void startTwiceSecondCallFails() throws Exception {
    when(mockSystem.start()).thenReturn(CompletableFuture.completedFuture(null));

    final CompletableFuture<Integer> first = agent.start(1234);
    assertThat(first.get()).isEqualTo(30303);

    final CompletableFuture<Integer> second = agent.start(1234);
    assertThat(second).isCompletedExceptionally();
    assertThat(second)
        .failsWithin(1, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(IllegalStateException.class)
        .withMessageContaining("Unable to start an already started PeerDiscoveryAgentV5");
  }

  @Test
  void startAfterStopFails() {
    agent.stop();

    final CompletableFuture<Integer> result = agent.start(1234);
    assertThat(result).isCompletedExceptionally();
    assertThat(result)
        .failsWithin(1, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(IllegalStateException.class)
        .withMessageContaining("after it has been stopped");

    verify(mockSystem, never()).start();
  }

  @Test
  void schedulerStartsOnlyAfterSystemStartCompletes() {
    final CompletableFuture<Void> startFuture = new CompletableFuture<>();
    when(mockSystem.start()).thenReturn(startFuture);

    agent.start(1234);

    // system.start() hasn't completed yet — scheduler should not have fired
    verify(mockSystem, never()).searchForNewPeers();

    // Complete system.start() — scheduler should now start and fire discovery
    startFuture.complete(null);

    Awaitility.await()
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .atMost(3, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(mockSystem, atLeastOnce()).searchForNewPeers());
  }

  @Test
  void asyncStartFailureCleansUpDiscoverySystem() {
    when(mockSystem.start())
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("bind failed")));

    final CompletableFuture<Integer> result = agent.start(1234);
    assertThat(result).isCompletedExceptionally();
    // Agent should not be in stopped state — start failed, not stopped
    assertThat(agent.isStopped()).isFalse();
    // Discovery system should have been cleaned up
    verify(mockSystem).stop();
  }

  @Test
  void synchronousInitFailureResetsStartedState() {
    // Factory throws during create() — synchronous failure before system.start()
    final PeerDiscoveryAgentV5 failingAgent =
        new PeerDiscoveryAgentV5(
            config,
            PeerPermissions.NOOP,
            forkIdManager,
            nodeRecordManager,
            rlpxAgent,
            new NoOpMetricsSystem(),
            false,
            (nodeRecord, listener) -> {
              throw new RuntimeException("factory exploded");
            });

    try {
      final CompletableFuture<Integer> result = failingAgent.start(1234);
      assertThat(result).isCompletedExceptionally();
      // Agent should not be in stopped state — start failed, not stopped
      assertThat(failingAgent.isStopped()).isFalse();
      // Verify started flag was reset — a second start() should fail with
      // "factory exploded" (not "already started")
      final CompletableFuture<Integer> retry = failingAgent.start(1234);
      assertThat(retry)
          .failsWithin(1, TimeUnit.SECONDS)
          .withThrowableOfType(ExecutionException.class)
          .withCauseInstanceOf(RuntimeException.class)
          .withMessageContaining("factory exploded");
    } finally {
      failingAgent.stop();
    }
  }

  @Test
  void startWhenDisabledReturnsZero() throws Exception {
    final NetworkingConfiguration disabledConfig =
        ImmutableNetworkingConfiguration.builder()
            .discoveryConfiguration(DiscoveryConfiguration.create().setEnabled(false))
            .build();

    final PeerDiscoveryAgentV5 disabledAgent =
        new PeerDiscoveryAgentV5(
            disabledConfig,
            PeerPermissions.NOOP,
            forkIdManager,
            nodeRecordManager,
            rlpxAgent,
            new NoOpMetricsSystem(),
            false,
            (nodeRecord, listener) -> mockSystem);

    try {
      final CompletableFuture<Integer> result = disabledAgent.start(1234);
      assertThat(result.get()).isEqualTo(0);

      // Verify no interaction with the discovery system
      verify(mockSystem, never()).start();
    } finally {
      disabledAgent.stop();
    }
  }

  @Test
  void candidatePeersFilteredByPeerPermissions() throws Exception {
    // Create a PeerPermissions that rejects all peers
    final PeerPermissions rejectAll =
        new PeerPermissions() {
          @Override
          public boolean isPermitted(
              final Peer localNode, final Peer remotePeer, final Action action) {
            return false;
          }
        };

    final NodeRecord peerRecord =
        NodeRecordFactory.DEFAULT.fromEnr(
            "enr:-KO4QK1ecw-CGrDDZ4YwFrhgqctD0tWMHKJhUVxsS4um3aUFe3yBHRtVL9uYKk16DurN1IdSKTOB1zNCvjBybjZ_KAq"
                + "GAYtJ5U8wg2V0aMfGhJsZKtCAgmlkgnY0gmlwhA_MtDmJc2VjcDI1NmsxoQNXD7fj3sscyOKBiHYy14igj1vJYWdKYZH7n3T8qRpIcYRzb"
                + "mFwwIN0Y3CCdl-DdWRwgnZf");

    when(mockSystem.start()).thenReturn(CompletableFuture.completedFuture(null));
    when(mockSystem.searchForNewPeers())
        .thenReturn(CompletableFuture.completedFuture(List.of(peerRecord)));
    when(mockSystem.streamLiveNodes()).thenAnswer(invocation -> Stream.of(peerRecord));

    final PeerDiscoveryAgentV5 restrictedAgent =
        new PeerDiscoveryAgentV5(
            config,
            rejectAll,
            forkIdManager,
            nodeRecordManager,
            rlpxAgent,
            new NoOpMetricsSystem(),
            false,
            (nodeRecord, listener) -> mockSystem);

    try {
      restrictedAgent.start(1234);

      // Wait for at least one discovery tick to complete
      Awaitility.await()
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .atMost(3, TimeUnit.SECONDS)
          .untilAsserted(() -> verify(mockSystem, atLeastOnce()).searchForNewPeers());

      // Peers should never be connected — they are rejected by permissions
      verify(rlpxAgent, never()).connect(any());
    } finally {
      restrictedAgent.stop();
    }
  }

  @Test
  void candidatePeersAllowedWithNoopPermissions() throws Exception {
    final NodeRecord peerRecord =
        NodeRecordFactory.DEFAULT.fromEnr(
            "enr:-KO4QK1ecw-CGrDDZ4YwFrhgqctD0tWMHKJhUVxsS4um3aUFe3yBHRtVL9uYKk16DurN1IdSKTOB1zNCvjBybjZ_KAq"
                + "GAYtJ5U8wg2V0aMfGhJsZKtCAgmlkgnY0gmlwhA_MtDmJc2VjcDI1NmsxoQNXD7fj3sscyOKBiHYy14igj1vJYWdKYZH7n3T8qRpIcYRzb"
                + "mFwwIN0Y3CCdl-DdWRwgnZf");

    when(mockSystem.start()).thenReturn(CompletableFuture.completedFuture(null));
    when(mockSystem.searchForNewPeers())
        .thenReturn(CompletableFuture.completedFuture(List.of(peerRecord)));
    when(mockSystem.streamLiveNodes()).thenAnswer(invocation -> Stream.of(peerRecord));

    // Agent with NOOP permissions (the default setUp agent) — permissions should not interfere
    agent.start(1234);

    // Wait for at least one discovery tick to complete
    Awaitility.await()
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .atMost(3, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(mockSystem, atLeastOnce()).searchForNewPeers());

    // With NOOP permissions, peers are not rejected by permissions.
    // (They may still be filtered by bonding status, which is a DiscV4 concept, but the
    // permission layer itself does not block them.)
  }

  @Test
  void metricsReflectDiscoverySystemBucketStats() throws Exception {
    final StubMetricsSystem stubMetrics = new StubMetricsSystem();

    final BucketStats bucketStats = mock(BucketStats.class);
    when(mockSystem.getBucketStats()).thenReturn(bucketStats);
    when(bucketStats.getTotalLiveNodeCount()).thenReturn(5);
    when(bucketStats.getTotalNodeCount()).thenReturn(12);

    final PeerDiscoveryAgentV5 metricsAgent =
        new PeerDiscoveryAgentV5(
            config,
            PeerPermissions.NOOP,
            forkIdManager,
            nodeRecordManager,
            rlpxAgent,
            stubMetrics,
            false,
            (nodeRecord, listener) -> mockSystem);
    try {
      when(mockSystem.start()).thenReturn(CompletableFuture.completedFuture(null));
      metricsAgent.start(1234).get();

      assertThat(stubMetrics.getGaugeValue("discv5_live_nodes_current")).isEqualTo(5.0);
      assertThat(stubMetrics.getGaugeValue("discv5_total_nodes_current")).isEqualTo(12.0);

      // Verify gauge is live — reflects updated values
      when(bucketStats.getTotalLiveNodeCount()).thenReturn(10);
      assertThat(stubMetrics.getGaugeValue("discv5_live_nodes_current")).isEqualTo(10.0);
    } finally {
      metricsAgent.stop();
    }
  }
}
