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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Transport;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatService;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockPeerDiscoveryAgent extends PeerDiscoveryAgentV4 {
  private static final Logger LOG = LoggerFactory.getLogger(MockPeerDiscoveryAgent.class);

  private final Deque<IncomingPacket> incomingPackets = new ArrayDeque<>();

  public MockPeerDiscoveryAgent(
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final Map<Bytes, MockPeerDiscoveryAgent> agentNetwork,
      final NatService natService,
      final ForkIdManager forkIdManager,
      final RlpxAgent rlpxAgent) {
    this(
        nodeKey,
        config,
        peerPermissions,
        agentNetwork,
        natService,
        forkIdManager,
        rlpxAgent,
        DaggerPacketPackage.create());
  }

  private MockPeerDiscoveryAgent(
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final Map<Bytes, MockPeerDiscoveryAgent> agentNetwork,
      final NatService natService,
      final ForkIdManager forkIdManager,
      final RlpxAgent rlpxAgent,
      final PacketPackage packetPackage) {
    super(
        nodeKey,
        config,
        peerPermissions,
        new NoOpMetricsSystem(),
        forkIdManager,
        new NodeRecordManager(
            new InMemoryKeyValueStorageProvider(), nodeKey, forkIdManager, natService),
        rlpxAgent,
        new PeerTable(nodeKey.getPublicKey().getEncodedBytes()),
        new MockTransport(agentNetwork, config),
        packetPackage.packetSerializer(),
        packetPackage.packetDeserializer());
    // Post-construction attach, mirroring Transport.setInboundHandler(...) /
    // PeerDiscoveryAgentV4.prepareHandlers() — 'this' isn't available until after super()
    // returns, so MockTransport can't be handed the owning agent at construction time.
    ((MockTransport) transport).setOwner(this);
  }

  public void processIncomingPacket(final MockPeerDiscoveryAgent fromAgent, final Packet packet) {
    // Cycle packet through encode / decode to clone any data, ensuring data is not shared
    final Packet packetClone = packetDeserializer.decode(packetSerializer.encode(packet));
    incomingPackets.add(new IncomingPacket(fromAgent, packetClone));
    handleIncomingPacket(fromAgent.getAdvertisedPeer().get().getEndpoint(), packetClone);
  }

  /**
   * Get and clear the list of any incoming packets to this agent.
   *
   * @return A list of packets received by this agent
   */
  public List<IncomingPacket> getIncomingPackets() {
    final List<IncomingPacket> packets =
        Arrays.asList(incomingPackets.toArray(new IncomingPacket[0]));
    incomingPackets.clear();
    return packets;
  }

  @Override
  protected TimerUtil createTimer() {
    return new MockTimerUtil();
  }

  @Override
  protected PeerDiscoveryController.AsyncExecutor createWorkerExecutor() {
    return new BlockingAsyncExecutor();
  }

  @Override
  protected PeerDiscoveryController.AsyncExecutor createDecodeExecutor() {
    return new BlockingAsyncExecutor();
  }

  @Override
  protected Executor createDispatchExecutor() {
    // Run dispatched callbacks synchronously on the calling thread so tests using
    // BlockingAsyncExecutor see a deterministic single-threaded execution model.
    return Runnable::run;
  }

  @Override
  public CompletableFuture<?> stop() {
    if (!stopGate.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    // Mirror NettyPeerDiscoveryAgent.stop(): controller + isStopped flag must be updated even
    // if transport stop fails, so isStopped() reflects reality and tests that recreate agents
    // don't see lingering controller interactions.
    return transport
        .stop()
        .whenComplete(
            (v, ex) -> {
              controller.ifPresent(PeerDiscoveryController::stop);
              isStopped = true;
            });
  }

  @Override
  protected void handleOutgoingPacketError(
      final Throwable err, final DiscoveryPeerV4 peer, final Packet packet) {
    LOG.warn(
        "Sending to peer {} failed, packet: {}, stacktrace: {}",
        peer,
        packetSerializer.encode(packet),
        err);
  }

  public NodeKey getNodeKey() {
    return nodeKey;
  }

  public static class IncomingPacket {
    public final MockPeerDiscoveryAgent fromAgent;
    public final Packet packet;

    public IncomingPacket(final MockPeerDiscoveryAgent fromAgent, final Packet packet) {
      this.fromAgent = fromAgent;
      this.packet = packet;
    }
  }

  /** Test-only Transport that routes outbound packets directly to the target mock agent. */
  private static class MockTransport implements Transport {
    private final Map<Bytes, MockPeerDiscoveryAgent> agentNetwork;
    private final InetSocketAddress listenAddress;
    private MockPeerDiscoveryAgent owner;
    private boolean isRunning = false;

    MockTransport(
        final Map<Bytes, MockPeerDiscoveryAgent> agentNetwork,
        final DiscoveryConfiguration config) {
      this.agentNetwork = agentNetwork;
      this.listenAddress = new InetSocketAddress(config.getBindHost(), config.getBindPort());
    }

    void setOwner(final MockPeerDiscoveryAgent owner) {
      this.owner = owner;
    }

    @Override
    public CompletableFuture<InetSocketAddress> start() {
      isRunning = true;
      return CompletableFuture.completedFuture(listenAddress);
    }

    @Override
    public CompletableFuture<Void> stop() {
      isRunning = false;
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> send(final InetSocketAddress recipient, final Bytes data) {
      final CompletableFuture<Void> result = new CompletableFuture<>();
      final MockPeerDiscoveryAgent fromAgent = owner;

      if (!isRunning) {
        result.completeExceptionally(
            new Exception("Attempt to send message from an inactive agent"));
        return result;
      }

      // Locate the target agent by matching its advertised endpoint address. Stopped agents are
      // excluded so a replaced/restarted agent's stale map entry can't be matched instead of the
      // live agent listening at the same address.
      final MockPeerDiscoveryAgent toAgent =
          agentNetwork.values().stream()
              .filter(a -> !a.isStopped())
              .filter(a -> a.getAdvertisedPeer().isPresent())
              .filter(
                  a -> {
                    final DiscoveryPeerV4 p = a.getAdvertisedPeer().get();
                    return p.getEndpoint().getHost().equals(recipient.getHostString())
                        && p.getEndpoint().getUdpPort() == recipient.getPort();
                  })
              .findFirst()
              .orElse(null);

      if (toAgent == null) {
        result.completeExceptionally(
            new Exception(
                "Attempt to send to unknown peer. Agents must be constructed through PeerDiscoveryTestHelper."));
        return result;
      }

      // Decode serialized bytes back to a Packet and forward; processIncomingPacket will
      // re-encode+decode to ensure data isolation between agents.
      final Packet packet = fromAgent.packetDeserializer.decode(data);
      toAgent.processIncomingPacket(fromAgent, packet);

      result.complete(null);
      return result;
    }

    @Override
    public void setInboundHandler(final InboundV4Handler handler) {
      // Mock bypasses the raw-bytes inbound path: delivery goes through processIncomingPacket
    }
  }
}
