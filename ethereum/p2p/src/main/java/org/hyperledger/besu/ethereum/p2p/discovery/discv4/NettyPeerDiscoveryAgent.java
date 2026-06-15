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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerDiscoveryController;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerDiscoveryController.AsyncExecutor;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerTable;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.ScheduledExecutorAsyncExecutor;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.ScheduledExecutorTimerUtil;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.TimerUtil;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketDeserializer;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketSerializer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.SocketException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.unix.Errors;
import io.netty.channel.unix.Errors.NativeIoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Netty-backed {@link PeerDiscoveryAgentV4}. Replaces {@code VertxPeerDiscoveryAgent}. */
public class NettyPeerDiscoveryAgent extends PeerDiscoveryAgentV4 {

  private static final Logger LOG = LoggerFactory.getLogger(NettyPeerDiscoveryAgent.class);

  private final ScheduledExecutorService timerScheduler;
  private final ExecutorService cryptoExecutor;

  private NettyPeerDiscoveryAgent(
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final MetricsSystem metricsSystem,
      final ForkIdManager forkIdManager,
      final NodeRecordManager nodeRecordManager,
      final RlpxAgent rlpxAgent,
      final PeerTable peerTable,
      final V4Transport transport,
      final PacketSerializer packetSerializer,
      final PacketDeserializer packetDeserializer) {
    super(
        nodeKey,
        config,
        peerPermissions,
        metricsSystem,
        forkIdManager,
        nodeRecordManager,
        rlpxAgent,
        peerTable,
        transport,
        packetSerializer,
        packetDeserializer);
    this.timerScheduler =
        Executors.newSingleThreadScheduledExecutor(
            (ThreadFactory) r -> new Thread(r, "discv4-timers"));
    this.cryptoExecutor =
        Executors.newFixedThreadPool(2, (ThreadFactory) r -> new Thread(r, "discv4-crypto"));
    addPeerRequirement(() -> rlpxAgent.getConnectionCount() >= rlpxAgent.getMaxPeers());
  }

  /** Creates an agent that sends/receives via a pre-built {@link V4Transport} (BOTH mode). */
  public static NettyPeerDiscoveryAgent createWithTransport(
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final NodeRecordManager nodeRecordManager,
      final ForkIdManager forkIdManager,
      final RlpxAgent rlpxAgent,
      final V4Transport transport) {
    final PacketPackage packetPackage = DaggerPacketPackage.create();
    final PeerTable peerTable = new PeerTable(nodeKey.getPublicKey().getEncodedBytes());
    return new NettyPeerDiscoveryAgent(
        nodeKey,
        config,
        peerPermissions,
        metricsSystem,
        forkIdManager,
        nodeRecordManager,
        rlpxAgent,
        peerTable,
        transport,
        packetPackage.packetSerializer(),
        packetPackage.packetDeserializer());
  }

  @Override
  protected TimerUtil createTimer() {
    return new ScheduledExecutorTimerUtil(timerScheduler);
  }

  @Override
  protected AsyncExecutor createWorkerExecutor() {
    return new ScheduledExecutorAsyncExecutor(cryptoExecutor);
  }

  /**
   * Returns the same single-threaded scheduler that drives timers, so timer callbacks and
   * dispatched packet handling share a single thread (matching the Vert.x event-loop ordering the
   * migration to Netty otherwise loses).
   */
  @Override
  protected Executor createDispatchExecutor() {
    return timerScheduler;
  }

  @Override
  public CompletableFuture<?> stop() {
    if (!stopGate.compareAndSet(false, true)) {
      return CompletableFuture.completedFuture(null);
    }
    return transport
        .stop()
        .whenComplete(
            (v, ex) -> {
              if (ex != null) {
                LOG.warn("Transport stop failed; continuing with executor shutdown", ex);
              }
              controller.ifPresent(PeerDiscoveryController::stop);
              timerScheduler.shutdownNow();
              cryptoExecutor.shutdownNow();
              isStopped = true;
            });
  }

  @Override
  protected void handleOutgoingPacketError(
      final Throwable err, final DiscoveryPeerV4 peer, final Packet packet) {
    if (err instanceof NativeIoException nativeErr) {
      if (nativeErr.expectedErr() == Errors.ERROR_ENETUNREACH_NEGATIVE) {
        LOG.atDebug()
            .setMessage("Peer {} is unreachable, native error code {}, packet: {}, stacktrace: {}")
            .addArgument(peer)
            .addArgument(nativeErr::expectedErr)
            .addArgument(() -> packetSerializer.encode(packet))
            .addArgument(err)
            .log();
      } else {
        LOG.atDebug()
            .setMessage(
                "Sending to peer {} failed, native error code {}, packet: {}, stacktrace: {}")
            .addArgument(peer)
            .addArgument(nativeErr::expectedErr)
            .addArgument(() -> packetSerializer.encode(packet))
            .addArgument(err)
            .log();
      }
    } else if (err instanceof SocketException && err.getMessage().contains("unreachable")) {
      LOG.atDebug()
          .setMessage("Peer {} is unreachable, packet: {}")
          .addArgument(peer)
          .addArgument(() -> packetSerializer.encode(packet))
          .addArgument(err)
          .log();
    } else if (err instanceof SocketException
        && err.getMessage().contentEquals("Operation not permitted")) {
      LOG.debug(
          "Operation not permitted sending to peer {}, this might be caused by firewall rules blocking traffic to a specific route.",
          peer,
          err);
    } else if (err instanceof UnsupportedAddressTypeException) {
      LOG.warn(
          "Unsupported address type exception when connecting to peer {}, this is likely due to ipv6 not being enabled at runtime. "
              + "Set logging level to TRACE to see full stacktrace",
          peer);
      LOG.atTrace()
          .setMessage("Sending to peer {} failed, packet: {}, stacktrace: {}")
          .addArgument(peer)
          .addArgument(() -> packetSerializer.encode(packet))
          .addArgument(err)
          .log();
    } else {
      LOG.atWarn()
          .setMessage("Sending to peer {} failed, packet: {}, stacktrace: {}")
          .addArgument(peer)
          .addArgument(() -> packetSerializer.encode(packet))
          .addArgument(err)
          .log();
    }
  }
}
