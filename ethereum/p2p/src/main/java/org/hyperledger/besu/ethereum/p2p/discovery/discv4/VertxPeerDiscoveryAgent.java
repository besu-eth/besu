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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tuweni.bytes.Bytes.wrapBuffer;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryServiceException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DiscoveryPeerV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerDiscoveryController;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerDiscoveryController.AsyncExecutor;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.PeerTable;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.TimerUtil;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.VertxTimerUtil;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketDeserializer;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketSerializer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.NetworkUtility;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.Errors;
import io.netty.channel.unix.Errors.NativeIoException;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import org.apache.tuweni.rlp.EndOfRLPException;
import org.ethereum.beacon.discovery.util.DecodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxPeerDiscoveryAgent extends PeerDiscoveryAgentV4 {
  private static final Logger LOG = LoggerFactory.getLogger(VertxPeerDiscoveryAgent.class);

  private final Vertx vertx;
  /* The vert.x UDP socket. */
  private DatagramSocket socket;

  private final PacketSerializer packetSerializer;
  private final PacketDeserializer packetDeserializer;

  private VertxPeerDiscoveryAgent(
      final Vertx vertx,
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final MetricsSystem metricsSystem,
      final ForkIdManager forkIdManager,
      final NodeRecordManager nodeRecordManager,
      final RlpxAgent rlpxAgent,
      final PeerTable peerTable,
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
        peerTable);
    checkArgument(vertx != null, "vertx instance cannot be null");
    this.vertx = vertx;
    this.packetSerializer = packetSerializer;
    this.packetDeserializer = packetDeserializer;

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.NETWORK,
        "vertx_eventloop_pending_tasks",
        "The number of pending tasks in the Vertx event loop",
        pendingTaskCounter(vertx.nettyEventLoopGroup()));
    addPeerRequirement(() -> rlpxAgent.getConnectionCount() >= rlpxAgent.getMaxPeers());
  }

  public static VertxPeerDiscoveryAgent create(
      final Vertx vertx,
      final NodeKey nodeKey,
      final DiscoveryConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager,
      final RlpxAgent rlpxAgent) {
    PacketPackage packetPackage = DaggerPacketPackage.create();
    PeerTable peerTable = new PeerTable(nodeKey.getPublicKey().getEncodedBytes());
    NodeRecordManager nodeRecordManager =
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);
    return new VertxPeerDiscoveryAgent(
        vertx,
        nodeKey,
        config,
        peerPermissions,
        metricsSystem,
        forkIdManager,
        nodeRecordManager,
        rlpxAgent,
        peerTable,
        packetPackage.packetSerializer(),
        packetPackage.packetDeserializer());
  }

  private IntSupplier pendingTaskCounter(final EventLoopGroup eventLoopGroup) {
    return () ->
        StreamSupport.stream(eventLoopGroup.spliterator(), false)
            .filter(eventExecutor -> eventExecutor instanceof SingleThreadEventExecutor)
            .mapToInt(eventExecutor -> ((SingleThreadEventExecutor) eventExecutor).pendingTasks())
            .sum();
  }

  @Override
  protected TimerUtil createTimer() {
    return new VertxTimerUtil(vertx);
  }

  @Override
  protected AsyncExecutor createWorkerExecutor() {
    return new VertxAsyncExecutor();
  }

  @Override
  protected CompletableFuture<InetSocketAddress> listenForConnections() {
    final CompletableFuture<InetSocketAddress> future = new CompletableFuture<>();
    vertx
        .createDatagramSocket(new DatagramSocketOptions().setIpV6(NetworkUtility.isIPv6Available()))
        .listen(
            config.getBindPort(), config.getBindHost(), res -> handleListenerSetup(res, future));
    return future;
  }

  protected void handleListenerSetup(
      final AsyncResult<DatagramSocket> listenResult,
      final CompletableFuture<InetSocketAddress> addressFuture) {
    if (listenResult.failed()) {
      Throwable cause = listenResult.cause();
      LOG.error("An exception occurred when starting the peer discovery agent", cause);

      if (cause instanceof BindException || cause instanceof SocketException) {
        cause =
            new PeerDiscoveryServiceException(
                String.format(
                    "Failed to bind Ethereum UDP discovery listener to %s:%d: %s",
                    config.getBindHost(), config.getBindPort(), cause.getMessage()));
      }
      addressFuture.completeExceptionally(cause);
      return;
    }

    this.socket = listenResult.result();

    // TODO: when using wildcard hosts (0.0.0.0), we need to handle multiple addresses by
    // selecting
    // the correct 'announce' address.
    final String effectiveHost = socket.localAddress().host();
    final int effectivePort = socket.localAddress().port();

    LOG.info(
        "Started peer discovery agent successfully, on effective host={} and port={}",
        effectiveHost,
        effectivePort);

    socket.exceptionHandler(this::handleException);
    socket.handler(this::handlePacket);

    final InetSocketAddress address =
        new InetSocketAddress(socket.localAddress().host(), socket.localAddress().port());
    addressFuture.complete(address);
  }

  @Override
  protected CompletableFuture<Void> sendOutgoingPacket(
      final DiscoveryPeerV4 peer, final Packet packet) {
    final CompletableFuture<Void> result = new CompletableFuture<>();
    if (socket == null) {
      result.completeExceptionally(
          new RuntimeException("Discovery socket already closed, because Besu is closing down"));
    } else {
      socket.send(
          packetSerializer.encode(packet),
          peer.getEndpoint().getUdpPort(),
          peer.getEnodeURL().getIpAsString(),
          ar -> {
            if (ar.failed()) {
              result.completeExceptionally(ar.cause());
            } else {
              result.complete(null);
            }
          });
    }
    return result;
  }

  @Override
  public CompletableFuture<?> stop() {
    if (socket == null) {
      return CompletableFuture.completedFuture(null);
    }

    final CompletableFuture<?> completion = new CompletableFuture<>();
    socket.close(
        ar -> {
          if (ar.succeeded()) {
            controller.ifPresent(PeerDiscoveryController::stop);
            socket = null;
            isStopped = true;
            completion.complete(null);
          } else {
            completion.completeExceptionally(ar.cause());
          }
        });
    return completion;
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
            .addArgument(() -> wrapBuffer(packetSerializer.encode(packet)))
            .addArgument(err)
            .log();
      } else {
        LOG.atDebug()
            .setMessage(
                "Sending to peer {} failed, native error code {}, packet: {}, stacktrace: {}")
            .addArgument(peer)
            .addArgument(nativeErr.expectedErr())
            .addArgument(wrapBuffer(packetSerializer.encode(packet)))
            .addArgument(err)
            .log();
      }
    } else if (err instanceof SocketException && err.getMessage().contains("unreachable")) {
      LOG.atDebug()
          .setMessage("Peer {} is unreachable, packet: {}")
          .addArgument(peer)
          .addArgument(() -> wrapBuffer(packetSerializer.encode(packet)))
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
          .addArgument(() -> wrapBuffer(packetSerializer.encode(packet)))
          .addArgument(err)
          .log();
    } else {
      LOG.warn(
          "Sending to peer {} failed, packet: {}, stacktrace: {}",
          peer,
          wrapBuffer(packetSerializer.encode(packet)),
          err);
    }
  }

  /**
   * For uncontrolled exceptions occurring in the packet handlers.
   *
   * @param exception the exception that was raised
   */
  private void handleException(final Throwable exception) {
    if (exception instanceof IOException || exception instanceof DecodeException) {
      LOG.debug("Packet handler exception", exception);
    } else {
      LOG.error("Packet handler exception", exception);
    }
  }

  /**
   * The UDP packet handler. This is the entrypoint for all received datagrams.
   *
   * @param datagram the received datagram.
   */
  private void handlePacket(final DatagramPacket datagram) {
    final int length = datagram.data().length();
    if (!validatePacketSize(length)) {
      LOG.debug("Discarding over-sized packet. Actual size (bytes): {}", length);
      return;
    }
    vertx.<Packet>executeBlocking(
        future -> {
          try {
            future.complete(packetDeserializer.decode(datagram.data()));
          } catch (final Throwable t) {
            future.fail(t);
          }
        },
        event -> {
          if (event.succeeded()) {
            // Acquire the senders coordinates to build a Peer representation from them.
            final String host = datagram.sender().host();
            final int port = datagram.sender().port();
            final Endpoint endpoint = new Endpoint(host, port, Optional.empty());
            handleIncomingPacket(endpoint, event.result());
          } else {
            if (event.cause() instanceof PeerDiscoveryPacketDecodingException
                || event.cause() instanceof DecodeException
                || event.cause() instanceof EndOfRLPException) {
              LOG.debug(
                  "Discarding invalid peer discovery packet: {}, {}",
                  event.cause().getMessage(),
                  event.cause());
            } else {
              LOG.error("Encountered error while handling packet", event.cause());
            }
          }
        });
  }

  private class VertxAsyncExecutor implements AsyncExecutor {

    @Override
    public <T> CompletableFuture<T> execute(final Supplier<T> action) {
      final CompletableFuture<T> result = new CompletableFuture<>();
      vertx.<T>executeBlocking(
          future -> {
            try {
              future.complete(action.get());
            } catch (final Throwable t) {
              future.fail(t);
            }
          },
          false,
          event -> {
            if (event.succeeded()) {
              result.complete(event.result());
            } else {
              result.completeExceptionally(event.cause());
            }
          });
      return result;
    }
  }
}
