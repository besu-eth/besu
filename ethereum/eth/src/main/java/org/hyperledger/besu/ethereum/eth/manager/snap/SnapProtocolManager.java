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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.p2p.network.ProtocolManager;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.framing.FramingException;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapProtocolManager implements ProtocolManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapProtocolManager.class);

  private final List<Capability> supportedCapabilities;
  private final EthPeers ethPeers;
  private final EthMessages snapMessages;
  private final EthScheduler ethScheduler;

  private final int maxConcurrentRequestsPerPeer;
  private final int maxConcurrentRequestsGlobal;
  private final AtomicInteger globalInFlightRequests = new AtomicInteger(0);
  private final Map<PeerConnection, AtomicInteger> perPeerInFlightRequests =
      new ConcurrentHashMap<>();
  private final Counter rejectedRequestsCounter;

  public SnapProtocolManager(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncConfiguration snapConfig,
      final EthPeers ethPeers,
      final EthMessages snapMessages,
      final EthScheduler ethScheduler,
      final ProtocolContext protocolContext,
      final Synchronizer synchronizer,
      final MetricsSystem metricsSystem) {
    this.ethPeers = ethPeers;
    this.snapMessages = snapMessages;
    this.ethScheduler = ethScheduler;
    this.supportedCapabilities = calculateCapabilities(snapConfig);
    this.maxConcurrentRequestsPerPeer = snapConfig.getMaxConcurrentSnapRequestsPerPeer();
    this.maxConcurrentRequestsGlobal = snapConfig.getMaxConcurrentSnapRequestsGlobal();
    new SnapServer(
        snapConfig, snapMessages, worldStateStorageCoordinator, protocolContext, synchronizer);

    metricsSystem.createIntegerGauge(
        BesuMetricCategory.PEERS,
        "snap_service_requests_in_flight_current",
        "The current number of snap sync GET_* requests concurrently scheduled for processing",
        globalInFlightRequests::get);
    this.rejectedRequestsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PEERS,
            "snap_service_requests_rejected_total",
            "Total number of snap sync GET_* requests answered with an empty response because a concurrency cap was reached");
  }

  private List<Capability> calculateCapabilities(final SnapSyncConfiguration snapConfig) {
    final ImmutableList.Builder<Capability> capabilities = ImmutableList.builder();
    capabilities.add(SnapProtocol.SNAP1);
    if (Boolean.TRUE.equals(snapConfig.isSnap2Enabled())) {
      capabilities.add(SnapProtocol.SNAP2);
    }

    return capabilities.build();
  }

  @Override
  public String getSupportedProtocol() {
    return SnapProtocol.NAME;
  }

  @Override
  public List<Capability> getSupportedCapabilities() {
    return supportedCapabilities;
  }

  @Override
  public void stop() {}

  @Override
  public void awaitStop() throws InterruptedException {}

  /**
   * This function is called by the P2P framework when a SNAP message has been received.
   *
   * @param cap The capability under which the message was transmitted.
   * @param message The message to be decoded.
   */
  @Override
  public void processMessage(final Capability cap, final Message message) {
    final int code = message.getData().getCode();
    LOG.trace("Process snap message {}, {}", cap, code);
    final EthPeer ethPeer = ethPeers.peer(message.getConnection());
    if (ethPeer == null) {
      LOG.debug(
          "Ignoring message received from unknown peer connection: {}", message.getConnection());
      return;
    }

    final EthMessage ethMessage = new EthMessage(ethPeer, message.getData());
    if (!ethPeer.validateReceivedMessage(ethMessage, getSupportedProtocol())) {
      LOG.debug("Unsolicited message {} received from, disconnecting: {}", code, ethPeer);
      ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_UNSOLICITED_MESSAGE_RECEIVED);
      return;
    }

    // Decode the snap message. FramingException (decompression failure) is a protocol violation.
    final MessageData messageData;
    try {
      messageData = AbstractSnapMessageData.create(message);
    } catch (final FramingException e) {
      LOG.atDebug()
          .setMessage("Disconnecting peer {} due to decompression failure for message code {}")
          .addArgument(ethPeer::getLoggableId)
          .addArgument(code)
          .setCause(e)
          .log();
      ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
      return;
    }
    final EthMessage decodedEthMessage = new EthMessage(ethPeer, messageData);

    // Dispatch to pending response handlers (no-op for inbound requests).
    ethPeers.dispatchMessage(ethPeer, decodedEthMessage, getSupportedProtocol());

    // GET_* requests are handled off the Netty event loop to avoid blocking ETH protocol traffic.
    if (SnapV1.REQUEST_CODES.contains(code) || SnapV2.REQUEST_CODES.contains(code)) {
      scheduleSnapRequest(ethPeer, decodedEthMessage, cap, code);
    }
  }

  private void scheduleSnapRequest(
      final EthPeer ethPeer,
      final EthMessage decodedEthMessage,
      final Capability cap,
      final int code) {
    if (!reserveSnapRequestSlot(ethPeer)) {
      respondEmptyDueToOverload(ethPeer, decodedEthMessage, code);
      return;
    }
    try {
      scheduleReservedSnapRequest(ethPeer, decodedEthMessage, cap, code);
    } catch (final RuntimeException e) {
      // Only reachable during shutdown: the services executor rejects synchronously once shut
      // down. Release the slot so it isn't leaked.
      releaseSnapRequestSlot(ethPeer);
      throw e;
    }
  }

  /** Cap was hit; reply empty instead of leaving the peer to time out. */
  private void respondEmptyDueToOverload(
      final EthPeer ethPeer, final EthMessage decodedEthMessage, final int code) {
    final BigInteger requestId;
    try {
      requestId = decodedEthMessage.getData().unwrapMessageData().getKey();
    } catch (final RLPException e) {
      LOG.debug(
          "Received malformed snap message code={} (BREACH_OF_PROTOCOL), disconnecting: {}",
          code,
          ethPeer,
          e);
      ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
      return;
    }
    sendSnapResponse(ethPeer, emptyResponseFor(code).wrapMessageData(requestId));
  }

  private static MessageData emptyResponseFor(final int code) {
    return switch (code) {
      case SnapV1.GET_ACCOUNT_RANGE -> SnapServer.EMPTY_ACCOUNT_RANGE;
      case SnapV1.GET_STORAGE_RANGE -> SnapServer.EMPTY_STORAGE_RANGE;
      case SnapV1.GET_BYTECODES -> SnapServer.EMPTY_BYTE_CODES_MESSAGE;
      case SnapV1.GET_TRIE_NODES -> SnapServer.EMPTY_TRIE_NODES_MESSAGE;
      case SnapV2.GET_BLOCK_ACCESS_LISTS -> SnapServer.EMPTY_BLOCK_ACCESS_LISTS;
      default -> throw new IllegalStateException("Unhandled snap GET_* code: " + code);
    };
  }

  private void scheduleReservedSnapRequest(
      final EthPeer ethPeer,
      final EthMessage decodedEthMessage,
      final Capability cap,
      final int code) {
    ethScheduler
        .scheduleServiceTask(
            () -> {
              Optional<MessageData> maybeResponseData = Optional.empty();
              try {
                final Map.Entry<BigInteger, MessageData> requestIdAndEthMessage =
                    decodedEthMessage.getData().unwrapMessageData();
                maybeResponseData =
                    snapMessages
                        .dispatch(new EthMessage(ethPeer, requestIdAndEthMessage.getValue()), cap)
                        .map(
                            responseData ->
                                responseData.wrapMessageData(requestIdAndEthMessage.getKey()));
              } catch (final FramingException | RLPException e) {
                LOG.debug(
                    "Received malformed snap message code={} (BREACH_OF_PROTOCOL), disconnecting: {}",
                    code,
                    ethPeer,
                    e);
                ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
              }
              maybeResponseData.ifPresent(responseData -> sendSnapResponse(ethPeer, responseData));
            })
        .exceptionally(
            e -> {
              if (!(e instanceof CancellationException)) {
                LOG.atWarn()
                    .setMessage("Unexpected error handling snap request code={} from peer {}")
                    .addArgument(code)
                    .addArgument(ethPeer::getLoggableId)
                    .setCause(e)
                    .log();
              }
              return null;
            })
        .whenComplete((result, error) -> releaseSnapRequestSlot(ethPeer));
  }

  /**
   * Reserves a global and per-peer slot before scheduling; increment-then-check avoids a TOCTOU
   * race.
   *
   * @return true if reserved; false if a cap was hit (request answered empty instead).
   */
  private boolean reserveSnapRequestSlot(final EthPeer ethPeer) {
    if (maxConcurrentRequestsGlobal > 0) {
      final int reservedGlobal = globalInFlightRequests.incrementAndGet();
      if (reservedGlobal > maxConcurrentRequestsGlobal) {
        globalInFlightRequests.decrementAndGet();
        rejectSnapRequest(ethPeer, "global");
        return false;
      }
    }
    if (maxConcurrentRequestsPerPeer > 0) {
      final AtomicInteger perPeerCount =
          perPeerInFlightRequests.computeIfAbsent(
              ethPeer.getConnection(), unused -> new AtomicInteger(0));
      final int reservedPerPeer = perPeerCount.incrementAndGet();
      if (reservedPerPeer > maxConcurrentRequestsPerPeer) {
        perPeerCount.decrementAndGet();
        if (maxConcurrentRequestsGlobal > 0) {
          globalInFlightRequests.decrementAndGet();
        }
        rejectSnapRequest(ethPeer, "per-peer");
        return false;
      }
    }
    return true;
  }

  private void releaseSnapRequestSlot(final EthPeer ethPeer) {
    if (maxConcurrentRequestsGlobal > 0) {
      globalInFlightRequests.decrementAndGet();
    }
    if (maxConcurrentRequestsPerPeer > 0) {
      final AtomicInteger perPeerCount = perPeerInFlightRequests.get(ethPeer.getConnection());
      if (perPeerCount != null) {
        perPeerCount.decrementAndGet();
      }
    }
  }

  private void rejectSnapRequest(final EthPeer ethPeer, final String scope) {
    rejectedRequestsCounter.inc();
    LOG.atDebug()
        .setMessage("Answering snap request from peer {} empty: {} concurrency cap reached")
        .addArgument(ethPeer::getLoggableId)
        .addArgument(scope)
        .log();
  }

  private void sendSnapResponse(final EthPeer ethPeer, final MessageData responseData) {
    try {
      ethPeer.send(responseData, getSupportedProtocol());
    } catch (final PeerConnection.PeerNotConnected e) {
      LOG.atTrace()
          .setMessage("Peer disconnected before we could respond - nothing to do {}")
          .addArgument(e.getMessage())
          .log();
    }
  }

  @Override
  public void handleNewConnection(final PeerConnection connection) {}

  @Override
  public void handleDisconnect(
      final PeerConnection connection,
      final DisconnectReason reason,
      final boolean initiatedByPeer) {
    perPeerInFlightRequests.remove(connection);
  }

  @Override
  public int getHighestProtocolVersion() {
    return getSupportedCapabilities().stream()
        .max(Comparator.comparing(Capability::getVersion))
        .map(Capability::getVersion)
        .orElse(0);
  }
}
