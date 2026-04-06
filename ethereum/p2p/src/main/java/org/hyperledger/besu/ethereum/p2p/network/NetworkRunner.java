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
package org.hyperledger.besu.ethereum.p2p.network;

import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkRunner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(NetworkRunner.class);

  private final CountDownLatch shutdown = new CountDownLatch(1);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private final P2PNetwork network;
  private final Map<String, SubProtocol> subProtocols;
  private final List<ProtocolManager> protocolManagers;
  private final LabelledMetric<Counter> inboundMessageCounter;
  private final LabelledMetric<Counter> inboundBytesCounter;

  private NetworkRunner(
      final P2PNetwork network,
      final Map<String, SubProtocol> subProtocols,
      final List<ProtocolManager> protocolManagers,
      final MetricsSystem metricsSystem) {
    this.network = network;
    this.protocolManagers = protocolManagers;
    this.subProtocols = subProtocols;
    this.inboundMessageCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "p2p_messages_inbound",
            "Count of each P2P message received inbound.",
            "protocol",
            "name",
            "code");
    this.inboundBytesCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.NETWORK,
            "p2p_bytes_inbound",
            "Count of bytes received inbound.",
            "protocol",
            "name",
            "code");
  }

  public P2PNetwork getNetwork() {
    return network;
  }

  public static Builder builder() {
    return new Builder();
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      LOG.info("Starting Network.");
      setupHandlers();
      network.start();

      LOG.info(
          "Supported capabilities: {}",
          protocolManagers.stream()
              .map(q -> String.format("%s", q.getSupportedCapabilities()))
              .collect(Collectors.joining(", ")));
    } else {
      LOG.error("Attempted to start already running network.");
    }
  }

  public void stop() {
    if (stopped.compareAndSet(false, true)) {
      LOG.info("Stopping Network.");
      network.stop();
      for (final ProtocolManager protocolManager : protocolManagers) {
        protocolManager.stop();
      }
      shutdown.countDown();
    } else {
      LOG.error("Attempted to stop already stopped network.");
    }
  }

  public void awaitStop() throws InterruptedException {
    shutdown.await();
    network.awaitStop();
    for (final ProtocolManager protocolManager : protocolManagers) {
      protocolManager.awaitStop();
    }
    LOG.info("Network stopped.");
  }

  private void setupHandlers() {
    // Setup message handlers
    for (final ProtocolManager protocolManager : protocolManagers) {
      for (final Capability supportedCapability : protocolManager.getSupportedCapabilities()) {
        final SubProtocol protocol = subProtocols.get(supportedCapability.getName());
        network.subscribe(
            supportedCapability,
            (cap, message) -> {
              final int code = message.getData().getCode();
              if (!protocol.isValidMessageCode(cap.getVersion(), code)) {
                inboundMessageCounter.labels(cap.toString(), "Invalid", "").inc();
                // Handle invalid messages by disconnecting
                LOG.debug(
                    "Invalid message code ({}-{}, {}) received from peer, disconnecting from: {}",
                    cap.getName(),
                    cap.getVersion(),
                    code,
                    message.getConnection().getPeerInfo().getNodeId());
                message
                    .getConnection()
                    .disconnect(
                        DisconnectReason.BREACH_OF_PROTOCOL_INVALID_MESSAGE_CODE_FOR_PROTOCOL);
                return;
              }
              inboundMessageCounter
                  .labels(
                      cap.toString(),
                      protocol.messageName(cap.getVersion(), code),
                      Integer.toString(code))
                  .inc();
              inboundBytesCounter
                  .labels(
                      cap.toString(),
                      protocol.messageName(cap.getVersion(), code),
                      Integer.toString(code))
                  .inc(message.getData().getSize());
              protocolManager.processMessage(cap, message);
            });
      }
    }

    // Setup (dis)connect handlers
    for (final ProtocolManager protocolManager : protocolManagers) {
      network.subscribeConnect(
          (connection) -> {
            if (Collections.disjoint(
                connection.getAgreedCapabilities(), protocolManager.getSupportedCapabilities())) {
              return;
            }
            protocolManager.handleNewConnection(connection);
          });

      network.subscribeDisconnect(
          (connection, disconnectReason, initiatedByPeer) -> {
            if (Collections.disjoint(
                connection.getAgreedCapabilities(), protocolManager.getSupportedCapabilities())) {
              return;
            }
            protocolManager.handleDisconnect(connection, disconnectReason, initiatedByPeer);
          });
    }
  }

  @Override
  public void close() {
    stop();
  }

  public Optional<RlpxAgent> getRlpxAgent() {
    return network.getRlpxAgent();
  }

  public static class Builder {
    private NetworkBuilder networkProvider;
    List<ProtocolManager> protocolManagers = new ArrayList<>();
    List<SubProtocol> subProtocols = new ArrayList<>();
    MetricsSystem metricsSystem;
    private BiFunction<Peer, Boolean, Optional<DisconnectReason>> peerConnectionGatekeeper =
        (peer, incoming) -> Optional.empty();

    public NetworkRunner build() {
      final Map<String, SubProtocol> subProtocolMap = new HashMap<>();
      for (final SubProtocol subProtocol : subProtocols) {
        subProtocolMap.put(subProtocol.getName(), subProtocol);
      }
      final List<Capability> caps =
          protocolManagers.stream()
              .flatMap(p -> p.getSupportedCapabilities().stream())
              .collect(Collectors.toList());
      for (final Capability cap : caps) {
        if (!subProtocolMap.containsKey(cap.getName())) {
          throw new IllegalStateException(
              "No sub-protocol found corresponding to supported capability: " + cap);
        }
      }
      final P2PNetwork network = networkProvider.build(caps);
      network
          .getRlpxAgent()
          .ifPresent(agent -> agent.setPeerConnectionGatekeeper(peerConnectionGatekeeper::apply));
      return new NetworkRunner(network, subProtocolMap, protocolManagers, metricsSystem);
    }

    public Builder protocolManagers(final List<ProtocolManager> protocolManagers) {
      this.protocolManagers = protocolManagers;
      return this;
    }

    public Builder network(final NetworkBuilder networkProvider) {
      this.networkProvider = networkProvider;
      return this;
    }

    public Builder subProtocols(final SubProtocol... subProtocols) {
      this.subProtocols.addAll(Arrays.asList(subProtocols));
      return this;
    }

    public Builder subProtocols(final List<SubProtocol> subProtocols) {
      this.subProtocols.addAll(subProtocols);
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder peerConnectionGatekeeper(
        final BiFunction<Peer, Boolean, Optional<DisconnectReason>> gatekeeper) {
      this.peerConnectionGatekeeper = gatekeeper;
      return this;
    }
  }

  @FunctionalInterface
  public interface NetworkBuilder {
    P2PNetwork build(List<Capability> caps);
  }
}
