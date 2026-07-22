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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.SECP256K1;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryMode;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryModeResolver;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.CompositePeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.NettyPeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.transport.NettyTransport;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.BesuNettyDiscoveryServer;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.NetworkUtility;

import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.beacon.discovery.network.NettyDiscoveryServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating a {@link CompositePeerDiscoveryAgent} that runs DiscV4 and DiscV5 discovery
 * on a single shared UDP socket. DiscV5 requires a secp256k1 node key; when the configured key is
 * on a different curve, V5 is skipped and only DiscV4 is wired.
 *
 * <p>This factory lives in the {@code discv5} package so it can access package-private constructors
 * of {@link PeerDiscoveryAgentFactoryV5} that accept pre-bound {@link NettyDiscoveryServer}
 * instances.
 */
public final class CompositePeerDiscoveryAgentFactory implements PeerDiscoveryAgentFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositePeerDiscoveryAgentFactory.class);

  private final NodeKey nodeKey;
  private final NetworkingConfiguration config;
  private final PeerPermissions peerPermissions;
  private final NatService natService;
  private final MetricsSystem metricsSystem;
  private final StorageProvider storageProvider;
  private final ForkIdManager forkIdManager;

  public CompositePeerDiscoveryAgentFactory(
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager) {
    this.nodeKey = Objects.requireNonNull(nodeKey, "nodeKey");
    this.config = Objects.requireNonNull(config, "config");
    this.peerPermissions = Objects.requireNonNull(peerPermissions, "peerPermissions");
    this.natService = Objects.requireNonNull(natService, "natService");
    this.metricsSystem = Objects.requireNonNull(metricsSystem, "metricsSystem");
    this.storageProvider = Objects.requireNonNull(storageProvider, "storageProvider");
    this.forkIdManager = Objects.requireNonNull(forkIdManager, "forkIdManager");
  }

  @Override
  public PeerDiscoveryAgent create(final RlpxAgent rlpxAgent) {
    final DiscoveryConfiguration discConfig = config.discoveryConfiguration();
    final DiscoveryMode mode = discConfig.getDiscoveryMode();
    final boolean v5CurveSupported = DiscoveryModeResolver.isV5CurveSupported();
    if (!v5CurveSupported && (mode == DiscoveryMode.V5 || mode == DiscoveryMode.BOTH)) {
      // Only warn when V5 was actually requested - a curve that can't support DiscV5 is
      // irrelevant under --discovery-mode=V4, where V5 was never going to run anyway.
      final String curve = SignatureAlgorithmFactory.getInstance().getCurveName();
      LOG.warn(
          "--discovery-mode={} requested DiscV5, but the node key curve '{}' does not support it"
              + " (requires '{}'). Falling back to V4-only discovery.",
          mode,
          curve,
          SECP256K1.CURVE_NAME);
    }
    // Shared with BesuCommand's bootnode/mode-mismatch warning so the two can never diverge on
    // which protocol(s) actually end up running.
    final DiscoveryModeResolver.Resolution resolution =
        DiscoveryModeResolver.resolve(mode, v5CurveSupported);
    final boolean v5Enabled = resolution.v5Enabled();
    final boolean v4Enabled = resolution.v4Enabled();

    LOG.info(
        "Peer discovery mode: {} (requested={}, V4={}, V5={})",
        v4Enabled && v5Enabled ? "BOTH" : v4Enabled ? "V4" : "V5",
        mode,
        v4Enabled,
        v5Enabled);

    // V5 masking-key per the discv5.1 spec: first 16 bytes of the local node-id
    // (keccak256(public-key)), used by the demux to detect inbound V5 packets.
    final byte[] maskingKey =
        Hash.keccak256(Bytes.wrap(nodeKey.getPublicKey().getEncodedBytes())).slice(0, 16).toArray();

    // --p2p-interface is normally IPv4, but CLI validation allows an IPv6 literal (e.g. "::")
    // when --p2p-interface-ipv6 isn't also set - a single-stack IPv6-only node. A null bindHost
    // falls through as false to the usual InetSocketAddress construction, which raises the
    // expected IllegalArgumentException.
    final boolean primaryIsIpv6 =
        discConfig.getBindHost() != null
            && !NetworkUtility.isIpV4Address(discConfig.getBindHost())
            && !NetworkUtility.INADDR_ANY.equals(discConfig.getBindHost());

    final Optional<InetSocketAddress> ipv4Bind;
    final Optional<InetSocketAddress> ipv6Bind;
    if (primaryIsIpv6) {
      // Single-stack IPv6-only: dual-stack CLI validation requires the primary to be IPv4
      // whenever --p2p-interface-ipv6 is also set, so reaching here means it wasn't.
      ipv4Bind = Optional.empty();
      ipv6Bind =
          Optional.of(new InetSocketAddress(discConfig.getBindHost(), discConfig.getBindPort()));
    } else {
      ipv4Bind =
          Optional.of(new InetSocketAddress(discConfig.getBindHost(), discConfig.getBindPort()));
      ipv6Bind =
          discConfig.isDualStackEnabled()
              ? Optional.of(
                  new InetSocketAddress(
                      discConfig
                          .getBindHostIpv6()
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      "Dual-stack discovery requires bindHostIpv6 to be set")),
                      discConfig.getBindPortIpv6()))
              : Optional.empty();
    }

    final SharedDiscoveryTransport transport =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ipv4Bind)
            .ipv6BindAddress(ipv6Bind)
            .maskingKey(maskingKey)
            .v4Enabled(v4Enabled)
            .v5Enabled(v5Enabled)
            .build();

    // Shared NodeRecordManager so both agents see the same local ENR state
    final NodeRecordManager nodeRecordManager =
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);

    final PeerDiscoveryAgent agentV5;
    if (v5Enabled) {
      final List<NettyDiscoveryServer> customServers = new ArrayList<>();
      if (transport.isDualStackMergedIntoSingleSocket()) {
        // One dual-stack socket serves both families, but both must still be registered: the
        // discv5 library keys its own outbound-dispatch channel map off each registered server's
        // listen-address family (see NettyDiscoveryClientImpl.send() upstream), so registering
        // only one family would make it silently drop every outbound packet addressed to the
        // other family. This does not double-process inbound packets - only one of the two V5
        // sinks is ever fed in the merged case (see SharedDiscoveryTransport.bindChannel()), so
        // the other family's incoming-packet stream here is simply always empty.
        customServers.add(new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET));
        customServers.add(new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET6));
      } else {
        if (ipv4Bind.isPresent()) {
          customServers.add(new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET));
        }
        if (ipv6Bind.isPresent()) {
          customServers.add(new BesuNettyDiscoveryServer(transport, StandardProtocolFamily.INET6));
        }
      }
      final PeerDiscoveryAgentFactoryV5 v5Factory =
          new PeerDiscoveryAgentFactoryV5(
              config,
              nodeKey,
              peerPermissions,
              forkIdManager,
              metricsSystem,
              nodeRecordManager,
              customServers);
      agentV5 = v5Factory.create(rlpxAgent);
    } else {
      agentV5 = null;
    }

    final PeerDiscoveryAgent agentV4;
    if (v4Enabled) {
      final NettyTransport v4Transport = NettyTransport.createShared(transport);
      agentV4 =
          NettyPeerDiscoveryAgent.createWithTransport(
              nodeKey,
              discConfig,
              peerPermissions,
              metricsSystem,
              nodeRecordManager,
              forkIdManager,
              rlpxAgent,
              v4Transport);
    } else {
      agentV4 = null;
    }

    return new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);
  }
}
