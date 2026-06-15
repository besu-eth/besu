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
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.CompositePeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.NettyPeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.transport.NettyV4Transport;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.BesuNettyDiscoveryServer;
import org.hyperledger.besu.ethereum.p2p.discovery.transport.SharedDiscoveryTransport;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.netty.channel.socket.InternetProtocolFamily;
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
    final boolean v5Enabled = isV5Supported();

    // V5 masking-key per the discv5.1 spec: the first 16 bytes of the local node-id, where
    // node-id = keccak256(public-key). Used by the demux to detect inbound V5 packets
    // addressed to this node.
    final byte[] maskingKey =
        Hash.keccak256(Bytes.wrap(nodeKey.getPublicKey().getEncodedBytes())).slice(0, 16).toArray();

    final InetSocketAddress ipv4Bind =
        new InetSocketAddress(discConfig.getBindHost(), discConfig.getBindPort());

    final Optional<InetSocketAddress> ipv6Bind =
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

    final SharedDiscoveryTransport transport =
        SharedDiscoveryTransport.builder()
            .ipv4BindAddress(ipv4Bind)
            .ipv6BindAddress(ipv6Bind)
            .maskingKey(maskingKey)
            .v4Enabled(true)
            .v5Enabled(v5Enabled)
            .metricsSystem(metricsSystem)
            .build();

    // Shared NodeRecordManager so both agents see the same local ENR state
    final NodeRecordManager nodeRecordManager =
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService);

    final PeerDiscoveryAgent agentV5;
    if (v5Enabled) {
      final List<NettyDiscoveryServer> customServers = new ArrayList<>();
      customServers.add(new BesuNettyDiscoveryServer(transport, InternetProtocolFamily.IPv4));
      if (ipv6Bind.isPresent()) {
        customServers.add(new BesuNettyDiscoveryServer(transport, InternetProtocolFamily.IPv6));
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

    final NettyV4Transport v4Transport = NettyV4Transport.createShared(transport);
    final PeerDiscoveryAgent agentV4 =
        NettyPeerDiscoveryAgent.createWithTransport(
            nodeKey,
            discConfig,
            peerPermissions,
            natService,
            metricsSystem,
            nodeRecordManager,
            forkIdManager,
            rlpxAgent,
            v4Transport);

    return new CompositePeerDiscoveryAgent(agentV4, agentV5, transport);
  }

  private boolean isV5Supported() {
    final String curve = SignatureAlgorithmFactory.getInstance().getCurveName();
    if (SECP256K1.CURVE_NAME.equals(curve)) {
      return true;
    }
    LOG.warn(
        "DiscV5 disabled: node key uses curve '{}', but DiscV5 requires '{}'. Only DiscV4 will run.",
        curve,
        SECP256K1.CURVE_NAME);
    return false;
  }
}
