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

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeerFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.NodeRecordManager;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.dns.EthereumNodeRecord;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeerId;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.ethereum.beacon.discovery.AddressAccessPolicy;
import org.ethereum.beacon.discovery.DiscoverySystemBuilder;
import org.ethereum.beacon.discovery.MutableDiscoverySystem;
import org.ethereum.beacon.discovery.crypto.Signer;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating DiscV5 {@link PeerDiscoveryAgent} instances.
 *
 * <p>This factory is responsible for wiring together the dependencies needed by {@link
 * PeerDiscoveryAgentV5}. It intentionally does <em>not</em> initialize the local node record or
 * build the {@link MutableDiscoverySystem} — both are deferred to {@link
 * PeerDiscoveryAgentV5#start(int)}, where the actual RLPx TCP port is known.
 *
 * <p>The resulting {@link PeerDiscoveryAgent} integrates DiscV5 discovery with Besu's P2P
 * networking stack.
 */
public final class PeerDiscoveryAgentFactoryV5 implements PeerDiscoveryAgentFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PeerDiscoveryAgentFactoryV5.class);

  private final NetworkingConfiguration config;

  private final NodeRecordManager nodeRecordManager;
  private final NodeKey nodeKey;
  private final PeerPermissions peerPermissions;
  private final ForkIdManager forkIdManager;
  private final MetricsSystem metricsSystem;

  /**
   * Creates a new DiscV5 peer discovery agent factory.
   *
   * @param nodeKey the local node key used for identity and signing
   * @param config the networking configuration
   * @param peerPermissions peer permissions to enforce on discovered peers (including subnet
   *     filtering via {@link org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionSubnet})
   * @param natService NAT service for external address discovery
   * @param metricsSystem metrics system for registering DiscV5 metrics (bucket stats, etc.)
   * @param storageProvider storage provider for persisting node records
   * @param forkIdManager manager providing fork ID information for peer compatibility
   */
  public PeerDiscoveryAgentFactoryV5(
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager) {
    this(
        config,
        nodeKey,
        peerPermissions,
        forkIdManager,
        metricsSystem,
        new NodeRecordManager(storageProvider, nodeKey, forkIdManager, natService));
  }

  /** Package-private constructor for testing with a pre-built {@link NodeRecordManager}. */
  PeerDiscoveryAgentFactoryV5(
      final NetworkingConfiguration config,
      final NodeKey nodeKey,
      final PeerPermissions peerPermissions,
      final ForkIdManager forkIdManager,
      final MetricsSystem metricsSystem,
      final NodeRecordManager nodeRecordManager) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.nodeKey = Objects.requireNonNull(nodeKey, "nodeKey must not be null");
    this.peerPermissions =
        Objects.requireNonNull(peerPermissions, "peerPermissions must not be null");
    this.forkIdManager = Objects.requireNonNull(forkIdManager, "forkIdManager must not be null");
    this.metricsSystem = Objects.requireNonNull(metricsSystem, "metricsSystem must not be null");
    this.nodeRecordManager =
        Objects.requireNonNull(nodeRecordManager, "nodeRecordManager must not be null");
  }

  /**
   * Creates a DiscV5 {@link PeerDiscoveryAgent}.
   *
   * <p>The local node record and discovery system are built lazily during {@link
   * PeerDiscoveryAgentV5#start(int)} so that the ENR {@code tcp}/{@code tcp6} fields receive the
   * actual RLPx TCP port rather than the discovery bind port.
   *
   * @param rlpxAgent the RLPx agent
   * @return a configured DiscV5 peer discovery agent ready to be started
   */
  @Override
  public PeerDiscoveryAgent create(final RlpxAgent rlpxAgent) {
    return new PeerDiscoveryAgentV5(
        config,
        peerPermissions,
        forkIdManager,
        nodeRecordManager,
        rlpxAgent,
        metricsSystem,
        config.discoveryConfiguration().isPreferIpv6Outbound(),
        buildDefaultDiscoverySystemFactory());
  }

  /** Creates the default {@link PeerDiscoveryAgentV5.DiscoverySystemFactory}. */
  private PeerDiscoveryAgentV5.DiscoverySystemFactory buildDefaultDiscoverySystemFactory() {
    final DiscoveryConfiguration discoveryConfig = config.discoveryConfiguration();

    return (localNodeRecord, nodeRecordListener) -> {
      final DiscoverySystemBuilder builder =
          new DiscoverySystemBuilder()
              .signer(new NodeKeySigner(nodeKey))
              .localNodeRecord(localNodeRecord)
              .localNodeRecordListener(nodeRecordListener)
              // Ignore peer-reported external addresses for now (always returns Optional.empty()).
              // For IPv4 this is covered by NatService; future IPv6 auto-discovery may relax
              // this: https://github.com/hyperledger/besu/issues/9874
              .newAddressHandler((nodeRecord, newAddress) -> Optional.empty())
              .addressAccessPolicy(createAddressAccessPolicy())
              .bootnodes(
                  discoveryConfig.getEnrBootnodes().stream()
                      .map(EthereumNodeRecord::nodeRecord)
                      .toList());

      if (discoveryConfig.isDualStackEnabled()) {
        final InetSocketAddress ipv4 =
            new InetSocketAddress(discoveryConfig.getBindHost(), discoveryConfig.getBindPort());
        final InetSocketAddress ipv6 =
            new InetSocketAddress(
                discoveryConfig
                    .getBindHostIpv6()
                    .orElseThrow(
                        () ->
                            new IllegalStateException(
                                "Discovery dual-stack is enabled but bindHostIpv6 is not set")),
                discoveryConfig.getBindPortIpv6());
        builder.listen(ipv4, ipv6);
      } else {
        builder.listen(discoveryConfig.getBindHost(), discoveryConfig.getBindPort());
      }

      return builder.buildMutable();
    };
  }

  /**
   * Creates an {@link AddressAccessPolicy} that delegates IP-level checks to {@link
   * PeerPermissions#isPermitted(InetSocketAddress)} and full peer identity checks to {@link
   * PeerPermissions#isPermitted(Peer, Peer, Action)}.
   *
   * <p>The {@code allow(InetSocketAddress)} method delegates to {@code
   * peerPermissions.isPermitted(address)}, which in turn checks subnet restrictions via {@link
   * org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissionSubnet} if configured. This
   * provides the earliest possible filtering — raw UDP packets from disallowed IPs are dropped
   * before protocol processing.
   *
   * <p>The {@code allow(NodeRecord)} method first checks the node record's addresses via the same
   * IP-level permissions, then creates a {@link DiscoveryPeer} from the ENR and checks full {@link
   * PeerPermissions} including node ID-based and enode-based rules.
   *
   * @return the address access policy
   */
  AddressAccessPolicy createAddressAccessPolicy() {
    if (peerPermissions == PeerPermissions.NOOP) {
      return AddressAccessPolicy.ALLOW_ALL;
    }

    return new AddressAccessPolicy() {
      @Override
      public boolean allow(final InetSocketAddress address) {
        return peerPermissions.isPermitted(address);
      }

      @Override
      public boolean allow(final NodeRecord record) {
        // Check IP-level permissions on all available addresses (IPv4 and IPv6).
        // A dual-stack ENR carries separate ip/ip6, udp/udp6, tcp/tcp6 fields;
        // every advertised address must pass the subnet check.
        final Optional<InetSocketAddress> udp = record.getUdpAddress();
        final Optional<InetSocketAddress> udp6 = record.getUdp6Address();
        final Optional<InetSocketAddress> tcp = record.getTcpAddress();
        final Optional<InetSocketAddress> tcp6 = record.getTcp6Address();

        if (!isAddressPermitted(udp)
            || !isAddressPermitted(udp6)
            || !isAddressPermitted(tcp)
            || !isAddressPermitted(tcp6)) {
          return false;
        }

        final Peer localNode =
            nodeRecordManager
                .getLocalNode()
                .map(Peer.class::cast)
                // Defensive: in practice the local node is always initialized before the
                // discovery system starts (see PeerDiscoveryAgentV5.initializeLocalNodeRecord),
                // so this branch should never execute. Reject rather than bypass identity
                // checks — the peer will be re-discovered on the next FINDNODE round.
                .orElse(null);
        if (localNode == null) {
          return false;
        }

        if (udp.isEmpty() && udp6.isEmpty() && tcp.isEmpty() && tcp6.isEmpty()) {
          // No address to build a full DiscoveryPeer — fall back to node-ID-only check.
          // Implementations that need an IP (e.g. subnet filters) default to true since
          // there is nothing to filter on; only ID-based checks (e.g. denylists) fire.
          try {
            final Bytes publicKey = EthereumNodeRecord.uncompressedPublicKey(record);
            return peerPermissions.isPermitted(
                localNode,
                new DefaultPeerId(publicKey),
                PeerPermissions.Action.DISCOVERY_ALLOW_IN_PEER_TABLE);
          } catch (final RuntimeException e) {
            LOG.debug("DiscV5: Rejecting peer with missing or malformed secp256k1 key", e);
            return false;
          }
        }

        try {
          final DiscoveryPeer remotePeer =
              DiscoveryPeerFactory.fromNodeRecord(
                  record, config.discoveryConfiguration().isPreferIpv6Outbound());
          return peerPermissions.isPermitted(
              localNode, remotePeer, PeerPermissions.Action.DISCOVERY_ALLOW_IN_PEER_TABLE);
        } catch (final RuntimeException e) {
          LOG.debug("DiscV5: Rejecting peer with malformed NodeRecord", e);
          return false;
        }
      }

      private boolean isAddressPermitted(final Optional<InetSocketAddress> address) {
        return address.isEmpty() || peerPermissions.isPermitted(address.get());
      }
    };
  }

  /**
   * An implementation of the {@link Signer} interface that uses the node's {@link NodeKey} for
   * signing and key agreement.
   */
  private static class NodeKeySigner implements Signer {
    private final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();

    private final NodeKey nodeKey;

    /**
     * Creates a new NodeKeySigner.
     *
     * @param nodeKey the node key to use for signing and key agreement
     */
    public NodeKeySigner(final NodeKey nodeKey) {
      this.nodeKey = nodeKey;
    }

    /**
     * Derives a shared secret using ECDH with the given peer public key.
     *
     * @param remotePubKey the destination peer's public key
     * @return the derived shared secret
     */
    @Override
    public Bytes deriveECDHKeyAgreement(final Bytes remotePubKey) {
      final Bytes uncompressedKey;
      if (remotePubKey.size() == 33) {
        // Compressed key (0x02/0x03 prefix) — decompress to the 64-byte format without prefix
        final byte[] encoded =
            signatureAlgorithm
                .getCurve()
                .getCurve()
                .decodePoint(remotePubKey.toArrayUnsafe())
                .getEncoded(false);
        uncompressedKey = Bytes.wrap(encoded, 1, 64);
      } else {
        uncompressedKey = remotePubKey;
      }
      final SECPPublicKey publicKey = signatureAlgorithm.createPublicKey(uncompressedKey);
      return nodeKey.calculateECDHKeyAgreementCompressed(publicKey);
    }

    /**
     * Creates a signature of message `x`.
     *
     * @param messageHash message, hashed
     * @return ECDSA signature with properties merged together: r || s
     */
    @Override
    public Bytes sign(final Bytes32 messageHash) {
      Bytes signature = nodeKey.sign(messageHash).encodedBytes();
      return signature.slice(0, 64);
    }

    /**
     * Derives the compressed public key corresponding to the private key held by this module.
     *
     * @return the compressed public key
     */
    @Override
    public Bytes deriveCompressedPublicKeyFromPrivate() {
      return Bytes.wrap(
          signatureAlgorithm.publicKeyAsEcPoint(nodeKey.getPublicKey()).getEncoded(true));
    }
  }
}
