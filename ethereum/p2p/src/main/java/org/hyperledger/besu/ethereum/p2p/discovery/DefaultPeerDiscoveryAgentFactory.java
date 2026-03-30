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
package org.hyperledger.besu.ethereum.p2p.discovery;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.PeerDiscoveryAgentFactoryV4;
import org.hyperledger.besu.ethereum.p2p.discovery.discv5.PeerDiscoveryAgentFactoryV5;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.vertx.core.Vertx;

public class DefaultPeerDiscoveryAgentFactory implements PeerDiscoveryAgentFactory {

  private final PeerDiscoveryAgentFactory delegate;

  private DefaultPeerDiscoveryAgentFactory(
      final Vertx vertx,
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final Blockchain blockchain,
      final List<Long> blockNumberForks,
      final List<Long> timestampForks) {

    final ForkIdManager forkIdManager =
        new ForkIdManager(blockchain, blockNumberForks, timestampForks);

    this.delegate =
        createPeerDiscoveryAgentFactory(
            vertx,
            nodeKey,
            config,
            peerPermissions,
            natService,
            metricsSystem,
            storageProvider,
            forkIdManager);
  }

  private static PeerDiscoveryAgentFactory createPeerDiscoveryAgentFactory(
      final Vertx vertx,
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager) {
    if (config.discoveryConfiguration().isDiscoveryV5Enabled()) {
      return new PeerDiscoveryAgentFactoryV5(
          nodeKey,
          config,
          peerPermissions,
          natService,
          metricsSystem,
          storageProvider,
          forkIdManager);
    }
    return new PeerDiscoveryAgentFactoryV4(
        vertx,
        nodeKey,
        config,
        peerPermissions,
        natService,
        metricsSystem,
        storageProvider,
        forkIdManager);
  }

  @Override
  public PeerDiscoveryAgent create(final RlpxAgent rlpxAgent) {
    return delegate.create(rlpxAgent);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private Vertx vertx;
    private NodeKey nodeKey;
    private NetworkingConfiguration config;
    private PeerPermissions peerPermissions;
    private NatService natService = new NatService(Optional.empty());
    private MetricsSystem metricsSystem;
    private StorageProvider storageProvider;
    private Blockchain blockchain;
    private List<Long> blockNumberForks;
    private List<Long> timestampForks;

    private Builder() {}

    public Builder vertx(final Vertx vertx) {
      this.vertx = vertx;
      return this;
    }

    public Builder nodeKey(final NodeKey nodeKey) {
      this.nodeKey = nodeKey;
      return this;
    }

    public Builder config(final NetworkingConfiguration config) {
      this.config = config;
      return this;
    }

    public Builder peerPermissions(final PeerPermissions peerPermissions) {
      this.peerPermissions = peerPermissions;
      return this;
    }

    public Builder natService(final NatService natService) {
      this.natService = natService;
      return this;
    }

    public Builder metricsSystem(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
      return this;
    }

    public Builder storageProvider(final StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
      return this;
    }

    public Builder blockchain(final Blockchain blockchain) {
      this.blockchain = blockchain;
      return this;
    }

    public Builder blockNumberForks(final List<Long> blockNumberForks) {
      this.blockNumberForks = blockNumberForks;
      return this;
    }

    public Builder timestampForks(final List<Long> timestampForks) {
      this.timestampForks = timestampForks;
      return this;
    }

    public DefaultPeerDiscoveryAgentFactory build() {
      validate();
      return new DefaultPeerDiscoveryAgentFactory(
          vertx,
          nodeKey,
          config,
          peerPermissions,
          natService,
          metricsSystem,
          storageProvider,
          blockchain,
          blockNumberForks,
          timestampForks);
    }

    private void validate() {
      Objects.requireNonNull(vertx, "vertx must be set");
      Objects.requireNonNull(nodeKey, "nodeKey must be set");
      Objects.requireNonNull(config, "config must be set");
      Objects.requireNonNull(peerPermissions, "peerPermissions must be set");
      Objects.requireNonNull(natService, "natService must be set");
      Objects.requireNonNull(metricsSystem, "metricsSystem must be set");
      Objects.requireNonNull(storageProvider, "storageProvider must be set");
      Objects.requireNonNull(blockchain, "blockchain must be set");
      Objects.requireNonNull(blockNumberForks, "blockNumberForks must be set");
      Objects.requireNonNull(timestampForks, "timestampForks must be set");
    }
  }
}
