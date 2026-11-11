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
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.discv5.CompositePeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Objects;
import java.util.Optional;

public class DefaultPeerDiscoveryAgentFactory implements PeerDiscoveryAgentFactory {

  private final PeerDiscoveryAgentFactory delegate;

  private DefaultPeerDiscoveryAgentFactory(
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager) {

    this.delegate =
        createPeerDiscoveryAgentFactory(
            nodeKey,
            config,
            peerPermissions,
            natService,
            metricsSystem,
            storageProvider,
            forkIdManager);
  }

  private static PeerDiscoveryAgentFactory createPeerDiscoveryAgentFactory(
      final NodeKey nodeKey,
      final NetworkingConfiguration config,
      final PeerPermissions peerPermissions,
      final NatService natService,
      final MetricsSystem metricsSystem,
      final StorageProvider storageProvider,
      final ForkIdManager forkIdManager) {
    return new CompositePeerDiscoveryAgentFactory(
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

    private NodeKey nodeKey;
    private NetworkingConfiguration config;
    private PeerPermissions peerPermissions;
    private NatService natService = new NatService(Optional.empty());
    private MetricsSystem metricsSystem;
    private StorageProvider storageProvider;
    private ForkIdManager forkIdManager;

    private Builder() {}

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

    /**
     * Sets the fork-ID manager the discovery record (ENR) and peer filtering use. Pass the node's
     * own instance, so that discovery advertises the same fork ID as the Status handshake.
     *
     * @param forkIdManager the node's fork-ID manager
     * @return this builder
     */
    public Builder forkIdManager(final ForkIdManager forkIdManager) {
      this.forkIdManager = forkIdManager;
      return this;
    }

    public DefaultPeerDiscoveryAgentFactory build() {
      validate();
      return new DefaultPeerDiscoveryAgentFactory(
          nodeKey,
          config,
          peerPermissions,
          natService,
          metricsSystem,
          storageProvider,
          forkIdManager);
    }

    private void validate() {
      Objects.requireNonNull(nodeKey, "nodeKey must be set");
      Objects.requireNonNull(config, "config must be set");
      Objects.requireNonNull(peerPermissions, "peerPermissions must be set");
      Objects.requireNonNull(natService, "natService must be set");
      Objects.requireNonNull(metricsSystem, "metricsSystem must be set");
      Objects.requireNonNull(storageProvider, "storageProvider must be set");
      Objects.requireNonNull(forkIdManager, "forkIdManager must be set");
    }
  }
}
