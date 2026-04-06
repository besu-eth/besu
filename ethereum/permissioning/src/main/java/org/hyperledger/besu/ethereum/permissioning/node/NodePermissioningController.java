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
package org.hyperledger.besu.ethereum.permissioning.node;

import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.plugin.services.permissioning.NodeConnectionPermissioningProvider;
import org.hyperledger.besu.util.Subscribers;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodePermissioningController {

  private static final Logger LOG = LoggerFactory.getLogger(NodePermissioningController.class);

  private Optional<ContextualNodePermissioningProvider> insufficientPeersPermissioningProvider =
      Optional.empty();
  private final List<NodeConnectionPermissioningProvider> providers;
  private final Subscribers<Runnable> permissioningUpdateSubscribers = Subscribers.create();

  public NodePermissioningController(final List<NodeConnectionPermissioningProvider> providers) {
    this.providers = providers;
  }

  public boolean isPermitted(final EnodeURLImpl sourceEnode, final EnodeURLImpl destinationEnode) {

    LOG.trace("Node permissioning: Checking {} -> {}", sourceEnode, destinationEnode);

    final Optional<Boolean> insufficientPeerPermission =
        insufficientPeersPermissioningProvider.flatMap(
            p -> p.isPermitted(sourceEnode, destinationEnode));

    if (insufficientPeerPermission.isPresent()) {
      final Boolean permitted = insufficientPeerPermission.get();

      LOG.trace(
          "Node permissioning - Insufficient Peers: {} {} -> {}",
          permitted ? "Permitted" : "Rejected",
          sourceEnode,
          destinationEnode);

      return permitted;
    }
    for (final NodeConnectionPermissioningProvider provider : providers) {
      if (!provider.isConnectionPermitted(sourceEnode, destinationEnode)) {
        LOG.trace(
            "Node permissioning - {}: Rejected {} -> {}",
            provider.getClass().getSimpleName(),
            sourceEnode,
            destinationEnode);

        return false;
      }
    }

    LOG.trace("Node permissioning: Permitted {} -> {}", sourceEnode, destinationEnode);

    return true;
  }

  public void setInsufficientPeersPermissioningProvider(
      final ContextualNodePermissioningProvider insufficientPeersPermissioningProvider) {
    insufficientPeersPermissioningProvider.subscribeToUpdates(
        () -> permissioningUpdateSubscribers.forEach(Runnable::run));
    this.insufficientPeersPermissioningProvider =
        Optional.of(insufficientPeersPermissioningProvider);
  }

  public List<NodeConnectionPermissioningProvider> getProviders() {
    return providers;
  }

  public long subscribeToUpdates(final Runnable callback) {
    return permissioningUpdateSubscribers.subscribe(callback);
  }

  public boolean unsubscribeFromUpdates(final long id) {
    return permissioningUpdateSubscribers.unsubscribe(id);
  }

  public Optional<NodeLocalConfigPermissioningController> localConfigController() {
    return getProviders().stream()
        .filter(p -> p instanceof NodeLocalConfigPermissioningController)
        .findFirst()
        .map(n -> (NodeLocalConfigPermissioningController) n);
  }
}
