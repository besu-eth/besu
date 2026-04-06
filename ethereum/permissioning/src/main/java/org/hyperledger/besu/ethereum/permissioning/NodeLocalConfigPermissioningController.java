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
package org.hyperledger.besu.ethereum.permissioning;

import org.hyperledger.besu.ethereum.p2p.discovery.NodeIdentifier;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.ALLOWLIST_TYPE;
import org.hyperledger.besu.ethereum.permissioning.node.NodeAllowlistUpdatedEvent;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.permissioning.NodeConnectionPermissioningProvider;
import org.hyperledger.besu.util.Subscribers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeLocalConfigPermissioningController implements NodeConnectionPermissioningProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeLocalConfigPermissioningController.class);

  private LocalPermissioningConfiguration configuration;
  private final List<NodeIdentifier> fixedNodes;
  private final Bytes localNodeId;
  private final List<EnodeURL> nodesAllowlist = new CopyOnWriteArrayList<>();
  private final AllowlistPersistor allowlistPersistor;
  private final Subscribers<Consumer<NodeAllowlistUpdatedEvent>> nodeAllowlistUpdatedObservers =
      Subscribers.create();

  private final Counter checkCounter;
  private final Counter checkCounterPermitted;
  private final Counter checkCounterUnpermitted;

  public NodeLocalConfigPermissioningController(
      final LocalPermissioningConfiguration permissioningConfiguration,
      final List<NodeIdentifier> fixedNodes,
      final Bytes localNodeId,
      final MetricsSystem metricsSystem) {
    this(
        permissioningConfiguration,
        fixedNodes,
        localNodeId,
        new AllowlistPersistor(permissioningConfiguration.getNodePermissioningConfigFilePath()),
        metricsSystem);
  }

  public NodeLocalConfigPermissioningController(
      final LocalPermissioningConfiguration configuration,
      final List<NodeIdentifier> fixedNodes,
      final Bytes localNodeId,
      final AllowlistPersistor allowlistPersistor,
      final MetricsSystem metricsSystem) {
    this.configuration = configuration;
    this.fixedNodes = fixedNodes;
    this.localNodeId = localNodeId;
    this.allowlistPersistor = allowlistPersistor;
    readNodesFromConfig(configuration);

    this.checkCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count",
            "Number of times the node local permissioning provider has been checked");
    this.checkCounterPermitted =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count_permitted",
            "Number of times the node local permissioning provider has been checked and returned permitted");
    this.checkCounterUnpermitted =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "node_local_check_count_unpermitted",
            "Number of times the node local permissioning provider has been checked and returned unpermitted");
  }

  private void readNodesFromConfig(final LocalPermissioningConfiguration configuration) {
    if (configuration.isNodeAllowlistEnabled() && configuration.getNodeAllowlist() != null) {
      for (EnodeURL enodeURL : configuration.getNodeAllowlist()) {
        addNode(enodeURL);
      }
    }
  }

  public NodesAllowlistResult addNodes(final List<String> enodeURLs) {
    final NodesAllowlistResult inputValidationResult = validInput(enodeURLs);
    if (inputValidationResult.result() != AllowlistOperationResult.SUCCESS) {
      return inputValidationResult;
    }
    final List<EnodeURL> peers =
        enodeURLs.stream()
            .map(url -> EnodeURLImpl.fromString(url, configuration.getEnodeDnsConfiguration()))
            .collect(Collectors.toList());

    for (EnodeURL peer : peers) {
      if (nodesAllowlist.contains(peer)) {
        return new NodesAllowlistResult(
            AllowlistOperationResult.ERROR_EXISTING_ENTRY,
            String.format("Specified peer: %s already exists in allowlist.", peer.getNodeId()));
      }
    }

    final List<EnodeURL> oldAllowlist = new ArrayList<>(this.nodesAllowlist);
    peers.forEach(this::addNode);
    notifyListUpdatedSubscribers(new NodeAllowlistUpdatedEvent(peers, Collections.emptyList()));

    final NodesAllowlistResult updateConfigFileResult = updateAllowlistInConfigFile(oldAllowlist);
    if (updateConfigFileResult.result() != AllowlistOperationResult.SUCCESS) {
      return updateConfigFileResult;
    }

    return new NodesAllowlistResult(AllowlistOperationResult.SUCCESS);
  }

  public boolean addNode(final EnodeURL enodeURL) {
    return nodesAllowlist.add(enodeURL);
  }

  public NodesAllowlistResult removeNodes(final List<String> enodeURLs) {
    final NodesAllowlistResult inputValidationResult = validInput(enodeURLs);
    if (inputValidationResult.result() != AllowlistOperationResult.SUCCESS) {
      return inputValidationResult;
    }
    final List<EnodeURLImpl> peers =
        enodeURLs.stream()
            .map(url -> EnodeURLImpl.fromString(url, configuration.getEnodeDnsConfiguration()))
            .collect(Collectors.toList());

    boolean anyBootnode = peers.stream().anyMatch(fixedNodes::contains);
    if (anyBootnode) {
      return new NodesAllowlistResult(AllowlistOperationResult.ERROR_FIXED_NODE_CANNOT_BE_REMOVED);
    }

    for (EnodeURL peer : peers) {
      if (!(nodesAllowlist.contains(peer))) {
        return new NodesAllowlistResult(
            AllowlistOperationResult.ERROR_ABSENT_ENTRY,
            String.format("Specified peer: %s does not exist in allowlist.", peer.getNodeId()));
      }
    }

    final List<EnodeURL> oldAllowlist = new ArrayList<>(this.nodesAllowlist);
    peers.forEach(this::removeNode);
    notifyListUpdatedSubscribers(new NodeAllowlistUpdatedEvent(Collections.emptyList(), peers));

    final NodesAllowlistResult updateConfigFileResult = updateAllowlistInConfigFile(oldAllowlist);
    if (updateConfigFileResult.result() != AllowlistOperationResult.SUCCESS) {
      return updateConfigFileResult;
    }

    return new NodesAllowlistResult(AllowlistOperationResult.SUCCESS);
  }

  private boolean removeNode(final EnodeURL enodeURL) {
    return nodesAllowlist.remove(enodeURL);
  }

  private NodesAllowlistResult updateAllowlistInConfigFile(final List<EnodeURL> oldAllowlist) {
    try {
      verifyConfigurationFileState(peerToEnodeURI(oldAllowlist));
      updateConfigurationFile(peerToEnodeURI(nodesAllowlist));
      verifyConfigurationFileState(peerToEnodeURI(nodesAllowlist));
    } catch (IOException e) {
      revertState(oldAllowlist);
      return new NodesAllowlistResult(AllowlistOperationResult.ERROR_ALLOWLIST_PERSIST_FAIL);
    } catch (AllowlistFileSyncException e) {
      return new NodesAllowlistResult(AllowlistOperationResult.ERROR_ALLOWLIST_FILE_SYNC);
    }

    return new NodesAllowlistResult(AllowlistOperationResult.SUCCESS);
  }

  private NodesAllowlistResult validInput(final List<String> peers) {
    if (peers == null || peers.isEmpty()) {
      return new NodesAllowlistResult(
          AllowlistOperationResult.ERROR_EMPTY_ENTRY, String.format("Null/empty peers list"));
    }

    if (peerListHasDuplicates(peers)) {
      return new NodesAllowlistResult(
          AllowlistOperationResult.ERROR_DUPLICATED_ENTRY,
          String.format("Specified peer list contains duplicates"));
    }

    return new NodesAllowlistResult(AllowlistOperationResult.SUCCESS);
  }

  private boolean peerListHasDuplicates(final List<String> peers) {
    return !peers.stream().allMatch(new HashSet<>()::add);
  }

  private void verifyConfigurationFileState(final Collection<String> oldNodes)
      throws IOException, AllowlistFileSyncException {
    allowlistPersistor.verifyConfigFileMatchesState(ALLOWLIST_TYPE.NODES, oldNodes);
  }

  private void updateConfigurationFile(final Collection<String> nodes) throws IOException {
    allowlistPersistor.updateConfig(ALLOWLIST_TYPE.NODES, nodes);
  }

  private void revertState(final List<EnodeURL> nodesAllowlist) {
    this.nodesAllowlist.clear();
    this.nodesAllowlist.addAll(nodesAllowlist);
  }

  private Collection<String> peerToEnodeURI(final Collection<EnodeURL> peers) {
    return peers.parallelStream().map(EnodeURL::toString).collect(Collectors.toList());
  }

  public boolean isPermitted(final String enodeURL) {
    return isPermitted(EnodeURLImpl.fromString(enodeURL, configuration.getEnodeDnsConfiguration()));
  }

  public boolean isPermitted(final EnodeURLImpl node) {
    if (Objects.equals(localNodeId, node.getNodeId())) {
      return true;
    }
    return nodesAllowlist.stream().anyMatch(p -> EnodeURLImpl.sameListeningEndpoint(p, node));
  }

  public List<String> getNodesAllowlist() {
    return nodesAllowlist.stream().map(Object::toString).collect(Collectors.toList());
  }

  public synchronized void reload() throws RuntimeException {
    final List<EnodeURL> currentAccountsList = new ArrayList<>(nodesAllowlist);
    nodesAllowlist.clear();

    try {
      final LocalPermissioningConfiguration updatedConfig =
          PermissioningConfigurationBuilder.permissioningConfiguration(
              configuration.isNodeAllowlistEnabled(),
              configuration.getEnodeDnsConfiguration(),
              configuration.getNodePermissioningConfigFilePath(),
              configuration.isAccountAllowlistEnabled(),
              configuration.getAccountPermissioningConfigFilePath());

      readNodesFromConfig(updatedConfig);
      configuration = updatedConfig;

      createNodeAllowlistModifiedEventAfterReload(currentAccountsList, nodesAllowlist);
    } catch (Exception e) {
      nodesAllowlist.clear();
      nodesAllowlist.addAll(currentAccountsList);
      throw new IllegalStateException(
          "Error reloading permissions file. In-memory nodes allowlist will be reverted to previous valid configuration",
          e);
    }
  }

  private void createNodeAllowlistModifiedEventAfterReload(
      final List<EnodeURL> previousNodeAllowlist, final List<EnodeURL> currentNodesList) {
    final List<EnodeURL> removedNodes =
        previousNodeAllowlist.stream()
            .filter(n -> !currentNodesList.contains(n))
            .collect(Collectors.toList());

    final List<EnodeURL> addedNodes =
        currentNodesList.stream()
            .filter(n -> !previousNodeAllowlist.contains(n))
            .collect(Collectors.toList());

    if (!removedNodes.isEmpty() || !addedNodes.isEmpty()) {
      notifyListUpdatedSubscribers(new NodeAllowlistUpdatedEvent(addedNodes, removedNodes));
    }
  }

  public long subscribeToListUpdatedEvent(final Consumer<NodeAllowlistUpdatedEvent> subscriber) {
    return nodeAllowlistUpdatedObservers.subscribe(subscriber);
  }

  private void notifyListUpdatedSubscribers(final NodeAllowlistUpdatedEvent event) {
    LOG.trace(
        "Sending NodeAllowlistUpdatedEvent (added: {}, removed {})",
        event.getAddedNodes().size(),
        event.getRemovedNodes().size());

    nodeAllowlistUpdatedObservers.forEach(c -> c.accept(event));
  }

  public static class NodesAllowlistResult {
    private final AllowlistOperationResult result;
    private final Optional<String> message;

    NodesAllowlistResult(final AllowlistOperationResult result, final String message) {
      this.result = result;
      this.message = Optional.of(message);
    }

    @VisibleForTesting
    public NodesAllowlistResult(final AllowlistOperationResult result) {
      this.result = result;
      this.message = Optional.empty();
    }

    public AllowlistOperationResult result() {
      return result;
    }

    public Optional<String> message() {
      return message;
    }
  }

  @Override
  public boolean isConnectionPermitted(
      final EnodeURL sourceEnode, final EnodeURL destinationEnode) {
    this.checkCounter.inc();
    if (isPermitted((EnodeURLImpl) sourceEnode) && isPermitted((EnodeURLImpl) destinationEnode)) {
      this.checkCounterPermitted.inc();
      return true;
    } else {
      this.checkCounterUnpermitted.inc();
      return false;
    }
  }
}
