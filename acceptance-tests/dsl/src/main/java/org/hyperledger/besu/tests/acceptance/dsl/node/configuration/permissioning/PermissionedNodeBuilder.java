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
package org.hyperledger.besu.tests.acceptance.dsl.node.configuration.permissioning;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.ALLOWLIST_TYPE;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.node.RunnableNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeFactory;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationProvider;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Charsets;
import org.assertj.core.util.Lists;

public class PermissionedNodeBuilder {

  private String name;
  private boolean localConfigNodesPermissioningEnabled = false;
  private Path localConfigNodesPermissioningFile = null;
  private Collection<URI> localConfigPermittedNodes = null;

  private boolean localConfigAccountsPermissioningEnabled = false;
  private Path localConfigAccountsPermissioningFile = null;
  private Collection<String> localConfigPermittedAccounts = null;

  private List<String> staticNodes = new ArrayList<>();
  private boolean isDnsEnabled = false;
  private boolean mining = true;
  private GenesisConfigurationProvider genesisConfigProvider;

  public PermissionedNodeBuilder name(final String name) {
    this.name = name;
    return this;
  }

  public PermissionedNodeBuilder nodesConfigFileEnabled() {
    this.localConfigNodesPermissioningEnabled = true;
    if (this.localConfigPermittedNodes == null) {
      this.localConfigPermittedNodes = new ArrayList<>();
    }
    return this;
  }

  public PermissionedNodeBuilder nodesConfigFile(final Path file) {
    this.localConfigNodesPermissioningFile = file;
    return this;
  }

  public PermissionedNodeBuilder nodesPermittedInConfig(final List<URI> nodes) {
    this.localConfigNodesPermissioningEnabled = true;
    this.localConfigPermittedNodes = new ArrayList<>(nodes);
    return this;
  }

  public PermissionedNodeBuilder nodesPermittedInConfig(final Node... nodes) {
    this.localConfigNodesPermissioningEnabled = true;
    this.localConfigPermittedNodes = new ArrayList<>(convertToEnodes(Lists.newArrayList(nodes)));
    return this;
  }

  public PermissionedNodeBuilder accountsConfigFileEnabled() {
    this.localConfigAccountsPermissioningEnabled = true;
    if (this.localConfigPermittedAccounts == null) {
      this.localConfigPermittedAccounts = new ArrayList<>();
    }
    return this;
  }

  public PermissionedNodeBuilder accountsConfigFile(final Path file) {
    this.localConfigAccountsPermissioningFile = file;
    return this;
  }

  public PermissionedNodeBuilder accountsPermittedInConfig(final List<String> accounts) {
    this.localConfigAccountsPermissioningEnabled = true;
    this.localConfigPermittedAccounts = new ArrayList<>(accounts);
    return this;
  }

  public PermissionedNodeBuilder staticNodes(final List<String> staticNodes) {
    this.staticNodes = staticNodes;
    return this;
  }

  public PermissionedNodeBuilder dnsEnabled(final boolean isDnsEnabled) {
    this.isDnsEnabled = isDnsEnabled;
    return this;
  }

  public PermissionedNodeBuilder disableMining() {
    this.mining = false;
    return this;
  }

  public PermissionedNodeBuilder genesisConfigProvider(
      final GenesisConfigurationProvider genesisConfigProvider) {
    this.genesisConfigProvider = genesisConfigProvider;
    return this;
  }

  public BesuNode build() {
    if (name == null) {
      name = "perm_node_" + UUID.randomUUID().toString().substring(0, 8);
    }

    Optional<LocalPermissioningConfiguration> localPermConfig = Optional.empty();
    if (localConfigNodesPermissioningEnabled || localConfigAccountsPermissioningEnabled) {
      localPermConfig = Optional.of(localConfigPermissioningConfiguration());
    }

    final PermissioningConfiguration permissioningConfiguration =
        new PermissioningConfiguration(localPermConfig);

    final BesuNodeConfigurationBuilder builder = new BesuNodeConfigurationBuilder();
    builder
        .name(name)
        .jsonRpcConfiguration(jsonRpcConfigWithPermApiEnabled())
        .permissioningConfiguration(permissioningConfiguration)
        .bootnodeEligible(false);

    if (mining) {
      builder.miningEnabled();
    }

    if (!staticNodes.isEmpty()) {
      builder.staticNodes(staticNodes);
    }

    builder.dnsEnabled(isDnsEnabled);

    builder.genesisConfigProvider(genesisConfigProvider);
    builder.devMode(false);

    try {
      return new BesuNodeFactory().create(builder.build());
    } catch (IOException e) {
      throw new RuntimeException("Error creating BesuNode", e);
    }
  }

  private LocalPermissioningConfiguration localConfigPermissioningConfiguration() {
    LocalPermissioningConfiguration localPermissioningConfiguration =
        LocalPermissioningConfiguration.createDefault();

    if (localConfigPermittedNodes != null) {
      if (localConfigNodesPermissioningFile == null) {
        localConfigNodesPermissioningFile = createTemporaryPermissionsFile();
      }

      final List<EnodeURLImpl> nodeAllowList =
          localConfigPermittedNodes.stream()
              .map(EnodeURLImpl::fromURI)
              .collect(Collectors.toList());

      initPermissioningConfigurationFile(
          ALLOWLIST_TYPE.NODES,
          nodeAllowList.stream().map(EnodeURL::toString).collect(Collectors.toList()),
          localConfigNodesPermissioningFile);

      localPermissioningConfiguration.setNodeAllowlist(nodeAllowList);
      localPermissioningConfiguration.setNodePermissioningConfigFilePath(
          localConfigNodesPermissioningFile.toAbsolutePath().toString());
    }

    if (localConfigPermittedAccounts != null) {
      if (localConfigAccountsPermissioningFile == null) {
        localConfigAccountsPermissioningFile = createTemporaryPermissionsFile();
      }

      initPermissioningConfigurationFile(
          ALLOWLIST_TYPE.ACCOUNTS,
          localConfigPermittedAccounts,
          localConfigAccountsPermissioningFile);

      localPermissioningConfiguration.setAccountAllowlist(localConfigPermittedAccounts);
      localPermissioningConfiguration.setAccountPermissioningConfigFilePath(
          localConfigAccountsPermissioningFile.toAbsolutePath().toString());
    }

    return localPermissioningConfiguration;
  }

  private Path createTemporaryPermissionsFile() {
    final File tempFile;
    try {
      tempFile = File.createTempFile("temp", "temp");
      tempFile.deleteOnExit();
    } catch (IOException e) {
      throw new RuntimeException("Error creating temporary permissioning file", e);
    }
    return tempFile.toPath();
  }

  private JsonRpcConfiguration jsonRpcConfigWithPermApiEnabled() {
    final JsonRpcConfiguration jsonRpcConfig = JsonRpcConfiguration.createDefault();
    jsonRpcConfig.setEnabled(true);
    jsonRpcConfig.setPort(0);
    jsonRpcConfig.setHostsAllowlist(singletonList("*"));
    jsonRpcConfig.setCorsAllowedDomains(singletonList("*"));
    final List<String> rpcApis = new ArrayList<>(jsonRpcConfig.getRpcApis());
    rpcApis.add(RpcApis.PERM.name());
    rpcApis.add(RpcApis.ADMIN.name());
    jsonRpcConfig.setRpcApis(rpcApis);
    return jsonRpcConfig;
  }

  private void initPermissioningConfigurationFile(
      final ALLOWLIST_TYPE listType,
      final Collection<String> allowlistVal,
      final Path configFilePath) {
    try {
      AllowlistPersistor.addNewConfigItem(listType, allowlistVal, configFilePath);

      Files.write(
          configFilePath,
          System.lineSeparator().getBytes(Charsets.UTF_8),
          StandardOpenOption.WRITE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException("Error populating permissioning file", e);
    }
  }

  private List<URI> convertToEnodes(final List<Node> nodes) {
    return nodes.stream()
        .map(node -> (RunnableNode) node)
        .map(RunnableNode::enodeUrl)
        .collect(toList());
  }
}
