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
package org.hyperledger.besu.tests.acceptance.dsl.node.configuration;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.ADMIN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.IBFT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.PLUGINS;
import static org.hyperledger.besu.tests.acceptance.dsl.transaction.bft.ConsensusType.QBFT;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.permissioning.LocalPermissioningConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.node.RunnableNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.apache.tuweni.bytes.Bytes;

public class BesuNodeFactory {

  private final NodeConfigurationFactory node = new NodeConfigurationFactory();

  public BesuNode create(final BesuNodeConfiguration config) throws IOException {
    return new BesuNode(
        config.getName(),
        config.getDataPath(),
        config.getMiningParameters(),
        config.getTransactionPoolConfiguration(),
        config.getJsonRpcConfiguration(),
        config.getEngineRpcConfiguration(),
        config.getWebSocketConfiguration(),
        config.getJsonRpcIpcConfiguration(),
        config.getInProcessRpcConfiguration(),
        config.getMetricsConfiguration(),
        config.getPermissioningConfiguration(),
        config.getApiConfiguration(),
        config.getDataStorageConfiguration(),
        config.getKeyFilePath(),
        config.isDevMode(),
        config.getNetwork(),
        config.getGenesisConfigProvider(),
        config.isP2pEnabled(),
        config.getP2pPort(),
        config.getNetworkingConfiguration(),
        config.isDiscoveryEnabled(),
        config.isBootnodeEligible(),
        config.isRevertReasonEnabled(),
        config.isSecp256k1Native(),
        config.isAltbn128Native(),
        config.getPlugins(),
        config.getRequestedPlugins(),
        config.getExtraCLIOptions(),
        config.getStaticNodes(),
        config.isDnsEnabled(),
        config.getRunCommand(),
        config.getKeyPair(),
        config.isStrictTxReplayProtectionEnabled(),
        config.getEnvironment(),
        config.storageImplementation(),
        config.getPluginConfiguration());
  }

  public BesuNode createArchiveNode(final String name) throws IOException {
    return createArchiveNode(name, UnaryOperator.identity());
  }

  public BesuNode createArchiveNode(
      final String name, final UnaryOperator<BesuNodeConfigurationBuilder> configModifier)
      throws IOException {
    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcTxPool()
            .webSocketEnabled();

    builder = configModifier.apply(builder);

    return create(builder.build());
  }

  public BesuNode createNode(
      final String name, final UnaryOperator<BesuNodeConfigurationBuilder> configModifier)
      throws IOException {
    final String[] enableRpcApis = new String[] {ADMIN.name()};

    final BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(node.createJsonRpcWithRpcApiEnabledConfig(enableRpcApis));

    return create(configModifier.apply(builder).build());
  }

  public Node createQbftNodeThatMustNotBeTheBootnode(final String name) throws IOException {
    return createQbftNode(name, nodeConfigBuilder -> nodeConfigBuilder.bootnodeEligible(false));
  }

  public BesuNode createArchiveNodeWithDiscoveryDisabledAndAdmin(final String name)
      throws IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(node.jsonRpcConfigWithAdmin())
            .webSocketEnabled()
            .discoveryEnabled(false)
            .build());
  }

  public BesuNode createArchiveNodeNetServicesEnabled(final String name) throws IOException {
    // TODO: Enable metrics coverage in the acceptance tests. See PIE-1606
    // final MetricsConfiguration metricsConfiguration = MetricsConfiguration.createDefault();
    // metricsConfiguration.setEnabled(true);
    // metricsConfiguration.setPort(0);
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            // .setMetricsConfiguration(metricsConfiguration)
            .jsonRpcConfiguration(node.jsonRpcConfigWithAdmin())
            .webSocketEnabled()
            .p2pEnabled(true)
            .build());
  }

  public BesuNode createArchiveNodeNetServicesDisabled(final String name) throws IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(node.jsonRpcConfigWithAdmin())
            .p2pEnabled(false)
            .build());
  }

  public BesuNode createNodeWithAuthentication(final String name, final String authFile)
      throws IOException, URISyntaxException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcAuthenticationConfiguration(authFile)
            .webSocketEnabled()
            .webSocketAuthenticationEnabled()
            .build());
  }

  public BesuNode createNodeWithAuthFileAndNoAuthApi(
      final String name, final String authFile, final List<String> noAuthApiMethods)
      throws URISyntaxException, IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcAuthenticationConfiguration(authFile, noAuthApiMethods)
            .webSocketEnabled()
            .webSocketAuthenticationEnabled()
            .build());
  }

  public BesuNode createWsNodeWithAuthFileAndNoAuthApi(
      final String name, final String authFile, final List<String> noAuthApiMethods)
      throws URISyntaxException, IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcAuthenticationConfiguration(authFile)
            .webSocketEnabled()
            .webSocketAuthenticationEnabledWithNoAuthMethods(noAuthApiMethods)
            .build());
  }

  public BesuNode createNodeWithAuthenticationUsingRsaJwtPublicKey(final String name)
      throws IOException, URISyntaxException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcAuthenticationUsingRSA()
            .webSocketEnabled()
            .webSocketAuthenticationUsingRsaPublicKeyEnabled()
            .build());
  }

  public BesuNode createNodeWithAuthenticationUsingEcdsaJwtPublicKey(final String name)
      throws IOException, URISyntaxException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .jsonRpcAuthenticationUsingECDSA()
            .webSocketEnabled()
            .webSocketAuthenticationUsingEcdsaPublicKeyEnabled()
            .build());
  }

  public BesuNode createArchiveNodeWithRpcDisabled(final String name) throws IOException {
    return create(new BesuNodeConfigurationBuilder().name(name).build());
  }

  public BesuNode createPluginsNode(
      final String name,
      final List<String> plugins,
      final List<String> extraCLIOptions,
      final String... extraRpcApis)
      throws IOException {

    final List<String> enableRpcApis = new ArrayList<>(Arrays.asList(extraRpcApis));
    enableRpcApis.addAll(List.of(IBFT.name(), ADMIN.name()));

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(
                node.createJsonRpcWithRpcApiEnabledConfig(enableRpcApis.toArray(String[]::new)))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .plugins(plugins)
            .extraCLIOptions(extraCLIOptions)
            .build());
  }

  public BesuNode createPluginsNode(
      final String name,
      final List<String> plugins,
      final UnaryOperator<BesuNodeConfigurationBuilder> configBuilder)
      throws IOException {

    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .plugins(plugins);

    return create(configBuilder.apply(builder).build());
  }

  public BesuNode createArchiveNodeWithRpcApis(final String name, final String... enabledRpcApis)
      throws IOException {
    final JsonRpcConfiguration jsonRpcConfig = node.createJsonRpcEnabledConfig();
    jsonRpcConfig.setRpcApis(asList(enabledRpcApis));
    final WebSocketConfiguration webSocketConfig = node.createWebSocketEnabledConfig();
    webSocketConfig.setRpcApis(asList(enabledRpcApis));

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(jsonRpcConfig)
            .webSocketConfiguration(webSocketConfig)
            .build());
  }

  public BesuNode createNodeWithNoDiscovery(final String name) throws IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .discoveryEnabled(false)
            .engineRpcEnabled(false)
            .build());
  }

  public BesuNode createQbftNode(final String name) throws IOException {
    return createQbftNode(name, UnaryOperator.identity());
  }

  public BesuNode createQbftNode(
      final String name, final UnaryOperator<BesuNodeConfigurationBuilder> configModifier)
      throws IOException {
    final String[] enableRpcApis = new String[] {QBFT.name(), ADMIN.name()};
    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(node.createJsonRpcWithRpcApiEnabledConfig(enableRpcApis))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .jsonRpcTxPool()
            .genesisConfigProvider(GenesisConfigurationFactory::createQbftGenesisConfig);

    builder = configModifier.apply(builder);

    return create(builder.build());
  }

  public BesuNode createQbftNodeWithRevertReasonEnabled(final String name) throws IOException {
    return createQbftNode(name, BesuNodeConfigurationBuilder::revertReasonEnabled);
  }

  public BesuNode createIbft2NonValidatorBootnode(final String name, final String genesisFile)
      throws IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(node.createJsonRpcWithIbft2AdminEnabledConfig())
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .genesisConfigProvider(
                validators ->
                    GenesisConfigurationFactory.createIbft2GenesisConfigFilterBootnode(
                        validators, genesisFile))
            .bootnodeEligible(true)
            .build());
  }

  public BesuNode createIbft2NodeWithLocalAccountPermissioning(
      final String name,
      final String genesisFile,
      final List<String> accountAllowList,
      final File configFile)
      throws IOException {
    final LocalPermissioningConfiguration config = LocalPermissioningConfiguration.createDefault();
    config.setAccountAllowlist(accountAllowList);
    config.setAccountPermissioningConfigFilePath(configFile.getAbsolutePath());
    final PermissioningConfiguration permissioningConfiguration =
        new PermissioningConfiguration(Optional.of(config));
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(node.createJsonRpcWithIbft2AdminEnabledConfig())
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .permissioningConfiguration(permissioningConfiguration)
            .devMode(false)
            .genesisConfigProvider(
                validators ->
                    GenesisConfigurationFactory.createIbft2GenesisConfigFilterBootnode(
                        validators, genesisFile))
            .bootnodeEligible(false)
            .build());
  }

  public BesuNode createIbft2Node(
      final String name, final boolean fixedPort, final DataStorageFormat storageFormat)
      throws IOException {
    JsonRpcConfiguration rpcConfig = node.createJsonRpcWithIbft2EnabledConfig(false);
    rpcConfig.addRpcApi("ADMIN,TXPOOL");
    if (fixedPort) {
      rpcConfig.setPort(
          Math.abs(name.hashCode() % 60000)
              + 1024); // Generate a consistent port for p2p based on node name
    }

    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(rpcConfig)
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .dataStorageConfiguration(
                storageFormat == DataStorageFormat.FOREST
                    ? DataStorageConfiguration.DEFAULT_FOREST_CONFIG
                    : DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
            .genesisConfigProvider(GenesisConfigurationFactory::createIbft2GenesisConfig);
    if (fixedPort) {
      builder.p2pPort(
          Math.abs(name.hashCode() % 60000)
              + 1024
              + 500); // Generate a consistent port for p2p based on node name (+ 500 to avoid
      // clashing with RPC port or other nodes with a similar name)
    }
    return create(builder.build());
  }

  public BesuNode createQbftNode(
      final String name, final boolean fixedPort, final DataStorageFormat storageFormat)
      throws IOException {
    JsonRpcConfiguration rpcConfig = node.createJsonRpcWithQbftEnabledConfig(false);
    rpcConfig.addRpcApi("ADMIN,TXPOOL");
    if (fixedPort) {
      rpcConfig.setPort(
          Math.abs(name.hashCode() % 60000)
              + 1024); // Generate a consistent port for p2p based on node name
    }

    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(rpcConfig)
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .dataStorageConfiguration(
                storageFormat == DataStorageFormat.FOREST
                    ? DataStorageConfiguration.DEFAULT_FOREST_CONFIG
                    : storageFormat == DataStorageFormat.BONSAI
                        ? DataStorageConfiguration.DEFAULT_BONSAI_CONFIG
                        : DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG)
            .genesisConfigProvider(GenesisConfigurationFactory::createQbftGenesisConfig);
    if (fixedPort) {
      builder.p2pPort(
          Math.abs(name.hashCode() % 60000)
              + 1024
              + 500); // Generate a consistent port for p2p based on node name (+ 500 to avoid
      // clashing with RPC port or other nodes with a similar name)
    }
    return create(builder.build());
  }

  public BesuNode createQbftPluginsNode(
      final String name,
      final List<String> plugins,
      final PluginConfiguration pluginConfiguration,
      final List<String> extraCLIOptions,
      final String... extraRpcApis)
      throws IOException {

    final List<String> enableRpcApis = new ArrayList<>(Arrays.asList(extraRpcApis));
    enableRpcApis.addAll(List.of(QBFT.name(), ADMIN.name(), PLUGINS.name()));

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcConfiguration(
                node.createJsonRpcWithRpcApiEnabledConfig(enableRpcApis.toArray(String[]::new)))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .plugins(plugins)
            .extraCLIOptions(extraCLIOptions)
            .devMode(false)
            .jsonRpcTxPool()
            .genesisConfigProvider(GenesisConfigurationFactory::createQbftLondonGenesisConfig)
            .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
            .pluginConfiguration(pluginConfiguration)
            .build());
  }

  public BesuNode createQbftMigrationNode(
      final String name, final boolean fixedPort, final DataStorageFormat storageFormat)
      throws IOException {
    JsonRpcConfiguration rpcConfig = node.createJsonRpcWithQbftEnabledConfig(false);
    rpcConfig.addRpcApi("ADMIN,TXPOOL");
    if (fixedPort) {
      rpcConfig.setPort(
          Math.abs(name.hashCode() % 60000)
              + 1024); // Generate a consistent port for p2p based on node name
    }

    BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(rpcConfig)
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .dataStorageConfiguration(
                storageFormat == DataStorageFormat.FOREST
                    ? DataStorageConfiguration.DEFAULT_FOREST_CONFIG
                    : DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
            .genesisConfigProvider(GenesisConfigurationFactory::createQbftMigrationGenesisConfig);
    if (fixedPort) {
      builder.p2pPort(
          Math.abs(name.hashCode() % 60000)
              + 1024
              + 500); // Generate a consistent port for p2p based on node name (+ 500 to avoid
      // clashing with RPC port or other nodes with a similar name)
    }
    return create(builder.build());
  }

  public BesuNode createCustomGenesisNode(
      final String name, final String genesisPath, final boolean canBeBootnode) throws IOException {
    return createCustomGenesisNode(name, genesisPath, canBeBootnode, false);
  }

  public BesuNode createCustomGenesisNode(
      final String name,
      final String genesisPath,
      final boolean canBeBootnode,
      final boolean mining)
      throws IOException {
    final String genesisFile = GenesisConfigurationFactory.readGenesisFile(genesisPath);
    final BesuNodeConfigurationBuilder builder =
        new BesuNodeConfigurationBuilder()
            .name(name)
            .jsonRpcEnabled()
            .webSocketEnabled()
            .genesisConfigProvider((a) -> Optional.of(genesisFile))
            .devMode(false)
            .bootnodeEligible(canBeBootnode);

    if (mining) {
      builder.miningEnabled();
    }

    return create(builder.build());
  }

  public BesuNode createExecutionEngineGenesisNode(final String name, final String genesisPath)
      throws IOException {
    final String genesisFile = GenesisConfigurationFactory.readGenesisFile(genesisPath);

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .genesisConfigProvider((a) -> Optional.of(genesisFile))
            .devMode(false)
            .bootnodeEligible(false)
            .miningConfiguration(
                MiningConfiguration.newDefault()
                    .setCoinbase(AddressHelpers.ofValue(1))
                    .setExtraData(Bytes.EMPTY)
                    .setMiningEnabled(true)
                    .setTargetGasLimit(60_000_000L)) // make tests agnostic to gas limit changes
            .jsonRpcEnabled()
            .jsonRpcTxPool()
            .engineRpcEnabled(true)
            .jsonRpcDebug()
            .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG)
            .build());
  }

  public BesuNode createIbft2NodeWithValidators(final String name, final String... validators)
      throws IOException {

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(node.createJsonRpcWithIbft2EnabledConfig(false))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .genesisConfigProvider(
                nodes ->
                    node.createGenesisConfigForValidators(
                        asList(validators),
                        nodes,
                        GenesisConfigurationFactory::createIbft2GenesisConfig))
            .build());
  }

  public BesuNode createQbftNodeWithValidators(final String name, final String... validators)
      throws IOException {

    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(node.createJsonRpcWithQbftEnabledConfig(false))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .genesisConfigProvider(
                nodes ->
                    node.createGenesisConfigForValidators(
                        asList(validators),
                        nodes,
                        GenesisConfigurationFactory::createQbftGenesisConfig))
            .build());
  }

  public BesuNode createQbftNodeWithContractBasedValidators(
      final String name, final String... validators) throws IOException {
    return create(
        new BesuNodeConfigurationBuilder()
            .name(name)
            .miningEnabled()
            .jsonRpcConfiguration(node.createJsonRpcWithQbftEnabledConfig(false))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .genesisConfigProvider(
                nodes ->
                    node.createGenesisConfigForValidators(
                        asList(validators),
                        nodes,
                        GenesisConfigurationFactory::createQbftValidatorContractGenesisConfig))
            .build());
  }

  public BesuNode createNodeWithStaticNodes(final String name, final List<Node> staticNodes)
      throws IOException {

    BesuNodeConfigurationBuilder builder =
        createConfigurationBuilderWithStaticNodes(name, staticNodes);
    return create(builder.build());
  }

  private BesuNodeConfigurationBuilder createConfigurationBuilderWithStaticNodes(
      final String name, final List<Node> staticNodes) {
    final List<String> staticNodesUrls =
        staticNodes.stream()
            .map(node -> (RunnableNode) node)
            .map(RunnableNode::enodeUrl)
            .map(URI::toASCIIString)
            .collect(toList());

    return new BesuNodeConfigurationBuilder()
        .name(name)
        .jsonRpcEnabled()
        .webSocketEnabled()
        .discoveryEnabled(false)
        .staticNodes(staticNodesUrls)
        .bootnodeEligible(false);
  }

  public BesuNode createNodeWithNonDefaultSignatureAlgorithm(
      final String name, final KeyPair keyPair, final List<Node> staticNodes) throws IOException {
    BesuNodeConfigurationBuilder builder =
        createNodeConfigurationWithNonDefaultSignatureAlgorithm(name, keyPair, staticNodes);
    return create(builder.build());
  }

  public BesuNodeConfigurationBuilder createNodeConfigurationWithNonDefaultSignatureAlgorithm(
      final String name, final KeyPair keyPair, final List<Node> staticNodes) {

    final String[] enableRpcApis = new String[] {QBFT.name(), ADMIN.name()};
    BesuNodeConfigurationBuilder builder =
        createConfigurationBuilderWithStaticNodes(name, staticNodes);

    builder =
        builder
            .jsonRpcConfiguration(node.createJsonRpcWithRpcApiEnabledConfig(enableRpcApis))
            .webSocketConfiguration(node.createWebSocketEnabledConfig())
            .devMode(false)
            .jsonRpcTxPool()
            .genesisConfigProvider(GenesisConfigurationFactory::createQbft256r1GenesisConfig);

    return builder.keyPair(keyPair);
  }

  public BesuNode runCommand(final String command) throws IOException {
    return create(new BesuNodeConfigurationBuilder().name("run " + command).run(command).build());
  }
}
