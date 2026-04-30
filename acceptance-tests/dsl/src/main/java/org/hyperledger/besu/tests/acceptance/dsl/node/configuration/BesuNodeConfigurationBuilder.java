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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ImmutableInProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationProvider;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BesuNodeConfigurationBuilder {

  private String name;
  private Optional<Path> dataPath = Optional.empty();
  private MiningConfiguration miningConfiguration =
      ImmutableMiningConfiguration.builder()
          .mutableInitValues(
              MutableInitValues.builder().coinbase(AddressHelpers.ofValue(1)).build())
          .build();
  private TransactionPoolConfiguration transactionPoolConfiguration =
      TransactionPoolConfiguration.DEFAULT;
  private JsonRpcConfiguration jsonRpcConfiguration = JsonRpcConfiguration.createDefault();
  private JsonRpcConfiguration engineRpcConfiguration = JsonRpcConfiguration.createEngineDefault();
  private WebSocketConfiguration webSocketConfiguration = WebSocketConfiguration.createDefault();
  private JsonRpcIpcConfiguration jsonRpcIpcConfiguration = new JsonRpcIpcConfiguration();
  private InProcessRpcConfiguration inProcessRpcConfiguration =
      ImmutableInProcessRpcConfiguration.builder().build();
  private MetricsConfiguration metricsConfiguration = MetricsConfiguration.builder().build();
  private Optional<PermissioningConfiguration> permissioningConfiguration = Optional.empty();
  private ApiConfiguration apiConfiguration = ImmutableApiConfiguration.builder().build();
  private DataStorageConfiguration dataStorageConfiguration =
      DataStorageConfiguration.DEFAULT_FOREST_CONFIG;
  private String keyFilePath = null;
  private boolean devMode = true;
  private GenesisConfigurationProvider genesisConfigProvider = ignore -> Optional.empty();
  private Boolean p2pEnabled = true;
  private int p2pPort = 0;
  private boolean discoveryEnabled = true;
  private boolean bootnodeEligible = true;
  private boolean revertReasonEnabled = false;
  private NetworkDefinition network = null;
  private boolean secp256K1Native = true;
  private boolean altbn128Native = true;
  private final List<String> plugins = new ArrayList<>();
  private final List<String> requestedPlugins = new ArrayList<>();
  private final List<String> extraCLIOptions = new ArrayList<>();
  private List<String> staticNodes = new ArrayList<>();
  private boolean isDnsEnabled = false;
  private List<String> runCommand = new ArrayList<>();
  private Optional<KeyPair> keyPair = Optional.empty();
  private Boolean strictTxReplayProtectionEnabled = false;
  private Map<String, String> environment = new HashMap<>();
  private SynchronizerConfiguration synchronizerConfiguration;
  private Optional<KeyValueStorageFactory> storageImplementation = Optional.empty();
  private PluginConfiguration pluginConfiguration;

  public BesuNodeConfigurationBuilder() {}

  public BesuNodeConfigurationBuilder name(final String name) {
    this.name = name;
    return this;
  }

  public BesuNodeConfigurationBuilder dataPath(final Path dataPath) {
    checkNotNull(dataPath);
    this.dataPath = Optional.of(dataPath);
    return this;
  }

  public BesuNodeConfigurationBuilder miningEnabled() {
    return miningEnabled(true);
  }

  public BesuNodeConfigurationBuilder miningEnabled(final boolean enabled) {
    this.miningConfiguration = miningConfiguration.setMiningEnabled(enabled);
    this.jsonRpcConfiguration.addRpcApi(RpcApis.MINER.name());
    return this;
  }

  public BesuNodeConfigurationBuilder miningConfiguration(
      final MiningConfiguration miningConfiguration) {
    this.miningConfiguration = miningConfiguration;
    this.jsonRpcConfiguration.addRpcApi(RpcApis.MINER.name());
    return this;
  }

  public BesuNodeConfigurationBuilder transactionPoolConfiguration(
      final TransactionPoolConfiguration transactionPoolConfiguration) {
    this.transactionPoolConfiguration = transactionPoolConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcConfiguration(
      final JsonRpcConfiguration jsonRpcConfiguration) {
    this.jsonRpcConfiguration = jsonRpcConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder engineJsonRpcConfiguration(
      final JsonRpcConfiguration engineConfig) {
    this.engineRpcConfiguration = engineConfig;
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcEnabled() {
    this.jsonRpcConfiguration.setEnabled(true);
    this.jsonRpcConfiguration.setPort(0);
    this.jsonRpcConfiguration.setHostsAllowlist(singletonList("*"));

    return this;
  }

  public BesuNodeConfigurationBuilder engineRpcEnabled(final boolean enabled) {
    this.engineRpcConfiguration.setEnabled(enabled);
    this.engineRpcConfiguration.setPort(0);
    this.engineRpcConfiguration.setHostsAllowlist(singletonList("*"));
    this.engineRpcConfiguration.setAuthenticationEnabled(false);

    return this;
  }

  public BesuNodeConfigurationBuilder metricsEnabled() {
    this.metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(true)
            .port(0)
            .hostsAllowlist(singletonList("*"))
            .build();

    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcTxPool() {
    this.jsonRpcConfiguration.addRpcApi(RpcApis.TXPOOL.name());
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcAdmin() {
    this.jsonRpcConfiguration.addRpcApi(RpcApis.ADMIN.name());
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcDebug() {
    this.jsonRpcConfiguration.addRpcApi(RpcApis.DEBUG.name());
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcAuthenticationConfiguration(final String authFile)
      throws URISyntaxException {
    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource(authFile).toURI()).toAbsolutePath().toString();

    this.jsonRpcConfiguration.setAuthenticationEnabled(true);
    this.jsonRpcConfiguration.setAuthenticationCredentialsFile(authTomlPath);

    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcAuthenticationConfiguration(
      final String authFile, final List<String> noAuthApiMethods) throws URISyntaxException {
    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource(authFile).toURI()).toAbsolutePath().toString();

    this.jsonRpcConfiguration.setAuthenticationEnabled(true);
    this.jsonRpcConfiguration.setAuthenticationCredentialsFile(authTomlPath);
    this.jsonRpcConfiguration.setNoAuthRpcApis(noAuthApiMethods);

    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcAuthenticationUsingRSA() throws URISyntaxException {
    final File jwtPublicKey =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_rsa").toURI())
            .toAbsolutePath()
            .toFile();

    this.jsonRpcConfiguration.setAuthenticationEnabled(true);
    this.jsonRpcConfiguration.setAuthenticationPublicKeyFile(jwtPublicKey);

    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcAuthenticationUsingECDSA() throws URISyntaxException {
    final File jwtPublicKey =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_ecdsa").toURI())
            .toAbsolutePath()
            .toFile();

    this.jsonRpcConfiguration.setAuthenticationEnabled(true);
    this.jsonRpcConfiguration.setAuthenticationPublicKeyFile(jwtPublicKey);
    this.jsonRpcConfiguration.setAuthenticationAlgorithm(JwtAlgorithm.ES256);

    return this;
  }

  public BesuNodeConfigurationBuilder webSocketConfiguration(
      final WebSocketConfiguration webSocketConfiguration) {
    this.webSocketConfiguration = webSocketConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder jsonRpcIpcConfiguration(
      final JsonRpcIpcConfiguration jsonRpcIpcConfiguration) {
    this.jsonRpcIpcConfiguration = jsonRpcIpcConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder inProcessRpcConfiguration(
      final InProcessRpcConfiguration inProcessRpcConfiguration) {
    this.inProcessRpcConfiguration = inProcessRpcConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder metricsConfiguration(
      final MetricsConfiguration metricsConfiguration) {
    this.metricsConfiguration = metricsConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder network(final NetworkDefinition network) {
    this.network = network;
    return this;
  }

  public BesuNodeConfigurationBuilder webSocketEnabled() {
    final WebSocketConfiguration config = WebSocketConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    config.setHostsAllowlist(Collections.singletonList("*"));

    this.webSocketConfiguration = config;
    return this;
  }

  public BesuNodeConfigurationBuilder bootnodeEligible(final boolean bootnodeEligible) {
    this.bootnodeEligible = bootnodeEligible;
    return this;
  }

  public BesuNodeConfigurationBuilder webSocketAuthenticationEnabled() throws URISyntaxException {
    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource("authentication/auth.toml").toURI())
            .toAbsolutePath()
            .toString();

    this.webSocketConfiguration.setAuthenticationEnabled(true);
    this.webSocketConfiguration.setAuthenticationCredentialsFile(authTomlPath);

    return this;
  }

  public BesuNodeConfigurationBuilder webSocketAuthenticationEnabledWithNoAuthMethods(
      final List<String> noAuthApiMethods) throws URISyntaxException {
    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource("authentication/auth.toml").toURI())
            .toAbsolutePath()
            .toString();

    this.webSocketConfiguration.setAuthenticationEnabled(true);
    this.webSocketConfiguration.setAuthenticationCredentialsFile(authTomlPath);
    this.webSocketConfiguration.setRpcApisNoAuth(noAuthApiMethods);

    return this;
  }

  public BesuNodeConfigurationBuilder webSocketAuthenticationUsingRsaPublicKeyEnabled()
      throws URISyntaxException {
    final File jwtPublicKey =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_rsa").toURI())
            .toAbsolutePath()
            .toFile();

    this.webSocketConfiguration.setAuthenticationEnabled(true);
    this.webSocketConfiguration.setAuthenticationPublicKeyFile(jwtPublicKey);

    return this;
  }

  public BesuNodeConfigurationBuilder webSocketAuthenticationUsingEcdsaPublicKeyEnabled()
      throws URISyntaxException {
    final File jwtPublicKey =
        Paths.get(ClassLoader.getSystemResource("authentication/jwt_public_key_ecdsa").toURI())
            .toAbsolutePath()
            .toFile();

    this.webSocketConfiguration.setAuthenticationEnabled(true);
    this.webSocketConfiguration.setAuthenticationPublicKeyFile(jwtPublicKey);
    this.webSocketConfiguration.setAuthenticationAlgorithm(JwtAlgorithm.ES256);

    return this;
  }

  public BesuNodeConfigurationBuilder permissioningConfiguration(
      final PermissioningConfiguration permissioningConfiguration) {
    this.permissioningConfiguration = Optional.of(permissioningConfiguration);
    return this;
  }

  public BesuNodeConfigurationBuilder keyFilePath(final String keyFilePath) {
    this.keyFilePath = keyFilePath;
    return this;
  }

  public BesuNodeConfigurationBuilder devMode(final boolean devMode) {
    this.devMode = devMode;
    return this;
  }

  public BesuNodeConfigurationBuilder genesisConfigProvider(
      final GenesisConfigurationProvider genesisConfigProvider) {
    this.genesisConfigProvider = genesisConfigProvider;
    return this;
  }

  public BesuNodeConfigurationBuilder p2pEnabled(final Boolean p2pEnabled) {
    this.p2pEnabled = p2pEnabled;
    return this;
  }

  public BesuNodeConfigurationBuilder p2pPort(final int p2pPort) {
    this.p2pPort = p2pPort;
    return this;
  }

  public BesuNodeConfigurationBuilder discoveryEnabled(final boolean discoveryEnabled) {
    this.discoveryEnabled = discoveryEnabled;
    return this;
  }

  public BesuNodeConfigurationBuilder plugins(final List<String> plugins) {
    this.plugins.clear();
    this.plugins.addAll(plugins);
    return this;
  }

  public BesuNodeConfigurationBuilder requestedPlugins(final List<String> requestedPlugins) {
    this.requestedPlugins.clear();
    this.requestedPlugins.addAll(requestedPlugins);
    return this;
  }

  public BesuNodeConfigurationBuilder extraCLIOptions(final List<String> extraCLIOptions) {
    this.extraCLIOptions.clear();
    this.extraCLIOptions.addAll(extraCLIOptions);
    return this;
  }

  public BesuNodeConfigurationBuilder revertReasonEnabled() {
    this.revertReasonEnabled = true;
    return this;
  }

  public BesuNodeConfigurationBuilder secp256k1Java() {
    this.secp256K1Native = true;
    return this;
  }

  public BesuNodeConfigurationBuilder altbn128Java() {
    this.altbn128Native = true;
    return this;
  }

  public BesuNodeConfigurationBuilder staticNodes(final List<String> staticNodes) {
    this.staticNodes = staticNodes;
    return this;
  }

  public BesuNodeConfigurationBuilder dnsEnabled(final boolean isDnsEnabled) {
    this.isDnsEnabled = isDnsEnabled;
    return this;
  }

  public BesuNodeConfigurationBuilder keyPair(final KeyPair keyPair) {
    this.keyPair = Optional.of(keyPair);
    return this;
  }

  public BesuNodeConfigurationBuilder run(final String... commands) {
    this.runCommand = List.of(commands);
    return this;
  }

  public BesuNodeConfigurationBuilder strictTxReplayProtectionEnabled(
      final Boolean strictTxReplayProtectionEnabled) {
    this.strictTxReplayProtectionEnabled = strictTxReplayProtectionEnabled;
    return this;
  }

  public BesuNodeConfigurationBuilder environment(final Map<String, String> environment) {
    this.environment = environment;
    return this;
  }

  public BesuNodeConfigurationBuilder apiConfiguration(final ApiConfiguration apiConfiguration) {
    this.apiConfiguration = apiConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder dataStorageConfiguration(
      final DataStorageConfiguration dataStorageConfiguration) {
    this.dataStorageConfiguration = dataStorageConfiguration;
    return this;
  }

  public BesuNodeConfigurationBuilder storageImplementation(
      final KeyValueStorageFactory storageFactory) {
    this.storageImplementation = Optional.of(storageFactory);
    return this;
  }

  public BesuNodeConfigurationBuilder synchronizerConfiguration(
      final SynchronizerConfiguration config) {
    this.synchronizerConfiguration = config;
    return this;
  }

  public BesuNodeConfigurationBuilder pluginConfiguration(
      final PluginConfiguration pluginConfiguration) {
    this.pluginConfiguration = pluginConfiguration;
    return this;
  }

  public BesuNodeConfiguration build() {
    if (name == null) {
      throw new IllegalStateException("Name is required");
    }

    // Check connections more frequently during acceptance tests to cut down on
    // intermittent failures due to the fact that we're running over a real network
    final NetworkingConfiguration networkingConfiguration =
        ImmutableNetworkingConfiguration.builder()
            .initiateConnectionsFrequency(Duration.ofSeconds(5))
            .discoveryConfiguration(DiscoveryConfiguration.create().setDiscoveryV5Enabled(true))
            .build();

    return new BesuNodeConfiguration(
        name,
        dataPath,
        miningConfiguration,
        transactionPoolConfiguration,
        jsonRpcConfiguration,
        Optional.of(engineRpcConfiguration),
        webSocketConfiguration,
        jsonRpcIpcConfiguration,
        inProcessRpcConfiguration,
        metricsConfiguration,
        permissioningConfiguration,
        apiConfiguration,
        dataStorageConfiguration,
        Optional.ofNullable(keyFilePath),
        devMode,
        network,
        genesisConfigProvider,
        p2pEnabled,
        p2pPort,
        networkingConfiguration,
        discoveryEnabled,
        bootnodeEligible,
        revertReasonEnabled,
        secp256K1Native,
        altbn128Native,
        plugins,
        requestedPlugins,
        extraCLIOptions,
        staticNodes,
        isDnsEnabled,
        runCommand,
        keyPair,
        strictTxReplayProtectionEnabled,
        environment,
        synchronizerConfiguration,
        storageImplementation,
        pluginConfiguration);
  }
}
