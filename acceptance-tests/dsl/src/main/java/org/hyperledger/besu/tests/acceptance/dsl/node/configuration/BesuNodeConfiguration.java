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

import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.ipc.JsonRpcIpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.permissioning.PermissioningConfiguration;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BesuNodeConfiguration {

  private final String name;
  private final Optional<Path> dataPath;
  private final MiningConfiguration miningConfiguration;
  private final TransactionPoolConfiguration transactionPoolConfiguration;
  private final JsonRpcConfiguration jsonRpcConfiguration;
  private final Optional<JsonRpcConfiguration> engineRpcConfiguration;
  private final WebSocketConfiguration webSocketConfiguration;
  private final JsonRpcIpcConfiguration jsonRpcIpcConfiguration;
  private final InProcessRpcConfiguration inProcessRpcConfiguration;
  private final MetricsConfiguration metricsConfiguration;
  private final Optional<PermissioningConfiguration> permissioningConfiguration;
  private final ApiConfiguration apiConfiguration;
  private final DataStorageConfiguration dataStorageConfiguration;
  private final Optional<String> keyFilePath;
  private final boolean devMode;
  private final GenesisConfigurationProvider genesisConfigProvider;
  private final boolean p2pEnabled;
  private final int p2pPort;
  private final NetworkingConfiguration networkingConfiguration;
  private final boolean discoveryEnabled;
  private final boolean bootnodeEligible;
  private final boolean revertReasonEnabled;
  private final boolean secp256k1Native;
  private final boolean altbn128Native;
  private final List<String> plugins;
  private final List<String> requestedPlugins;
  private final List<String> extraCLIOptions;
  private final List<String> staticNodes;
  private final boolean isDnsEnabled;
  private final List<String> runCommand;
  private final NetworkDefinition network;
  private final Optional<KeyPair> keyPair;
  private final boolean strictTxReplayProtectionEnabled;
  private final Map<String, String> environment;
  private final SynchronizerConfiguration synchronizerConfiguration;
  private final Optional<String> storageImplementation;
  private final PluginConfiguration pluginConfiguration;

  BesuNodeConfiguration(
      final String name,
      final Optional<Path> dataPath,
      final MiningConfiguration miningConfiguration,
      final TransactionPoolConfiguration transactionPoolConfiguration,
      final JsonRpcConfiguration jsonRpcConfiguration,
      final Optional<JsonRpcConfiguration> engineRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final JsonRpcIpcConfiguration jsonRpcIpcConfiguration,
      final InProcessRpcConfiguration inProcessRpcConfiguration,
      final MetricsConfiguration metricsConfiguration,
      final Optional<PermissioningConfiguration> permissioningConfiguration,
      final ApiConfiguration apiConfiguration,
      final DataStorageConfiguration dataStorageConfiguration,
      final Optional<String> keyFilePath,
      final boolean devMode,
      final NetworkDefinition network,
      final GenesisConfigurationProvider genesisConfigProvider,
      final boolean p2pEnabled,
      final int p2pPort,
      final NetworkingConfiguration networkingConfiguration,
      final boolean discoveryEnabled,
      final boolean bootnodeEligible,
      final boolean revertReasonEnabled,
      final boolean secp256k1Native,
      final boolean altbn128Native,
      final List<String> plugins,
      final List<String> requestedPlugins,
      final List<String> extraCLIOptions,
      final List<String> staticNodes,
      final boolean isDnsEnabled,
      final List<String> runCommand,
      final Optional<KeyPair> keyPair,
      final boolean strictTxReplayProtectionEnabled,
      final Map<String, String> environment,
      final SynchronizerConfiguration synchronizerConfiguration,
      final Optional<String> storageImplementation,
      final PluginConfiguration pluginConfiguration) {
    this.name = name;
    this.miningConfiguration = miningConfiguration;
    this.transactionPoolConfiguration = transactionPoolConfiguration;
    this.jsonRpcConfiguration = jsonRpcConfiguration;
    this.engineRpcConfiguration = engineRpcConfiguration;
    this.webSocketConfiguration = webSocketConfiguration;
    this.jsonRpcIpcConfiguration = jsonRpcIpcConfiguration;
    this.inProcessRpcConfiguration = inProcessRpcConfiguration;
    this.metricsConfiguration = metricsConfiguration;
    this.permissioningConfiguration = permissioningConfiguration;
    this.apiConfiguration = apiConfiguration;
    this.dataStorageConfiguration = dataStorageConfiguration;
    this.keyFilePath = keyFilePath;
    this.dataPath = dataPath;
    this.devMode = devMode;
    this.network = network;
    this.genesisConfigProvider = genesisConfigProvider;
    this.p2pEnabled = p2pEnabled;
    this.p2pPort = p2pPort;
    this.networkingConfiguration = networkingConfiguration;
    this.discoveryEnabled = discoveryEnabled;
    this.bootnodeEligible = bootnodeEligible;
    this.revertReasonEnabled = revertReasonEnabled;
    this.secp256k1Native = secp256k1Native;
    this.altbn128Native = altbn128Native;
    this.plugins = plugins;
    this.requestedPlugins = requestedPlugins;
    this.extraCLIOptions = extraCLIOptions;
    this.staticNodes = staticNodes;
    this.isDnsEnabled = isDnsEnabled;
    this.runCommand = runCommand;
    this.keyPair = keyPair;
    this.strictTxReplayProtectionEnabled = strictTxReplayProtectionEnabled;
    this.environment = environment;
    this.synchronizerConfiguration = synchronizerConfiguration;
    this.storageImplementation = storageImplementation;
    this.pluginConfiguration = pluginConfiguration;
  }

  public String getName() {
    return name;
  }

  public MiningConfiguration getMiningParameters() {
    return miningConfiguration;
  }

  public TransactionPoolConfiguration getTransactionPoolConfiguration() {
    return transactionPoolConfiguration;
  }

  public JsonRpcConfiguration getJsonRpcConfiguration() {
    return jsonRpcConfiguration;
  }

  public Optional<JsonRpcConfiguration> getEngineRpcConfiguration() {
    return engineRpcConfiguration;
  }

  public WebSocketConfiguration getWebSocketConfiguration() {
    return webSocketConfiguration;
  }

  public JsonRpcIpcConfiguration getJsonRpcIpcConfiguration() {
    return jsonRpcIpcConfiguration;
  }

  public InProcessRpcConfiguration getInProcessRpcConfiguration() {
    return inProcessRpcConfiguration;
  }

  public MetricsConfiguration getMetricsConfiguration() {
    return metricsConfiguration;
  }

  public Optional<PermissioningConfiguration> getPermissioningConfiguration() {
    return permissioningConfiguration;
  }

  public ApiConfiguration getApiConfiguration() {
    return apiConfiguration;
  }

  public DataStorageConfiguration getDataStorageConfiguration() {
    return dataStorageConfiguration;
  }

  public Optional<String> getKeyFilePath() {
    return keyFilePath;
  }

  public Optional<Path> getDataPath() {
    return dataPath;
  }

  public boolean isDevMode() {
    return devMode;
  }

  public boolean isDiscoveryEnabled() {
    return discoveryEnabled;
  }

  public GenesisConfigurationProvider getGenesisConfigProvider() {
    return genesisConfigProvider;
  }

  public boolean isP2pEnabled() {
    return p2pEnabled;
  }

  public int getP2pPort() {
    return p2pPort;
  }

  public NetworkingConfiguration getNetworkingConfiguration() {
    return networkingConfiguration;
  }

  public boolean isBootnodeEligible() {
    return bootnodeEligible;
  }

  public List<String> getPlugins() {
    return plugins;
  }

  public List<String> getRequestedPlugins() {
    return requestedPlugins;
  }

  public List<String> getExtraCLIOptions() {
    return extraCLIOptions;
  }

  public boolean isRevertReasonEnabled() {
    return revertReasonEnabled;
  }

  public boolean isSecp256k1Native() {
    return secp256k1Native;
  }

  public boolean isAltbn128Native() {
    return altbn128Native;
  }

  public List<String> getStaticNodes() {
    return staticNodes;
  }

  public boolean isDnsEnabled() {
    return isDnsEnabled;
  }

  public List<String> getRunCommand() {
    return runCommand;
  }

  public NetworkDefinition getNetwork() {
    return network;
  }

  public Optional<KeyPair> getKeyPair() {
    return keyPair;
  }

  public boolean isStrictTxReplayProtectionEnabled() {
    return strictTxReplayProtectionEnabled;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public SynchronizerConfiguration getSynchronizerConfiguration() {
    return synchronizerConfiguration;
  }

  public Optional<String> storageImplementation() {
    return storageImplementation;
  }

  public PluginConfiguration getPluginConfiguration() {
    return pluginConfiguration;
  }
}
