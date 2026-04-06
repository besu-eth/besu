/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LAYERED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LEGACY;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.SEQUENCED;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.cli.config.InternalProfileName;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class ConfigurationOverviewBuilderTest {
  private ConfigurationOverviewBuilder builder;

  @BeforeEach
  void setUp() {
    builder = new ConfigurationOverviewBuilder(mock(Logger.class));
  }

  @Test
  void setNetwork() {
    final String noNetworkSet = builder.build();
    assertThat(noNetworkSet).doesNotContain("Network:");

    builder.setNetwork("foobar");
    final String networkSet = builder.build();
    assertThat(networkSet).contains("Network: foobar");
  }

  @Test
  void setGenesisFile() {
    final String noGenesisSet = builder.build();
    assertThat(noGenesisSet).doesNotContain("Network: Custom genesis file specified");

    builder.setNetwork("foobar");
    final String networkSet = builder.build();
    assertThat(networkSet).contains("Network: foobar");

    builder.setHasCustomGenesis(true);
    builder.setCustomGenesis("file.name");
    final String genesisSet = builder.build();
    assertThat(genesisSet).contains("Network: Custom genesis file");
    assertThat(genesisSet).doesNotContain("Network: foobar");
  }

  @Test
  void setNetworkId() {
    final String noNetworkIdSet = builder.build();
    assertThat(noNetworkIdSet).doesNotContain("Network Id:");

    builder.setNetworkId(BigInteger.ONE);
    final String networkIdSet = builder.build();
    assertThat(networkIdSet).contains("Network Id: 1");
  }

  @Test
  void setDataStorage() {
    final String noDataStorageSet = builder.build();
    assertThat(noDataStorageSet).doesNotContain("Data storage:");

    builder.setDataStorage("bonsai");
    final String dataStorageSet = builder.build();
    assertThat(dataStorageSet).contains("Data storage: bonsai");
  }

  @Test
  void setSyncMode() {
    final String noSyncModeSet = builder.build();
    assertThat(noSyncModeSet).doesNotContain("Sync mode:");

    builder.setSyncMode("fast");
    final String syncModeSet = builder.build();
    assertThat(syncModeSet).contains("Sync mode: fast");
  }

  @Test
  void setSyncMinPeers() {
    final String noSyncMinPeersSet = builder.build();
    assertThat(noSyncMinPeersSet).doesNotContain("Sync min peers:");

    builder.setSyncMinPeers(3);
    final String syncMinPeersSet = builder.build();
    assertThat(syncMinPeersSet).contains("Sync min peers: 3");
  }

  @Test
  void setRpcPort() {
    final String noRpcPortSet = builder.build();
    assertThat(noRpcPortSet).doesNotContain("RPC HTTP port:");

    builder.setRpcPort(42);
    final String rpcPortSet = builder.build();
    assertThat(rpcPortSet).contains("RPC HTTP port: 42");
  }

  @Test
  void setRpcHttpApis() {
    final String noRpcApisSet = builder.build();
    assertThat(noRpcApisSet).doesNotContain("RPC HTTP APIs:");

    final Collection<String> rpcApis = new ArrayList<>();
    rpcApis.add("api1");
    rpcApis.add("api2");
    builder.setRpcHttpApis(rpcApis);
    final String rpcApisSet = builder.build();
    assertThat(rpcApisSet).contains("RPC HTTP APIs: api1,api2");
  }

  @Test
  void setEnginePort() {
    final String noEnginePortSet = builder.build();
    assertThat(noEnginePortSet).doesNotContain("Engine port:");

    builder.setEnginePort(42);
    final String enginePortSet = builder.build();
    assertThat(enginePortSet).contains("Engine port: 42");
  }

  @Test
  void setEngineApis() {
    final String noEngineApisSet = builder.build();
    assertThat(noEngineApisSet).doesNotContain("Engine APIs:");

    final Collection<String> engineApis = new ArrayList<>();
    engineApis.add("api1");
    engineApis.add("api2");
    builder.setEngineApis(engineApis);
    final String engineApisSet = builder.build();
    assertThat(engineApisSet).contains("Engine APIs: api1,api2");
  }

  @Test
  void setHighSpecEnabled() {
    final String highSpecNotEnabled = builder.build();
    assertThat(highSpecNotEnabled).doesNotContain("Experimental high spec configuration enabled");

    builder.setHighSpecEnabled();
    final String highSpecEnabled = builder.build();
    assertThat(highSpecEnabled).contains("Experimental high spec configuration enabled");
  }

  @Test
  void setPathBasedLimitTrieLogsEnabled() {
    final String noTrieLogRetentionLimitSet = builder.build();
    assertThat(noTrieLogRetentionLimitSet).doesNotContain("Limit trie logs enabled");

    builder.setLimitTrieLogsEnabled();
    builder.setTrieLogRetentionLimit(42);
    String trieLogRetentionLimitSet = builder.build();
    assertThat(trieLogRetentionLimitSet)
        .contains("Limit trie logs enabled")
        .contains("retention: 42");
    assertThat(trieLogRetentionLimitSet).doesNotContain("prune window");

    builder.setTrieLogsPruningWindowSize(1000);
    trieLogRetentionLimitSet = builder.build();
    assertThat(trieLogRetentionLimitSet).contains("prune window: 1000");
  }

  @Test
  void setTxPoolImplementationLayered() {
    builder.setTxPoolImplementation(LAYERED);
    final String layeredTxPoolSelected = builder.build();
    assertThat(layeredTxPoolSelected).contains("Using LAYERED transaction pool implementation");
  }

  @Test
  void setTxPoolImplementationLegacy() {
    builder.setTxPoolImplementation(LEGACY);
    final String legacyTxPoolSelected = builder.build();
    assertThat(legacyTxPoolSelected).contains("Using LEGACY transaction pool implementation");
  }

  @Test
  void setTxPoolImplementationSequenced() {
    builder.setTxPoolImplementation(SEQUENCED);
    final String sequencedTxPoolSelected = builder.build();
    assertThat(sequencedTxPoolSelected).contains("Using SEQUENCED transaction pool implementation");
  }

  @Test
  void setWorldStateUpdateModeDefault() {
    builder.setWorldStateUpdateMode(EvmConfiguration.DEFAULT.worldUpdaterMode());
    final String layeredTxPoolSelected = builder.build();
    assertThat(layeredTxPoolSelected).contains("Using STACKED worldstate update mode");
  }

  @Test
  void setWorldStateUpdateModeStacked() {
    builder.setWorldStateUpdateMode(EvmConfiguration.WorldUpdaterMode.STACKED);
    final String layeredTxPoolSelected = builder.build();
    assertThat(layeredTxPoolSelected).contains("Using STACKED worldstate update mode");
  }

  @Test
  void setWorldStateUpdateModeJournaled() {
    builder.setWorldStateUpdateMode(EvmConfiguration.WorldUpdaterMode.JOURNALED);
    final String layeredTxPoolSelected = builder.build();
    assertThat(layeredTxPoolSelected).contains("Using JOURNALED worldstate update mode");
  }

  @Test
  void setProfile() {
    builder.setProfile(InternalProfileName.DEV.name());
    final String profileSelected = builder.build();
    assertThat(profileSelected).contains("Profile: DEV");
  }

  @Test
  void setTargetGasLimitGreaterThanMillion() {
    final long targetGasLimit = 50_000_000L;
    builder.setTargetGasLimit(targetGasLimit);
    final String targetGasLimitSelected = builder.build();
    final String targetGasLimitValue = ConfigurationOverviewBuilder.normalizeGas(targetGasLimit);
    assertThat(targetGasLimitSelected)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitValue));
  }

  @Test
  void setTargetGasLimitGreaterThanMillionWith2Digits() {
    final long targetGasLimit = 50_550_000L;
    builder.setTargetGasLimit(targetGasLimit);
    final String targetGasLimitSelected = builder.build();
    final String targetGasLimitValue = ConfigurationOverviewBuilder.normalizeGas(targetGasLimit);
    assertThat(targetGasLimitSelected)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitValue));
  }

  @Test
  void setTargetGasLimitGreaterThanMillionWith3Digits() {
    final long targetGasLimit = 50_555_000L;
    builder.setTargetGasLimit(targetGasLimit);
    final String targetGasLimitSelected = builder.build();
    final String targetGasLimitValue = ConfigurationOverviewBuilder.normalizeGas(targetGasLimit);
    assertThat(targetGasLimitSelected)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitValue));
  }

  @Test
  void setTargetGasLimitGreaterThanMillionWith4Digits() {
    final long targetGasLimit = 50_555_500L;
    builder.setTargetGasLimit(targetGasLimit);
    final String targetGasLimitSelected = builder.build();
    final String targetGasLimitValue = ConfigurationOverviewBuilder.normalizeGas(targetGasLimit);
    System.out.println("targetGasLimitValue:" + targetGasLimitValue);
    assertThat(targetGasLimitSelected)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitValue));
  }

  @Test
  void setTargetGasLimitLowerThanMillion() {
    final long targetGasLimit = 100_000L;
    builder.setTargetGasLimit(targetGasLimit);
    final String targetGasLimitSelected = builder.build();
    final String targetGasLimitValue = ConfigurationOverviewBuilder.normalizeGas(targetGasLimit);
    assertThat(targetGasLimitSelected)
        .contains(String.format("%s: %s", "Target Gas Limit", targetGasLimitValue));
  }

  @Test
  void setMaxBlobsPerTransaction() {
    final String noMaxBlobsSet = builder.build();
    assertThat(noMaxBlobsSet).doesNotContain("Max Blobs Per Transaction");

    builder.setMaxBlobsPerTransaction(6);
    final String maxBlobsSet = builder.build();
    assertThat(maxBlobsSet).contains("Max Blobs Per Transaction: 6");
  }

  @Test
  void setDiscoveryDisabled() {
    final String discoveryEnabledByDefault = builder.build();
    assertThat(discoveryEnabledByDefault).doesNotContain("Discovery: disabled");

    builder.setDiscoveryEnabled(false);
    final String discoveryDisabled = builder.build();
    assertThat(discoveryDisabled).contains("Discovery: disabled");
  }

  @Test
  void setDiscoveryEnabled() {
    builder.setDiscoveryEnabled(true);
    final String discoveryEnabled = builder.build();
    assertThat(discoveryEnabled).doesNotContain("Discovery: disabled");
  }

  @Test
  void setChainPruningDisabled() {
    final String noChainPruningSet = builder.build();
    assertThat(noChainPruningSet).doesNotContain("Chain pruning enabled");
  }

  @Test
  void setChainPruningEnabledNoneMode() {
    builder.setChainPruningEnabled(ChainPruningStrategy.NONE, 113_152L, 113_152L);
    final String chainPruningSet = builder.build();
    assertThat(chainPruningSet).doesNotContain("Chain pruning enabled");
  }

  @Test
  void setChainPruningEnabledAllMode() {
    builder.setChainPruningEnabled(ChainPruningStrategy.ALL, 113_152L, 113_152L);
    final String chainPruningSet = builder.build();
    assertThat(chainPruningSet)
        .contains("Chain and BAL pruning enabled (retained BALs: 113152; Blocks: 113152)");
  }

  @Test
  void setChainPruningEnabledAllModeWithDifferentRetentionValues() {
    builder.setChainPruningEnabled(ChainPruningStrategy.ALL, 150_000L, 113_152L);
    final String chainPruningSet = builder.build();
    assertThat(chainPruningSet)
        .contains("Chain and BAL pruning enabled (retained BALs: 113152; Blocks: 150000)");
  }

  @Test
  void setChainPruningEnabledBalModeOnly() {
    builder.setChainPruningEnabled(ChainPruningStrategy.BAL, null, 50_000L);
    final String chainPruningSet = builder.build();
    assertThat(chainPruningSet)
        .contains("BAL pruning enabled (retained BALs: 50000)")
        .doesNotContain("blocks:")
        .doesNotContain(";");
  }
}
