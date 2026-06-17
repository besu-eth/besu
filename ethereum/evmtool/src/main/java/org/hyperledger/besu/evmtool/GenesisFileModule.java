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
package org.hyperledger.besu.evmtool;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.NetworkDefinition;
import org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Optional;
import javax.inject.Named;
import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;
import io.vertx.core.json.JsonObject;

/**
 * This class, GenesisFileModule, is a Dagger module that provides dependencies for the GenesisFile.
 * It contains options for setting up the GenesisFile, such as the genesis configuration, genesis
 * state, block header functions, and the genesis block.
 *
 * <p>The class uses Dagger annotations to define these options, which can be provided via the
 * command line when running the EVM tool. Each option has a corresponding provider method that
 * Dagger uses to inject the option's value where needed.
 */
@Module
public class GenesisFileModule {

  private final String genesisConfig;

  /**
   * Constructs a new GenesisFileModule with the specified genesis configuration.
   *
   * @param genesisConfig The configuration for the genesis file. This is typically a JSON string
   *     that specifies various parameters for the genesis block of the blockchain.
   */
  protected GenesisFileModule(final String genesisConfig) {
    this.genesisConfig = genesisConfig;
  }

  @Singleton
  @Provides
  GenesisConfig providesGenesisConfig() {
    return GenesisConfig.fromConfig(genesisConfig);
  }

  @Singleton
  @Provides
  GenesisConfigOptions provideGenesisConfigOptions(final GenesisConfig genesisConfig) {
    return genesisConfig.getConfigOptions();
  }

  @Singleton
  @Provides
  @SuppressWarnings("UnusedVariable")
  ProtocolSchedule provideProtocolSchedule(
      final GenesisConfigOptions configOptions,
      @Named("Fork") final Optional<String> fork,
      @Named("RevertReasonEnabled") final boolean revertReasonEnabled,
      final EvmConfiguration evmConfiguration) {
    throw new RuntimeException("Abstract");
  }

  @Singleton
  @Provides
  GenesisState provideGenesisState(
      final GenesisConfig genesisConfig,
      final ProtocolSchedule protocolSchedule,
      final PathBasedCodeCache codeCache) {
    return GenesisState.fromConfig(genesisConfig, protocolSchedule, codeCache);
  }

  @Singleton
  @Provides
  BlockHeaderFunctions blockHashFunction() {
    throw new RuntimeException("Abstract");
  }

  @Singleton
  @Provides
  @Named("GenesisBlock")
  Block provideGenesisBlock(final GenesisState genesisState) {
    return genesisState.getBlock();
  }

  static GenesisFileModule createGenesisModule(final NetworkDefinition networkDefinition) {
    return createGenesisModule(EthNetworkConfig.jsonConfig(networkDefinition));
  }

  static GenesisFileModule createGenesisModule(final File genesisFile) throws IOException {
    return createGenesisModule(Files.readString(genesisFile.toPath(), Charset.defaultCharset()));
  }

  static GenesisFileModule createGenesisModule() {
    final JsonObject genesis = new JsonObject();
    final JsonObject config = new JsonObject();
    config.put("depositContractAddress", "0x00000000219ab540356cbb839cbe05303d7705fa");
    config.put("withdrawalRequestContractAddress", "0x00000961ef480eb55e80d19ad83579a64c007002");
    config.put("consolidationRequestContractAddress", "0x0000bbddc7ce488642fb579f8b00f3a590007251");
    genesis.put("config", config);
    config.put("chainId", 1337);
    config.put(MainnetHardforkId.mostRecent().toString().toLowerCase(Locale.ROOT) + "Time", 0);
    genesis.put("baseFeePerGas", "0x3b9aca00");
    genesis.put("gasLimit", "0x2540be400");
    genesis.put("difficulty", "0x0");
    genesis.put("mixHash", "0x0000000000000000000000000000000000000000000000000000000000000000");
    genesis.put("coinbase", "0x0000000000000000000000000000000000000000");

    return createGenesisModule(genesis.toString());
  }

  private static GenesisFileModule createGenesisModule(final String genesisConfig) {
    final JsonObject genesis = new JsonObject(genesisConfig);
    final JsonObject config = genesis.getJsonObject("config");
    if (config.containsKey("ibft") || config.containsKey("clique") || config.containsKey("qbft")) {
      throw new RuntimeException("Only Ethash and Merge configs accepted as genesis files");
    }
    return new MainnetGenesisFileModule(genesisConfig);
  }
}
