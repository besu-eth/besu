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
package org.hyperledger.besu.ethereum.core;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBinTrieInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Function;

public class ExecutionContextTestFixture {

  private final Block genesis;
  private final KeyValueStorage blockchainKeyValueStorage;
  private final KeyValueStorage variablesKeyValueStorage;
  private final MutableBlockchain blockchain;
  private final WorldStateArchive stateArchive;

  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;

  private ExecutionContextTestFixture(
      final GenesisConfig genesisConfig,
      final ProtocolSchedule protocolSchedule,
      final KeyValueStorage blockchainKeyValueStorage,
      final KeyValueStorage variablesKeyValueStorage,
      final Optional<DataStorageFormat> dataStorageFormat) {
    final GenesisState genesisState =
        GenesisState.fromConfig(genesisConfig, protocolSchedule, new CodeCache());
    this.genesis = genesisState.getBlock();
    this.blockchainKeyValueStorage = blockchainKeyValueStorage;
    this.variablesKeyValueStorage = variablesKeyValueStorage;
    this.blockchain =
        DefaultBlockchain.createMutable(
            genesis,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                blockchainKeyValueStorage,
                new VariablesKeyValueStorage(variablesKeyValueStorage),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0);
    if (dataStorageFormat.isPresent()) {
      if (dataStorageFormat.get().equals(DataStorageFormat.BONSAI)) {
        this.stateArchive = createBonsaiInMemoryWorldStateArchive(blockchain);
      } else if (dataStorageFormat.get().equals(DataStorageFormat.BINTRIE)) {
        this.stateArchive = createBinTrieInMemoryWorldStateArchive(blockchain);
      } else {
        this.stateArchive = createInMemoryWorldStateArchive();
      }
    } else {
      this.stateArchive = createInMemoryWorldStateArchive();
    }
    this.protocolSchedule = protocolSchedule;
    this.protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(stateArchive)
            .withConsensusContext(new ConsensusContextFixture())
            .build();
    genesisState.writeStateTo(stateArchive.getWorldState());
  }

  public static ExecutionContextTestFixture create() {
    return new Builder(GenesisConfig.mainnet()).build();
  }

  public static Builder builder(final GenesisConfig genesisConfig) {
    return new Builder(genesisConfig);
  }

  public Block getGenesis() {
    return genesis;
  }

  public KeyValueStorage getBlockchainKeyValueStorage() {
    return blockchainKeyValueStorage;
  }

  public KeyValueStorage getVariablesKeyValueStorage() {
    return variablesKeyValueStorage;
  }

  public MutableBlockchain getBlockchain() {
    return blockchain;
  }

  public WorldStateArchive getStateArchive() {
    return stateArchive;
  }

  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  public ProtocolContext getProtocolContext() {
    return protocolContext;
  }

  public static class Builder {
    private final GenesisConfig genesisConfig;
    private KeyValueStorage variablesKeyValueStorage;
    private KeyValueStorage blockchainKeyValueStorage;
    private ProtocolSchedule protocolSchedule;
    private Optional<DataStorageFormat> dataStorageFormat = Optional.empty();

    public Builder(final GenesisConfig genesisConfig) {
      this.genesisConfig = genesisConfig;
    }

    public Builder variablesKeyValueStorage(final KeyValueStorage keyValueStorage) {
      this.variablesKeyValueStorage = keyValueStorage;
      return this;
    }

    public Builder blockchainKeyValueStorage(final KeyValueStorage keyValueStorage) {
      this.blockchainKeyValueStorage = keyValueStorage;
      return this;
    }

    public Builder protocolSchedule(final ProtocolSchedule protocolSchedule) {
      this.protocolSchedule = protocolSchedule;
      return this;
    }

    public Builder dataStorageFormat(final DataStorageFormat dataStorageFormat) {
      this.dataStorageFormat = Optional.of(dataStorageFormat);
      return this;
    }

    public ExecutionContextTestFixture build() {
      if (protocolSchedule == null) {
        protocolSchedule =
            new ProtocolScheduleBuilder(
                    genesisConfig.getConfigOptions(),
                    Optional.of(BigInteger.valueOf(42)),
                    ProtocolSpecAdapters.create(0, Function.identity()),
                    false,
                    EvmConfiguration.DEFAULT,
                    MiningConfiguration.MINING_DISABLED,
                    new BadBlockManager(),
                    false,
                    ImmutableBalConfiguration.builder().build(),
                    new NoOpMetricsSystem())
                .createProtocolSchedule();
      }
      if (blockchainKeyValueStorage == null) {
        blockchainKeyValueStorage = new InMemoryKeyValueStorage();
      }
      if (variablesKeyValueStorage == null) {
        variablesKeyValueStorage = new InMemoryKeyValueStorage();
      }

      return new ExecutionContextTestFixture(
          genesisConfig,
          protocolSchedule,
          blockchainKeyValueStorage,
          variablesKeyValueStorage,
          dataStorageFormat);
    }
  }
}
