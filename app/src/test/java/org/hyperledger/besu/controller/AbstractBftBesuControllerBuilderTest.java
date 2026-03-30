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
package org.hyperledger.besu.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.CheckpointConfigOptions;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractBftBesuControllerBuilderTest {

  protected BesuControllerBuilder bftBesuControllerBuilder;
  @Mock protected GenesisConfig genesisConfig;
  @Mock protected GenesisConfigOptions genesisConfigOptions;
  @Mock private SynchronizerConfiguration synchronizerConfiguration;
  @Mock private EthProtocolConfiguration ethProtocolConfiguration;
  @Mock CheckpointConfigOptions checkpointConfigOptions;
  @Mock private Clock clock;
  @Mock private StorageProvider storageProvider;
  @Mock private WorldStatePreimageStorage worldStatePreimageStorage;
  private static final BigInteger networkId = BigInteger.ONE;
  private static final NodeKey nodeKey = NodeKeyUtils.generate();
  private final TransactionPoolConfiguration poolConfiguration =
      TransactionPoolConfiguration.DEFAULT;
  private final ObservableMetricsSystem observableMetricsSystem = new NoOpMetricsSystem();
  protected final ObjectMapper objectMapper = new ObjectMapper();
  private final MiningConfiguration miningConfiguration = MiningConfiguration.newDefault();
  @TempDir Path tempDir;

  @BeforeEach
  public void setup() throws JsonProcessingException {
    // besu controller setup
    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        mock(ForestWorldStateKeyValueStorage.class);
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    lenient().when(genesisConfig.getParentHash()).thenReturn(Hash.ZERO.getBytes().toHexString());
    lenient().when(genesisConfig.getDifficulty()).thenReturn(Bytes.of(0).toHexString());
    lenient().when(genesisConfig.getMixHash()).thenReturn(Hash.ZERO.getBytes().toHexString());
    lenient().when(genesisConfig.getNonce()).thenReturn(Long.toHexString(1));
    lenient().when(genesisConfig.getConfigOptions()).thenReturn(genesisConfigOptions);
    lenient().when(genesisConfigOptions.getCheckpointOptions()).thenReturn(checkpointConfigOptions);
    lenient()
        .when(storageProvider.createBlockchainStorage(any(), any(), any()))
        .thenReturn(
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false));
    lenient()
        .when(
            storageProvider.createWorldStateStorageCoordinator(
                DataStorageConfiguration.DEFAULT_FOREST_CONFIG))
        .thenReturn(worldStateStorageCoordinator);
    lenient().when(worldStateKeyValueStorage.isWorldStateAvailable(any())).thenReturn(true);
    lenient()
        .when(worldStateKeyValueStorage.updater())
        .thenReturn(mock(ForestWorldStateKeyValueStorage.Updater.class));
    lenient()
        .when(worldStatePreimageStorage.updater())
        .thenReturn(mock(WorldStatePreimageStorage.Updater.class));
    lenient()
        .when(storageProvider.createWorldStatePreimageStorage())
        .thenReturn(worldStatePreimageStorage);
    lenient().when(synchronizerConfiguration.getDownloaderParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getTransactionsParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getComputationParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getSyncMode()).thenReturn(SyncMode.FULL);

    lenient()
        .when(synchronizerConfiguration.getBlockPropagationRange())
        .thenReturn(Range.closed(1L, 2L));

    lenient()
        .when(ethProtocolConfiguration.getMaxEthCapability())
        .thenReturn(EthProtocolConfiguration.DEFAULT_MAX_CAPABILITY);
    setupBftGenesisConfig();

    bftBesuControllerBuilder =
        createBftControllerBuilder()
            .genesisConfig(genesisConfig)
            .synchronizerConfiguration(synchronizerConfiguration)
            .ethProtocolConfiguration(ethProtocolConfiguration)
            .networkId(networkId)
            .miningParameters(miningConfiguration)
            .metricsSystem(observableMetricsSystem)
            .dataDirectory(tempDir)
            .clock(clock)
            .transactionPoolConfiguration(poolConfiguration)
            .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_FOREST_CONFIG)
            .nodeKey(nodeKey)
            .storageProvider(storageProvider)
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .besuComponent(mock(BesuComponent.class))
            .networkConfiguration(NetworkingConfiguration.DEFAULT)
            .apiConfiguration(ImmutableApiConfiguration.builder().build());
  }

  protected abstract void setupBftGenesisConfig() throws JsonProcessingException;

  protected abstract BesuControllerBuilder createBftControllerBuilder();

  @Test
  public void miningParametersBlockPeriodSecondsIsUpdatedOnTransition() {
    final var besuController = bftBesuControllerBuilder.build();
    final var protocolContext = besuController.getProtocolContext();
    final var protocolSchedule = besuController.getProtocolSchedule();

    final BlockHeader header1 =
        new BlockHeader(
            protocolContext.getBlockchain().getChainHeadHash(),
            Hash.EMPTY_TRIE_HASH,
            Address.ZERO,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY_TRIE_HASH,
            LogsBloomFilter.builder().build(),
            Difficulty.ONE,
            1,
            0,
            0,
            0,
            protocolContext.getBlockchain().getChainHead().getBlockHeader().getExtraData(),
            Wei.ZERO,
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null, // slotNumber
            getBlockHeaderFunctions());
    final Block block1 = new Block(header1, BlockBody.empty());

    protocolContext.getBlockchain().appendBlock(block1, List.of());

    assertThat(miningConfiguration.getBlockPeriodSeconds()).isNotEmpty().hasValue(2);
    assertThat(
            miningConfiguration.getBlockTxsSelectionMaxTime(
                protocolSchedule.getByBlockHeader(header1).isPoS()))
        .isEqualTo(Duration.ofMillis(2000 * 75 / 100));
  }

  protected abstract BlockHeaderFunctions getBlockHeaderFunctions();
}
