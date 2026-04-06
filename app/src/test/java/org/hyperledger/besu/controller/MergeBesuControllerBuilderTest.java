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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.components.BesuComponent;
import org.hyperledger.besu.config.CheckpointConfigOptions;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.common.collect.Range;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MergeBesuControllerBuilderTest {

  private MergeBesuControllerBuilder besuControllerBuilder;
  private static final NodeKey nodeKey = NodeKeyUtils.generate();

  @Mock GenesisConfig genesisConfig;
  @Mock GenesisConfigOptions genesisConfigOptions;
  @Mock SynchronizerConfiguration synchronizerConfiguration;
  @Mock EthProtocolConfiguration ethProtocolConfiguration;
  @Mock CheckpointConfigOptions checkpointConfigOptions;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  MiningConfiguration miningConfiguration;

  @Mock Clock clock;
  @Mock StorageProvider storageProvider;
  @Mock GasLimitCalculator gasLimitCalculator;
  @Mock WorldStatePreimageStorage worldStatePreimageStorage;

  BigInteger networkId = BigInteger.ONE;
  private final BlockHeaderTestFixture headerGenerator = new BlockHeaderTestFixture();
  private final BaseFeeMarket feeMarket = FeeMarket.london(0, Optional.of(Wei.of(42)));
  private final TransactionPoolConfiguration poolConfiguration =
      TransactionPoolConfiguration.DEFAULT;
  private final ObservableMetricsSystem observableMetricsSystem = new NoOpMetricsSystem();

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {

    final ForestWorldStateKeyValueStorage worldStateKeyValueStorage =
        mock(ForestWorldStateKeyValueStorage.class);
    final WorldStateStorageCoordinator worldStateStorageCoordinator =
        new WorldStateStorageCoordinator(worldStateKeyValueStorage);

    lenient().when(genesisConfig.getParentHash()).thenReturn(Hash.ZERO.getBytes().toHexString());
    lenient().when(genesisConfig.getDifficulty()).thenReturn(Bytes.of(0).toHexString());
    lenient().when(genesisConfig.getExtraData()).thenReturn(Bytes.EMPTY.toHexString());
    lenient().when(genesisConfig.getMixHash()).thenReturn(Hash.ZERO.getBytes().toHexString());
    lenient().when(genesisConfig.getNonce()).thenReturn(Long.toHexString(1));
    lenient().when(genesisConfig.getConfigOptions()).thenReturn(genesisConfigOptions);
    lenient().when(genesisConfigOptions.getCheckpointOptions()).thenReturn(checkpointConfigOptions);
    when(genesisConfigOptions.getTerminalTotalDifficulty())
        .thenReturn((Optional.of(UInt256.valueOf(100L))));
    when(genesisConfigOptions.getTerminalBlockHash()).thenReturn(Optional.of(Hash.ZERO));
    lenient().when(genesisConfigOptions.getTerminalBlockNumber()).thenReturn(OptionalLong.of(1L));
    lenient()
        .when(storageProvider.createBlockchainStorage(any(), any(), any()))
        .thenReturn(
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false));
    lenient()
        .when(storageProvider.getStorageBySegmentIdentifier(any()))
        .thenReturn(new InMemoryKeyValueStorage());
    lenient().when(synchronizerConfiguration.getDownloaderParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getTransactionsParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getComputationParallelism()).thenReturn(1);
    lenient().when(synchronizerConfiguration.getSyncMode()).thenReturn(SyncMode.FULL);

    lenient()
        .when(synchronizerConfiguration.getBlockPropagationRange())
        .thenReturn(Range.closed(1L, 2L));

    lenient()
        .when(
            storageProvider.createWorldStateStorageCoordinator(
                DataStorageConfiguration.DEFAULT_FOREST_CONFIG))
        .thenReturn(worldStateStorageCoordinator);
    lenient()
        .when(storageProvider.createWorldStatePreimageStorage())
        .thenReturn(worldStatePreimageStorage);

    lenient().when(worldStateKeyValueStorage.isWorldStateAvailable(any())).thenReturn(true);
    lenient()
        .when(worldStatePreimageStorage.updater())
        .thenReturn(mock(WorldStatePreimageStorage.Updater.class));
    lenient()
        .when(worldStateKeyValueStorage.updater())
        .thenReturn(mock(ForestWorldStateKeyValueStorage.Updater.class));
    lenient().when(miningConfiguration.getTargetGasLimit()).thenReturn(OptionalLong.empty());
    lenient()
        .when(ethProtocolConfiguration.getMaxEthCapability())
        .thenReturn(EthProtocolConfiguration.DEFAULT_MAX_CAPABILITY);

    besuControllerBuilder = visitWithMockConfigs(new MergeBesuControllerBuilder());
  }

  MergeBesuControllerBuilder visitWithMockConfigs(final MergeBesuControllerBuilder builder) {
    return (MergeBesuControllerBuilder)
        builder
            .genesisConfig(genesisConfig)
            .synchronizerConfiguration(synchronizerConfiguration)
            .ethProtocolConfiguration(ethProtocolConfiguration)
            .miningParameters(miningConfiguration)
            .metricsSystem(observableMetricsSystem)
            .dataDirectory(tempDir)
            .clock(clock)
            .transactionPoolConfiguration(poolConfiguration)
            .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_FOREST_CONFIG)
            .nodeKey(nodeKey)
            .storageProvider(storageProvider)
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .networkConfiguration(NetworkingConfiguration.DEFAULT)
            .besuComponent(mock(BesuComponent.class))
            .networkId(networkId)
            .apiConfiguration(ImmutableApiConfiguration.builder().build());
  }

  @Test
  public void assertTerminalTotalDifficultyInMergeContext() {
    when(genesisConfigOptions.getTerminalTotalDifficulty())
        .thenReturn(Optional.of(UInt256.valueOf(1500L)));

    final Difficulty terminalTotalDifficulty =
        visitWithMockConfigs(new MergeBesuControllerBuilder())
            .build()
            .getProtocolContext()
            .getConsensusContext(MergeContext.class)
            .getTerminalTotalDifficulty();

    assertThat(terminalTotalDifficulty).isEqualTo(Difficulty.of(1500L));
  }

  @Test
  public void assertConfiguredBlock() {
    final Blockchain mockChain = mock(Blockchain.class);
    when(mockChain.getBlockHeader(anyLong())).thenReturn(Optional.of(mock(BlockHeader.class)));
    final MergeContext mergeContext =
        besuControllerBuilder.createConsensusContext(
            mockChain,
            mock(WorldStateArchive.class),
            this.besuControllerBuilder.createProtocolSchedule());
    assertThat(mergeContext).isNotNull();
    assertThat(mergeContext.getTerminalPoWBlock()).isPresent();
  }

  @Test
  public void assertBuiltContextMonitorsTTD() {
    final GenesisState genesisState =
        GenesisState.fromConfig(
            genesisConfig, this.besuControllerBuilder.createProtocolSchedule(), new CodeCache());
    final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());
    final MergeContext mergeContext =
        spy(
            besuControllerBuilder.createConsensusContext(
                blockchain,
                mock(WorldStateArchive.class),
                this.besuControllerBuilder.createProtocolSchedule()));
    assertThat(mergeContext).isNotNull();
    final Difficulty over = Difficulty.of(10000L);
    final Difficulty under = Difficulty.of(10L);

    final BlockHeader parent =
        headerGenerator
            .difficulty(under)
            .parentHash(genesisState.getBlock().getHash())
            .number(genesisState.getBlock().getHeader().getNumber() + 1)
            .gasLimit(genesisState.getBlock().getHeader().getGasLimit())
            .stateRoot(genesisState.getBlock().getHeader().getStateRoot())
            .buildHeader();
    blockchain.appendBlock(new Block(parent, BlockBody.empty()), Collections.emptyList());

    final BlockHeader terminal =
        headerGenerator
            .difficulty(over)
            .parentHash(parent.getHash())
            .number(parent.getNumber() + 1)
            .gasLimit(parent.getGasLimit())
            .stateRoot(parent.getStateRoot())
            .buildHeader();

    blockchain.appendBlock(new Block(terminal, BlockBody.empty()), Collections.emptyList());
    assertThat(mergeContext.isPostMerge()).isTrue();
  }

  @Test
  public void assertNoFinalizedBlockWhenNotStored() {
    final Blockchain mockChain = mock(Blockchain.class);
    when(mockChain.getFinalized()).thenReturn(Optional.empty());
    final MergeContext mergeContext =
        besuControllerBuilder.createConsensusContext(
            mockChain,
            mock(WorldStateArchive.class),
            this.besuControllerBuilder.createProtocolSchedule());
    assertThat(mergeContext).isNotNull();
    assertThat(mergeContext.getFinalized()).isEmpty();
  }

  @Test
  public void assertFinalizedBlockIsPresentWhenStored() {
    final BlockHeader finalizedHeader = finalizedBlockHeader();

    final Blockchain mockChain = mock(Blockchain.class);
    when(mockChain.getFinalized()).thenReturn(Optional.of(finalizedHeader.getHash()));
    when(mockChain.getBlockHeader(finalizedHeader.getHash()))
        .thenReturn(Optional.of(finalizedHeader));
    final MergeContext mergeContext =
        besuControllerBuilder.createConsensusContext(
            mockChain,
            mock(WorldStateArchive.class),
            this.besuControllerBuilder.createProtocolSchedule());
    assertThat(mergeContext).isNotNull();
    assertThat(mergeContext.getFinalized().get()).isEqualTo(finalizedHeader);
  }

  private BlockHeader finalizedBlockHeader() {
    final long blockNumber = 42;
    final Hash magicHash = Hash.wrap(Bytes32.leftPad(Bytes.ofUnsignedInt(42)));

    return headerGenerator
        .difficulty(Difficulty.MAX_VALUE)
        .parentHash(magicHash)
        .number(blockNumber)
        .baseFeePerGas(feeMarket.computeBaseFee(blockNumber, Wei.of(0x3b9aca00), 0, 15000000l))
        .gasLimit(30000000l)
        .stateRoot(magicHash)
        .buildHeader();
  }
}
