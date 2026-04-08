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

import static org.assertj.core.util.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.hyperledger.besu.evm.internal.EvmConfiguration.DEFAULT;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.BONSAI;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.util.RawBlockIterator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.BlockTestUtil;
import org.hyperledger.besu.testutil.BlockTestUtil.ChainResources;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlockchainSetupUtil {
  private final GenesisState genesisState;
  private final MutableBlockchain blockchain;
  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final WorldStateArchive worldArchive;
  private final TransactionPool transactionPool;
  private final List<Block> blocks;
  private final EthScheduler scheduler;
  private long maxBlockNumber;

  private BlockchainSetupUtil(
      final GenesisState genesisState,
      final MutableBlockchain blockchain,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final WorldStateArchive worldArchive,
      final TransactionPool transactionPool,
      final List<Block> blocks,
      final EthScheduler scheduler) {
    this.genesisState = genesisState;
    this.blockchain = blockchain;
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.worldArchive = worldArchive;
    this.transactionPool = transactionPool;
    this.blocks = blocks;
    this.scheduler = scheduler;
  }

  public Blockchain importAllBlocks() {
    importBlocks(blocks);
    return blockchain;
  }

  public Blockchain importAllBlocks(
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    importBlocks(blocks, headerValidationMode, ommerValidationMode);
    return blockchain;
  }

  public void importFirstBlocks(final int count) {
    importBlocks(blocks.subList(0, count));
  }

  public void importBlockAtIndex(final int index) {
    importBlocks(Collections.singletonList(blocks.get(index)));
  }

  public Block getBlock(final int index) {
    checkArgument(index < blocks.size(), "Invalid block index");
    return blocks.get(index);
  }

  public List<Block> getBlocks() {
    return blocks;
  }

  public int blockCount() {
    return blocks.size();
  }

  public static BlockchainSetupUtil forTesting(final DataStorageFormat storageFormat) {
    return createForEthashChain(BlockTestUtil.getTestChainResources(), storageFormat);
  }

  public static BlockchainSetupUtil forHiveTesting(final DataStorageFormat storageFormat) {
    return createForEthashChain(BlockTestUtil.getHiveTestChainResources(), storageFormat);
  }

  public static BlockchainSetupUtil forMainnet() {
    return createForEthashChain(BlockTestUtil.getMainnetResources(), BONSAI);
  }

  public static BlockchainSetupUtil forOutdatedFork() {
    return createForEthashChain(BlockTestUtil.getOutdatedForkResources(), DataStorageFormat.FOREST);
  }

  public static BlockchainSetupUtil forUpgradedFork() {
    return createForEthashChain(BlockTestUtil.getUpgradedForkResources(), DataStorageFormat.FOREST);
  }

  public static BlockchainSetupUtil forSnapTesting(final DataStorageFormat storageFormat) {
    return createForEthashChain(BlockTestUtil.getSnapTestChainResources(), storageFormat);
  }

  public static BlockchainSetupUtil createForEthashChain(
      final ChainResources chainResources, final DataStorageFormat storageFormat) {
    return create(
        chainResources,
        storageFormat,
        BlockchainSetupUtil::mainnetProtocolScheduleProvider,
        BlockchainSetupUtil::mainnetProtocolContextProvider,
        new EthScheduler(1, 1, 1, 1, new NoOpMetricsSystem()));
  }

  private static ProtocolSchedule mainnetProtocolScheduleProvider(
      final GenesisConfig genesisConfig) {
    return MainnetProtocolSchedule.fromConfig(
        genesisConfig.getConfigOptions(),
        DEFAULT,
        MiningConfiguration.newDefault(),
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem());
  }

  private static ProtocolContext mainnetProtocolContextProvider(
      final MutableBlockchain blockchain, final WorldStateArchive worldStateArchive) {
    return new ProtocolContext.Builder()
        .withBlockchain(blockchain)
        .withWorldStateArchive(worldStateArchive)
        .withConsensusContext(new ConsensusContextFixture())
        .build();
  }

  private static BlockchainSetupUtil create(
      final ChainResources chainResources,
      final DataStorageFormat storageFormat,
      final ProtocolScheduleProvider protocolScheduleProvider,
      final ProtocolContextProvider protocolContextProvider,
      final EthScheduler scheduler) {
    try {
      final GenesisConfig genesisConfig = GenesisConfig.fromSource(chainResources.getGenesisURL());
      final ProtocolSchedule protocolSchedule = protocolScheduleProvider.get(genesisConfig);

      // only used in tests no global code cache is needed
      final GenesisState genesisState =
          GenesisState.fromConfig(genesisConfig, protocolSchedule, new CodeCache());
      final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());
      final WorldStateArchive worldArchive =
          storageFormat == DataStorageFormat.BONSAI
              ? createBonsaiInMemoryWorldStateArchive(blockchain, null)
              : createInMemoryWorldStateArchive();
      final TransactionPool transactionPool = mock(TransactionPool.class);

      genesisState.writeStateTo(worldArchive.getWorldState());
      final ProtocolContext protocolContext = protocolContextProvider.get(blockchain, worldArchive);

      final Path blocksPath = Path.of(chainResources.getBlocksURL().toURI());
      final List<Block> blocks = new ArrayList<>();
      final BlockHeaderFunctions blockHeaderFunctions =
          ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
      try (final RawBlockIterator iterator =
          new RawBlockIterator(blocksPath, blockHeaderFunctions)) {
        while (iterator.hasNext()) {
          blocks.add(iterator.next());
        }
      }
      return new BlockchainSetupUtil(
          genesisState,
          blockchain,
          protocolContext,
          protocolSchedule,
          worldArchive,
          transactionPool,
          blocks,
          scheduler);
    } catch (final IOException | URISyntaxException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public long getMaxBlockNumber() {
    return maxBlockNumber;
  }

  public GenesisState getGenesisState() {
    return genesisState;
  }

  public MutableBlockchain getBlockchain() {
    return blockchain;
  }

  public ProtocolContext getProtocolContext() {
    return protocolContext;
  }

  public ProtocolSchedule getProtocolSchedule() {
    return protocolSchedule;
  }

  public WorldStateArchive getWorldArchive() {
    return worldArchive;
  }

  public EthScheduler getScheduler() {
    return scheduler;
  }

  public TransactionPool getTransactionPool() {
    return transactionPool;
  }

  private void importBlocks(final List<Block> blocks) {
    importBlocks(blocks, HeaderValidationMode.FULL, HeaderValidationMode.FULL);
  }

  private void importBlocks(
      final List<Block> blocks,
      final HeaderValidationMode headerValidationMode,
      final HeaderValidationMode ommerValidationMode) {
    for (final Block block : blocks) {
      if (block.getHeader().getNumber() == BlockHeader.GENESIS_BLOCK_NUMBER) {
        continue;
      }
      final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
      final BlockImporter blockImporter = protocolSpec.getBlockImporter();
      final BlockImportResult result =
          blockImporter.importBlock(
              protocolContext, block, headerValidationMode, ommerValidationMode);
      if (!result.isImported()) {
        throw new IllegalStateException("Unable to import block " + block.getHeader().getNumber());
      }
    }
    this.maxBlockNumber = blockchain.getChainHeadBlockNumber();
  }

  private interface ProtocolScheduleProvider {
    ProtocolSchedule get(GenesisConfig genesisConfig);
  }

  private interface ProtocolContextProvider {
    ProtocolContext get(MutableBlockchain blockchain, WorldStateArchive worldStateArchive);
  }
}
