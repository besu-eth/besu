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
package org.hyperledger.besu.ethereum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BlockImportExceptionHandlingTest {

  private final MainnetTransactionProcessor transactionProcessor =
      mock(MainnetTransactionProcessor.class);
  private final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory =
      mock(AbstractBlockProcessor.TransactionReceiptFactory.class);

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final BlockProcessor blockProcessor =
      new MainnetBlockProcessor(
          transactionProcessor,
          transactionReceiptFactory,
          Wei.ZERO,
          BlockHeader::getCoinbase,
          true,
          protocolSchedule,
          BalConfiguration.DEFAULT);
  private final BlockHeaderValidator blockHeaderValidator = mock(BlockHeaderValidator.class);
  private final BlockBodyValidator blockBodyValidator = mock(BlockBodyValidator.class);
  private final BlockAccessListValidator blockAccessListValidator =
      mock(BlockAccessListValidator.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
  private final GasCalculator gasCalculator = mock(GasCalculator.class);
  private final GasLimitCalculator gasLimitCalculator = mock(GasLimitCalculator.class);
  private final FeeMarket feeMarket = mock(FeeMarket.class);
  protected final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();

  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      new WorldStateStorageCoordinator(
          new BonsaiWorldStateKeyValueStorage(
              storageProvider,
              new NoOpMetricsSystem(),
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));

  private final WorldStateArchive worldStateArchive =
      // contains a BonsaiWorldState which we need to spy on.
      // do we need to also test with a DefaultWorldStateArchive?
      spy(InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain));

  private final BonsaiWorldState persisted =
      spy(
          new BonsaiWorldState(
              (BonsaiWorldStateProvider) worldStateArchive,
              (BonsaiWorldStateKeyValueStorage)
                  worldStateStorageCoordinator.worldStateKeyValueStorage(),
              EvmConfiguration.DEFAULT,
              createStatefulConfigWithTrie(),
              new CodeCache()));

  private final BadBlockManager badBlockManager = new BadBlockManager();

  private BlockValidator mainnetBlockValidator;

  @BeforeEach
  public void setup() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(new FrontierPreExecutionProcessor());
    when(protocolSpec.getGasCalculator()).thenReturn(gasCalculator);
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(protocolSpec.getFeeMarket()).thenReturn(feeMarket);
    when(blockAccessListValidator.validate(any(), any(), anyInt())).thenReturn(true);
    when(protocolSpec.getStateRootCommitterFactory())
        .thenReturn(new DefaultStateRootCommitterFactory());
    mainnetBlockValidator =
        MainnetBlockValidatorBuilder.frontier(
            blockHeaderValidator, blockBodyValidator, blockProcessor, blockAccessListValidator);
  }

  @Test
  void shouldNotBadBlockWhenInternalErrorDuringPersisting() {

    Mockito.doThrow(new StorageException("database problem")).when(persisted).persist(any(), any());
    Mockito.doReturn(persisted).when(worldStateArchive).getWorldState();
    Mockito.doReturn(Optional.of(persisted)).when(worldStateArchive).getWorldState(any());

    Block goodBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(0)
                    .hasTransactions(false)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenReturn(Optional.of(new BlockHeaderTestFixture().buildHeader()));
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class),
            any(BlockHeader.class),
            eq(protocolContext),
            eq(HeaderValidationMode.DETACHED_ONLY)))
        .thenReturn(true);

    when(blockBodyValidator.validateBody(
            eq(protocolContext),
            eq(goodBlock),
            any(),
            any(),
            eq(HeaderValidationMode.DETACHED_ONLY),
            any(),
            any()))
        .thenReturn(true);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    mainnetBlockValidator.validateAndProcessBlock(
        protocolContext,
        goodBlock,
        HeaderValidationMode.DETACHED_ONLY,
        HeaderValidationMode.DETACHED_ONLY);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }

  @Test
  void shouldNotBadBlockWhenInternalErrorOnBlockLookup() {

    Block goodBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(0)
                    .hasTransactions(false)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenThrow(new StorageException("database problem"));
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class),
            any(BlockHeader.class),
            eq(protocolContext),
            eq(HeaderValidationMode.DETACHED_ONLY)))
        .thenReturn(true);

    when(blockBodyValidator.validateBody(
            eq(protocolContext),
            eq(goodBlock),
            any(),
            any(),
            eq(HeaderValidationMode.DETACHED_ONLY),
            any(),
            any()))
        .thenReturn(true);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    mainnetBlockValidator.validateAndProcessBlock(
        protocolContext,
        goodBlock,
        HeaderValidationMode.DETACHED_ONLY,
        HeaderValidationMode.DETACHED_ONLY);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }

  @Test
  void shouldNotBadBlockWhenInternalErrorDuringValidateHeader() {

    Block goodBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(0)
                    .hasTransactions(false)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenReturn(Optional.of(new BlockHeaderTestFixture().buildHeader()));
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class),
            any(BlockHeader.class),
            eq(protocolContext),
            eq(HeaderValidationMode.DETACHED_ONLY)))
        .thenThrow(new StorageException("database problem"));

    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    mainnetBlockValidator.validateAndProcessBlock(
        protocolContext,
        goodBlock,
        HeaderValidationMode.DETACHED_ONLY,
        HeaderValidationMode.DETACHED_ONLY);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }

  @Test
  void shouldNotBadBlockWhenInternalErrorDuringValidateBody() {
    Mockito.doNothing().when(persisted).persist(any(), any());
    Mockito.doReturn(persisted).when(worldStateArchive).getWorldState();
    Mockito.doReturn(Optional.of(persisted)).when(worldStateArchive).getWorldState(any());

    Block goodBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(0)
                    .hasTransactions(false)
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenReturn(Optional.of(new BlockHeaderTestFixture().buildHeader()));
    when(blockHeaderValidator.validateHeader(
            any(BlockHeader.class),
            any(BlockHeader.class),
            eq(protocolContext),
            eq(HeaderValidationMode.DETACHED_ONLY)))
        .thenReturn(true);

    when(blockBodyValidator.validateBody(
            eq(protocolContext),
            eq(goodBlock),
            any(),
            any(),
            eq(HeaderValidationMode.DETACHED_ONLY),
            any(),
            any()))
        .thenThrow(new StorageException("database problem"));
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    mainnetBlockValidator.validateAndProcessBlock(
        protocolContext,
        goodBlock,
        HeaderValidationMode.DETACHED_ONLY,
        HeaderValidationMode.DETACHED_ONLY);
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
  }
}
