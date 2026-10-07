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
package org.hyperledger.besu.consensus.ibft.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithBftExtraDataEncoder;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BaseBftProtocolScheduleBuilder;
import org.hyperledger.besu.consensus.common.bft.BftBlockHashing;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftBlockCreator;
import org.hyperledger.besu.consensus.ibft.IbftBlockHeaderValidationRulesetFactory;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.AbstractBlockCreator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BftBlockCreatorTest {
  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();

  @Test
  public void createdBlockPassesValidationRulesAndHasAppropriateHashAndMixHash() {
    // Construct a parent block.
    final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();
    blockHeaderBuilder.gasLimit(5000); // required to pass validation rule checks.
    final BlockHeader parentHeader = blockHeaderBuilder.buildHeader();
    final Optional<BlockHeader> optionalHeader = Optional.of(parentHeader);

    // Construct a blockchain and world state
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(blockchain.getBlockHeader(any())).thenReturn(optionalHeader);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    final List<Address> initialValidatorList = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      initialValidatorList.add(AddressHelpers.ofValue(i));
    }

    final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();

    final BaseBftProtocolScheduleBuilder bftProtocolSchedule =
        new BaseBftProtocolScheduleBuilder() {
          @Override
          public BlockHeaderValidator.Builder createBlockHeaderRuleset(
              final BftConfigOptions config, final FeeMarket feeMarket) {
            return IbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), Optional.empty());
          }
        };
    final GenesisConfigOptions configOptions =
        GenesisConfig.fromConfig("{\"config\": {\"spuriousDragonBlock\":0}}").getConfigOptions();
    final ForksSchedule<BftConfigOptions> forksSchedule =
        new ForksSchedule<>(List.of(new ForkSpec<>(0, configOptions.getBftConfigOptions())));
    final ProtocolSchedule protocolSchedule =
        bftProtocolSchedule.createProtocolSchedule(
            configOptions,
            forksSchedule,
            false,
            bftExtraDataEncoder,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final ProtocolContext protContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(createInMemoryWorldStateArchive())
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(initialValidatorList, bftExtraDataEncoder))
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(1).build();

    final GasPricePendingTransactionsSorter pendingTransactions =
        new GasPricePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            protContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    transactionPool.setEnabled();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(
                        bftExtraDataEncoder.encode(
                            new BftExtraData(
                                Bytes.wrap(new byte[32]),
                                Collections.emptyList(),
                                Optional.empty(),
                                0,
                                initialValidatorList)))
                    .minTransactionGasPrice(Wei.ZERO)
                    .coinbase(AddressHelpers.ofValue(1))
                    .build())
            .build();

    final BftBlockCreator blockCreator =
        new BftBlockCreator(
            miningConfiguration,
            forksSchedule,
            initialValidatorList.get(0),
            parent ->
                bftExtraDataEncoder.encode(
                    new BftExtraData(
                        Bytes.wrap(new byte[32]),
                        Collections.emptyList(),
                        Optional.empty(),
                        0,
                        initialValidatorList)),
            transactionPool,
            protContext,
            protocolSchedule,
            bftExtraDataEncoder,
            new DeterministicEthScheduler());

    final int secondsBetweenBlocks = 1;
    final Block block =
        blockCreator.createBlock(parentHeader.getTimestamp() + 1, parentHeader).getBlock();

    final BlockHeaderValidator rules =
        IbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(secondsBetweenBlocks), Optional.empty())
            .build();

    // NOTE: The header will not contain commit seals, so can only do light validation on header.
    final boolean validationResult =
        rules.validateHeader(
            block.getHeader(), parentHeader, protContext, HeaderValidationMode.LIGHT);

    assertThat(validationResult).isTrue();

    final BlockHeader header = block.getHeader();
    final BftExtraData extraData = bftExtraDataEncoder.decode(header);
    assertThat(block.getHash())
        .isEqualTo(
            new BftBlockHashing(bftExtraDataEncoder)
                .calculateDataHashForCommittedSeal(header, extraData));
    assertThat(header.getMixHash()).isEqualTo(BftHelpers.EXPECTED_MIX_HASH);
  }

  @Test
  public void createdBlockCheckNonZeroCustomMiningBeneficiaryReturned() {
    // Construct a parent block.
    final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();
    blockHeaderBuilder.gasLimit(5000); // required to pass validation rule checks.
    final BlockHeader parentHeader = blockHeaderBuilder.buildHeader();
    final Optional<BlockHeader> optionalHeader = Optional.of(parentHeader);

    // Construct a blockchain and world state
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(blockchain.getBlockHeader(any())).thenReturn(optionalHeader);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    final List<Address> initialValidatorList = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      initialValidatorList.add(AddressHelpers.ofValue(i));
    }

    final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();

    final BaseBftProtocolScheduleBuilder bftProtocolSchedule =
        new BaseBftProtocolScheduleBuilder() {
          @Override
          public BlockHeaderValidator.Builder createBlockHeaderRuleset(
              final BftConfigOptions config, final FeeMarket feeMarket) {
            return IbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), Optional.empty());
          }
        };

    // Custom (non-zero) mining beneficiary should be returned when beneficiary is calculated
    final GenesisConfigOptions configOptions =
        GenesisConfig.fromConfig(
                "{\"config\": {\"berlinBlock\":0, \"qbft\": {\"blockperiodseconds\": 2, \"miningbeneficiary\":\"0x000000000000000000000000000000000000abcd\"}}}")
            .getConfigOptions();

    final ForksSchedule<BftConfigOptions> forksSchedule =
        new ForksSchedule<>(List.of(new ForkSpec<>(0, configOptions.getBftConfigOptions())));
    final ProtocolSchedule protocolSchedule =
        bftProtocolSchedule.createProtocolSchedule(
            configOptions,
            forksSchedule,
            false,
            bftExtraDataEncoder,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final ProtocolContext protContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(createInMemoryWorldStateArchive())
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(initialValidatorList, bftExtraDataEncoder))
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(1).build();

    final GasPricePendingTransactionsSorter pendingTransactions =
        new GasPricePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            protContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    transactionPool.setEnabled();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(
                        bftExtraDataEncoder.encode(
                            new BftExtraData(
                                Bytes.wrap(new byte[32]),
                                Collections.emptyList(),
                                Optional.empty(),
                                0,
                                initialValidatorList)))
                    .minTransactionGasPrice(Wei.ZERO)
                    .coinbase(AddressHelpers.ofValue(1))
                    .build())
            .build();

    final BftBlockCreator blockCreator =
        new BftBlockCreator(
            miningConfiguration,
            forksSchedule,
            initialValidatorList.get(3),
            parent ->
                bftExtraDataEncoder.encode(
                    new BftExtraData(
                        Bytes.wrap(new byte[32]),
                        Collections.emptyList(),
                        Optional.empty(),
                        0,
                        initialValidatorList)),
            transactionPool,
            protContext,
            protocolSchedule,
            bftExtraDataEncoder,
            new DeterministicEthScheduler());

    final Block block =
        blockCreator.createBlock(parentHeader.getTimestamp() + 1, parentHeader).getBlock();

    // Pass in non-zero coinbase, check we get zero address back based on bft config
    AbstractBlockCreator.MiningBeneficiaryCalculator calculatedBeneficiary =
        BftBlockCreator.miningBeneficiaryCalculator(protocolSchedule);
    // Our custom mining beneficiary should be returned when beneficiary is calculated
    assertThat(calculatedBeneficiary.getMiningBeneficiary(0, block.getHeader()))
        .isEqualTo(Address.fromHexString("0x00000000000000000000000000000000000abcd"));
  }

  @Test
  public void createdBlockCheckZeroAddressCustomMiningBeneficiaryReturned() {
    // Construct a parent block.
    final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();
    blockHeaderBuilder.gasLimit(5000); // required to pass validation rule checks.
    final BlockHeader parentHeader = blockHeaderBuilder.buildHeader();
    final Optional<BlockHeader> optionalHeader = Optional.of(parentHeader);

    // Construct a blockchain and world state
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(blockchain.getBlockHeader(any())).thenReturn(optionalHeader);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    final List<Address> initialValidatorList = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      initialValidatorList.add(AddressHelpers.ofValue(i));
    }

    final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();

    final BaseBftProtocolScheduleBuilder bftProtocolSchedule =
        new BaseBftProtocolScheduleBuilder() {
          @Override
          public BlockHeaderValidator.Builder createBlockHeaderRuleset(
              final BftConfigOptions config, final FeeMarket feeMarket) {
            return IbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), Optional.empty());
          }
        };

    // Custom (but zero) mining beneficiary should be returned when beneficiary is calculated
    final GenesisConfigOptions configOptions =
        GenesisConfig.fromConfig(
                "{\"config\": {\"berlinBlock\":0, \"qbft\": {\"blockperiodseconds\": 2, \"miningbeneficiary\":\"0x0000000000000000000000000000000000000000\"}}}")
            .getConfigOptions();

    final ForksSchedule<BftConfigOptions> forksSchedule =
        new ForksSchedule<>(List.of(new ForkSpec<>(0, configOptions.getBftConfigOptions())));
    final ProtocolSchedule protocolSchedule =
        bftProtocolSchedule.createProtocolSchedule(
            configOptions,
            forksSchedule,
            false,
            bftExtraDataEncoder,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final ProtocolContext protContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(createInMemoryWorldStateArchive())
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(initialValidatorList, bftExtraDataEncoder))
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(1).build();

    final GasPricePendingTransactionsSorter pendingTransactions =
        new GasPricePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            protContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    transactionPool.setEnabled();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(
                        bftExtraDataEncoder.encode(
                            new BftExtraData(
                                Bytes.wrap(new byte[32]),
                                Collections.emptyList(),
                                Optional.empty(),
                                0,
                                initialValidatorList)))
                    .minTransactionGasPrice(Wei.ZERO)
                    .coinbase(AddressHelpers.ofValue(1))
                    .build())
            .build();

    final BftBlockCreator blockCreator =
        new BftBlockCreator(
            miningConfiguration,
            forksSchedule,
            initialValidatorList.get(3),
            parent ->
                bftExtraDataEncoder.encode(
                    new BftExtraData(
                        Bytes.wrap(new byte[32]),
                        Collections.emptyList(),
                        Optional.empty(),
                        0,
                        initialValidatorList)),
            transactionPool,
            protContext,
            protocolSchedule,
            bftExtraDataEncoder,
            new DeterministicEthScheduler());

    final Block block =
        blockCreator.createBlock(parentHeader.getTimestamp() + 1, parentHeader).getBlock();

    // Pass in non-zero coinbase, check we get zero address back based on bft config
    AbstractBlockCreator.MiningBeneficiaryCalculator calculatedBeneficiary =
        BftBlockCreator.miningBeneficiaryCalculator(protocolSchedule);

    // Our custom mining beneficiary should be returned when beneficiary is calculated
    assertThat(calculatedBeneficiary.getMiningBeneficiary(0, block.getHeader()))
        .isEqualTo(Address.fromHexString("0x000000000000000000000000000000000000000"));
  }

  @Test
  public void createdBlockCheckEmptyCustomMiningBeneficiaryReturnsHeaderCoinbase() {
    // Construct a parent block.
    final BlockHeaderTestFixture blockHeaderBuilder = new BlockHeaderTestFixture();
    blockHeaderBuilder.gasLimit(5000); // required to pass validation rule checks.
    final BlockHeader parentHeader = blockHeaderBuilder.buildHeader();
    final Optional<BlockHeader> optionalHeader = Optional.of(parentHeader);

    // Construct a blockchain and world state
    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getChainHeadHash()).thenReturn(parentHeader.getHash());
    when(blockchain.getBlockHeader(any())).thenReturn(optionalHeader);
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    final List<Address> initialValidatorList = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      initialValidatorList.add(AddressHelpers.ofValue(i));
    }

    final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();

    final BaseBftProtocolScheduleBuilder bftProtocolSchedule =
        new BaseBftProtocolScheduleBuilder() {
          @Override
          public BlockHeaderValidator.Builder createBlockHeaderRuleset(
              final BftConfigOptions config, final FeeMarket feeMarket) {
            return IbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
                Duration.ofSeconds(5), Optional.empty());
          }
        };

    // Empty mining beneficiary should result in block coinbase being returned
    final GenesisConfigOptions configOptions =
        GenesisConfig.fromConfig(
                "{\"config\": {\"berlinBlock\":0, \"qbft\": {\"blockperiodseconds\": 2, \"miningbeneficiary\":\"\"}}}")
            .getConfigOptions();

    final ForksSchedule<BftConfigOptions> forksSchedule =
        new ForksSchedule<>(List.of(new ForkSpec<>(0, configOptions.getBftConfigOptions())));
    final ProtocolSchedule protocolSchedule =
        bftProtocolSchedule.createProtocolSchedule(
            configOptions,
            forksSchedule,
            false,
            bftExtraDataEncoder,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final ProtocolContext protContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(createInMemoryWorldStateArchive())
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(initialValidatorList, bftExtraDataEncoder))
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(1).build();

    final GasPricePendingTransactionsSorter pendingTransactions =
        new GasPricePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            protContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    transactionPool.setEnabled();

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .extraData(
                        bftExtraDataEncoder.encode(
                            new BftExtraData(
                                Bytes.wrap(new byte[32]),
                                Collections.emptyList(),
                                Optional.empty(),
                                0,
                                initialValidatorList)))
                    .minTransactionGasPrice(Wei.ZERO)
                    .coinbase(AddressHelpers.ofValue(1))
                    .build())
            .build();

    final BftBlockCreator blockCreator =
        new BftBlockCreator(
            miningConfiguration,
            forksSchedule,
            initialValidatorList.get(3),
            parent ->
                bftExtraDataEncoder.encode(
                    new BftExtraData(
                        Bytes.wrap(new byte[32]),
                        Collections.emptyList(),
                        Optional.empty(),
                        0,
                        initialValidatorList)),
            transactionPool,
            protContext,
            protocolSchedule,
            bftExtraDataEncoder,
            new DeterministicEthScheduler());

    final Block block =
        blockCreator.createBlock(parentHeader.getTimestamp() + 1, parentHeader).getBlock();

    // Pass in non-zero coinbase, check we get zero address back based on bft config
    AbstractBlockCreator.MiningBeneficiaryCalculator calculatedBeneficiary =
        BftBlockCreator.miningBeneficiaryCalculator(protocolSchedule);

    // Coinbase should be returned when beneficiary is calculated, not the empty "" custom mining
    // beneficiary
    assertThat(calculatedBeneficiary.getMiningBeneficiary(0, block.getHeader()))
        .isEqualTo(initialValidatorList.get(3));
  }
}
