/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.ARROW_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BERLIN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO1;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO2;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO3;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO4;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO5;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BYZANTIUM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CONSTANTINOPLE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.DAO_RECOVERY_INIT;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.DAO_RECOVERY_TRANSITION;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.EXPERIMENTAL_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FRONTIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FUTURE_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.GRAY_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.HOMESTEAD;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.ISTANBUL;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.MUIR_GLACIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PETERSBURG;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SPURIOUS_DRAGON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.TANGERINE_WHISTLE;
import static org.hyperledger.besu.ethereum.mainnet.requests.MainnetRequestsProcessor.pragueRequestsProcessors;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.PowAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.MainnetBlockValidatorBuilder;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.TransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.blockhash.CancunPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.blockhash.FrontierPreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PraguePreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.parallelization.MainnetParallelBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.requests.MainnetRequestsValidator;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestContractAddresses;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitterFactory;
import org.hyperledger.besu.ethereum.mainnet.transactionpool.OsakaTransactionPoolPreProcessor;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.contractvalidation.MaxCodeSizeRule;
import org.hyperledger.besu.evm.contractvalidation.PrefixCodeRule;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ByzantiumGasCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.HomesteadGasCalculator;
import org.hyperledger.besu.evm.gascalculator.IstanbulGasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ShanghaiGasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.TangerineWhistleGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
import org.hyperledger.besu.evm.processor.ContractCreationProcessor;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.worldstate.CodeDelegationService;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.common.io.Resources;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides the various {@link ProtocolSpec}s on mainnet hard forks. */
public abstract class MainnetProtocolSpecs {

  private static final Address RIPEMD160_PRECOMPILE =
      Address.fromHexString("0x0000000000000000000000000000000000000003");

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  // A consensus bug at Ethereum mainnet transaction 0xcf416c53
  // deleted an empty account even when the message execution scope
  // failed, but the transaction itself succeeded.
  private static final HashSet<Address> SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES;

  private static final Wei FRONTIER_BLOCK_REWARD = Wei.fromEth(5);

  private static final Wei BYZANTIUM_BLOCK_REWARD = Wei.fromEth(3);

  private static final Wei CONSTANTINOPLE_BLOCK_REWARD = Wei.fromEth(2);

  private static final Logger LOG = LoggerFactory.getLogger(MainnetProtocolSpecs.class);
  private static final int POW_SLOT_TIME_ESTIMATION = 13;

  static {
    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES = new HashSet<>();
    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES.add(RIPEMD160_PRECOMPILE);
  }

  private MainnetProtocolSpecs() {}

  public static ProtocolSpecBuilder frontierDefinition(
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return new ProtocolSpecBuilder()
        .gasCalculator(FrontierGasCalculator::new)
        .gasLimitCalculatorBuilder(
            (feeMarket, gasCalculator, blobSchedule) -> new FrontierTargetingGasLimitCalculator())
        .evmBuilder(MainnetEVMs::frontier)
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::frontier)
        .messageCallProcessorBuilder(MessageCallProcessor::new)
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm, false, Collections.singletonList(MaxCodeSizeRule.from(evm)), 0))
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(), gasLimitCalculator, false, Optional.empty()))
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidatorFactory,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidatorFactory)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(false)
                    .warmCoinbase(false)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(FeeMarket.legacy())
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.frontier())
                    .build())
        .difficultyCalculator(MainnetDifficultyCalculators.FRONTIER)
        .blockHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) -> MainnetBlockHeaderValidator.create())
        .ommerHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                MainnetBlockHeaderValidator.createLegacyFeeMarketOmmerValidator())
        .blockBodyValidatorBuilder(MainnetBlockBodyValidator::new)
        .blockAccessListValidatorBuilder(__ -> BlockAccessListValidator.ALWAYS_REJECT_BAL)
        .transactionReceiptFactory(new FrontierTransactionReceiptFactory())
        .blockReward(FRONTIER_BLOCK_REWARD)
        .skipZeroBlockRewards(false)
        .balConfiguration(balConfiguration)
        .blockProcessorBuilder(
            isParallelTxProcessingEnabled
                ? new MainnetParallelBlockProcessor.ParallelBlockProcessorBuilder(metricsSystem)
                : new MainnetBlockProcessor.MainnetBlockProcessorBuilder(metricsSystem))
        .blockValidatorBuilder(MainnetBlockValidatorBuilder::frontier)
        .blockImporterBuilder(MainnetBlockImporter::new)
        .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
        .miningBeneficiaryCalculator(BlockHeader::getCoinbase)
        .evmConfiguration(evmConfiguration)
        .preExecutionProcessor(new FrontierPreExecutionProcessor())
        .slotDuration(getSlotDurationFromGenesis(genesisConfigOptions))
        .hardforkId(FRONTIER);
  }

  private static Duration getSlotDurationFromGenesis(
      final GenesisConfigOptions genesisConfigOptions) {

    if (genesisConfigOptions.isIbft2()) {
      return Duration.ofSeconds(genesisConfigOptions.getBftConfigOptions().getBlockPeriodSeconds());
    }
    if (genesisConfigOptions.isQbft()) {
      return Duration.ofSeconds(
          genesisConfigOptions.getQbftConfigOptions().getBlockPeriodSeconds());
    }
    if (genesisConfigOptions.isClique()) {
      return Duration.ofSeconds(
          genesisConfigOptions.getCliqueConfigOptions().getBlockPeriodSeconds());
    }
    // if no hints are present in the genesis file, then we are in PoW and there is not predefined
    // block period, so just return an estimation.
    // We also get here if we are in PoS mode, but the right value for PoS slot duration will
    // override this value.
    return Duration.ofSeconds(POW_SLOT_TIME_ESTIMATION);
  }

  public static PoWHasher powHasher(final PowAlgorithm powAlgorithm) {
    if (powAlgorithm == null) {
      return PoWHasher.UNSUPPORTED;
    }
    return powAlgorithm == PowAlgorithm.ETHASH ? PoWHasher.ETHASH_LIGHT : PoWHasher.UNSUPPORTED;
  }

  public static ProtocolSpecBuilder homesteadDefinition(
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return frontierDefinition(
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(HomesteadGasCalculator::new)
        .evmBuilder(MainnetEVMs::homestead)
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm, true, Collections.singletonList(MaxCodeSizeRule.from(evm)), 0))
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(), gasLimitCalculator, true, Optional.empty()))
        .difficultyCalculator(MainnetDifficultyCalculators.HOMESTEAD)
        .hardforkId(HOMESTEAD);
  }

  public static ProtocolSpecBuilder daoRecoveryInitDefinition(
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return homesteadDefinition(
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .blockHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                MainnetBlockHeaderValidator.createDaoValidator())
        .blockProcessorBuilder(
            (transactionProcessor,
                transactionReceiptFactory,
                blockReward,
                miningBeneficiaryCalculator,
                skipZeroBlockRewards,
                protocolSchedule,
                balConfig) ->
                new DaoBlockProcessor(
                    isParallelTxProcessingEnabled
                        ? new MainnetParallelBlockProcessor(
                            transactionProcessor,
                            transactionReceiptFactory,
                            blockReward,
                            miningBeneficiaryCalculator,
                            skipZeroBlockRewards,
                            protocolSchedule,
                            balConfig,
                            metricsSystem)
                        : new MainnetBlockProcessor(
                            transactionProcessor,
                            transactionReceiptFactory,
                            blockReward,
                            miningBeneficiaryCalculator,
                            skipZeroBlockRewards,
                            protocolSchedule,
                            balConfig,
                            metricsSystem)))
        .hardforkId(DAO_RECOVERY_INIT);
  }

  public static ProtocolSpecBuilder daoRecoveryTransitionDefinition(
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return daoRecoveryInitDefinition(
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .blockProcessorBuilder(
            isParallelTxProcessingEnabled
                ? new MainnetParallelBlockProcessor.ParallelBlockProcessorBuilder(metricsSystem)
                : new MainnetBlockProcessor.MainnetBlockProcessorBuilder(metricsSystem))
        .hardforkId(DAO_RECOVERY_TRANSITION);
  }

  public static ProtocolSpecBuilder tangerineWhistleDefinition(
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return homesteadDefinition(
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(TangerineWhistleGasCalculator::new)
        .hardforkId(TANGERINE_WHISTLE);
  }

  public static ProtocolSpecBuilder spuriousDragonDefinition(
      final Optional<BigInteger> chainId,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return tangerineWhistleDefinition(
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .isReplayProtectionSupported(true)
        .gasCalculator(SpuriousDragonGasCalculator::new)
        .skipZeroBlockRewards(true)
        .messageCallProcessorBuilder(
            (evm, precompileContractRegistry) ->
                new MessageCallProcessor(
                    evm,
                    precompileContractRegistry,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES))
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm,
                    true,
                    Collections.singletonList(MaxCodeSizeRule.from(evm)),
                    1,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES))
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(), gasLimitCalculator, true, chainId))
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidator,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidator)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(true)
                    .warmCoinbase(false)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(feeMarket)
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.frontier())
                    .build())
        .hardforkId(SPURIOUS_DRAGON);
  }

  public static ProtocolSpecBuilder byzantiumDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return spuriousDragonDefinition(
            chainId,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(ByzantiumGasCalculator::new)
        .evmBuilder(MainnetEVMs::byzantium)
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::byzantium)
        .difficultyCalculator(MainnetDifficultyCalculators.BYZANTIUM)
        .transactionReceiptFactory(new ByzantiumTransactionReceiptFactory(enableRevertReason))
        .blockReward(BYZANTIUM_BLOCK_REWARD)
        .hardforkId(BYZANTIUM);
  }

  public static ProtocolSpecBuilder constantinopleDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return byzantiumDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .difficultyCalculator(MainnetDifficultyCalculators.CONSTANTINOPLE)
        .gasCalculator(ConstantinopleGasCalculator::new)
        .evmBuilder(MainnetEVMs::constantinople)
        .blockReward(CONSTANTINOPLE_BLOCK_REWARD)
        .hardforkId(CONSTANTINOPLE);
  }

  public static ProtocolSpecBuilder petersburgDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return constantinopleDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(PetersburgGasCalculator::new)
        .hardforkId(PETERSBURG);
  }

  public static ProtocolSpecBuilder istanbulDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return petersburgDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(IstanbulGasCalculator::new)
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.istanbul(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::istanbul)
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm,
                    true,
                    Collections.singletonList(MaxCodeSizeRule.from(evm)),
                    1,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES))
        .hardforkId(ISTANBUL);
  }

  static ProtocolSpecBuilder muirGlacierDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return istanbulDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .difficultyCalculator(MainnetDifficultyCalculators.MUIR_GLACIER)
        .hardforkId(MUIR_GLACIER);
  }

  static ProtocolSpecBuilder berlinDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return muirGlacierDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(BerlinGasCalculator::new)
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(),
                    gasLimitCalculator,
                    true,
                    chainId,
                    Set.of(TransactionType.FRONTIER, TransactionType.ACCESS_LIST)))
        .transactionReceiptFactory(new BerlinTransactionReceiptFactory(enableRevertReason))
        .hardforkId(BERLIN);
  }

  static ProtocolSpecBuilder londonDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    final long londonForkBlockNumber =
        genesisConfigOptions.getLondonBlockNumber().orElse(Long.MAX_VALUE);
    return berlinDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .feeMarketBuilder(
            createFeeMarket(
                londonForkBlockNumber,
                genesisConfigOptions.isZeroBaseFee(),
                genesisConfigOptions.isFixedBaseFee(),
                false,
                miningConfiguration.getMinTransactionGasPrice(),
                (blobSchedule) ->
                    FeeMarket.london(
                        londonForkBlockNumber, genesisConfigOptions.getBaseFeePerGas())))
        .gasCalculator(LondonGasCalculator::new)
        .gasLimitCalculatorBuilder(
            (feeMarket, gasCalculator, blobSchedule) ->
                new LondonTargetingGasLimitCalculator(
                    londonForkBlockNumber, (BaseFeeMarket) feeMarket))
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(),
                    gasLimitCalculator,
                    feeMarket,
                    true,
                    chainId,
                    Set.of(
                        TransactionType.FRONTIER,
                        TransactionType.ACCESS_LIST,
                        TransactionType.EIP1559),
                    Integer.MAX_VALUE))
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidatorFactory,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidatorFactory)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(true)
                    .warmCoinbase(false)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(feeMarket)
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
                    .build())
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm,
                    true,
                    List.of(MaxCodeSizeRule.from(evm), PrefixCodeRule.of()),
                    1,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES))
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.london(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        .difficultyCalculator(MainnetDifficultyCalculators.LONDON)
        .blockHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                MainnetBlockHeaderValidator.createBaseFeeMarketValidator((BaseFeeMarket) feeMarket))
        .ommerHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                MainnetBlockHeaderValidator.createBaseFeeMarketOmmerValidator(
                    (BaseFeeMarket) feeMarket))
        .blockBodyValidatorBuilder(BaseFeeBlockBodyValidator::new)
        .hardforkId(LONDON);
  }

  static ProtocolSpecBuilder arrowGlacierDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return londonDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .difficultyCalculator(MainnetDifficultyCalculators.ARROW_GLACIER)
        .hardforkId(ARROW_GLACIER);
  }

  static ProtocolSpecBuilder grayGlacierDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return arrowGlacierDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .difficultyCalculator(MainnetDifficultyCalculators.GRAY_GLACIER)
        .hardforkId(GRAY_GLACIER);
  }

  static ProtocolSpecBuilder parisDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {

    return grayGlacierDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.paris(gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        .difficultyCalculator(MainnetDifficultyCalculators.PROOF_OF_STAKE_DIFFICULTY)
        .blockHeaderValidatorBuilder(MainnetBlockHeaderValidator::mergeBlockHeaderValidator)
        .blockReward(Wei.ZERO)
        .skipZeroBlockRewards(true)
        .isPoS(true)
        .slotDuration(Duration.ofSeconds(miningConfiguration.getUnstable().getPosSlotDuration()))
        .hardforkId(PARIS);
  }

  static ProtocolSpecBuilder shanghaiDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return parisDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        // gas calculator has new code to support EIP-3860 limit and meter initcode
        .gasCalculator(ShanghaiGasCalculator::new)
        // EVM has a new operation for EIP-3855 PUSH0 instruction
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.shanghai(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        // we need to flip the Warm Coinbase flag for EIP-3651 warm coinbase
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidatorFactory,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidatorFactory)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(true)
                    .warmCoinbase(true)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(feeMarket)
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
                    .build())
        // Contract creation rules for EIP-3860 Limit and meter intitcode
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(),
                    gasLimitCalculator,
                    feeMarket,
                    true,
                    chainId,
                    Set.of(
                        TransactionType.FRONTIER,
                        TransactionType.ACCESS_LIST,
                        TransactionType.EIP1559),
                    evm.getMaxInitcodeSize()))
        .withdrawalsProcessor(new WithdrawalsProcessor())
        .withdrawalsValidator(new WithdrawalsValidator.AllowedWithdrawals())
        .blockHeaderValidatorBuilder(MainnetBlockHeaderValidator::noBlobBlockHeaderValidator)
        .hardforkId(SHANGHAI);
  }

  static ProtocolSpecBuilder cancunDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    final long londonForkBlockNumber = genesisConfigOptions.getLondonBlockNumber().orElse(0L);

    return shanghaiDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .feeMarketBuilder(
            createFeeMarket(
                londonForkBlockNumber,
                genesisConfigOptions.isZeroBaseFee(),
                genesisConfigOptions.isFixedBaseFee(),
                true,
                miningConfiguration.getMinTransactionGasPrice(),
                (blobSchedule) ->
                    FeeMarket.cancun(
                        londonForkBlockNumber,
                        genesisConfigOptions.getBaseFeePerGas(),
                        blobSchedule)))
        .blobSchedule(
            genesisConfigOptions
                .getBlobScheduleOptions()
                .flatMap(BlobScheduleOptions::getCancun)
                .orElse(BlobSchedule.CANCUN_DEFAULT))
        // gas calculator for EIP-4844 blob gas
        .gasCalculator(CancunGasCalculator::new)
        // gas limit with EIP-4844 max blob gas per block
        .gasLimitCalculatorBuilder(
            (feeMarket, gasCalculator, blobSchedule) ->
                new CancunTargetingGasLimitCalculator(
                    londonForkBlockNumber,
                    (BaseFeeMarket) feeMarket,
                    gasCalculator,
                    blobSchedule.getMax(),
                    blobSchedule.getTarget()))
        // EVM changes to support EIP-1153: TSTORE and EIP-5656: MCOPY
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.cancun(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        // use Cancun fee market
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidator,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidator)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(true)
                    .warmCoinbase(true)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(feeMarket)
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
                    .build())
        // change to check for max blob gas per block for EIP-4844
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(),
                    gasLimitCalculator,
                    feeMarket,
                    true,
                    chainId,
                    Set.of(
                        TransactionType.FRONTIER,
                        TransactionType.ACCESS_LIST,
                        TransactionType.EIP1559,
                        TransactionType.BLOB),
                    Set.of(BlobType.KZG_PROOF),
                    evm.getMaxInitcodeSize()))
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::cancun)
        .blockHeaderValidatorBuilder(MainnetBlockHeaderValidator::blobAwareBlockHeaderValidator)
        .preExecutionProcessor(getPreExecutionProcessor(genesisConfigOptions))
        .hardforkId(CANCUN);
  }

  private static PreExecutionProcessor getPreExecutionProcessor(
      final GenesisConfigOptions genesisConfigOptions) {
    if (isPoAConsensus(genesisConfigOptions) && !hasSystemContractAddresses(genesisConfigOptions)) {
      return new FrontierPreExecutionProcessor();
    }

    return new CancunPreExecutionProcessor();
  }

  private static PreExecutionProcessor getPraguePreExecutionProcessor(
      final GenesisConfigOptions genesisConfigOptions) {
    if (isPoAConsensus(genesisConfigOptions) && !hasSystemContractAddresses(genesisConfigOptions)) {
      return new FrontierPreExecutionProcessor();
    }

    return new PraguePreExecutionProcessor();
  }

  static ProtocolSpecBuilder pragueDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder pragueSpecBuilder =
        cancunDefinition(
                chainId,
                enableRevertReason,
                genesisConfigOptions,
                evmConfiguration,
                miningConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .blobSchedule(
                genesisConfigOptions
                    .getBlobScheduleOptions()
                    .flatMap(BlobScheduleOptions::getPrague)
                    .orElse(BlobSchedule.PRAGUE_DEFAULT))
            .gasCalculator(PragueGasCalculator::new)
            .evmBuilder(
                (gasCalculator, jdCacheConfig) ->
                    MainnetEVMs.prague(
                        gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))

            // EIP-2537 BLS12-381 precompiles
            .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::prague)

            // EIP-7002 Withdrawals / EIP-6610 Deposits / EIP-7685 Requests
            .requestsValidator(new MainnetRequestsValidator())

            // change to accept EIP-7702 transactions
            .transactionValidatorFactoryBuilder(
                (evm, gasLimitCalculator, feeMarket) ->
                    new TransactionValidatorFactory(
                        evm.getGasCalculator(),
                        gasLimitCalculator,
                        feeMarket,
                        true,
                        chainId,
                        Set.of(
                            TransactionType.FRONTIER,
                            TransactionType.ACCESS_LIST,
                            TransactionType.EIP1559,
                            TransactionType.BLOB,
                            TransactionType.DELEGATE_CODE),
                        Set.of(BlobType.KZG_PROOF),
                        evm.getMaxInitcodeSize()))
            // CodeDelegationProcessor
            .transactionProcessorBuilder(
                (gasCalculator,
                    feeMarket,
                    transactionValidator,
                    contractCreationProcessor,
                    messageCallProcessor) ->
                    MainnetTransactionProcessor.builder()
                        .gasCalculator(gasCalculator)
                        .transactionValidatorFactory(transactionValidator)
                        .contractCreationProcessor(contractCreationProcessor)
                        .messageCallProcessor(messageCallProcessor)
                        .clearEmptyAccounts(true)
                        .warmCoinbase(true)
                        .maxStackSize(evmConfiguration.evmStackSize())
                        .feeMarket(feeMarket)
                        .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
                        .codeDelegationProcessor(
                            new CodeDelegationProcessor(
                                chainId,
                                SIGNATURE_ALGORITHM.getHalfCurveOrder(),
                                new CodeDelegationService()))
                        .build())
            // EIP-2935 Blockhash processor
            .preExecutionProcessor(getPraguePreExecutionProcessor(genesisConfigOptions))
            .hardforkId(PRAGUE);
    if (isPoAConsensus(genesisConfigOptions) && !hasSystemContractAddresses(genesisConfigOptions)) {
      LOG.warn(
          "Skipping system contract request processors for PoA consensus (clique/ibft/qbft) without system contract addresses.");
      pragueSpecBuilder.requestProcessorCoordinator(RequestProcessorCoordinator.noOp());
    } else {
      try {
        RequestContractAddresses requestContractAddresses =
            RequestContractAddresses.fromGenesis(genesisConfigOptions);

        pragueSpecBuilder.requestProcessorCoordinator(
            pragueRequestsProcessors(requestContractAddresses));
      } catch (NoSuchElementException nsee) {
        LOG.warn("Prague definitions require system contract addresses in genesis");
        throw nsee;
      }
    }

    return pragueSpecBuilder;
  }

  private static boolean isPoAConsensus(final GenesisConfigOptions genesisConfigOptions) {
    return genesisConfigOptions.isClique()
        || genesisConfigOptions.isIbft2()
        || genesisConfigOptions.isQbft();
  }

  private static boolean hasSystemContractAddresses(
      final GenesisConfigOptions genesisConfigOptions) {
    return genesisConfigOptions.getDepositContractAddress().isPresent()
        && genesisConfigOptions.getWithdrawalRequestContractAddress().isPresent()
        && genesisConfigOptions.getConsolidationRequestContractAddress().isPresent();
  }

  static ProtocolSpecBuilder osakaDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    final long londonForkBlockNumber = genesisConfigOptions.getLondonBlockNumber().orElse(0L);

    return pragueDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(OsakaGasCalculator::new)
        // tx gas limit cap EIP-7825
        .gasLimitCalculatorBuilder(
            (feeMarket, gasCalculator, blobSchedule) ->
                new OsakaTargetingGasLimitCalculator(
                    londonForkBlockNumber,
                    (BaseFeeMarket) feeMarket,
                    gasCalculator,
                    blobSchedule.getMax(),
                    blobSchedule.getTarget(),
                    miningConfiguration.getMaxBlobsPerTransaction(),
                    miningConfiguration.getMaxBlobsPerBlock()))
        .evmBuilder(
            (gasCalculator, __) ->
                MainnetEVMs.osaka(gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        .transactionValidatorFactoryBuilder(
            (evm, gasLimitCalculator, feeMarket) ->
                new TransactionValidatorFactory(
                    evm.getGasCalculator(),
                    gasLimitCalculator,
                    feeMarket,
                    true,
                    chainId,
                    Set.of(
                        TransactionType.FRONTIER,
                        TransactionType.ACCESS_LIST,
                        TransactionType.EIP1559,
                        TransactionType.BLOB,
                        TransactionType.DELEGATE_CODE),
                    Set.of(BlobType.KZG_CELL_PROOFS),
                    evm.getMaxInitcodeSize()))
        .transactionPoolPreProcessor(new OsakaTransactionPoolPreProcessor())
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::osaka)
        .blockValidatorBuilder(MainnetBlockValidatorBuilder::osaka)
        .hardforkId(OSAKA);
  }

  static ProtocolSpecBuilder bpo1Definition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder builder =
        osakaDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);
    return applyBlobSchedule(
        builder,
        genesisConfigOptions,
        BlobScheduleOptions::getBpo1,
        GenesisConfigOptions::getBpo1Time,
        BPO1);
  }

  static ProtocolSpecBuilder bpo2Definition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder builder =
        bpo1Definition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);
    return applyBlobSchedule(
        builder,
        genesisConfigOptions,
        BlobScheduleOptions::getBpo2,
        GenesisConfigOptions::getBpo2Time,
        BPO2);
  }

  static ProtocolSpecBuilder bpo3Definition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder builder =
        bpo2Definition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);
    return applyBlobSchedule(
        builder,
        genesisConfigOptions,
        BlobScheduleOptions::getBpo3,
        GenesisConfigOptions::getBpo3Time,
        BPO3);
  }

  static ProtocolSpecBuilder bpo4Definition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder builder =
        bpo3Definition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);
    return applyBlobSchedule(
        builder,
        genesisConfigOptions,
        BlobScheduleOptions::getBpo4,
        GenesisConfigOptions::getBpo4Time,
        BPO4);
  }

  static ProtocolSpecBuilder bpo5Definition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    ProtocolSpecBuilder builder =
        bpo4Definition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);
    return applyBlobSchedule(
        builder,
        genesisConfigOptions,
        BlobScheduleOptions::getBpo5,
        GenesisConfigOptions::getBpo5Time,
        BPO5);
  }

  static ProtocolSpecBuilder amsterdamDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return bpo5Definition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .gasCalculator(AmsterdamGasCalculator::new)
        // EIP-7708: Override evmBuilder to use Amsterdam EVM with transfer logging
        .evmBuilder(
            (gasCalculator, __) ->
                MainnetEVMs.amsterdam(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        // EIP-7708: ContractCreationProcessor with transfer log emission enabled
        .contractCreationProcessorBuilder(
            evm ->
                new ContractCreationProcessor(
                    evm,
                    true,
                    List.of(MaxCodeSizeRule.from(evm), PrefixCodeRule.of()),
                    1,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES,
                    EIP7708TransferLogEmitter.INSTANCE))
        // EIP-7708: MessageCallProcessor with transfer log emission enabled
        .messageCallProcessorBuilder(
            (evm, precompileContractRegistry) ->
                new MessageCallProcessor(
                    evm,
                    precompileContractRegistry,
                    SPURIOUS_DRAGON_FORCE_DELETE_WHEN_EMPTY_ADDRESSES,
                    EIP7708TransferLogEmitter.INSTANCE))
        // EIP-7708: TransactionProcessor configured for Amsterdam with transfer log emission
        .transactionProcessorBuilder(
            (gasCalculator,
                feeMarket,
                transactionValidator,
                contractCreationProcessor,
                messageCallProcessor) ->
                MainnetTransactionProcessor.builder()
                    .gasCalculator(gasCalculator)
                    .transactionValidatorFactory(transactionValidator)
                    .contractCreationProcessor(contractCreationProcessor)
                    .messageCallProcessor(messageCallProcessor)
                    .clearEmptyAccounts(true)
                    .warmCoinbase(true)
                    .maxStackSize(evmConfiguration.evmStackSize())
                    .feeMarket(feeMarket)
                    .coinbaseFeePriceCalculator(CoinbaseFeePriceCalculator.eip1559())
                    .codeDelegationProcessor(
                        new CodeDelegationProcessor(
                            chainId,
                            SIGNATURE_ALGORITHM.getHalfCurveOrder(),
                            new CodeDelegationService()))
                    .transferLogEmitter(EIP7708TransferLogEmitter.INSTANCE)
                    .build())
        .blockAccessListFactory(new BlockAccessListFactory())
        .blockAccessListValidatorBuilder(MainnetBlockAccessListValidator::create)
        .stateRootCommitterFactory(new BalStateRootCommitterFactory(balConfiguration))
        // EIP-8037: Disable validation-time TX_MAX_GAS_LIMIT cap (enforced at runtime on regular
        // gas)
        .gasLimitCalculatorBuilder(
            (feeMarket, gasCalculator, blobSchedule) -> {
              final long londonForkBlock = genesisConfigOptions.getLondonBlockNumber().orElse(0L);
              return new AmsterdamTargetingGasLimitCalculator(
                  londonForkBlock,
                  (BaseFeeMarket) feeMarket,
                  gasCalculator,
                  blobSchedule.getMax(),
                  blobSchedule.getTarget(),
                  miningConfiguration.getMaxBlobsPerTransaction(),
                  miningConfiguration.getMaxBlobsPerBlock());
            })
        // EIP-8037: Amsterdam gas calculator with state gas cost support
        .gasCalculator(AmsterdamGasCalculator::new)
        // Amsterdam (EIP-7778 + EIP-8037): Pre-refund 2D gas accounting
        .blockGasAccountingStrategy(BlockGasAccountingStrategy.AMSTERDAM)
        // Amsterdam: Validator uses pre-refund gas_metered = max(regular, state) from processing
        .blockGasUsedValidator(BlockGasUsedValidator.AMSTERDAM)
        .hardforkId(AMSTERDAM);
  }

  private static ProtocolSpecBuilder applyBlobSchedule(
      final ProtocolSpecBuilder builder,
      final GenesisConfigOptions genesisConfigOptions,
      final Function<BlobScheduleOptions, Optional<BlobSchedule>> blobGetter,
      final Function<GenesisConfigOptions, OptionalLong> blobScheduleTimestampGetter,
      final HardforkId hardforkId) {
    // Only apply a fork's blob schedule if the fork is actually activated (has a timestamp).
    // This prevents inactive BPO forks from overriding the blob schedule with stale values
    // from the genesis config.
    if (blobScheduleTimestampGetter.apply(genesisConfigOptions).isPresent()) {
      genesisConfigOptions
          .getBlobScheduleOptions()
          .flatMap(blobGetter)
          .ifPresent(builder::blobSchedule);
    }
    return builder.hardforkId(hardforkId);
  }

  static ProtocolSpecBuilder futureEipsDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    return amsterdamDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .precompileContractRegistryBuilder(MainnetPrecompiledContractRegistries::futureEips)
        .hardforkId(FUTURE_EIPS);
  }

  static ProtocolSpecBuilder experimentalEipsDefinition(
      final Optional<BigInteger> chainId,
      final boolean enableRevertReason,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {

    return futureEipsDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            miningConfiguration,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .evmBuilder(
            (gasCalculator, jdCacheConfig) ->
                MainnetEVMs.experimentalEips(
                    gasCalculator, chainId.orElse(BigInteger.ZERO), evmConfiguration))
        .hardforkId(EXPERIMENTAL_EIPS);
  }

  private static class FrontierTransactionReceiptFactory implements TransactionReceiptFactory {

    @Override
    public TransactionReceipt create(
        final TransactionType transactionType,
        final TransactionProcessingResult result,
        final WorldState worldState,
        final long gasUsed) {
      return new TransactionReceipt(
          worldState.frontierRootHash(),
          gasUsed,
          result.getLogs(),
          Optional.empty()); // No revert reason in Frontier
    }

    @Override
    public TransactionReceipt create(
        final TransactionType transactionType,
        final TransactionProcessingResult result,
        final long gasUsed) {
      throw new UnsupportedOperationException("No stateless transaction receipt in Frontier");
    }
  }

  private abstract static class PostFrontierTransactionReceiptFactory
      implements TransactionReceiptFactory {
    protected final boolean revertReasonEnabled;

    public PostFrontierTransactionReceiptFactory(final boolean revertReasonEnabled) {
      this.revertReasonEnabled = revertReasonEnabled;
    }

    @Override
    public TransactionReceipt create(
        final TransactionType transactionType,
        final TransactionProcessingResult result,
        final WorldState worldState,
        final long gasUsed) {
      return create(transactionType, result, gasUsed);
    }
  }

  static class ByzantiumTransactionReceiptFactory extends PostFrontierTransactionReceiptFactory {
    public ByzantiumTransactionReceiptFactory(final boolean revertReasonEnabled) {
      super(revertReasonEnabled);
    }

    @Override
    public TransactionReceipt create(
        final TransactionType transactionType,
        final TransactionProcessingResult result,
        final long gasUsed) {
      return new TransactionReceipt(
          result.isSuccessful() ? 1 : 0,
          gasUsed,
          result.getLogs(),
          revertReasonEnabled ? result.getRevertReason() : Optional.empty());
    }
  }

  static class BerlinTransactionReceiptFactory extends PostFrontierTransactionReceiptFactory {

    public BerlinTransactionReceiptFactory(final boolean revertReasonEnabled) {
      super(revertReasonEnabled);
    }

    @Override
    public TransactionReceipt create(
        final TransactionType transactionType,
        final TransactionProcessingResult result,
        final long gasUsed) {
      return new TransactionReceipt(
          transactionType,
          result.isSuccessful() ? 1 : 0,
          gasUsed,
          result.getLogs(),
          revertReasonEnabled ? result.getRevertReason() : Optional.empty());
    }
  }

  private record DaoBlockProcessor(BlockProcessor wrapped) implements BlockProcessor {

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block) {
      updateWorldStateForDao(worldState);
      return wrapped.processBlock(
          protocolContext,
          blockchain,
          worldState,
          block,
          new AbstractBlockProcessor.PreprocessingFunction.NoPreprocessing());
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList) {
      updateWorldStateForDao(worldState);
      return wrapped.processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final AbstractBlockProcessor.PreprocessingFunction preprocessingBlockFunction) {
      return processBlock(
          protocolContext,
          blockchain,
          worldState,
          block,
          Optional.empty(),
          preprocessingBlockFunction);
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList,
        final AbstractBlockProcessor.PreprocessingFunction preprocessingBlockFunction) {
      updateWorldStateForDao(worldState);
      return wrapped.processBlock(
          protocolContext,
          blockchain,
          worldState,
          block,
          blockAccessList,
          preprocessingBlockFunction);
    }

    private static final Address DAO_REFUND_CONTRACT_ADDRESS =
        Address.fromHexString("0xbf4ed7b27f1d666546e30d74d50d173d20bca754");

    private void updateWorldStateForDao(final MutableWorldState worldState) {
      try {
        final JsonArray json =
            new JsonArray(
                Resources.toString(
                    Objects.requireNonNull(this.getClass().getResource("/daoAddresses.json")),
                    StandardCharsets.UTF_8));
        final List<Address> addresses =
            IntStream.range(0, json.size())
                .mapToObj(json::getString)
                .map(Address::fromHexString)
                .toList();
        final WorldUpdater worldUpdater = worldState.updater();
        final MutableAccount daoRefundContract =
            worldUpdater.getOrCreate(DAO_REFUND_CONTRACT_ADDRESS);
        for (final Address address : addresses) {
          final MutableAccount account = worldUpdater.getOrCreate(address);
          final Wei balance = account.getBalance();
          account.decrementBalance(balance);
          daoRefundContract.incrementBalance(balance);
        }
        worldUpdater.commit();
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  static ProtocolSpecBuilder.FeeMarketBuilder createFeeMarket(
      final long londonForkBlockNumber,
      final boolean isZeroBaseFee,
      final boolean isFixedBaseFee,
      final boolean supportsBlobs,
      final Wei minTransactionGasPrice,
      final ProtocolSpecBuilder.FeeMarketBuilder feeMarketBuilder) {
    if (isZeroBaseFee) {
      var baseFeeMarket =
          supportsBlobs
              ? FeeMarket.zeroBlobFee(londonForkBlockNumber)
              : FeeMarket.zeroBaseFee(londonForkBlockNumber);
      return blobSchedule -> baseFeeMarket;
    }
    if (isFixedBaseFee) {
      return blobSchedule -> FeeMarket.fixedBaseFee(londonForkBlockNumber, minTransactionGasPrice);
    }
    return feeMarketBuilder;
  }
}
