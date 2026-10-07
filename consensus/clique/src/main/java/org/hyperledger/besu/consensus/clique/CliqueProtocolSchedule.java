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
package org.hyperledger.besu.consensus.clique;

import org.hyperledger.besu.config.CliqueConfigOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.MainnetBlockValidatorBuilder;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockImporter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Defines the protocol behaviours for a blockchain using Clique. */
public class CliqueProtocolSchedule {

  private static final BigInteger DEFAULT_CHAIN_ID = BigInteger.valueOf(4);

  /** Default constructor. */
  CliqueProtocolSchedule() {}

  /**
   * Create protocol schedule.
   *
   * @param config the config
   * @param forksSchedule the transitions
   * @param nodeKey the node key
   * @param isRevertReasonEnabled the is revert reason enabled
   * @param evmConfiguration the evm configuration
   * @param miningConfiguration the mining configuration
   * @param badBlockManager the cache to use to keep invalid blocks
   * @param isParallelTxProcessingEnabled indicates whether parallel transaction is enabled
   * @param balConfiguration configuration related to block-level access lists
   * @param metricsSystem A metricSystem instance to be able to expose metrics in the underlying
   *     calls
   * @return the protocol schedule
   */
  public static ProtocolSchedule create(
      final GenesisConfigOptions config,
      final ForksSchedule<CliqueConfigOptions> forksSchedule,
      final NodeKey nodeKey,
      final boolean isRevertReasonEnabled,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {

    final CliqueConfigOptions cliqueConfig = config.getCliqueConfigOptions();

    final Address localNodeAddress = Util.publicKeyToAddress(nodeKey.getPublicKey());

    if (cliqueConfig.getEpochLength() <= 0) {
      throw new IllegalArgumentException("Epoch length in config must be greater than zero");
    }

    final EpochManager epochManager = new EpochManager(cliqueConfig.getEpochLength());

    final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> specMap = new HashMap<>();
    forksSchedule
        .getForks()
        .forEach(
            forkSpec ->
                specMap.put(
                    forkSpec.getBlock(),
                    builder ->
                        applyCliqueSpecificModifications(
                            epochManager,
                            forkSpec.getValue().getBlockPeriodSeconds(),
                            forkSpec.getValue().getCreateEmptyBlocks(),
                            localNodeAddress,
                            builder)));
    final ProtocolSpecAdapters specAdapters = new ProtocolSpecAdapters(specMap);

    return new ProtocolScheduleBuilder(
            config,
            Optional.of(DEFAULT_CHAIN_ID),
            specAdapters,
            isRevertReasonEnabled,
            evmConfiguration,
            miningConfiguration,
            badBlockManager,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .createProtocolSchedule();
  }

  private static ProtocolSpecBuilder applyCliqueSpecificModifications(
      final EpochManager epochManager,
      final long secondsBetweenBlocks,
      final boolean createEmptyBlocks,
      final Address localNodeAddress,
      final ProtocolSpecBuilder specBuilder) {

    return specBuilder
        .blockHeaderValidatorBuilder(
            (baseFeeMarket, gasCalculator, gasLimitCalculator) ->
                getBlockHeaderValidator(
                    epochManager, secondsBetweenBlocks, createEmptyBlocks, baseFeeMarket))
        .ommerHeaderValidatorBuilder(
            (baseFeeMarket, gasCalculator, gasLimitCalculator) ->
                getBlockHeaderValidator(
                    epochManager, secondsBetweenBlocks, createEmptyBlocks, baseFeeMarket))
        .blockBodyValidatorBuilder(MainnetBlockBodyValidator::new)
        .blockValidatorBuilder(MainnetBlockValidatorBuilder::frontier)
        .blockImporterBuilder(MainnetBlockImporter::new)
        .difficultyCalculator(new CliqueDifficultyCalculator(localNodeAddress))
        .blockReward(Wei.ZERO)
        .miningBeneficiaryCalculator(CliqueHelpers::getProposerOfBlock)
        .blockHeaderFunctions(new CliqueBlockHeaderFunctions());
  }

  private static BlockHeaderValidator.Builder getBlockHeaderValidator(
      final EpochManager epochManager,
      final long secondsBetweenBlocks,
      final boolean createEmptyBlocks,
      final FeeMarket feeMarket) {
    Optional<BaseFeeMarket> baseFeeMarket =
        Optional.of(feeMarket).filter(FeeMarket::implementsBaseFee).map(BaseFeeMarket.class::cast);

    return BlockHeaderValidationRulesetFactory.cliqueBlockHeaderValidator(
        secondsBetweenBlocks, createEmptyBlocks, epochManager, baseFeeMarket);
  }
}
