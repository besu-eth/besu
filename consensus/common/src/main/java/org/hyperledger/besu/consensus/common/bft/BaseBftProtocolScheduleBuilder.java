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
package org.hyperledger.besu.consensus.common.bft;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.MainnetBlockValidatorBuilder;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockImporter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Defines the protocol behaviours for a blockchain using a BFT consensus mechanism. */
public abstract class BaseBftProtocolScheduleBuilder {

  private static final BigInteger DEFAULT_CHAIN_ID = BigInteger.ONE;

  /** Default constructor. */
  protected BaseBftProtocolScheduleBuilder() {}

  /**
   * Create protocol schedule.
   *
   * @param config the config
   * @param forksSchedule the forks schedule
   * @param isRevertReasonEnabled the is revert reason enabled
   * @param bftExtraDataCodec the bft extra data codec
   * @param evmConfiguration the evm configuration
   * @param miningConfiguration the mining parameters
   * @param badBlockManager the cache to use to keep invalid blocks
   * @param isParallelTxProcessingEnabled indicates whether parallel transaction is enabled.
   * @param balConfiguration configuration related to block access lists.
   * @param metricsSystem metricsSystem A metricSystem instance to be able to expose metrics in the
   *     underlying calls
   * @return the protocol schedule
   */
  public BftProtocolSchedule createProtocolSchedule(
      final GenesisConfigOptions config,
      final ForksSchedule<? extends BftConfigOptions> forksSchedule,
      final boolean isRevertReasonEnabled,
      final BftExtraDataCodec bftExtraDataCodec,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    final Map<Long, Function<ProtocolSpecBuilder, ProtocolSpecBuilder>> specMap = new HashMap<>();

    forksSchedule
        .getForks()
        .forEach(
            forkSpec ->
                specMap.put(
                    forkSpec.getBlock(),
                    builder -> applyBftChanges(builder, forkSpec.getValue(), bftExtraDataCodec)));

    final ProtocolSpecAdapters specAdapters = new ProtocolSpecAdapters(specMap);

    final ProtocolSchedule protocolSchedule =
        new ProtocolScheduleBuilder(
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
    final BftProtocolSchedule bftSchedule =
        new BftProtocolSchedule((DefaultProtocolSchedule) protocolSchedule);

    // Once we have the schedule we can update the fork schedule with the type of each milestone
    forksSchedule.applyMilestoneTypes(bftSchedule);

    return bftSchedule;
  }

  /**
   * Create block header ruleset.
   *
   * @param config the config
   * @param feeMarket the fee market
   * @return the block header validator . builder
   */
  protected abstract BlockHeaderValidator.Builder createBlockHeaderRuleset(
      final BftConfigOptions config, final FeeMarket feeMarket);

  private ProtocolSpecBuilder applyBftChanges(
      final ProtocolSpecBuilder builder,
      final BftConfigOptions configOptions,
      final BftExtraDataCodec bftExtraDataCodec) {
    if (configOptions.getEpochLength() <= 0) {
      throw new IllegalArgumentException("Epoch length in config must be greater than zero");
    }
    if (configOptions.getBlockRewardWei().signum() < 0) {
      throw new IllegalArgumentException("Bft Block reward in config cannot be negative");
    }

    return builder
        .blockHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                createBlockHeaderRuleset(configOptions, feeMarket))
        .ommerHeaderValidatorBuilder(
            (feeMarket, gasCalculator, gasLimitCalculator) ->
                createBlockHeaderRuleset(configOptions, feeMarket))
        .blockBodyValidatorBuilder(MainnetBlockBodyValidator::new)
        .blockValidatorBuilder(MainnetBlockValidatorBuilder::frontier)
        .blockImporterBuilder(MainnetBlockImporter::new)
        .difficultyCalculator((time, parent) -> BigInteger.ONE)
        .skipZeroBlockRewards(true)
        .blockHeaderFunctions(BftBlockHeaderFunctions.forOnchainBlock(bftExtraDataCodec))
        .blockReward(Wei.of(configOptions.getBlockRewardWei()))
        .withdrawalsValidator(new WithdrawalsValidator.NotApplicableWithdrawals())
        .miningBeneficiaryCalculator(
            header -> configOptions.getMiningBeneficiary().orElseGet(header::getCoinbase));
  }
}
