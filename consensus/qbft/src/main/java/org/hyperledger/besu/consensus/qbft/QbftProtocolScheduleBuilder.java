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
package org.hyperledger.besu.consensus.qbft;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BaseBftProtocolScheduleBuilder;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.Optional;

/** Defines the protocol behaviours for a blockchain using a QBFT consensus mechanism. */
public class QbftProtocolScheduleBuilder extends BaseBftProtocolScheduleBuilder {

  /** Default constructor */
  QbftProtocolScheduleBuilder() {}

  /**
   * Create protocol schedule.
   *
   * @param config the config
   * @param qbftForksSchedule the qbft forks schedule
   * @param isRevertReasonEnabled the is revert reason enabled
   * @param bftExtraDataCodec the bft extra data codec
   * @param evmConfiguration the evm configuration
   * @param miningConfiguration The mining parameters
   * @param badBlockManager the cache to use to keep invalid blocks
   * @param isParallelTxProcessingEnabled indicates whether parallel transaction is enabled.
   * @param balConfiguration configuration related to block-level access lists
   * @param metricsSystem A metricSystem instance to be able to expose metrics in the underlying
   *     calls
   * @param blockGasLimit the genesis block gas limit, to compare against any configured per-tx gas
   *     limit
   * @return the protocol schedule
   */
  public static BftProtocolSchedule create(
      final GenesisConfigOptions config,
      final ForksSchedule<QbftConfigOptions> qbftForksSchedule,
      final boolean isRevertReasonEnabled,
      final BftExtraDataCodec bftExtraDataCodec,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final long blockGasLimit) {
    return new QbftProtocolScheduleBuilder()
        .createProtocolSchedule(
            config,
            qbftForksSchedule,
            isRevertReasonEnabled,
            bftExtraDataCodec,
            evmConfiguration,
            miningConfiguration,
            badBlockManager,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem,
            blockGasLimit);
  }

  /**
   * Create protocol schedule.
   *
   * @param config the config
   * @param qbftForksSchedule the qbft forks schedule
   * @param bftExtraDataCodec the bft extra data codec
   * @param evmConfiguration the evm configuration
   * @param miningConfiguration The mining parameters
   * @param badBlockManager the cache to use to keep invalid blocks
   * @param isParallelTxProcessingEnabled indicates whether parallel transaction is enabled.
   * @param balConfiguration configuration related to block-level access lists
   * @param metricsSystem A metricSystem instance to be able to expose metrics in the underlying
   *     calls
   * @param blockGasLimit the genesis block gas limit, to compare against any configured per-tx gas
   *     limit
   * @return the protocol schedule
   */
  public static BftProtocolSchedule create(
      final GenesisConfigOptions config,
      final ForksSchedule<QbftConfigOptions> qbftForksSchedule,
      final BftExtraDataCodec bftExtraDataCodec,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final long blockGasLimit) {
    return create(
        config,
        qbftForksSchedule,
        false,
        bftExtraDataCodec,
        evmConfiguration,
        miningConfiguration,
        badBlockManager,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem,
        blockGasLimit);
  }

  /**
   * Creates a protocol schedule with the specified block gas limit for validation.
   *
   * @param config the config
   * @param forksSchedule the forks schedule
   * @param isRevertReasonEnabled the is revert reason enabled
   * @param bftExtraDataCodec the bft extra data codec
   * @param evmConfiguration the evm configuration
   * @param miningConfiguration the mining parameters
   * @param badBlockManager the cache to use to keep invalid blocks
   * @param isParallelTxProcessingEnabled indicates whether parallel transaction is enabled.
   * @param balConfiguration configuration related to block-level access lists
   * @param metricsSystem A metricSystem instance to be able to expose metrics in the underlying
   *     calls
   * @param blockGasLimit the genesis block gas limit, to compare against any configured per-tx gas
   *     limit
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
      final MetricsSystem metricsSystem,
      final long blockGasLimit) {
    this.blockGasLimit = blockGasLimit;
    return super.createProtocolSchedule(
        config,
        forksSchedule,
        isRevertReasonEnabled,
        bftExtraDataCodec,
        evmConfiguration,
        miningConfiguration,
        badBlockManager,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  @Override
  protected BlockHeaderValidator.Builder createBlockHeaderRuleset(
      final BftConfigOptions config, final FeeMarket feeMarket) {
    checkArgument(
        config instanceof QbftConfigOptions,
        "QbftProtocolScheduleBuilder must use QbftConfigOptions");
    final QbftConfigOptions qbftConfigOptions = (QbftConfigOptions) config;
    final Optional<BaseFeeMarket> baseFeeMarket =
        Optional.of(feeMarket).filter(FeeMarket::implementsBaseFee).map(BaseFeeMarket.class::cast);

    return QbftBlockHeaderValidationRulesetFactory.blockHeaderValidator(
        qbftConfigOptions.getBlockPeriodMilliseconds() > 0
            ? Duration.ofMillis(qbftConfigOptions.getBlockPeriodMilliseconds())
            : Duration.ofSeconds(qbftConfigOptions.getBlockPeriodSeconds()),
        qbftConfigOptions.isValidatorContractMode(),
        qbftConfigOptions.isGoQuorumCompatibilityMode(),
        baseFeeMarket);
  }
}
