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
package org.hyperledger.besu.ethereum.components;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.ChainId;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;

/** Provides the protocol schedule for the network. */
@Module
public class ProtocolScheduleModule {

  /** Default constructor. */
  public ProtocolScheduleModule() {}

  /**
   * Provides the protocol schedule builder.
   *
   * @param config the genesis config options
   * @param protocolSpecAdapters the protocol spec adapters
   * @param isRevertReasonEnabled whether revert reason is enabled
   * @param evmConfiguration the EVM configuration
   * @param badBlockManager the bad block manager
   * @param isParallelTxProcessingEnabled whether parallel tx processing is enabled
   * @param balConfiguration configuration for block-level access lists
   * @param metricsSystem the metrics system
   * @param miningConfiguration the mining parameters
   * @return the protocol schedule builder
   */
  @Singleton
  @Provides
  public ProtocolScheduleBuilder provideProtocolScheduleBuilder(
      final GenesisConfigOptions config,
      final ProtocolSpecAdapters protocolSpecAdapters,
      final boolean isRevertReasonEnabled,
      final EvmConfiguration evmConfiguration,
      final BadBlockManager badBlockManager,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem,
      final MiningConfiguration miningConfiguration) {

    ProtocolScheduleBuilder builder =
        new ProtocolScheduleBuilder(
            config,
            config.getChainId(),
            protocolSpecAdapters,
            isRevertReasonEnabled,
            evmConfiguration,
            miningConfiguration,
            badBlockManager,
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem);

    return builder;
  }

  /**
   * Provides the protocol schedule.
   *
   * @param builder the protocol schedule builder
   * @param config the genesis config options
   * @return the protocol schedule
   */
  @Provides
  public ProtocolSchedule createProtocolSchedule(
      final ProtocolScheduleBuilder builder, final GenesisConfigOptions config) {
    final ChainId chainId =
        ChainId.fromOptional(config.getChainId().or(builder::getDefaultChainId));
    DefaultProtocolSchedule protocolSchedule = new DefaultProtocolSchedule(chainId);
    builder.initSchedule(protocolSchedule);
    return protocolSchedule;
  }
}
