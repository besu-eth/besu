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
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.blockNumber;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecModification;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;

import org.junit.jupiter.api.Test;

class TransitionBesuControllerBuilderTest {

  private static final String GENESIS =
      "{\"config\":{\"chainId\":1234,\"homesteadBlock\":0,\"terminalTotalDifficulty\":0}}";

  @Test
  void theCustomizationIsPropagatedToBothHalves() {
    final BesuControllerBuilder preMergeBuilder = mock(BesuControllerBuilder.class);
    final MergeBesuControllerBuilder mergeBuilder = mock(MergeBesuControllerBuilder.class);
    final TransitionBesuControllerBuilder transition =
        new TransitionBesuControllerBuilder(preMergeBuilder, mergeBuilder);
    final ProtocolScheduleCustomization customization = ProtocolScheduleCustomization.none();

    transition.protocolScheduleCustomization(customization);

    verify(preMergeBuilder).protocolScheduleCustomization(same(customization));
    verify(mergeBuilder).protocolScheduleCustomization(same(customization));
  }

  @Test
  void aCustomizationAppliesToTheSchedulesBothHalvesBuild() {
    final BesuControllerBuilder preMergeBuilder = configured(new MainnetBesuControllerBuilder());
    final MergeBesuControllerBuilder mergeBuilder =
        (MergeBesuControllerBuilder) configured(new MergeBesuControllerBuilder());
    final TransitionBesuControllerBuilder transition =
        new TransitionBesuControllerBuilder(preMergeBuilder, mergeBuilder);
    final ProtocolScheduleCustomization customization =
        new ProtocolScheduleCustomization(
            "example-chain",
            List.of(
                new ProtocolSpecModification(
                    blockNumber(1), builder -> builder.blockReward(Wei.of(42)))));

    transition.protocolScheduleCustomization(customization);

    assertThat(rewardAtBlockOne(preMergeBuilder.createProtocolSchedule())).isEqualTo(Wei.of(42));
    assertThat(rewardAtBlockOne(mergeBuilder.createProtocolSchedule())).isEqualTo(Wei.of(42));
  }

  @Test
  void aTransitionAppliesCustomizationsOnlyWhenBothHalvesDo() {
    final MergeBesuControllerBuilder mergeBuilder = new MergeBesuControllerBuilder();

    assertThat(
            new TransitionBesuControllerBuilder(new MainnetBesuControllerBuilder(), mergeBuilder)
                .supportsProtocolScheduleCustomization())
        .isTrue();
    assertThat(
            new TransitionBesuControllerBuilder(new CliqueBesuControllerBuilder(), mergeBuilder)
                .supportsProtocolScheduleCustomization())
        .isFalse();
  }

  private static Wei rewardAtBlockOne(final ProtocolSchedule schedule) {
    return schedule
        .getByBlockHeader(new BlockHeaderTestFixture().number(1L).buildHeader())
        .getBlockReward();
  }

  private static BesuControllerBuilder configured(final BesuControllerBuilder builder) {
    return builder
        .genesisConfig(GenesisConfig.fromConfig(GENESIS))
        .miningParameters(MiningConfiguration.MINING_DISABLED)
        .evmConfiguration(EvmConfiguration.DEFAULT)
        .metricsSystem(new NoOpMetricsSystem());
  }
}
