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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.blockNumber;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.timestamp;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecModification;

import java.util.List;

import org.junit.jupiter.api.Test;

class BesuControllerBuilderProtocolScheduleCustomizationTest {

  private static final String POW_GENESIS =
      "{\"config\":{\"chainId\":1234,\"homesteadBlock\":10,\"ethash\":{}}}";

  private static final String QBFT_GENESIS = "{\"config\":{\"chainId\":1234,\"qbft\":{}}}";

  private static final ProtocolScheduleCustomization CUSTOMIZATION =
      new ProtocolScheduleCustomization(
          "example-chain",
          List.of(
              new ProtocolSpecModification(blockNumber(11), builder -> builder),
              new ProtocolSpecModification(timestamp(1_000), builder -> builder)));

  @Test
  void contributedActivationsStayOutOfTheGenesisConfig() {
    final GenesisConfig genesisConfig = GenesisConfig.fromConfig(POW_GENESIS);

    final BesuControllerBuilder builder =
        new BesuController.Builder()
            .protocolScheduleCustomization(CUSTOMIZATION)
            .fromGenesisFile(genesisConfig, SyncMode.FULL);

    // the builder carries the customization, from which it builds the node's one fork-ID manager
    assertThat(builder.protocolScheduleCustomization).isSameAs(CUSTOMIZATION);
    // the config keeps describing the genesis file alone: schedule construction, and anyone else
    // holding the config, sees only the forks the genesis declares
    assertThat(builder.genesisConfigOptions.getForkBlockNumbers()).containsExactly(10L);
    assertThat(builder.genesisConfigOptions.getForkBlockTimestamps()).isEmpty();
  }

  @Test
  void anUnsupportedConsensusMechanismIsRefusedWhenTheBuilderIsSelected() {
    final GenesisConfig genesisConfig = GenesisConfig.fromConfig(QBFT_GENESIS);
    final BesuController.Builder controllerBuilder =
        new BesuController.Builder().protocolScheduleCustomization(CUSTOMIZATION);

    assertThatThrownBy(() -> controllerBuilder.fromGenesisFile(genesisConfig, SyncMode.FULL))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("example-chain")
        .hasMessageContaining("QbftBesuControllerBuilder");
  }

  @Test
  void aNullCustomizationIsRefusedWhereItIsSet() {
    assertThatThrownBy(() -> new BesuController.Builder().protocolScheduleCustomization(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("protocolScheduleCustomization");
  }

  @Test
  void anAbsentCustomizationLeavesTheForkScheduleAlone() {
    final BesuControllerBuilder builder =
        new BesuController.Builder()
            .fromGenesisFile(GenesisConfig.fromConfig(POW_GENESIS), SyncMode.FULL);

    assertThat(builder.genesisConfigOptions.getForkBlockNumbers()).containsExactly(10L);
    assertThat(builder.protocolScheduleCustomization.modifications()).isEmpty();
  }

  @Test
  void aBuilderThatDoesNotApplyCustomizationsRefusesToStart() {
    final BesuControllerBuilder builder =
        new CliqueBesuControllerBuilder()
            .genesisConfig(GenesisConfig.fromConfig(POW_GENESIS))
            .protocolScheduleCustomization(CUSTOMIZATION);

    assertThatThrownBy(builder::verifyProtocolScheduleCustomizationIsSupported)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("example-chain")
        .hasMessageContaining("CliqueBesuControllerBuilder");
  }

  @Test
  void mainnetAndMergeBuildersApplyCustomizations() {
    assertThat(new MainnetBesuControllerBuilder().supportsProtocolScheduleCustomization()).isTrue();
    assertThat(new MergeBesuControllerBuilder().supportsProtocolScheduleCustomization()).isTrue();
    assertThat(new CliqueBesuControllerBuilder().supportsProtocolScheduleCustomization()).isFalse();
    assertThat(new QbftBesuControllerBuilder().supportsProtocolScheduleCustomization()).isFalse();
  }
}
