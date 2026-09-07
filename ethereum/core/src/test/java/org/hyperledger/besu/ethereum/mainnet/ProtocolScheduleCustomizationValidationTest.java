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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.blockNumber;
import static org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation.timestamp;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

class ProtocolScheduleCustomizationValidationTest {

  @Test
  void theScheduleBuilderRefusesRatherThanTheFactoriesThatCallIt() {
    // the check lives with the builder, so a caller that composes adapters and builds a schedule
    // itself -- rather than going through one of the schedule factories -- is guarded too
    final GenesisConfig genesis = GenesisConfig.fromConfig("{\"config\":{\"shanghaiTime\":0}}");
    final ProtocolScheduleBuilder builder =
        new ProtocolScheduleBuilder(
            genesis.getConfigOptions(),
            Optional.of(BigInteger.ONE),
            ProtocolSpecAdapters.compose(
                Map.of(0L, Function.identity()), customization(blockNumber(10))),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    assertThatThrownBy(builder::createProtocolSchedule)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not before the first fork timestamp");
  }

  @Test
  void aTimestampActivationBeforeTheLastBlockActivationIsRejected() {
    assertThatThrownBy(
            () ->
                customization(timestamp(500))
                    .validateAgainst(options("{\"config\":{\"byzantiumBlock\":1000}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not after the last block activation");
  }

  @Test
  void aTimestampActivationAtGenesisIsAllowedWhenNoBlockForkPrecedesIt() {
    assertThatCode(
            () ->
                customization(timestamp(0))
                    .validateAgainst(options("{\"config\":{\"shanghaiTime\":0}}")))
        .doesNotThrowAnyException();
  }

  @Test
  void aBlockActivationBehindTheFirstForkTimestampIsRejected() {
    assertThatThrownBy(
            () ->
                customization(blockNumber(10))
                    .validateAgainst(options("{\"config\":{\"shanghaiTime\":0}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not before the first fork timestamp");
  }

  @Test
  void aBlockActivationInsideTheDaoRecoveryWindowIsRejected() {
    assertThatThrownBy(
            () ->
                customization(blockNumber(105))
                    .validateAgainst(options("{\"config\":{\"daoForkBlock\":100}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("DAO recovery window");
  }

  @Test
  void aBlockActivationAtTheDaoForkItselfIsRejected() {
    // the recovery writes its own spec at the fork itself
    assertThatThrownBy(
            () ->
                customization(blockNumber(100))
                    .validateAgainst(options("{\"config\":{\"daoForkBlock\":100}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("DAO recovery window");
  }

  @Test
  void aBlockActivationAtTheDaoRestorationBlockIsRejected() {
    // daoForkBlock + 10 is where the recovery reinstates the spec that preceded the fork, replacing
    // whatever milestone sits there. The recovery owns both ends of its window.
    assertThatThrownBy(
            () ->
                customization(blockNumber(110))
                    .validateAgainst(options("{\"config\":{\"daoForkBlock\":100}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("DAO recovery window");
  }

  @Test
  void aBlockActivationJustPastTheDaoWindowIsAllowed() {
    assertThatCode(
            () ->
                customization(blockNumber(111))
                    .validateAgainst(options("{\"config\":{\"daoForkBlock\":100}}")))
        .doesNotThrowAnyException();
  }

  @Test
  void aTimestampActivationInsideTheDaoRecoveryWindowIsRejected() {
    // the recovery reinstates the previous spec at daoForkBlock + 10 as a block milestone, which
    // outranks a timestamp milestone below it, so the contributed overlay would vanish there while
    // its activation stayed advertised
    assertThatThrownBy(
            () ->
                customization(timestamp(105))
                    .validateAgainst(options("{\"config\":{\"daoForkBlock\":100}}")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not after the last block activation");
  }

  private static ProtocolScheduleCustomization customization(
      final ProtocolScheduleActivation activation) {
    return new ProtocolScheduleCustomization(
        "example-chain",
        List.of(new ProtocolSpecModification(activation, UnaryOperator.identity())));
  }

  private static GenesisConfigOptions options(final String json) {
    return GenesisConfig.fromConfig(json).getConfigOptions();
  }
}
