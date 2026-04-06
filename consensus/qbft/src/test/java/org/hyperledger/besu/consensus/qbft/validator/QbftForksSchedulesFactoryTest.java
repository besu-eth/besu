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
package org.hyperledger.besu.consensus.qbft.validator;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.config.QbftFork;
import org.hyperledger.besu.config.QbftFork.VALIDATOR_SELECTION_MODE;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.config.TransitionsConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BaseForksSchedulesFactoryTest;
import org.hyperledger.besu.consensus.qbft.MutableQbftConfigOptions;
import org.hyperledger.besu.consensus.qbft.QbftForksSchedulesFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.AddressHelpers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

public class QbftForksSchedulesFactoryTest
    extends BaseForksSchedulesFactoryTest<QbftConfigOptions, MutableQbftConfigOptions> {

  @Test
  public void createsScheduleWithForkThatOverridesGenesisValues() {
    final MutableQbftConfigOptions configOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                BftFork.VALIDATORS_KEY,
                List.of("1", "2", "3"),
                BftFork.BLOCK_PERIOD_SECONDS_KEY,
                10,
                BftFork.EMPTY_BLOCK_PERIOD_SECONDS_KEY,
                60,
                BftFork.BLOCK_REWARD_KEY,
                "5",
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.CONTRACT,
                QbftFork.VALIDATOR_CONTRACT_ADDRESS_KEY,
                "10"));

    final ForksSchedule<QbftConfigOptions> forksSchedule =
        QbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork));
    assertThat(forksSchedule.getFork(0, 0))
        .usingRecursiveComparison()
        .isEqualTo(new ForkSpec<>(0, configOptions));

    final Map<String, Object> forkOptions = new HashMap<>(configOptions.asMap());
    forkOptions.put(BftFork.BLOCK_PERIOD_SECONDS_KEY, 10);
    forkOptions.put(BftFork.EMPTY_BLOCK_PERIOD_SECONDS_KEY, 60);
    forkOptions.put(BftFork.BLOCK_REWARD_KEY, "5");
    forkOptions.put(QbftFork.VALIDATOR_SELECTION_MODE_KEY, "5");
    forkOptions.put(QbftFork.VALIDATOR_CONTRACT_ADDRESS_KEY, "10");
    final QbftConfigOptions expectedForkConfig =
        new MutableQbftConfigOptions(
            new JsonQbftConfigOptions(JsonUtil.objectNodeFromMap(forkOptions)));

    final ForkSpec<QbftConfigOptions> expectedFork = new ForkSpec<>(1, expectedForkConfig);
    assertThat(forksSchedule.getFork(1, 0)).usingRecursiveComparison().isEqualTo(expectedFork);
    assertThat(forksSchedule.getFork(2, 0)).usingRecursiveComparison().isEqualTo(expectedFork);
  }

  @Test
  public void creatingScheduleThrowsErrorForContractForkWithoutContractAddress() {
    final MutableQbftConfigOptions configOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.CONTRACT));
    createGenesisConfig(configOptions, fork);

    assertThatThrownBy(
            () -> QbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork)))
        .hasMessage("QBFT transition has config with contract mode but no contract address");
  }

  @Test
  public void switchingToBlockHeaderRemovesValidatorContractAddress() {
    final MutableQbftConfigOptions configOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    configOptions.setValidatorContractAddress(Optional.of("10"));
    List<Address> validators = List.of(AddressHelpers.ofValue(1));
    final List<TextNode> jsonValidators =
        validators.stream()
            .map(v -> TextNode.valueOf(v.getBytes().toHexString()))
            .collect(Collectors.toList());

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.BLOCKHEADER,
                BftFork.VALIDATORS_KEY,
                JsonUtil.getObjectMapper().createArrayNode().addAll(jsonValidators)));

    final ForksSchedule<QbftConfigOptions> forksSchedule =
        QbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork));

    assertThat(forksSchedule.getFork(1, 0).getValue().getValidatorContractAddress()).isEmpty();
  }

  @Test
  public void switchingToBlockHeaderRequiresValidators() {
    final MutableQbftConfigOptions configOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    configOptions.setValidatorContractAddress(Optional.of("10"));

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.BLOCKHEADER));

    assertThatThrownBy(
            () -> QbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork)))
        .hasMessage(
            "QBFT transition to blockheader mode requires a validators list containing at least one validator");
  }

  @Test
  public void switchingToBlockHeaderRequiresNonEmptyValidators() {
    final MutableQbftConfigOptions configOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    configOptions.setValidatorContractAddress(Optional.of("10"));

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                QbftFork.VALIDATOR_SELECTION_MODE_KEY,
                VALIDATOR_SELECTION_MODE.BLOCKHEADER,
                BftFork.VALIDATORS_KEY,
                JsonUtil.getObjectMapper().createArrayNode()));

    assertThatThrownBy(
            () -> QbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork)))
        .hasMessage(
            "QBFT transition to blockheader mode requires a validators list containing at least one validator");
  }

  @Override
  protected GenesisConfigOptions createGenesisConfig(
      final QbftConfigOptions configOptions, final ObjectNode... forks) {
    final StubGenesisConfigOptions genesisConfigOptions = new StubGenesisConfigOptions();
    genesisConfigOptions.qbftConfigOptions(configOptions);
    genesisConfigOptions.transitions(
        new TransitionsConfigOptions(
            JsonUtil.objectNodeFromMap(Map.of("qbft", Arrays.asList(forks)))));
    return genesisConfigOptions;
  }

  @Override
  protected ForksSchedule<QbftConfigOptions> createForkSchedule(
      final GenesisConfigOptions genesisConfigOptions) {
    return QbftForksSchedulesFactory.create(genesisConfigOptions);
  }

  @Override
  protected QbftConfigOptions createBftOptions(
      final Consumer<MutableQbftConfigOptions> optionModifier) {
    final MutableQbftConfigOptions options =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    optionModifier.accept(options);
    return options;
  }
}
