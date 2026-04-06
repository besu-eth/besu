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
package org.hyperledger.besu.consensus.ibft;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.config.TransitionsConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BaseForksSchedulesFactoryTest;
import org.hyperledger.besu.consensus.common.bft.MutableBftConfigOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

public class IbftForksSchedulesFactoryTest
    extends BaseForksSchedulesFactoryTest<BftConfigOptions, MutableBftConfigOptions> {

  @Test
  public void createsScheduleWithForkThatOverridesGenesisValues() {
    final MutableBftConfigOptions configOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);

    final ObjectNode fork =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                BftFork.BLOCK_PERIOD_SECONDS_KEY,
                10,
                BftFork.BLOCK_REWARD_KEY,
                "5"));

    final ForksSchedule<BftConfigOptions> forksSchedule =
        IbftForksSchedulesFactory.create(createGenesisConfig(configOptions, fork));
    assertThat(forksSchedule.getFork(0, 0))
        .usingRecursiveComparison()
        .isEqualTo(new ForkSpec<>(0, configOptions));

    final Map<String, Object> forkOptions = new HashMap<>(configOptions.asMap());
    forkOptions.put(BftFork.BLOCK_PERIOD_SECONDS_KEY, 10);
    forkOptions.put(BftFork.BLOCK_REWARD_KEY, "5");

    final BftConfigOptions expectedForkConfig =
        new MutableBftConfigOptions(
            new JsonBftConfigOptions(JsonUtil.objectNodeFromMap(forkOptions)));

    final ForkSpec<BftConfigOptions> expectedFork = new ForkSpec<>(1, expectedForkConfig);
    assertThat(forksSchedule.getFork(1, 0)).usingRecursiveComparison().isEqualTo(expectedFork);
    assertThat(forksSchedule.getFork(2, 0)).usingRecursiveComparison().isEqualTo(expectedFork);
  }

  @Override
  protected GenesisConfigOptions createGenesisConfig(
      final BftConfigOptions configOptions, final ObjectNode... fork) {
    final StubGenesisConfigOptions genesisConfigOptions = new StubGenesisConfigOptions();
    genesisConfigOptions.bftConfigOptions(configOptions);
    genesisConfigOptions.transitions(
        new TransitionsConfigOptions(
            JsonUtil.objectNodeFromMap(Map.of("ibft2", Arrays.asList(fork)))));
    return genesisConfigOptions;
  }

  @Override
  protected ForksSchedule<BftConfigOptions> createForkSchedule(
      final GenesisConfigOptions genesisConfigOptions) {
    return IbftForksSchedulesFactory.create(genesisConfigOptions);
  }

  @Override
  protected BftConfigOptions createBftOptions(
      final Consumer<MutableBftConfigOptions> optionModifier) {
    final MutableBftConfigOptions options =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    optionModifier.accept(options);
    return options;
  }
}
