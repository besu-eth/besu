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
package org.hyperledger.besu.consensus.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.consensus.common.ForksScheduleFactory.SpecCreator;
import org.hyperledger.besu.consensus.common.bft.MutableBftConfigOptions;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ForksScheduleFactoryTest {

  @Test
  @SuppressWarnings("unchecked")
  public void throwsErrorIfHasForkForGenesisBlock() {
    final BftConfigOptions genesisConfigOptions = JsonBftConfigOptions.DEFAULT;
    final BftFork fork = createFork(0, 10, 30);
    final SpecCreator<BftConfigOptions, BftFork> specCreator = Mockito.mock(SpecCreator.class);

    assertThatThrownBy(
            () -> ForksScheduleFactory.create(genesisConfigOptions, List.of(fork), specCreator))
        .hasMessage("Transition cannot be created for genesis block");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void throwsErrorIfHasForksWithDuplicateBlock() {
    final BftConfigOptions genesisConfigOptions = JsonBftConfigOptions.DEFAULT;
    final BftFork fork1 = createFork(1, 10, 30);
    final BftFork fork2 = createFork(1, 20, 60);
    final BftFork fork3 = createFork(2, 30, 90);
    final SpecCreator<BftConfigOptions, BftFork> specCreator = Mockito.mock(SpecCreator.class);

    assertThatThrownBy(
            () ->
                ForksScheduleFactory.create(
                    genesisConfigOptions, List.of(fork1, fork2, fork3), specCreator))
        .hasMessage("Duplicate transitions cannot be created for the same block");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createsScheduleUsingSpecCreator() {
    final BftConfigOptions genesisConfigOptions = JsonBftConfigOptions.DEFAULT;
    final ForkSpec<BftConfigOptions> genesisForkSpec = new ForkSpec<>(0, genesisConfigOptions);
    final BftFork fork1 = createFork(1, 10, 20);
    final BftFork fork2 = createFork(2, 20, 40);
    final SpecCreator<BftConfigOptions, BftFork> specCreator = Mockito.mock(SpecCreator.class);

    final BftConfigOptions configOptions1 = createBftConfigOptions(10, 30);
    final BftConfigOptions configOptions2 = createBftConfigOptions(20, 60);
    when(specCreator.create(genesisForkSpec, fork1)).thenReturn(configOptions1);
    when(specCreator.create(new ForkSpec<>(1, configOptions1), fork2)).thenReturn(configOptions2);

    final ForksSchedule<BftConfigOptions> schedule =
        ForksScheduleFactory.create(genesisConfigOptions, List.of(fork1, fork2), specCreator);
    assertThat(schedule.getFork(0, 0)).isEqualTo(genesisForkSpec);
    assertThat(schedule.getFork(1, 0)).isEqualTo(new ForkSpec<>(1, configOptions1));
    assertThat(schedule.getFork(2, 0)).isEqualTo(new ForkSpec<>(2, configOptions2));
  }

  private MutableBftConfigOptions createBftConfigOptions(
      final int blockPeriodSeconds, final int emptyBlockPeriodSeconds) {
    final MutableBftConfigOptions bftConfigOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    bftConfigOptions.setBlockPeriodSeconds(blockPeriodSeconds);
    bftConfigOptions.setEmptyBlockPeriodSeconds(emptyBlockPeriodSeconds);
    return bftConfigOptions;
  }

  private BftFork createFork(
      final long block, final long blockPeriodSeconds, final long emptyBlockPeriodSeconds) {
    return new BftFork(
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                block,
                BftFork.BLOCK_PERIOD_SECONDS_KEY,
                blockPeriodSeconds,
                BftFork.EMPTY_BLOCK_PERIOD_SECONDS_KEY,
                emptyBlockPeriodSeconds)));
  }
}
