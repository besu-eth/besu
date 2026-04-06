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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.datatypes.Address;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

public abstract class BaseForksSchedulesFactoryTest<
    C extends BftConfigOptions, M extends MutableBftConfigOptions> {

  @Test
  public void createsScheduleForJustGenesisConfig() {
    final C configOptions = createBftOptions();
    final ForkSpec<C> expectedForkSpec = new ForkSpec<>(0, createBftOptions());
    final GenesisConfigOptions genesisConfigOptions = createGenesisConfig(configOptions);

    final ForksSchedule<C> forksSchedule = createForkSchedule(genesisConfigOptions);
    assertThat(forksSchedule.getFork(0, 0)).usingRecursiveComparison().isEqualTo(expectedForkSpec);
    assertThat(forksSchedule.getFork(1, 0)).usingRecursiveComparison().isEqualTo(expectedForkSpec);
    assertThat(forksSchedule.getFork(2, 0)).usingRecursiveComparison().isEqualTo(expectedForkSpec);
  }

  @Test
  public void createsScheduleThatChangesMiningBeneficiary_beneficiaryInitiallyEmpty() {
    final Address beneficiaryAddress =
        Address.fromHexString("0x1111111111111111111111111111111111111111");
    final C qbftConfigOptions = createBftOptions();

    final ObjectNode forkWithBeneficiary =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                1,
                BftFork.MINING_BENEFICIARY_KEY,
                beneficiaryAddress.getBytes().toHexString()));
    final ObjectNode forkWithNoBeneficiary =
        JsonUtil.objectNodeFromMap(
            Map.of(BftFork.FORK_BLOCK_KEY, 2, BftFork.MINING_BENEFICIARY_KEY, ""));

    final GenesisConfigOptions genesisConfigOptions =
        createGenesisConfig(qbftConfigOptions, forkWithBeneficiary, forkWithNoBeneficiary);
    final ForksSchedule<C> forksSchedule = createForkSchedule(genesisConfigOptions);

    assertThat(forksSchedule.getFork(0, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(1, 0).getValue().getMiningBeneficiary())
        .contains(beneficiaryAddress);
    assertThat(forksSchedule.getFork(2, 0).getValue().getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void createsScheduleThatChangesMiningBeneficiary_beneficiaryInitiallyNonEmpty() {
    final Address beneficiaryAddress =
        Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address beneficiaryAddress2 = Address.fromHexString("0x02");
    final C qbftConfigOptions =
        createBftOptions(o -> o.setMiningBeneficiary(Optional.of(beneficiaryAddress)));

    final ObjectNode forkWithBeneficiary =
        JsonUtil.objectNodeFromMap(
            Map.of(BftFork.FORK_BLOCK_KEY, 1, BftFork.MINING_BENEFICIARY_KEY, ""));
    final ObjectNode forkWithNoBeneficiary =
        JsonUtil.objectNodeFromMap(
            Map.of(
                BftFork.FORK_BLOCK_KEY,
                2,
                BftFork.MINING_BENEFICIARY_KEY,
                beneficiaryAddress2.getBytes().toUnprefixedHexString()));

    final GenesisConfigOptions genesisConfigOptions =
        createGenesisConfig(qbftConfigOptions, forkWithBeneficiary, forkWithNoBeneficiary);
    final ForksSchedule<C> forksSchedule = createForkSchedule(genesisConfigOptions);

    assertThat(forksSchedule.getFork(0, 0).getValue().getMiningBeneficiary())
        .contains(beneficiaryAddress);
    assertThat(forksSchedule.getFork(1, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(2, 0).getValue().getMiningBeneficiary())
        .contains(beneficiaryAddress2);
  }

  @Test
  public void
      createsScheduleThatChangesMiningBeneficiary_shouldNotModifyBeneficiaryUnlessExplicitlyConfigured() {
    final Address initialBeneficiaryAddress =
        Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address beneficiaryAddress2 = Address.fromHexString("0x02");
    final C qbftConfigOptions =
        createBftOptions(o -> o.setMiningBeneficiary(Optional.of(initialBeneficiaryAddress)));

    ObjectNode[] forks = {
      // No change to beneficiary
      JsonUtil.objectNodeFromMap(
          Map.of(BftFork.FORK_BLOCK_KEY, 1, BftFork.BLOCK_PERIOD_SECONDS_KEY, 2)),
      // Clear beneficiary
      JsonUtil.objectNodeFromMap(
          Map.of(BftFork.FORK_BLOCK_KEY, 3, BftFork.MINING_BENEFICIARY_KEY, "")),
      // No change to beneficiary
      JsonUtil.objectNodeFromMap(
          Map.of(BftFork.FORK_BLOCK_KEY, 5, BftFork.BLOCK_PERIOD_SECONDS_KEY, 4)),
      // Set beneficiary
      JsonUtil.objectNodeFromMap(
          Map.of(
              BftFork.FORK_BLOCK_KEY,
              7,
              BftFork.MINING_BENEFICIARY_KEY,
              beneficiaryAddress2.getBytes().toUnprefixedHexString()))
    };

    final GenesisConfigOptions genesisConfigOptions = createGenesisConfig(qbftConfigOptions, forks);
    final ForksSchedule<C> forksSchedule = createForkSchedule(genesisConfigOptions);

    assertThat(forksSchedule.getFork(0, 0).getValue().getMiningBeneficiary())
        .contains(initialBeneficiaryAddress);
    assertThat(forksSchedule.getFork(1, 0).getValue().getMiningBeneficiary())
        .contains(initialBeneficiaryAddress);
    assertThat(forksSchedule.getFork(2, 0).getValue().getMiningBeneficiary())
        .contains(initialBeneficiaryAddress);
    assertThat(forksSchedule.getFork(3, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(4, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(5, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(6, 0).getValue().getMiningBeneficiary()).isEmpty();
    assertThat(forksSchedule.getFork(7, 0).getValue().getMiningBeneficiary())
        .contains(beneficiaryAddress2);
    assertThat(forksSchedule.getFork(8, 0).getValue().getMiningBeneficiary())
        .contains(beneficiaryAddress2);
  }

  @Test
  public void createsScheduleWithInvalidMiningBeneficiary_shouldThrow() {
    final C qbftConfigOptions = createBftOptions();

    final ObjectNode invalidFork =
        JsonUtil.objectNodeFromMap(
            Map.of(BftFork.FORK_BLOCK_KEY, 1, BftFork.MINING_BENEFICIARY_KEY, "bla"));

    final GenesisConfigOptions genesisConfigOptions =
        createGenesisConfig(qbftConfigOptions, invalidFork);

    assertThatThrownBy(() -> createForkSchedule(genesisConfigOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Mining beneficiary in transition config is not a valid ethereum address");
  }

  @Test
  public void createsScheduleThatChangesBlockPeriodAtTimestamp() {
    final C qbftConfigOptions = createBftOptions(o -> o.setBlockPeriodSeconds(2));

    ObjectNode[] forks = {
      // Change block period 2->3 seconds
      JsonUtil.objectNodeFromMap(
          Map.of(BftFork.FORK_BLOCK_KEY, 1234567, BftFork.BLOCK_PERIOD_SECONDS_KEY, 3)),
      // Change block period 3->4 seconds
      JsonUtil.objectNodeFromMap(
          Map.of(BftFork.FORK_BLOCK_KEY, 1234568, BftFork.BLOCK_PERIOD_SECONDS_KEY, 4))
    };

    final GenesisConfigOptions genesisConfigOptions = createGenesisConfig(qbftConfigOptions, forks);
    final ForksSchedule<C> forksSchedule = createForkSchedule(genesisConfigOptions);

    for (ForkSpec<C> f : forksSchedule.getForks()) {
      f.setForkType(ForkSpec.ForkScheduleType.BLOCK);
    }

    // Should ignore block timestamp if forks are block type
    assertThat(forksSchedule.getFork(0, 1234566).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forksSchedule.getFork(0, 1234567).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forksSchedule.getFork(0, 1234568).getValue().getBlockPeriodSeconds()).isEqualTo(2);

    for (ForkSpec<C> f : forksSchedule.getForks()) {
      f.setForkType(ForkSpec.ForkScheduleType.TIME);
    }

    // Should ignore block timestamp if forks are block type
    assertThat(forksSchedule.getFork(1234566, 0).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forksSchedule.getFork(1234567, 0).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forksSchedule.getFork(1234568, 0).getValue().getBlockPeriodSeconds()).isEqualTo(2);

    // Should reflect changes based on timestamp
    assertThat(forksSchedule.getFork(0, 1234566).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forksSchedule.getFork(0, 1234567).getValue().getBlockPeriodSeconds()).isEqualTo(3);
    assertThat(forksSchedule.getFork(0, 1234568).getValue().getBlockPeriodSeconds()).isEqualTo(4);
  }

  protected abstract C createBftOptions(final Consumer<M> optionModifier);

  protected C createBftOptions() {
    return createBftOptions(__ -> {});
  }

  protected abstract GenesisConfigOptions createGenesisConfig(
      final C configOptions, final ObjectNode... forks);

  protected abstract ForksSchedule<C> createForkSchedule(
      final GenesisConfigOptions genesisConfigOptions);
}
