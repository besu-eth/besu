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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.consensus.common.bft.BaseBftProtocolScheduleBuilder;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.MutableBftConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

public class ForksScheduleTest {

  @Test
  public void retrievesGenesisFork() {
    final ForkSpec<BftConfigOptions> genesisForkSpec =
        new ForkSpec<>(0, JsonBftConfigOptions.DEFAULT);
    final ForkSpec<BftConfigOptions> forkSpec1 = createForkSpec(10, 10);

    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(forkSpec1, genesisForkSpec));
    assertThat(schedule.getFork(0, 0)).isEqualTo(genesisForkSpec);
    assertThat(schedule.getFork(1, 0)).isEqualTo(genesisForkSpec);
  }

  @Test
  public void retrievesLatestForkByBlockNumber() {
    final ForkSpec<BftConfigOptions> genesisForkSpec =
        new ForkSpec<>(0, JsonBftConfigOptions.DEFAULT);
    final ForkSpec<BftConfigOptions> forkSpec1 = createForkSpec(1, 10);
    final ForkSpec<BftConfigOptions> forkSpec2 = createForkSpec(2, 20);
    final Optional<Address> miningBeneficiary3 =
        Optional.of(Address.fromHexString("0xdee0519f7c7cb0f9843fa1e93b99255c89507a9c"));
    final ForkSpec<BftConfigOptions> forkSpec3 =
        createForkSpecWithMiningBeneficiary(3, miningBeneficiary3);
    final ForkSpec<BftConfigOptions> forkSpec4 =
        createForkSpecWithMiningBeneficiary(4, Optional.empty());

    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(genesisForkSpec, forkSpec1, forkSpec2, forkSpec3, forkSpec4));

    assertThat(schedule.getFork(0, 0)).isEqualTo(genesisForkSpec);
    assertThat(schedule.getFork(1, 0)).isEqualTo(forkSpec1);
    assertThat(schedule.getFork(2, 0)).isEqualTo(forkSpec2);
    assertThat(schedule.getFork(3, 0)).isEqualTo(forkSpec3);
    assertThat(schedule.getFork(3, 0).getValue().getMiningBeneficiary())
        .isEqualTo(miningBeneficiary3);
    assertThat(schedule.getFork(4, 0)).isEqualTo(forkSpec4);
    assertThat(schedule.getFork(4, 0).getValue().getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void retrievesLatestForkByTimestamp() {
    final ForkSpec<BftConfigOptions> genesisForkSpec =
        new ForkSpec<>(0, JsonBftConfigOptions.DEFAULT);
    final ForkSpec<BftConfigOptions> forkSpec1 = createForkSpec(1_681_338_455L, 10);
    final ForkSpec<BftConfigOptions> forkSpec2 = createForkSpec(1_681_338_555L, 20);
    final Optional<Address> miningBeneficiary3 =
        Optional.of(Address.fromHexString("0xdee0519f7c7cb0f9843fa1e93b99255c89507a9c"));
    final ForkSpec<BftConfigOptions> forkSpec3 =
        createForkSpecWithMiningBeneficiary(1_681_338_655L, miningBeneficiary3);
    final ForkSpec<BftConfigOptions> forkSpec4 =
        createForkSpecWithMiningBeneficiary(1_681_338_755L, Optional.empty());

    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(genesisForkSpec, forkSpec1, forkSpec2, forkSpec3, forkSpec4));

    final GenesisConfigOptions genesisMilestones = mock(GenesisConfigOptions.class);
    when(genesisMilestones.getLondonBlockNumber()).thenReturn(OptionalLong.of(50));
    when(genesisMilestones.getShanghaiTime()).thenReturn(OptionalLong.of(1_681_338_455L));

    // Create a protocol schedule based on the genesis config, which applies types (block or
    // timestamp) to all of the forks
    createProtocolSchedule(schedule, genesisMilestones);

    assertThat(schedule.getFork(0, 0)).isEqualTo(genesisForkSpec);
    assertThat(schedule.getFork(0, 1_681_338_455L)).isEqualTo(forkSpec1);
    assertThat(schedule.getFork(0, 1_681_338_555L)).isEqualTo(forkSpec2);
    assertThat(schedule.getFork(0, 1_681_338_655L)).isEqualTo(forkSpec3);
    assertThat(schedule.getFork(0, 1_681_338_655L).getValue().getMiningBeneficiary())
        .isEqualTo(miningBeneficiary3);
    assertThat(schedule.getFork(0, 1_681_338_755L)).isEqualTo(forkSpec4);
    assertThat(schedule.getFork(0, 1_681_338_755L).getValue().getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void applyMilestoneTypesThrowsForTimestampForkBeforeJan2023() {
    // Need genesis fork at 0 so the transition fork at 1_672_531_199L lands in headSet
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(0, 10), createForkSpec(1_672_531_199L, 10)));

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(anyLong(), anyLong()))
        .thenReturn(ForkSpec.ForkScheduleType.TIME);
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of());

    assertThatThrownBy(() -> schedule.applyMilestoneTypes(mockSchedule))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("TIMESTAMP")
        .hasMessageContaining("1681338455");
  }

  @Test
  public void applyMilestoneTypesDoesNotThrowForTimestampForkAtJan2023Boundary() {
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(1_672_531_200L, 10)));

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(anyLong(), anyLong()))
        .thenReturn(ForkSpec.ForkScheduleType.TIME);
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of());

    assertThatCode(() -> schedule.applyMilestoneTypes(mockSchedule)).doesNotThrowAnyException();
  }

  @Test
  public void applyMilestoneTypesDoesNotThrowForBlockTypeForkWithLowValue() {
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(100, 10)));

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(anyLong(), anyLong()))
        .thenReturn(ForkSpec.ForkScheduleType.BLOCK);
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of());

    assertThatCode(() -> schedule.applyMilestoneTypes(mockSchedule)).doesNotThrowAnyException();
  }

  @Test
  public void applyMilestoneTypesThrowsForInvalidTimestampEvmSpecWhenInTimeMode() {
    // Genesis fork at 0 + transition fork at 800 (TIME type, below the minimum epoch)
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(0, 10), createForkSpec(800L, 10)));
    final long currentBlockNumber = 1_000_000L;

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(eq(0L), eq(0L)))
        .thenReturn(ForkSpec.ForkScheduleType.BLOCK);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(eq(800L), eq(800L)))
        .thenReturn(ForkSpec.ForkScheduleType.TIME);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(
            eq(currentBlockNumber), eq(currentBlockNumber)))
        .thenReturn(ForkSpec.ForkScheduleType.TIME);

    final ScheduledProtocolSpec mockTimestampSpec =
        mock(ScheduledProtocolSpec.TimestampProtocolSpec.class);
    when(mockTimestampSpec.fork()).thenReturn(new ScheduledProtocolSpec.Hardfork("test", 800L));
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of(mockTimestampSpec));

    assertThatThrownBy(() -> schedule.applyMilestoneTypes(mockSchedule))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("TIMESTAMP")
        .hasMessageContaining("1681338455");
  }

  @Test
  public void applyMilestoneTypesDoesNotThrowForTimestampEvmSpecWhenCurrentSpecIsBlockType() {
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(0, 10)));

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(anyLong(), anyLong()))
        .thenReturn(ForkSpec.ForkScheduleType.BLOCK);

    final ScheduledProtocolSpec mockTimestampSpec =
        mock(ScheduledProtocolSpec.TimestampProtocolSpec.class);
    when(mockTimestampSpec.fork()).thenReturn(new ScheduledProtocolSpec.Hardfork("test", 800L));
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of(mockTimestampSpec));

    assertThatCode(() -> schedule.applyMilestoneTypes(mockSchedule)).doesNotThrowAnyException();
  }

  @Test
  public void applyMilestoneTypesDoesNotThrowForValidTimestampEvmSpecAtJan2023Boundary() {
    final ForksSchedule<BftConfigOptions> schedule =
        new ForksSchedule<>(List.of(createForkSpec(0, 10)));

    final BftProtocolSchedule mockSchedule = mock(BftProtocolSchedule.class);
    // Genesis fork (block 0) is BLOCK-type; the current-block lookup uses Long.MAX_VALUE → TIME
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(anyLong(), anyLong()))
        .thenReturn(ForkSpec.ForkScheduleType.TIME);
    when(mockSchedule.getSpecTypeByBlockNumberOrTimestamp(eq(0L), eq(0L)))
        .thenReturn(ForkSpec.ForkScheduleType.BLOCK);

    final ScheduledProtocolSpec mockTimestampSpec =
        mock(ScheduledProtocolSpec.TimestampProtocolSpec.class);
    when(mockTimestampSpec.fork())
        .thenReturn(new ScheduledProtocolSpec.Hardfork("test", 1_672_531_200L));
    when(mockSchedule.getScheduledProtocolSpecs()).thenReturn(List.of(mockTimestampSpec));

    assertThatCode(() -> schedule.applyMilestoneTypes(mockSchedule)).doesNotThrowAnyException();
  }

  private ForkSpec<BftConfigOptions> createForkSpecWithMiningBeneficiary(
      final long block, final Optional<Address> beneficiary) {
    final MutableBftConfigOptions bftConfigOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    bftConfigOptions.setMiningBeneficiary(beneficiary);
    return new ForkSpec<>(block, bftConfigOptions);
  }

  private ForkSpec<BftConfigOptions> createForkSpec(
      final long block, final int blockPeriodSeconds) {
    final MutableBftConfigOptions bftConfigOptions = createBftConfigOptions(blockPeriodSeconds);
    return new ForkSpec<>(block, bftConfigOptions);
  }

  private MutableBftConfigOptions createBftConfigOptions(final int blockPeriodSeconds) {
    final MutableBftConfigOptions bftConfigOptions =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    bftConfigOptions.setBlockPeriodSeconds(blockPeriodSeconds);
    return bftConfigOptions;
  }

  private ProtocolSchedule createProtocolSchedule(
      final ForksSchedule<BftConfigOptions> forkSchedule,
      final GenesisConfigOptions genesisConfig) {
    final BaseBftProtocolScheduleBuilder bftProtocolSchedule =
        new BaseBftProtocolScheduleBuilder() {
          @Override
          protected BlockHeaderValidator.Builder createBlockHeaderRuleset(
              final BftConfigOptions config, final FeeMarket feeMarket) {
            return new BlockHeaderValidator.Builder();
          }
        };
    final BftProtocolSchedule protocolSchedule =
        bftProtocolSchedule.createProtocolSchedule(
            genesisConfig,
            forkSchedule,
            false,
            mock(BftExtraDataCodec.class),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    return protocolSchedule;
  }
}
