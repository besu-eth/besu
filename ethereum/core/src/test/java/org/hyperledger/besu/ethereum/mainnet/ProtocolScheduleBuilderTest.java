/*
 * Copyright 2020 ConsenSys AG.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BERLIN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO1;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO2;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO3;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO4;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO5;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BYZANTIUM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.DAO_RECOVERY_INIT;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.DAO_RECOVERY_TRANSITION;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FRONTIER;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.HOMESTEAD;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.MainnetBlockValidator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MilestoneStreamingProtocolSchedule;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
class ProtocolScheduleBuilderTest {
  private final long PRE_SHANGHAI_TIMESTAMP = 1680488620L; // Mon, 03 Apr 2023 02:23:40 UTC
  @Mock GenesisConfigOptions configOptions;
  @Mock private Function<ProtocolSpecBuilder, ProtocolSpecBuilder> modifier;
  private static final BigInteger CHAIN_ID = BigInteger.ONE;
  private ProtocolScheduleBuilder builder;

  @BeforeEach
  public void setup() {
    builder =
        new ProtocolScheduleBuilder(
            configOptions,
            Optional.of(CHAIN_ID),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
  }

  @Test
  void createProtocolScheduleInOrder() {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(1L));
    when(configOptions.getDaoForkBlock()).thenReturn(OptionalLong.of(2L));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(13L));
    when(configOptions.getMergeNetSplitBlockNumber()).thenReturn(OptionalLong.of(15L));
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 1));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 3));
    when(configOptions.getPragueTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 5));
    when(configOptions.getOsakaTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 7));
    when(configOptions.getBpo1Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 9));
    when(configOptions.getBpo2Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 11));
    when(configOptions.getBpo3Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 13));
    when(configOptions.getBpo4Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 15));
    when(configOptions.getBpo5Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 17));
    when(configOptions.getAmsterdamTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 19));
    when(configOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.of(Address.ZERO));
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    assertThat(protocolSchedule.getChainId()).contains(CHAIN_ID);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(0)).getHardforkId())
        .isEqualTo(FRONTIER);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(1)).getHardforkId())
        .isEqualTo(HOMESTEAD);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(2)).getHardforkId())
        .isEqualTo(DAO_RECOVERY_INIT);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(3)).getHardforkId())
        .isEqualTo(DAO_RECOVERY_TRANSITION);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(12)).getHardforkId())
        .isEqualTo(HOMESTEAD);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(13)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(14)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(15)).getHardforkId()).isEqualTo(PARIS);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(50)).getHardforkId()).isEqualTo(PARIS);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(51, PRE_SHANGHAI_TIMESTAMP + 1))
                .getHardforkId())
        .isEqualTo(SHANGHAI);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(52, PRE_SHANGHAI_TIMESTAMP + 2))
                .getHardforkId())
        .isEqualTo(SHANGHAI);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(53, PRE_SHANGHAI_TIMESTAMP + 3))
                .getHardforkId())
        .isEqualTo(CANCUN);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(54, PRE_SHANGHAI_TIMESTAMP + 4))
                .getHardforkId())
        .isEqualTo(CANCUN);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(55, PRE_SHANGHAI_TIMESTAMP + 5))
                .getHardforkId())
        .isEqualTo(PRAGUE);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(56, PRE_SHANGHAI_TIMESTAMP + 7))
                .getHardforkId())
        .isEqualTo(OSAKA);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(57, PRE_SHANGHAI_TIMESTAMP + 9))
                .getHardforkId())
        .isEqualTo(BPO1);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(58, PRE_SHANGHAI_TIMESTAMP + 11))
                .getHardforkId())
        .isEqualTo(BPO2);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(59, PRE_SHANGHAI_TIMESTAMP + 13))
                .getHardforkId())
        .isEqualTo(BPO3);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(60, PRE_SHANGHAI_TIMESTAMP + 15))
                .getHardforkId())
        .isEqualTo(BPO4);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(61, PRE_SHANGHAI_TIMESTAMP + 17))
                .getHardforkId())
        .isEqualTo(BPO5);
    assertThat(
            protocolSchedule
                .getByBlockHeader(blockHeader(62, PRE_SHANGHAI_TIMESTAMP + 19))
                .getHardforkId())
        .isEqualTo(AMSTERDAM);
  }

  @Test
  void milestoneForShouldQueryAllAvailableHardforks() {
    final long BPO5_TIME = 1722333828L;
    final long AMSTERDAM_TIME = BPO5_TIME + 1L;

    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getConstantinopleBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPetersburgBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getIstanbulBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBerlinBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getLondonBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPragueTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getOsakaTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo1Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo2Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo3Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo4Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo5Time()).thenReturn(OptionalLong.of(BPO5_TIME));
    when(configOptions.getAmsterdamTime()).thenReturn(OptionalLong.of(AMSTERDAM_TIME));
    when(configOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.of(Address.ZERO));
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    final Optional<Long> maybeBerlinMileStone = protocolSchedule.milestoneFor(BERLIN);
    assertThat(maybeBerlinMileStone).isPresent();
    assertThat(maybeBerlinMileStone.get()).isEqualTo(0);

    final Optional<Long> maybeLondonMileStone = protocolSchedule.milestoneFor(LONDON);
    assertThat(maybeLondonMileStone).isPresent();
    assertThat(maybeLondonMileStone.get()).isEqualTo(0);

    final Optional<Long> maybeShanghaiMileStone = protocolSchedule.milestoneFor(SHANGHAI);
    assertThat(maybeShanghaiMileStone).isPresent();
    assertThat(maybeShanghaiMileStone.get()).isEqualTo(0);

    final Optional<Long> maybeCancunMileStone = protocolSchedule.milestoneFor(CANCUN);
    assertThat(maybeCancunMileStone).isPresent();
    assertThat(maybeCancunMileStone.get()).isEqualTo(0);

    final Optional<Long> maybePragueMileStone = protocolSchedule.milestoneFor(PRAGUE);
    assertThat(maybePragueMileStone).isPresent();
    assertThat(maybePragueMileStone.get()).isEqualTo(0);

    final Optional<Long> maybeOsakaMileStone = protocolSchedule.milestoneFor(OSAKA);
    assertThat(maybeOsakaMileStone).isPresent();
    assertThat(maybeOsakaMileStone.get()).isEqualTo(0);

    final Optional<Long> maybeBpo1MileStone = protocolSchedule.milestoneFor(BPO1);
    assertThat(maybeBpo1MileStone).isPresent();
    assertThat(maybeBpo1MileStone.get()).isEqualTo(0);

    final Optional<Long> maybeBpo2MileStone = protocolSchedule.milestoneFor(BPO2);
    assertThat(maybeBpo2MileStone).isPresent();
    assertThat(maybeBpo2MileStone.get()).isEqualTo(0);

    final Optional<Long> maybeBpo3MileStone = protocolSchedule.milestoneFor(BPO3);
    assertThat(maybeBpo3MileStone).isPresent();
    assertThat(maybeBpo3MileStone.get()).isEqualTo(0);

    final Optional<Long> maybeBpo4MileStone = protocolSchedule.milestoneFor(BPO4);
    assertThat(maybeBpo4MileStone).isPresent();
    assertThat(maybeBpo4MileStone.get()).isEqualTo(0);

    final Optional<Long> maybeBpo5MileStone = protocolSchedule.milestoneFor(BPO5);
    assertThat(maybeBpo5MileStone).isPresent();
    assertThat(maybeBpo5MileStone.get()).isEqualTo(BPO5_TIME);

    final Optional<Long> maybeAmsterdamMileStone = protocolSchedule.milestoneFor(AMSTERDAM);
    assertThat(maybeAmsterdamMileStone).isPresent();
    assertThat(maybeAmsterdamMileStone.get()).isEqualTo(AMSTERDAM_TIME);
  }

  @Test
  void createProtocolScheduleOverlappingUsesLatestFork() {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(0L));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(0L));
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    assertThat(protocolSchedule.getChainId()).contains(CHAIN_ID);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(0)).getHardforkId())
        .isEqualTo(BYZANTIUM);
    assertThat(protocolSchedule.getByBlockHeader(blockHeader(1)).getHardforkId())
        .isEqualTo(BYZANTIUM);
  }

  @Test
  void createProtocolScheduleOutOfOrderThrows() {
    when(configOptions.getArrowGlacierBlockNumber()).thenReturn(OptionalLong.of(12L));
    when(configOptions.getGrayGlacierBlockNumber()).thenReturn(OptionalLong.of(11L));
    assertThatThrownBy(() -> builder.createProtocolSchedule())
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Genesis Config Error: 'GRAY_GLACIER' is scheduled for milestone 11 but it must be on or after milestone 12.");
  }

  @Test
  void createProtocolScheduleWithTimestampsOutOfOrderThrows() {
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(3L));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(2L));
    assertThatThrownBy(() -> builder.createProtocolSchedule())
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Genesis Config Error: 'CANCUN' is scheduled for milestone 2 but it must be on or after milestone 3.");
  }

  @Test
  void modifierInsertedBetweenBlocksIsAppliedToLaterAndCreatesInterimMilestone() {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(5L));

    when(modifier.apply(any()))
        .thenAnswer((Answer<ProtocolSpecBuilder>) invocation -> invocation.getArgument(0));

    final MilestoneStreamingProtocolSchedule schedule = createScheduleModifiedAt(2);

    // A default spec exists at 0 (frontier), then the spec as requested in config, then another
    // added at the point at which the modifier is applied.
    assertThat(schedule.streamMilestoneBlocks().collect(Collectors.toList()))
        .containsExactly(0L, 2L, 5L);
    assertThat(schedule.getByBlockHeader(blockHeader(0)).getHardforkId()).isEqualTo(FRONTIER);
    assertThat(schedule.getByBlockHeader(blockHeader(2)).getHardforkId()).isEqualTo(FRONTIER);
    assertThat(schedule.getByBlockHeader(blockHeader(5)).getHardforkId()).isEqualTo(HOMESTEAD);

    verify(modifier, times(2)).apply(any());
  }

  @Test
  void modifierPastEndOfDefinedMilestonesGetsItsOwnMilestoneCreated() {
    when(modifier.apply(any()))
        .thenAnswer((Answer<ProtocolSpecBuilder>) invocation -> invocation.getArgument(0));

    final MilestoneStreamingProtocolSchedule schedule = createScheduleModifiedAt(2);

    // A default spec exists at 0 (frontier), then the spec as requested in config, then another
    // added at the point at which the modifier is applied.
    assertThat(schedule.streamMilestoneBlocks().collect(Collectors.toList()))
        .containsExactly(0L, 2L);
    assertThat(schedule.getByBlockHeader(blockHeader(0)).getHardforkId()).isEqualTo(FRONTIER);
    assertThat(schedule.getByBlockHeader(blockHeader(2)).getHardforkId()).isEqualTo(FRONTIER);

    verify(modifier, times(1)).apply(any());
  }

  @Test
  void modifierOnDefinedMilestoneIsAppliedButDoesNotGetAnExtraMilestoneCreated() {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(5L));
    when(modifier.apply(any()))
        .thenAnswer((Answer<ProtocolSpecBuilder>) invocation -> invocation.getArgument(0));

    final MilestoneStreamingProtocolSchedule schedule = createScheduleModifiedAt(5);

    // A default spec exists at 0 (frontier), then the spec as requested in config, then another
    // added at the point at which the modifier is applied.
    assertThat(schedule.streamMilestoneBlocks().collect(Collectors.toList()))
        .containsExactly(0L, 5L);
    assertThat(schedule.getByBlockHeader(blockHeader(0)).getHardforkId()).isEqualTo(FRONTIER);
    assertThat(schedule.getByBlockHeader(blockHeader(5)).getHardforkId()).isEqualTo(HOMESTEAD);
    verify(modifier, times(1)).apply(any());
  }

  private MilestoneStreamingProtocolSchedule createScheduleModifiedAt(final int blockNumber) {
    final ProtocolScheduleBuilder builder =
        new ProtocolScheduleBuilder(
            configOptions,
            Optional.of(CHAIN_ID),
            ProtocolSpecAdapters.create(blockNumber, modifier),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    return new MilestoneStreamingProtocolSchedule(
        (DefaultProtocolSchedule) builder.createProtocolSchedule());
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture()
        .number(number)
        .timestamp(PRE_SHANGHAI_TIMESTAMP)
        .buildHeader();
  }

  private BlockHeader blockHeader(final long number, final long timestamp) {
    return new BlockHeaderTestFixture().number(number).timestamp(timestamp).buildHeader();
  }

  @Test
  void amsterdamHasBlockAccessListFactoryWithForkActivated() {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getConstantinopleBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPetersburgBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getIstanbulBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBerlinBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getLondonBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPragueTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getOsakaTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo1Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo2Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo3Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo4Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo5Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getAmsterdamTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.of(Address.ZERO));
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    // Get the Amsterdam protocol spec
    final ProtocolSpec amsterdamSpec = protocolSchedule.getByBlockHeader(blockHeader(1, 1));
    assertThat(amsterdamSpec.getHardforkId()).isEqualTo(AMSTERDAM);

    // Verify that BlockAccessListFactory is present and fork-activated
    assertThat(amsterdamSpec.getBlockAccessListFactory()).isPresent();
  }

  @Test
  void balIsValidatedForAmsterdamButNotForOsaka() throws Exception {
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(1L));
    when(configOptions.getDaoForkBlock()).thenReturn(OptionalLong.of(2L));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(13L));
    when(configOptions.getMergeNetSplitBlockNumber()).thenReturn(OptionalLong.of(15L));
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 1));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 3));
    when(configOptions.getPragueTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 5));
    when(configOptions.getOsakaTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 7));
    when(configOptions.getBpo1Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 9));
    when(configOptions.getBpo2Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 11));
    when(configOptions.getBpo3Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 13));
    when(configOptions.getBpo4Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 15));
    when(configOptions.getBpo5Time()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 17));
    when(configOptions.getAmsterdamTime()).thenReturn(OptionalLong.of(PRE_SHANGHAI_TIMESTAMP + 19));
    when(configOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.of(Address.ZERO));
    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();

    final BlockValidator amsterdamBlockValidator =
        protocolSchedule
            .getByBlockHeader(blockHeader(62, PRE_SHANGHAI_TIMESTAMP + 19))
            .getBlockValidator();
    final BlockValidator osakaBlockValidator =
        protocolSchedule
            .getByBlockHeader(blockHeader(56, PRE_SHANGHAI_TIMESTAMP + 7))
            .getBlockValidator();

    assertThat(amsterdamBlockValidator).isInstanceOf(MainnetBlockValidator.class);
    assertThat(osakaBlockValidator).isInstanceOf(MainnetBlockValidator.class);

    final BlockAccessListValidator amsterdamBalValidator =
        getBlockAccessListValidator((MainnetBlockValidator) amsterdamBlockValidator);
    final BlockAccessListValidator osakaBalValidator =
        getBlockAccessListValidator((MainnetBlockValidator) osakaBlockValidator);

    // Invalid BAL: duplicate address
    final BlockAccessList.AccountChanges account1 =
        new BlockAccessList.AccountChanges(
            Address.fromHexString("0x1000000000000000000000000000000000000001"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    final BlockAccessList.AccountChanges account2SameAddress =
        new BlockAccessList.AccountChanges(
            Address.fromHexString("0x1000000000000000000000000000000000000001"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    final BlockAccessList invalidBal = new BlockAccessList(List.of(account1, account2SameAddress));
    final BlockHeader amsterdamHeader =
        new BlockHeaderTestFixture()
            .number(62)
            .timestamp(PRE_SHANGHAI_TIMESTAMP + 19)
            .gasLimit(30_000_000L)
            .balHash(BodyValidation.balHash(invalidBal))
            .buildHeader();
    final BlockHeader osakaHeader =
        new BlockHeaderTestFixture()
            .number(56)
            .timestamp(PRE_SHANGHAI_TIMESTAMP + 7)
            .gasLimit(30_000_000L)
            .balHash(BodyValidation.balHash(invalidBal))
            .buildHeader();

    assertThat(amsterdamBalValidator.validate(Optional.of(invalidBal), amsterdamHeader, 1))
        .as("Amsterdam must validate BAL and reject invalid BAL")
        .isFalse();
    assertThat(osakaBalValidator.validate(Optional.of(invalidBal), osakaHeader, 1))
        .as("Before Amsterdam, any block with a BAL must be rejected")
        .isFalse();

    // Amsterdam accepts a valid BAL; before Amsterdam any BAL is rejected
    final BlockAccessList validBal = new BlockAccessList(List.of());
    final BlockHeader amsterdamHeaderValidBal =
        new BlockHeaderTestFixture()
            .number(62)
            .timestamp(PRE_SHANGHAI_TIMESTAMP + 19)
            .gasLimit(30_000_000L)
            .balHash(BodyValidation.balHash(validBal))
            .buildHeader();
    final BlockHeader osakaHeaderValidBal =
        new BlockHeaderTestFixture()
            .number(56)
            .timestamp(PRE_SHANGHAI_TIMESTAMP + 7)
            .gasLimit(30_000_000L)
            .balHash(BodyValidation.balHash(validBal))
            .buildHeader();
    assertThat(amsterdamBalValidator.validate(Optional.of(validBal), amsterdamHeaderValidBal, 1))
        .as("Amsterdam must accept valid BAL")
        .isTrue();
    assertThat(osakaBalValidator.validate(Optional.of(validBal), osakaHeaderValidBal, 1))
        .as("Before Amsterdam, any BAL (even empty) must be rejected")
        .isFalse();
  }

  private static BlockAccessListValidator getBlockAccessListValidator(
      final MainnetBlockValidator blockValidator) throws Exception {
    final Field field = MainnetBlockValidator.class.getDeclaredField("blockAccessListValidator");
    field.setAccessible(true);
    return (BlockAccessListValidator) field.get(blockValidator);
  }

  @Test
  void inactiveBpoForkBlobScheduleIsNotApplied() {
    // Activate all forks through BPO2, skip BPO3-BPO5 (no timestamps), activate Amsterdam.
    // BPO3-BPO5 have blob schedule entries but should NOT be applied since they lack timestamps.
    when(configOptions.getHomesteadBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getByzantiumBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getConstantinopleBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPetersburgBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getIstanbulBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBerlinBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getLondonBlockNumber()).thenReturn(OptionalLong.of(0));
    when(configOptions.getShanghaiTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getCancunTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getPragueTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getOsakaTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo1Time()).thenReturn(OptionalLong.of(0));
    when(configOptions.getBpo2Time()).thenReturn(OptionalLong.of(0));
    // BPO3, BPO4, BPO5 are NOT activated (Mockito returns OptionalLong.empty() by default)
    when(configOptions.getAmsterdamTime()).thenReturn(OptionalLong.of(0));
    when(configOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(configOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.of(Address.ZERO));

    // Set up blob schedule: BPO2 has max=21, BPO3-5 have max=9 (deliberately different)
    final BlobScheduleOptions blobScheduleOptions = mock(BlobScheduleOptions.class);
    when(blobScheduleOptions.getBpo2()).thenReturn(Optional.of(BlobSchedule.BPO2_DEFAULT));
    // BPO3-5 stubs are lenient because these forks lack timestamps, so the production code
    // never calls getBpo3/4/5 — but we set them up to verify they are NOT applied.
    lenient()
        .when(blobScheduleOptions.getBpo3())
        .thenReturn(Optional.of(BlobSchedule.PRAGUE_DEFAULT));
    lenient()
        .when(blobScheduleOptions.getBpo4())
        .thenReturn(Optional.of(BlobSchedule.PRAGUE_DEFAULT));
    lenient()
        .when(blobScheduleOptions.getBpo5())
        .thenReturn(Optional.of(BlobSchedule.PRAGUE_DEFAULT));
    when(configOptions.getBlobScheduleOptions()).thenReturn(Optional.of(blobScheduleOptions));

    final ProtocolSchedule protocolSchedule = builder.createProtocolSchedule();
    final ProtocolSpec amsterdamSpec = protocolSchedule.getByBlockHeader(blockHeader(1, 1));
    assertThat(amsterdamSpec.getHardforkId()).isEqualTo(AMSTERDAM);

    // BPO2_DEFAULT has max=21, so blob gas limit = 21 * 131072 = 2,752,512.
    // If inactive BPO5's PRAGUE_DEFAULT (max=9) were applied, this would be 9 * 131072 = 1,179,648.
    assertThat(amsterdamSpec.getGasLimitCalculator().currentBlobGasLimit())
        .as("Amsterdam should use BPO2 blob schedule (max=21) since BPO3-BPO5 are inactive")
        .isEqualTo(21L * 131072L);
  }
}
