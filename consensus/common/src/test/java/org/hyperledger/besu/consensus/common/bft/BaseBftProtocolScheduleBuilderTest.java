/*
 * Copyright ConsenSys AG.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.TransitionsConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.MilestoneStreamingProtocolSchedule;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

public class BaseBftProtocolScheduleBuilderTest {

  private final GenesisConfigOptions genesisConfig = mock(GenesisConfigOptions.class);
  private final BftExtraDataCodec bftExtraDataCodec = mock(BftExtraDataCodec.class);

  @Test
  public void ensureBlockRewardAndMiningBeneficiaryInProtocolSpecMatchConfig() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(5);
    final Address miningBeneficiary = Address.fromHexString("0x1");
    final BftConfigOptions configOptions =
        createBftConfig(arbitraryBlockReward, Optional.of(miningBeneficiary));
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);

    final ProtocolSchedule schedule =
        createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions)));
    final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(1));

    assertThat(spec.getBlockReward()).isEqualTo(Wei.of(arbitraryBlockReward));
    assertThat(spec.getMiningBeneficiaryCalculator().calculateBeneficiary(mock(BlockHeader.class)))
        .isEqualTo(miningBeneficiary);
  }

  @Test
  public void missingMiningBeneficiaryInConfigWillPayCoinbaseInHeader() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(3);
    final BftConfigOptions configOptions = createBftConfig(arbitraryBlockReward);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    final ProtocolSchedule schedule =
        createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions)));
    final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(1));

    final Address headerCoinbase = Address.fromHexString("0x123");
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getCoinbase()).thenReturn(headerCoinbase);

    assertThat(spec.getBlockReward()).isEqualTo(Wei.of(arbitraryBlockReward));
    assertThat(spec.getMiningBeneficiaryCalculator().calculateBeneficiary(header))
        .isEqualTo(headerCoinbase);
  }

  @Test
  public void ensureForksAreRespected_beginWithNonEmptyMiningBeneficiary() {
    ensureForksAreRespected(false);
  }

  @Test
  public void ensureForksAreRespected_beginWithEmptyMiningBeneficiary() {
    ensureForksAreRespected(true);
  }

  private void ensureForksAreRespected(final boolean initialBeneficiaryIsEmpty) {
    final Address headerCoinbase = Address.fromHexString("0x123");
    final Address beneficiary1 = Address.fromHexString("0x01");
    final Address beneficiary2 = Address.fromHexString("0x02");
    final Address beneficiary3 = Address.fromHexString("0x03");
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getCoinbase()).thenReturn(headerCoinbase);

    // Alternate empty and non-empty mining beneficiary config
    final BftConfigOptions initialConfig =
        createBftConfig(
            BigInteger.valueOf(3),
            Optional.of(beneficiary1).filter(__ -> !initialBeneficiaryIsEmpty));
    final BftConfigOptions fork1 =
        createBftConfig(
            BigInteger.valueOf(2),
            Optional.of(beneficiary2).filter(__ -> initialBeneficiaryIsEmpty));
    final BftConfigOptions fork2 =
        createBftConfig(
            BigInteger.valueOf(1),
            Optional.of(beneficiary3).filter(__ -> !initialBeneficiaryIsEmpty));

    when(genesisConfig.getBftConfigOptions()).thenReturn(initialConfig);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    final ProtocolSchedule schedule =
        createProtocolSchedule(
            List.of(
                new ForkSpec<>(0, initialConfig),
                new ForkSpec<>(2, fork1),
                new ForkSpec<>(5, fork2)));

    // Check initial config
    for (int i = 0; i < 2; i++) {
      final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(i));
      final Address expectedBeneficiary = initialBeneficiaryIsEmpty ? headerCoinbase : beneficiary1;
      assertThat(spec.getBlockReward()).isEqualTo(Wei.of(BigInteger.valueOf(3)));
      assertThat(spec.getMiningBeneficiaryCalculator().calculateBeneficiary(header))
          .isEqualTo(expectedBeneficiary);
    }

    // Check fork1
    for (int i = 2; i < 5; i++) {
      final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(i));
      final Address expectedBeneficiary = initialBeneficiaryIsEmpty ? beneficiary2 : headerCoinbase;
      assertThat(spec.getBlockReward()).isEqualTo(Wei.of(BigInteger.valueOf(2)));
      assertThat(spec.getMiningBeneficiaryCalculator().calculateBeneficiary(header))
          .isEqualTo(expectedBeneficiary);
    }

    // Check fork2
    for (int i = 5; i < 8; i++) {
      final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(i));
      final Address expectedBeneficiary = initialBeneficiaryIsEmpty ? headerCoinbase : beneficiary3;
      assertThat(spec.getBlockReward()).isEqualTo(Wei.of(BigInteger.valueOf(1)));
      assertThat(spec.getMiningBeneficiaryCalculator().calculateBeneficiary(header))
          .isEqualTo(expectedBeneficiary);
    }
  }

  @Test
  public void negativeBlockRewardThrowsException() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(-3);
    final BftConfigOptions configOptions = createBftConfig(arbitraryBlockReward);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    assertThatThrownBy(() -> createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Bft Block reward in config cannot be negative");
  }

  @Test
  public void zeroEpochLengthThrowsException() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(3);
    final BftConfigOptions configOptions = createBftConfig(arbitraryBlockReward);
    when(configOptions.getEpochLength()).thenReturn(0L);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    assertThatThrownBy(() -> createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Epoch length in config must be greater than zero");
  }

  @Test
  public void negativeEpochLengthThrowsException() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(3);
    final BftConfigOptions configOptions = createBftConfig(arbitraryBlockReward);
    when(configOptions.getEpochLength()).thenReturn(-3000L);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);

    assertThatThrownBy(() -> createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Epoch length in config must be greater than zero");
  }

  @Test
  public void ensureNotApplicableWithdrawalsValidatorIsUsed() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(5);
    final BftConfigOptions configOptions = createBftConfig(arbitraryBlockReward);
    when(genesisConfig.getTransitions()).thenReturn(TransitionsConfigOptions.DEFAULT);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);

    final ProtocolSchedule schedule =
        createProtocolSchedule(List.of(new ForkSpec<>(0, configOptions)));
    final ProtocolSpec spec = schedule.getByBlockHeader(blockHeader(1));

    assertThat(spec.getWithdrawalsValidator())
        .isInstanceOf(WithdrawalsValidator.NotApplicableWithdrawals.class);
  }

  @Test
  public void blockRewardSpecifiedInTransitionCreatesNewMilestone() {
    final BigInteger arbitraryBlockReward = BigInteger.valueOf(5);
    final Address miningBeneficiary = Address.fromHexString("0x1");
    final BftConfigOptions configOptions =
        createBftConfig(arbitraryBlockReward, Optional.of(miningBeneficiary));
    when(genesisConfig.getBftConfigOptions()).thenReturn(configOptions);
    when(genesisConfig.isIbft2()).thenReturn(true);

    final long transitionBlock = 5L;
    final BigInteger forkBlockReward = arbitraryBlockReward.multiply(BigInteger.valueOf(2));
    final MutableBftConfigOptions blockRewardTransition =
        new MutableBftConfigOptions(JsonBftConfigOptions.DEFAULT);
    blockRewardTransition.setBlockRewardWei(forkBlockReward);

    final MilestoneStreamingProtocolSchedule schedule =
        new MilestoneStreamingProtocolSchedule(
            (DefaultProtocolSchedule)
                createProtocolSchedule(
                    List.of(
                        new ForkSpec<>(0, configOptions),
                        new ForkSpec<>(transitionBlock, blockRewardTransition))));

    assertThat(schedule.streamMilestoneBlocks().count()).isEqualTo(2);
    assertThat(schedule.getByBlockHeader(blockHeader(0)).getBlockReward())
        .isEqualTo(Wei.of(arbitraryBlockReward));
    assertThat(schedule.getByBlockHeader(blockHeader(transitionBlock)).getBlockReward())
        .isEqualTo(Wei.of(forkBlockReward));
  }

  private ProtocolSchedule createProtocolSchedule(final List<ForkSpec<BftConfigOptions>> forks) {
    return createProtocolSchedule(new ForksSchedule<>(forks));
  }

  private ProtocolSchedule createProtocolSchedule(
      final ForksSchedule<BftConfigOptions> forkSchedule) {
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
            bftExtraDataCodec,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    return protocolSchedule;
  }

  @Test
  public void ensureBlockPeriodInProtocolSpecMatchConfig() {
    final BftConfigOptions configBlockPeriod2 = createBftConfigBlockPeriod(2);
    when(genesisConfig.getBftConfigOptions()).thenReturn(configBlockPeriod2);
    when(genesisConfig.getLondonBlockNumber()).thenReturn(OptionalLong.of(50));

    // All forks after block 100 should be time based forks and retrieval from the fork
    // schedule should be based on block timestamp not block number
    when(genesisConfig.getShanghaiTime()).thenReturn(OptionalLong.of(100));
    TransitionsConfigOptions transitions = TransitionsConfigOptions.DEFAULT;
    when(genesisConfig.getTransitions()).thenReturn(transitions);

    final BftConfigOptions configBlockPeriod3 = createBftConfigBlockPeriod(3);
    final BftConfigOptions configBlockPeriod4 = createBftConfigBlockPeriod(4);

    ForksSchedule<BftConfigOptions> forkSchedule =
        new ForksSchedule<>(
            List.of(
                new ForkSpec<>(0, configBlockPeriod2),
                new ForkSpec<>(123456, configBlockPeriod3),
                new ForkSpec<>(999999, configBlockPeriod4)));

    createProtocolSchedule(forkSchedule);

    // Creating a protocol schedule updates the fork schedule with fork types
    assertThat(forkSchedule.getFork(0, 0).getForkType()).isEqualTo(ForkSpec.ForkScheduleType.BLOCK);
    assertThat(forkSchedule.getFork(10, 0).getForkType())
        .isEqualTo(ForkSpec.ForkScheduleType.BLOCK);
    assertThat(forkSchedule.getFork(10, 0).getValue().getBlockPeriodSeconds()).isEqualTo(2);
    assertThat(forkSchedule.getFork(10, 123456).getForkType())
        .isEqualTo(ForkSpec.ForkScheduleType.TIME);
    assertThat(forkSchedule.getFork(10, 123456).getValue().getBlockPeriodSeconds()).isEqualTo(3);
    assertThat(forkSchedule.getFork(10, 999999).getForkType())
        .isEqualTo(ForkSpec.ForkScheduleType.TIME);
    assertThat(forkSchedule.getFork(10, 999999).getValue().getBlockPeriodSeconds()).isEqualTo(4);
  }

  private BftConfigOptions createBftConfig(final BigInteger blockReward) {
    return createBftConfig(blockReward, Optional.empty());
  }

  private BftConfigOptions createBftConfig(
      final BigInteger blockReward, final Optional<Address> miningBeneficiary) {
    final BftConfigOptions bftConfig = mock(JsonBftConfigOptions.class);
    when(bftConfig.getMiningBeneficiary()).thenReturn(miningBeneficiary);
    when(bftConfig.getBlockRewardWei()).thenReturn(blockReward);
    when(bftConfig.getEpochLength()).thenReturn(3000L);

    return bftConfig;
  }

  private BftConfigOptions createBftConfigBlockPeriod(final int blockPeriod) {
    final BftConfigOptions bftConfig = mock(JsonBftConfigOptions.class);
    when(bftConfig.getBlockPeriodSeconds()).thenReturn(blockPeriod);
    when(bftConfig.getBlockRewardWei()).thenReturn(BigInteger.ONE);
    when(bftConfig.getEpochLength()).thenReturn(3000L);

    return bftConfig;
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
