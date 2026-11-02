/*
 * Copyright contributors to Besu.
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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;

import org.junit.jupiter.api.Test;

class BlockRewardProcessorTest {

  private static final Wei BLOCK_REWARD = Wei.fromEth(2);
  private static final Address BENEFICIARY = Address.fromHexString("0x01");
  private static final Address OMMER_COINBASE = Address.fromHexString("0x02");

  private final BlockRewardProcessor processor = new MainnetBlockRewardProcessor(BLOCK_REWARD);

  @Test
  void coinbaseRewardIsTheBlockRewardWithoutOmmers() {
    assertThat(processor.getCoinbaseReward(0)).isEqualTo(BLOCK_REWARD);
  }

  @Test
  void coinbaseRewardAddsOneThirtySecondPerOmmer() {
    // 2 ETH + 2 * (2 ETH / 32)
    assertThat(processor.getCoinbaseReward(2)).isEqualTo(Wei.of(2_125_000_000_000_000_000L));
  }

  @Test
  void ommerRewardSubtractsOneEighthPerBlockOfDistance() {
    // 2 ETH - 1 * (2 ETH / 8)
    assertThat(processor.getOmmerReward(100, 99)).isEqualTo(Wei.of(1_750_000_000_000_000_000L));
    // 2 ETH - 6 * (2 ETH / 8), the oldest ommer allowed
    assertThat(processor.getOmmerReward(100, 94)).isEqualTo(Wei.of(500_000_000_000_000_000L));
  }

  @Test
  void rewardsTheBeneficiaryAndTheOmmerCoinbase() {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final BlockHeader header = new BlockHeaderTestFixture().number(100).buildHeader();
    final BlockHeader ommer =
        new BlockHeaderTestFixture().number(99).coinbase(OMMER_COINBASE).buildHeader();

    assertThat(processor.rewardBeneficiaries(worldState, header, List.of(ommer), BENEFICIARY))
        .isTrue();

    // 2 ETH + 1 * (2 ETH / 32)
    assertThat(worldState.get(BENEFICIARY).getBalance())
        .isEqualTo(Wei.of(2_062_500_000_000_000_000L));
    assertThat(worldState.get(OMMER_COINBASE).getBalance())
        .isEqualTo(Wei.of(1_750_000_000_000_000_000L));
  }

  @Test
  void noRewardsLeavesTheWorldStateUntouched() {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final BlockHeader header = new BlockHeaderTestFixture().number(100).buildHeader();

    BlockRewardProcessor.NO_REWARDS.rewardBeneficiaries(worldState, header, List.of(), BENEFICIARY);

    assertThat(worldState.get(BENEFICIARY)).isNull();
  }

  @Test
  void mainnetRulesRejectAZeroReward() {
    assertThatThrownBy(() -> new MainnetBlockRewardProcessor(Wei.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NO_REWARDS");
  }

  @Test
  void noRewardsPaysNothing() {
    assertThat(BlockRewardProcessor.NO_REWARDS.getBlockReward()).isEqualTo(Wei.ZERO);
    assertThat(BlockRewardProcessor.NO_REWARDS.getCoinbaseReward(2)).isEqualTo(Wei.ZERO);
    assertThat(BlockRewardProcessor.NO_REWARDS.getOmmerReward(100, 99)).isEqualTo(Wei.ZERO);
  }

  @Test
  void zeroConfiguredRewardPaysNothing() {
    assertThat(BlockRewardProcessor.of(Wei.ZERO)).isSameAs(BlockRewardProcessor.NO_REWARDS);
  }

  @Test
  void configuredRewardUsesTheMainnetRules() {
    final BlockRewardProcessor configured = BlockRewardProcessor.of(BLOCK_REWARD);

    assertThat(configured).isInstanceOf(MainnetBlockRewardProcessor.class);
    assertThat(configured.getBlockReward()).isEqualTo(BLOCK_REWARD);
  }

  @Test
  void ommerSixGenerationsOldIsRewarded() {
    final MutableWorldState worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();

    assertThat(rewardWithOmmerAt(worldState, 94)).isTrue();

    // 2 ETH - 6 * (2 ETH / 8)
    assertThat(worldState.get(OMMER_COINBASE).getBalance())
        .isEqualTo(Wei.of(500_000_000_000_000_000L));
  }

  @Test
  void ommerOutsideSixGenerationsIsRejectedWithoutCreditingAnything() {
    // Seven blocks old, the same height, and ommers numbered above the block would get a negative
    // or overflowing reward.
    for (final long ommerNumber : new long[] {93, 100, 101, 107}) {
      final MutableWorldState worldState =
          InMemoryKeyValueStorageProvider.createInMemoryWorldState();

      assertThat(rewardWithOmmerAt(worldState, ommerNumber)).as("ommer %s", ommerNumber).isFalse();

      assertThat(worldState.get(BENEFICIARY)).as("ommer %s", ommerNumber).isNull();
      assertThat(worldState.get(OMMER_COINBASE)).as("ommer %s", ommerNumber).isNull();
    }
  }

  private boolean rewardWithOmmerAt(final MutableWorldState worldState, final long ommerNumber) {
    final BlockHeader header = new BlockHeaderTestFixture().number(100).buildHeader();
    final BlockHeader ommer =
        new BlockHeaderTestFixture().number(ommerNumber).coinbase(OMMER_COINBASE).buildHeader();
    return processor.rewardBeneficiaries(worldState, header, List.of(ommer), BENEFICIARY);
  }
}
