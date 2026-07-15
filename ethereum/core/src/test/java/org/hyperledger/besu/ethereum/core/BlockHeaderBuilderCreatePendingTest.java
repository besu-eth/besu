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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.DifficultyCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

public class BlockHeaderBuilderCreatePendingTest {

  private static final long PARENT_GAS_LIMIT = 30_000_000L;
  private static final long CONFIG_TARGET = 36_000_000L;
  private static final long FCU_TARGET = 45_000_000L;

  @Test
  public void usesFcuTargetGasLimitWhenPresent() {
    final AtomicLong observedTarget = new AtomicLong();
    final ProtocolSpec protocolSpec = stubProtocolSpec(observedTarget);
    final BlockHeader parent = parentHeader();
    final MiningConfiguration miningConfiguration = miningConfigurationWithTarget(CONFIG_TARGET);

    BlockHeaderBuilder.createPending(
        protocolSpec,
        parent,
        miningConfiguration,
        parent.getTimestamp() + 1,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(FCU_TARGET));

    assertThat(observedTarget.get()).isEqualTo(FCU_TARGET);
  }

  @Test
  public void fallsBackToMiningConfigurationWhenFcuTargetIsAbsent() {
    final AtomicLong observedTarget = new AtomicLong();
    final ProtocolSpec protocolSpec = stubProtocolSpec(observedTarget);
    final BlockHeader parent = parentHeader();
    final MiningConfiguration miningConfiguration = miningConfigurationWithTarget(CONFIG_TARGET);

    BlockHeaderBuilder.createPending(
        protocolSpec,
        parent,
        miningConfiguration,
        parent.getTimestamp() + 1,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    assertThat(observedTarget.get()).isEqualTo(CONFIG_TARGET);
  }

  @Test
  public void fallsBackToParentGasLimitWhenNoTargetConfigured() {
    final AtomicLong observedTarget = new AtomicLong();
    final ProtocolSpec protocolSpec = stubProtocolSpec(observedTarget);
    final BlockHeader parent = parentHeader();
    final MiningConfiguration miningConfiguration = MiningConfiguration.newDefault();

    BlockHeaderBuilder.createPending(
        protocolSpec,
        parent,
        miningConfiguration,
        parent.getTimestamp() + 1,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    assertThat(observedTarget.get()).isEqualTo(PARENT_GAS_LIMIT);
  }

  private static BlockHeader parentHeader() {
    return new BlockHeaderTestFixture()
        .number(100L)
        .timestamp(1_000L)
        .gasLimit(PARENT_GAS_LIMIT)
        .buildHeader();
  }

  private static MiningConfiguration miningConfigurationWithTarget(final long target) {
    final MiningConfiguration miningConfiguration = MiningConfiguration.newDefault();
    miningConfiguration.setTargetGasLimit(target);
    return miningConfiguration;
  }

  private static ProtocolSpec stubProtocolSpec(final AtomicLong observedTarget) {
    final GasLimitCalculator gasLimitCalculator =
        (currentGasLimit, targetGasLimit, newBlockNumber) -> {
          observedTarget.set(targetGasLimit);
          return targetGasLimit;
        };
    final DifficultyCalculator difficultyCalculator = (time, parent) -> BigInteger.ZERO;
    final FeeMarket feeMarket = mock(FeeMarket.class);
    when(feeMarket.implementsBaseFee()).thenReturn(false);

    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSpec.getGasLimitCalculator()).thenReturn(gasLimitCalculator);
    when(protocolSpec.getDifficultyCalculator()).thenReturn(difficultyCalculator);
    when(protocolSpec.getFeeMarket()).thenReturn(feeMarket);
    return protocolSpec;
  }
}
