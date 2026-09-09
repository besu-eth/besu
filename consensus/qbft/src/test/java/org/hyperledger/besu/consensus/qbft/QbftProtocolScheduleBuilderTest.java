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
package org.hyperledger.besu.consensus.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.JsonGenesisConfigOptions;
import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.ForkSpec;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

public class QbftProtocolScheduleBuilderTest {

  private final BftExtraDataCodec bftExtraDataCodec = mock(BftExtraDataCodec.class);
  private static final long BLOCK_GAS_LIMIT = 30_000_000L;

  @Test
  public void absentTransactionGasLimitDelegatesToMainnetCalculator() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(
            genesisConfig, List.of(new ForkSpec<>(0, JsonQbftConfigOptions.DEFAULT)));

    final ProtocolSpec spec = schedule.getByBlockNumberOrTimestamp(0, 0);
    // When per-tx gas limit cap is not configured, the mainnet calculator is used directly
    assertThat(spec.getGasLimitCalculator().transactionGasLimitCap()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void zeroTransactionGasLimitReturnsLongMaxValue() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final JsonQbftConfigOptions qbftConfig =
        new JsonQbftConfigOptions(
            JsonUtil.objectNodeFromMap(
                java.util.Map.of(JsonBftConfigOptions.TRANSACTION_GAS_LIMIT, 0)));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(genesisConfig, List.of(new ForkSpec<>(0, qbftConfig)));

    final ProtocolSpec spec = schedule.getByBlockNumberOrTimestamp(0, 0);
    assertThat(spec.getGasLimitCalculator().transactionGasLimitCap()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void positiveTransactionGasLimitReturnsConfiguredValue() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final JsonQbftConfigOptions qbftConfig =
        new JsonQbftConfigOptions(
            JsonUtil.objectNodeFromMap(
                java.util.Map.of(JsonBftConfigOptions.TRANSACTION_GAS_LIMIT, 8_000_000)));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(genesisConfig, List.of(new ForkSpec<>(0, qbftConfig)));

    final ProtocolSpec spec = schedule.getByBlockNumberOrTimestamp(0, 0);
    assertThat(spec.getGasLimitCalculator().transactionGasLimitCap()).isEqualTo(8_000_000L);
  }

  @Test
  public void negativeTransactionGasLimitThrowsException() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final JsonQbftConfigOptions qbftConfig =
        new JsonQbftConfigOptions(
            JsonUtil.objectNodeFromMap(
                java.util.Map.of(JsonBftConfigOptions.TRANSACTION_GAS_LIMIT, -1)));

    assertThatThrownBy(
            () -> createProtocolSchedule(genesisConfig, List.of(new ForkSpec<>(0, qbftConfig))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "config.bft.pertxgaslimit (-1) must be >= 0 and <= the genesis block gas limit");
  }

  @Test
  public void transactionGasLimitExceedingBlockGasLimitThrowsException() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final long cap = BLOCK_GAS_LIMIT + 1;
    final JsonQbftConfigOptions qbftConfig =
        new JsonQbftConfigOptions(
            JsonUtil.objectNodeFromMap(
                java.util.Map.of(JsonBftConfigOptions.TRANSACTION_GAS_LIMIT, cap)));

    assertThatThrownBy(
            () -> createProtocolSchedule(genesisConfig, List.of(new ForkSpec<>(0, qbftConfig))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "config.bft.pertxgaslimit ("
                + cap
                + ") must be >= 0 and <= the genesis block gas limit");
  }

  @Test
  public void blockReplayReturnsCorrectTransactionGasLimitPerFork() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final MutableQbftConfigOptions fork0 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork0.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    final MutableQbftConfigOptions fork10 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork10.setTransactionGasLimit(OptionalLong.of(16_000_000L));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(
            genesisConfig, List.of(new ForkSpec<>(0, fork0), new ForkSpec<>(10, fork10)));

    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(0, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(8_000_000L);
    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(9, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(8_000_000L);
    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(10, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(16_000_000L);
    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(20, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(16_000_000L);
  }

  @Test
  public void transactionGasLimitEqualToBlockGasLimitIsAllowed() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final JsonQbftConfigOptions qbftConfig =
        new JsonQbftConfigOptions(
            JsonUtil.objectNodeFromMap(
                java.util.Map.of(JsonBftConfigOptions.TRANSACTION_GAS_LIMIT, BLOCK_GAS_LIMIT)));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(genesisConfig, List.of(new ForkSpec<>(0, qbftConfig)));

    final ProtocolSpec spec = schedule.getByBlockNumberOrTimestamp(0, 0);
    assertThat(spec.getGasLimitCalculator().transactionGasLimitCap()).isEqualTo(BLOCK_GAS_LIMIT);
  }

  @Test
  public void zeroOnTransitionForkReturnsLongMaxValue() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final MutableQbftConfigOptions fork0 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork0.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    final MutableQbftConfigOptions fork10 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork10.setTransactionGasLimit(OptionalLong.of(0L));

    final BftProtocolSchedule schedule =
        createProtocolSchedule(
            genesisConfig, List.of(new ForkSpec<>(0, fork0), new ForkSpec<>(10, fork10)));

    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(10, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void negativeOnTransitionForkThrowsException() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final MutableQbftConfigOptions fork0 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork0.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    final MutableQbftConfigOptions fork10 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork10.setTransactionGasLimit(OptionalLong.of(-1L));

    assertThatThrownBy(
            () ->
                createProtocolSchedule(
                    genesisConfig, List.of(new ForkSpec<>(0, fork0), new ForkSpec<>(10, fork10))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be >= 0");
  }

  @Test
  public void exceedingBlockGasLimitOnTransitionForkThrowsException() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final MutableQbftConfigOptions fork0 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork0.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    final long cap = BLOCK_GAS_LIMIT + 1;
    final MutableQbftConfigOptions fork10 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork10.setTransactionGasLimit(OptionalLong.of(cap));

    assertThatThrownBy(
            () ->
                createProtocolSchedule(
                    genesisConfig, List.of(new ForkSpec<>(0, fork0), new ForkSpec<>(10, fork10))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be >= 0");
  }

  @Test
  public void forkOmittingKeyRetainsPriorValue() {
    final JsonGenesisConfigOptions genesisConfig =
        JsonGenesisConfigOptions.fromJsonObject(JsonUtil.objectNodeFromMap(java.util.Map.of()));

    final MutableQbftConfigOptions fork0 =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    fork0.setTransactionGasLimit(OptionalLong.of(8_000_000L));

    // fork10 does NOT set transactionGasLimit — should retain fork0's value (8_000_000)
    final BftProtocolSchedule schedule =
        createProtocolSchedule(
            genesisConfig,
            List.of(new ForkSpec<>(0, fork0), new ForkSpec<>(10, JsonQbftConfigOptions.DEFAULT)));

    assertThat(
            schedule
                .getByBlockNumberOrTimestamp(10, 0)
                .getGasLimitCalculator()
                .transactionGasLimitCap())
        .isEqualTo(8_000_000L);
  }

  @Test
  public void shanghaiQbftSpecRejectsByzantineInjectedWithdrawals() {
    // Regression test for GHSA-p4h2-gvh4-pv6j: verify that the BFT protocol schedule wires
    // NotApplicableWithdrawals correctly for Shanghai, and that the validator rejects a
    // Byzantine proposer's non-empty withdrawal list before it reaches WithdrawalsProcessor.
    final long shanghaiTime = 0;
    final ObjectNode genesisConfigNode = JsonUtil.createEmptyObjectNode();
    genesisConfigNode.put("shanghaitime", shanghaiTime);

    final BftProtocolSchedule schedule =
        createProtocolSchedule(
            JsonGenesisConfigOptions.fromJsonObject(genesisConfigNode),
            List.of(new ForkSpec<>(0, JsonQbftConfigOptions.DEFAULT)));

    final ProtocolSpec shanghaiSpec = schedule.getByBlockNumberOrTimestamp(1, shanghaiTime);
    final WithdrawalsValidator validator = shanghaiSpec.getWithdrawalsValidator();

    // Schedule must wire NotApplicableWithdrawals, not AllowedWithdrawals
    assertThat(validator).isInstanceOf(WithdrawalsValidator.NotApplicableWithdrawals.class);

    // Empty list (honest proposer) must be accepted
    assertThat(validator.validateWithdrawals(Optional.of(List.of()))).isTrue();

    // Byzantine proposer injects one withdrawal — must be rejected before execution
    final List<Withdrawal> injected =
        List.of(
            new Withdrawal(
                UInt64.valueOf(9001),
                UInt64.valueOf(313),
                Address.fromHexString("0x000000000000000000000000000000000000c0de"),
                GWei.of(11)));
    assertThat(validator.validateWithdrawals(Optional.of(injected))).isFalse();

    // Multiple injected withdrawals also rejected
    assertThat(
            validator.validateWithdrawals(Optional.of(List.of(injected.get(0), injected.get(0)))))
        .isFalse();
  }

  private BftProtocolSchedule createProtocolSchedule(
      final JsonGenesisConfigOptions genesisConfig, final List<ForkSpec<QbftConfigOptions>> forks) {
    return QbftProtocolScheduleBuilder.create(
        genesisConfig,
        new ForksSchedule<>(forks),
        false,
        bftExtraDataCodec,
        EvmConfiguration.DEFAULT,
        MiningConfiguration.MINING_DISABLED,
        new BadBlockManager(),
        false,
        BalConfiguration.DEFAULT,
        new NoOpMetricsSystem(),
        BLOCK_GAS_LIMIT);
  }
}
