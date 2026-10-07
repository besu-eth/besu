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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.ImmutableCliqueConfigOptions;
import org.hyperledger.besu.config.JsonBftConfigOptions;
import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestContractAddresses;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MainnetProtocolSpecsTest {

  @Mock(lenient = true)
  private GenesisConfigOptions genesisConfigOptions;

  @Mock(lenient = true)
  private BlobSchedule pragueBlobSchedule;

  @Mock(lenient = true)
  private BlobScheduleOptions blobScheduleOptions;

  @Mock private ProtocolSchedule protocolSchedule;

  @Mock private BadBlockManager badBlockManager;

  private final EvmConfiguration evmConfiguration = EvmConfiguration.DEFAULT;
  private final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
  private final Optional<BigInteger> chainId = Optional.of(BigInteger.ONE);
  private final boolean enableRevertReason = false;
  private final boolean isParallelTxProcessingEnabled = false;
  private final BalConfiguration balConfiguration = BalConfiguration.DEFAULT;
  private final long londonForkBlockNumber = 0L;

  @BeforeEach
  public void setUp() {
    when(genesisConfigOptions.getLondonBlockNumber())
        .thenReturn(OptionalLong.of(londonForkBlockNumber));
    when(genesisConfigOptions.getBlobScheduleOptions())
        .thenReturn(Optional.of(blobScheduleOptions));
    when(blobScheduleOptions.getPrague()).thenReturn(Optional.of(pragueBlobSchedule));
    when(pragueBlobSchedule.getTarget()).thenReturn(6);
    when(pragueBlobSchedule.getMax()).thenReturn(9);
    when(pragueBlobSchedule.getBaseFeeUpdateFraction()).thenReturn(1234);
  }

  @Test
  public void pragueDefinitionShouldDefaultWithdrawalRequestContractAddressWhenMissing() {
    // Given
    when(genesisConfigOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(genesisConfigOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(genesisConfigOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.empty());

    // When/Then
    assertThat(pragueRequestContractConfigs())
        .containsEntry(
            "WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS",
            RequestContractAddresses.DEFAULT_WITHDRAWAL_REQUEST_CONTRACT_ADDRESS.toHexString())
        .containsEntry("CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS", Address.ZERO.toHexString());
  }

  @Test
  public void pragueDefinitionShouldThrowExceptionWhenDepositContractAddressIsMissing() {
    // Given
    when(genesisConfigOptions.getDepositContractAddress()).thenReturn(Optional.empty());
    when(genesisConfigOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));
    when(genesisConfigOptions.getWithdrawalRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));

    // When/Then
    assertThatExceptionOfType(NoSuchElementException.class)
        .isThrownBy(
            () ->
                MainnetProtocolSpecs.pragueDefinition(
                    chainId,
                    enableRevertReason,
                    genesisConfigOptions,
                    evmConfiguration,
                    MiningConfiguration.newDefault(),
                    isParallelTxProcessingEnabled,
                    balConfiguration,
                    metricsSystem))
        .withMessageContaining("Deposit Contract Address not found");
  }

  @Test
  public void pragueDefinitionShouldDefaultConsolidationRequestContractAddressWhenMissing() {
    // Given
    when(genesisConfigOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(genesisConfigOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.empty());
    when(genesisConfigOptions.getWithdrawalRequestContractAddress())
        .thenReturn(Optional.of(Address.ZERO));

    // When/Then
    assertThat(pragueRequestContractConfigs())
        .containsEntry("WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS", Address.ZERO.toHexString())
        .containsEntry(
            "CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS",
            RequestContractAddresses.DEFAULT_CONSOLIDATION_REQUEST_CONTRACT_ADDRESS.toHexString());
  }

  @Test
  public void pragueDefinitionShouldSkipRequestProcessorsForPoAWithOnlyDepositContractAddress() {
    // Given
    when(genesisConfigOptions.isQbft()).thenReturn(true);
    when(genesisConfigOptions.getQbftConfigOptions())
        .thenReturn(new JsonQbftConfigOptions(JsonNodeFactory.instance.objectNode()));
    when(genesisConfigOptions.getDepositContractAddress()).thenReturn(Optional.of(Address.ZERO));
    when(genesisConfigOptions.getConsolidationRequestContractAddress())
        .thenReturn(Optional.empty());
    when(genesisConfigOptions.getWithdrawalRequestContractAddress()).thenReturn(Optional.empty());

    // When/Then
    assertThat(pragueRequestContractConfigs()).isEmpty();
  }

  private Map<String, String> pragueRequestContractConfigs() {
    return MainnetProtocolSpecs.pragueDefinition(
            chainId,
            enableRevertReason,
            genesisConfigOptions,
            evmConfiguration,
            MiningConfiguration.newDefault(),
            isParallelTxProcessingEnabled,
            balConfiguration,
            metricsSystem)
        .badBlocksManager(badBlockManager)
        .build(protocolSchedule)
        .getRequestProcessorCoordinator()
        .orElseThrow()
        .getContractConfigs();
  }

  @Test
  public void genesisCliqueBlockPeriodIsReturnedAsSlotTime() {
    when(genesisConfigOptions.isClique()).thenReturn(true);
    when(genesisConfigOptions.getCliqueConfigOptions())
        .thenReturn(
            ImmutableCliqueConfigOptions.builder()
                .blockPeriodSeconds(2)
                .createEmptyBlocks(false)
                .epochLength(30)
                .build());

    final var protocolSpec =
        MainnetProtocolSpecs.frontierDefinition(
                genesisConfigOptions,
                evmConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration()).hasSeconds(2);
  }

  @Test
  public void genesisQbftBlockPeriodIsReturnedAsSlotTime() {

    final var jsonQbftOptions = new JsonQbftConfigOptions(JsonNodeFactory.instance.objectNode());

    when(genesisConfigOptions.isQbft()).thenReturn(true);
    when(genesisConfigOptions.getQbftConfigOptions()).thenReturn(jsonQbftOptions);

    final var protocolSpec =
        MainnetProtocolSpecs.frontierDefinition(
                genesisConfigOptions,
                evmConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration()).hasSeconds(1);
  }

  @Test
  public void genesisIbft2BlockPeriodIsReturnedAsSlotTime() {

    final var jsonBftOptions = new JsonBftConfigOptions(JsonNodeFactory.instance.objectNode());

    when(genesisConfigOptions.isIbft2()).thenReturn(true);
    when(genesisConfigOptions.getBftConfigOptions()).thenReturn(jsonBftOptions);

    final var protocolSpec =
        MainnetProtocolSpecs.frontierDefinition(
                genesisConfigOptions,
                evmConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration()).hasSeconds(1);
  }

  @Test
  public void defaultToPowEstimatedSlotTimePrePos() {
    final var protocolSpec =
        MainnetProtocolSpecs.frontierDefinition(
                genesisConfigOptions,
                evmConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration()).hasSeconds(13);
  }

  @Test
  public void posSlotTimeIsReturnedFromParisOnward() {
    final var miningConfiguration = MiningConfiguration.newDefault();
    final var protocolSpec =
        MainnetProtocolSpecs.parisDefinition(
                Optional.of(BigInteger.ONE),
                true,
                genesisConfigOptions,
                evmConfiguration,
                miningConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration())
        .hasSeconds(miningConfiguration.getUnstable().getPosSlotDuration());
  }

  @Test
  public void changeDefaultPosSlotTimeByConfiguration() {
    final var miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .unstable(ImmutableMiningConfiguration.Unstable.builder().posSlotDuration(2).build())
            .build();
    final var protocolSpec =
        MainnetProtocolSpecs.cancunDefinition(
                Optional.of(BigInteger.ONE),
                true,
                genesisConfigOptions,
                evmConfiguration,
                miningConfiguration,
                isParallelTxProcessingEnabled,
                balConfiguration,
                metricsSystem)
            .badBlocksManager(badBlockManager)
            .build(protocolSchedule);

    assertThat(protocolSpec.getSlotDuration())
        .hasSeconds(miningConfiguration.getUnstable().getPosSlotDuration());
  }
}
