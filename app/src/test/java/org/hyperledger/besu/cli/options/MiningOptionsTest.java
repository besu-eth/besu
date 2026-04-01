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
package org.hyperledger.besu.cli.options;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.Unstable.DEFAULT_POS_BLOCK_CREATION_MAX_TIME;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.Unstable;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MiningOptionsTest extends AbstractCLIOptionsTest<MiningConfiguration, MiningOptions> {

  @Test
  public void blockProducingOptionsSucceedWhenPoAQBFT() throws IOException {

    final Path genesisFileQBFT = createFakeGenesisFile(VALID_GENESIS_QBFT_POST_LONDON);
    internalTestSuccess(
        miningOpts ->
            verify(mockLogger, atMost(0))
                .warn(
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture()),
        "--genesis-file",
        genesisFileQBFT.toString(),
        "--min-gas-price",
        "42",
        "--miner-extra-data",
        "0x1122334455667788990011223344556677889900112233445566778899001122");
  }

  @Test
  public void blockProducingOptionsSucceedWhenPoAIBFT2() throws IOException {

    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);
    internalTestSuccess(
        miningOpts ->
            verify(mockLogger, atMost(0))
                .warn(
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture()),
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--min-gas-price",
        "42",
        "--miner-extra-data",
        "0x1122334455667788990011223344556677889900112233445566778899001122");
  }

  @Test
  public void blockProducingOptionsSucceedWhenMergeEnabled() {

    internalTestSuccess(
        miningOpt ->
            verify(mockLogger, atMost(0))
                .warn(
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture()),
        "--min-gas-price",
        "42",
        "--miner-extra-data",
        "0x1122334455667788990011223344556677889900112233445566778899001122");
  }

  @Test
  public void minGasPriceSucceedsWhenPoAQBFT() throws IOException {
    final Path genesisFileQBFT = createFakeGenesisFile(VALID_GENESIS_QBFT_POST_LONDON);
    internalTestSuccess(
        miningOpt ->
            verify(mockLogger, atMost(0))
                .warn(
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture()),
        "--genesis-file",
        genesisFileQBFT.toString(),
        "--min-gas-price",
        "0");
  }

  @Test
  public void minGasPriceSucceedsWhenPoAIBFT2() throws IOException {
    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);

    internalTestSuccess(
        miningOpt ->
            verify(mockLogger, atMost(0))
                .warn(
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture(),
                    stringArgumentCaptor.capture()),
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--min-gas-price",
        "0");
  }

  @Test
  public void miningParametersAreCaptured() {
    final String extraDataString =
        "0x1122334455667788990011223344556677889900112233445566778899001122";
    internalTestSuccess(
        miningParams -> {
          assertThat(miningParams.getMinTransactionGasPrice()).isEqualTo(Wei.of(15));
          assertThat(miningParams.getExtraData()).isEqualTo(Bytes.fromHexString(extraDataString));
        },
        "--min-gas-price=15",
        "--miner-extra-data=" + extraDataString);
  }

  @Test
  public void targetGasLimitIsEnabledWhenSpecified() {
    internalTestSuccess(
        miningParams ->
            assertThat(miningParams.getTargetGasLimit().getAsLong()).isEqualTo(10_000_000L),
        "--target-gas-limit=10000000");
  }

  @Test
  public void posBlockCreationMaxTimeDefaultValue() {
    internalTestSuccess(
        miningParams ->
            assertThat(miningParams.getUnstable().getPosBlockCreationMaxTime())
                .isEqualTo(DEFAULT_POS_BLOCK_CREATION_MAX_TIME));
  }

  @Test
  public void posBlockCreationMaxTimeOption() {
    internalTestSuccess(
        miningParams ->
            assertThat(miningParams.getUnstable().getPosBlockCreationMaxTime()).isEqualTo(7000L),
        "--Xpos-block-creation-max-time",
        "7000");
  }

  @Test
  public void posBlockCreationMaxTimeOutOfAllowedRange() {
    internalTestFailure(
        "--Xpos-block-creation-max-time must be positive and ≤ 12000",
        "--Xpos-block-creation-max-time",
        "17000");
  }

  @Test
  public void blockTxsSelectionMaxTimeDefaultValue() {
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getPosBlockTxsSelectionMaxTime())
                .isEqualTo(DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME));
  }

  @Test
  public void blockTxsSelectionMaxTimeOption() {
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getBlockTxsSelectionMaxTime(true))
                .isEqualTo(Duration.ofMillis(1700L)),
        "--block-txs-selection-max-time",
        "1700");
  }

  @Test
  public void blockTxsSelectionMaxTimeIncompatibleWithoutPoSTransition() throws IOException {
    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);
    internalTestFailure(
        "--block-txs-selection-max-time can only be used on networks with PoS support in the genesis file, see --poa-block-txs-selection-max-time instead",
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--block-txs-selection-max-time",
        "2");
  }

  @Test
  public void blockTxsSelectionMaxTimeRequiresPoSTransition() throws IOException {
    final Path genesisFilePoS = createFakeGenesisFile(VALID_GENESIS_CLIQUE_WITH_POS_TRANSITION);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getPosBlockTxsSelectionMaxTime())
                .isEqualTo(PositiveNumber.fromInt(2)),
        "--genesis-file",
        genesisFilePoS.toString(),
        "--block-txs-selection-max-time",
        "2");
  }

  @Test
  public void bothBlockTxsSelectionMaxTimeOptionsAllowedWhenPoSTransitionIsPresent_PreTransition()
      throws IOException {
    final Path genesisFilePoS = createFakeGenesisFile(VALID_GENESIS_CLIQUE_WITH_POS_TRANSITION);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams -> {
          assertThat(miningParams.getPosBlockTxsSelectionMaxTime())
              .isEqualTo(PositiveNumber.fromInt(2000));
          assertThat(miningParams.getPoaBlockTxsSelectionMaxTime())
              .isEqualTo(PositiveNumber.fromInt(80));
          // pre transition PoA conf is used
          assertThat(miningParams.getBlockTxsSelectionMaxTime(false))
              .isEqualTo(Duration.ofSeconds(4));
        },
        "--genesis-file",
        genesisFilePoS.toString(),
        "--block-txs-selection-max-time",
        "2000",
        "--poa-block-txs-selection-max-time",
        "80");
  }

  @Test
  public void bothBlockTxsSelectionMaxTimeOptionsAllowedWhenPoSTransitionIsPresent_PostTransition()
      throws IOException {
    final Path genesisFilePoS = createFakeGenesisFile(VALID_GENESIS_CLIQUE_WITH_POS_TRANSITION);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams -> {
          assertThat(miningParams.getPosBlockTxsSelectionMaxTime())
              .isEqualTo(PositiveNumber.fromInt(2000));
          assertThat(miningParams.getPoaBlockTxsSelectionMaxTime())
              .isEqualTo(PositiveNumber.fromInt(80));
          // post transition nonPoA conf is used
          assertThat(miningParams.getBlockTxsSelectionMaxTime(true))
              .isEqualTo(Duration.ofSeconds(2));
        },
        "--genesis-file",
        genesisFilePoS.toString(),
        "--block-txs-selection-max-time",
        "2000",
        "--poa-block-txs-selection-max-time",
        "80");
  }

  @Test
  public void poaBlockTxsSelectionMaxTimeDefaultValue() {
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getPoaBlockTxsSelectionMaxTime())
                .isEqualTo(DEFAULT_POA_BLOCK_TXS_SELECTION_MAX_TIME));
  }

  @Test
  public void poaBlockTxsSelectionMaxTimeOption() throws IOException {
    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getPoaBlockTxsSelectionMaxTime())
                .isEqualTo(PositiveNumber.fromInt(80)),
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--poa-block-txs-selection-max-time",
        "80");
  }

  @Test
  public void poaBlockTxsSelectionMaxTimeOptionOver100Percent() throws IOException {
    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams -> {
          assertThat(miningParams.getPoaBlockTxsSelectionMaxTime())
              .isEqualTo(PositiveNumber.fromInt(200));
          assertThat(miningParams.getBlockTxsSelectionMaxTime(false))
              .isEqualTo(Duration.ofSeconds(POA_BLOCK_PERIOD_SECONDS * 2));
        },
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--poa-block-txs-selection-max-time",
        "200");
  }

  @Test
  public void poaBlockTxsSelectionMaxTimeOnlyCompatibleWithPoaNetworks() {
    internalTestFailure(
        "--poa-block-txs-selection-max-time can be only used with PoA networks, see --block-txs-selection-max-time instead",
        "--poa-block-txs-selection-max-time",
        "90");
  }

  @Test
  public void pluginBlockTxsSelectionMaxTimeDefaultValue() {
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(miningParams.getPluginBlockTxsSelectionMaxTime())
                .isEqualTo(DEFAULT_PLUGIN_BLOCK_TXS_SELECTION_MAX_TIME));
  }

  @Test
  public void pluginBlockTxsSelectionMaxTimeOptionOnPoaNetwork() throws IOException {
    final Path genesisFileIBFT2 = createFakeGenesisFile(VALID_GENESIS_IBFT2_POST_LONDON);
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(
                    miningParams.getPluginTxsSelectionMaxTime(
                        miningParams.getBlockTxsSelectionMaxTime(false)))
                .isEqualTo(Duration.ofSeconds(1)),
        "--genesis-file",
        genesisFileIBFT2.toString(),
        "--poa-block-txs-selection-max-time",
        "80",
        "--plugin-block-txs-selection-max-time",
        "25");
  }

  @Test
  public void pluginBlockTxsSelectionMaxTimeOptionNonPoaNetwork() {
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams ->
            assertThat(
                    miningParams.getPluginTxsSelectionMaxTime(
                        miningParams.getBlockTxsSelectionMaxTime(false)))
                .isEqualTo(Duration.ofMillis(800)),
        "--block-txs-selection-max-time",
        "2000",
        "--plugin-block-txs-selection-max-time",
        "40");
  }

  @Test
  public void extraDataDefaultValueIsBesuVersion() {
    // Detect also a string with null at the end (case in which metadata are missing because the
    // JAR is not generated)
    final var expectedRegex = "besu (\\d+\\.\\d+(\\.\\d+|\\-develop\\-\\p{XDigit}+)|null)";
    internalTestSuccess(
        this::runtimeConfiguration,
        miningParams -> {
          assertThat(new String(miningParams.getExtraData().toArray(), StandardCharsets.UTF_8))
              .matches(expectedRegex);
        });
  }

  @Test
  public void maxBlobsDefaultValue() {
    internalTestSuccess(
        miningParams -> assertThat(miningParams.getMaxBlobsPerTransaction()).isEmpty());
  }

  @Test
  public void maxBlobsOption() {
    internalTestSuccess(
        miningParams -> assertThat(miningParams.getMaxBlobsPerTransaction()).hasValue(3),
        "--max-blobs-per-transaction",
        "3");
  }

  @Test
  public void maxBlobsOptionWithZero() {
    internalTestSuccess(
        miningParams -> assertThat(miningParams.getMaxBlobsPerTransaction()).hasValue(0),
        "--max-blobs-per-transaction",
        "0");
  }

  @Test
  public void maxBlobsOptionWithNegativeValue() {
    internalTestFailure(
        "--max-blobs-per-transaction must be a non-negative value",
        "--max-blobs-per-transaction",
        "-9");
  }

  @Override
  protected MiningConfiguration createDefaultDomainObject() {
    return MiningConfiguration.newDefault();
  }

  @Override
  protected MiningConfiguration createCustomizedDomainObject() {
    return ImmutableMiningConfiguration.builder()
        .mutableInitValues(
            MutableInitValues.builder().extraData(Bytes.fromHexString("0xabc321")).build())
        .unstable(Unstable.builder().posBlockCreationMaxTime(1000).build())
        .build();
  }

  @Override
  protected MiningOptions optionsFromDomainObject(final MiningConfiguration domainObject) {
    return MiningOptions.fromConfig(domainObject);
  }

  @Override
  protected MiningOptions getOptionsFromBesuCommand(final TestBesuCommand besuCommand) {
    return besuCommand.getMiningOptions();
  }

  @Override
  protected String[] getNonOptionFields() {
    return new String[] {"transactionSelectionService"};
  }

  private MiningConfiguration runtimeConfiguration(
      final TestBesuCommand besuCommand, final MiningConfiguration miningConfiguration) {
    if (besuCommand.getGenesisConfigOptionsSupplier().get().isPoa()) {
      miningConfiguration.setBlockPeriodSeconds(POA_BLOCK_PERIOD_SECONDS);
    }
    return miningConfiguration;
  }
}
