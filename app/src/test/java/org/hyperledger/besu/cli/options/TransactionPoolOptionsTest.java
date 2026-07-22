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
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LAYERED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.LEGACY;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration.Implementation.SEQUENCED;

import org.hyperledger.besu.cli.converter.DurationMillisConverter;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.util.number.Percentage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionPoolOptionsTest
    extends AbstractCLIOptionsTest<TransactionPoolConfiguration, TransactionPoolOptions> {

  @Test
  public void strictTxReplayProtection_enabled() {
    internalTestSuccess(
        config -> assertThat(config.getStrictTransactionReplayProtectionEnabled()).isTrue(),
        "--strict-tx-replay-protection-enabled");
  }

  @Test
  public void strictTxReplayProtection_enabledWithBooleanArg() {
    internalTestSuccess(
        config -> assertThat(config.getStrictTransactionReplayProtectionEnabled()).isTrue(),
        "--strict-tx-replay-protection-enabled=true");
  }

  @Test
  public void strictTxReplayProtection_disabled() {
    internalTestSuccess(
        config -> assertThat(config.getStrictTransactionReplayProtectionEnabled()).isFalse(),
        "--strict-tx-replay-protection-enabled=false");
  }

  @Test
  public void strictTxReplayProtection_default() {
    internalTestSuccess(
        config -> assertThat(config.getStrictTransactionReplayProtectionEnabled()).isFalse());
  }

  @Test
  public void pendingTransactionRetentionPeriodLegacy() {
    final int pendingTxRetentionHours = 999;
    internalTestSuccess(
        config ->
            assertThat(config.getPendingTxRetentionPeriod()).isEqualTo(pendingTxRetentionHours),
        "--tx-pool-retention-hours",
        String.valueOf(pendingTxRetentionHours),
        "--tx-pool=legacy");
  }

  @Test
  public void pendingTransactionRetentionPeriodSequenced() {
    final int pendingTxRetentionHours = 999;
    internalTestSuccess(
        config ->
            assertThat(config.getPendingTxRetentionPeriod()).isEqualTo(pendingTxRetentionHours),
        "--tx-pool-retention-hours",
        String.valueOf(pendingTxRetentionHours),
        "--tx-pool=sequenced");
  }

  @Test
  public void noLocalPriorityDefault() {
    internalTestSuccess(config -> assertThat(config.getNoLocalPriority()).isFalse());
  }

  @Test
  public void noLocalPriorityOn() {
    internalTestSuccess(
        config -> assertThat(config.getNoLocalPriority()).isTrue(),
        "--tx-pool-no-local-priority=true");
  }

  @Test
  public void noLocalPriorityOff() {
    internalTestSuccess(
        config -> assertThat(config.getNoLocalPriority()).isFalse(),
        "--tx-pool-no-local-priority=false");
  }

  @Test
  public void saveToFileDisabledByDefault() {
    internalTestSuccess(config -> assertThat(config.getEnableSaveRestore()).isFalse());
  }

  @Test
  public void saveToFileEnabledDefaultPath() {
    internalTestSuccess(
        config -> assertThat(config.getEnableSaveRestore()).isTrue(),
        "--tx-pool-enable-save-restore=true");
  }

  @Test
  public void saveToFileEnabledCustomPath() {
    internalTestSuccess(
        config -> {
          assertThat(config.getEnableSaveRestore()).isTrue();
          assertThat(config.getSaveFile()).hasName("my.save.file");
        },
        "--tx-pool-enable-save-restore=true",
        "--tx-pool-save-file=my.save.file");
  }

  @Test
  public void senderLimited_derived() {
    internalTestSuccess(
        config -> assertThat(config.getTxPoolMaxFutureTransactionByAccount()).isEqualTo(9),
        "--tx-pool-limit-by-account-percentage=0.002",
        "--tx-pool=legacy");
  }

  @Test
  public void senderLimitedFloor_derived() {
    internalTestSuccess(
        config -> assertThat(config.getTxPoolMaxFutureTransactionByAccount()).isEqualTo(1),
        "--tx-pool-limit-by-account-percentage=0.0001",
        "--tx-pool=legacy");
  }

  @Test
  public void senderLimitedCeiling_violated() {
    internalTestFailure(
        "Invalid value for option '--tx-pool-limit-by-account-percentage'",
        "--tx-pool-limit-by-account-percentage=1.00002341",
        "--tx-pool=legacy");
  }

  @Test
  public void priceBump() {
    final Percentage priceBump = Percentage.fromInt(13);
    internalTestSuccess(
        config -> assertThat(config.getPriceBump()).isEqualTo(priceBump),
        "--tx-pool-price-bump",
        priceBump.toString());
  }

  @Test
  public void invalidPriceBumpShouldFail() {
    internalTestFailure(
        "Invalid value: 101, should be a number between 0 and 100 inclusive",
        "--tx-pool-price-bump",
        "101");
  }

  @Test
  public void blobPriceBump() {
    final Percentage blobPriceBump = Percentage.fromInt(50);
    internalTestSuccess(
        config -> assertThat(config.getBlobPriceBump()).isEqualTo(blobPriceBump),
        "--tx-pool-blob-price-bump",
        blobPriceBump.toString());
  }

  @Test
  public void invalidBlobPriceBumpShouldFail() {
    internalTestFailure(
        "Invalid value: 101, should be a number between 0 and 100 inclusive",
        "--tx-pool-blob-price-bump",
        "101");
  }

  @Test
  public void defaultBlobPriceBump() {
    internalTestSuccess(
        config ->
            assertThat(config.getBlobPriceBump())
                .isEqualTo(TransactionPoolConfiguration.DEFAULT_BLOB_PRICE_BUMP));
  }

  @Test
  public void txFeeCap() {
    final Wei txFeeCap = Wei.fromEth(2);
    internalTestSuccess(
        config -> assertThat(config.getTxFeeCap()).isEqualTo(txFeeCap),
        "--rpc-tx-feecap",
        OptionParser.format(txFeeCap));
  }

  @Test
  public void invalidTxFeeCapShouldFail() {
    internalTestFailure(
        "Invalid value for option '--rpc-tx-feecap'",
        "cannot convert 'abcd' to Wei",
        "--rpc-tx-feecap",
        "abcd");
  }

  @Test
  public void p2pTxFeeCap() {
    final Wei p2pTxFeeCap = Wei.fromEth(2);
    internalTestSuccess(
        config -> assertThat(config.getP2pTxFeeCap()).isEqualTo(p2pTxFeeCap),
        "--p2p-tx-feecap",
        OptionParser.format(p2pTxFeeCap));
  }

  @Test
  public void p2pTxFeeCapDefaultsToOneEth() {
    internalTestSuccess(config -> assertThat(config.getP2pTxFeeCap()).isEqualTo(Wei.fromEth(1)));
  }

  @Test
  public void invalidP2pTxFeeCapShouldFail() {
    internalTestFailure(
        "Invalid value for option '--p2p-tx-feecap'",
        "cannot convert 'abcd' to Wei",
        "--p2p-tx-feecap",
        "abcd");
  }

  @Test
  public void selectLayeredImplementationByDefault() {
    internalTestSuccess(config -> assertThat(config.getTxPoolImplementation()).isEqualTo(LAYERED));
  }

  @Test
  public void selectLayeredImplementationByArg() {
    internalTestSuccess(
        config -> assertThat(config.getTxPoolImplementation()).isEqualTo(LAYERED),
        "--tx-pool=layered");
  }

  @Test
  public void selectLegacyImplementationByArg() {
    internalTestSuccess(
        config -> assertThat(config.getTxPoolImplementation()).isEqualTo(LEGACY),
        "--tx-pool=legacy");
  }

  @Test
  public void selectSequencedImplementationByArg() {
    internalTestSuccess(
        config -> assertThat(config.getTxPoolImplementation()).isEqualTo(SEQUENCED),
        "--tx-pool=sequenced");
  }

  @Test
  public void failIfLegacyOptionsWhenLayeredSelectedByDefault() {
    internalTestFailure(
        "Could not use legacy or sequenced transaction pool options with layered implementation",
        "--tx-pool-max-size=1000");
  }

  @Test
  public void failIfLegacyOptionsWhenLayeredSelectedByArg() {
    internalTestFailure(
        "Could not use legacy or sequenced transaction pool options with layered implementation",
        "--tx-pool=layered",
        "--tx-pool-max-size=1000");
  }

  @Test
  public void failIfLayeredOptionsWhenLegacySelectedByArg() {
    internalTestFailure(
        "Could not use layered transaction pool options with legacy or sequenced implementation",
        "--tx-pool=legacy",
        "--tx-pool-max-prioritized=1000");
  }

  @Test
  public void failIfLayeredOptionsWhenSequencedSelectedByArg() {
    internalTestFailure(
        "Could not use layered transaction pool options with legacy or sequenced implementation",
        "--tx-pool=sequenced",
        "--tx-pool-max-prioritized=1000");
  }

  @Test
  public void byDefaultNoPrioritySenders() {
    internalTestSuccess(config -> assertThat(config.getPrioritySenders()).isEmpty());
  }

  @Test
  public void onePrioritySenderWorks() {
    final Address prioritySender = Address.fromHexString("0xABC123");
    internalTestSuccess(
        config -> assertThat(config.getPrioritySenders()).containsExactly(prioritySender),
        "--tx-pool-priority-senders",
        prioritySender.getBytes().toHexString());
  }

  @Test
  public void morePrioritySendersWorks() {
    final Address prioritySender1 = Address.fromHexString("0xABC123");
    final Address prioritySender2 = Address.fromHexString("0xDEF456");
    final Address prioritySender3 = Address.fromHexString("0x789000");
    internalTestSuccess(
        config ->
            assertThat(config.getPrioritySenders())
                .containsExactly(prioritySender1, prioritySender2, prioritySender3),
        "--tx-pool-priority-senders",
        prioritySender1.getBytes().toHexString()
            + ","
            + prioritySender2.getBytes().toHexString()
            + ","
            + prioritySender3.getBytes().toHexString());
  }

  @Test
  public void atLeastOnePrioritySenders() {
    internalTestFailure(
        "Missing required parameter for option '--tx-pool-priority-senders' at index 0 (Comma separated list of addresses)",
        "--tx-pool-priority-senders");
  }

  @Test
  public void malformedListOfPrioritySenders() {
    final Address prioritySender1 = Address.fromHexString("0xABC123");
    final Address prioritySender2 = Address.fromHexString("0xDEF456");
    final Address prioritySender3 = Address.fromHexString("0x789000");
    internalTestFailure(
        "Invalid value for option '--tx-pool-priority-senders' at index 0 (Comma separated list of addresses): "
            + "cannot convert '0x0000000000000000000000000000000000abc123;0x0000000000000000000000000000000000def456' "
            + "to Address (java.lang.IllegalArgumentException: Invalid odd-length hex binary representation)",
        "--tx-pool-priority-senders",
        prioritySender1.getBytes().toHexString()
            + ";"
            + prioritySender2.getBytes().toHexString()
            + ","
            + prioritySender3.getBytes().toHexString());
  }

  @Test
  public void txMessageKeepAliveSeconds() {
    final int txMessageKeepAliveSeconds = 999;
    internalTestSuccess(
        config ->
            assertThat(config.getUnstable().getTxMessageKeepAliveSeconds())
                .isEqualTo(txMessageKeepAliveSeconds),
        "--Xincoming-tx-messages-keep-alive-seconds",
        String.valueOf(txMessageKeepAliveSeconds));
  }

  @Test
  public void txMessageKeepAliveSecondsWithInvalidInputShouldFail() {
    internalTestFailure(
        "Invalid value for option '--Xincoming-tx-messages-keep-alive-seconds': 'acbd' is not an int",
        "--Xincoming-tx-messages-keep-alive-seconds",
        "acbd");
  }

  @Test
  public void eth65TrxAnnouncedBufferingPeriod() {
    final Duration eth65TrxAnnouncedBufferingPeriod = Duration.ofMillis(999);
    internalTestSuccess(
        config ->
            assertThat(config.getUnstable().getEth65TrxAnnouncedBufferingPeriod())
                .isEqualTo(eth65TrxAnnouncedBufferingPeriod),
        "--Xeth65-tx-announced-buffering-period-milliseconds",
        new DurationMillisConverter().format(eth65TrxAnnouncedBufferingPeriod));
  }

  @Test
  public void eth65TrxAnnouncedBufferingPeriodWithInvalidInputShouldFail() {
    internalTestFailure(
        "Invalid value for option '--Xeth65-tx-announced-buffering-period-milliseconds': cannot convert 'acbd' to Duration (org.hyperledger.besu.cli.converter.exception.DurationConversionException: 'acbd' is not a long)",
        "--Xeth65-tx-announced-buffering-period-milliseconds",
        "acbd");
  }

  @Test
  public void eth65TrxAnnouncedBufferingPeriodWithInvalidInputShouldFail2() {
    internalTestFailure(
        "Invalid value for option '--Xeth65-tx-announced-buffering-period-milliseconds': cannot convert '-1' to Duration (org.hyperledger.besu.cli.converter.exception.DurationConversionException: negative value '-1' is not allowed)",
        "--Xeth65-tx-announced-buffering-period-milliseconds",
        "-1");
  }

  @Test
  public void maxPrioritizedTxsPerType() {
    final int maxBlobs = 2;
    final int maxFrontier = 200;
    internalTestSuccess(
        config -> {
          assertThat(config.getMaxPrioritizedTransactionsByType().get(TransactionType.BLOB))
              .isEqualTo(maxBlobs);
          assertThat(config.getMaxPrioritizedTransactionsByType().get(TransactionType.FRONTIER))
              .isEqualTo(maxFrontier);
        },
        "--tx-pool-max-prioritized-by-type",
        "BLOB=" + maxBlobs + ",FRONTIER=" + maxFrontier);
  }

  @Test
  public void maxPrioritizedTxsPerTypeConfigFile() throws IOException {
    final int maxBlobs = 2;
    final int maxFrontier = 200;
    final Path tempConfigFilePath =
        createTempFile(
            "config",
            String.format(
                """
    tx-pool-max-prioritized-by-type=["BLOB=%s","FRONTIER=%s"]
    """,
                maxBlobs, maxFrontier));
    internalTestSuccess(
        config -> {
          assertThat(config.getMaxPrioritizedTransactionsByType().get(TransactionType.BLOB))
              .isEqualTo(maxBlobs);
          assertThat(config.getMaxPrioritizedTransactionsByType().get(TransactionType.FRONTIER))
              .isEqualTo(maxFrontier);
        },
        "--config-file",
        tempConfigFilePath.toString());
  }

  @Test
  public void maxPrioritizedTxsPerTypeWrongTxType() {
    internalTestFailure(
        "Invalid value for option '--tx-pool-max-prioritized-by-type' (MAP<TYPE,INTEGER>): expected one of [FRONTIER, ACCESS_LIST, EIP1559, BLOB, DELEGATE_CODE] (case-insensitive) but was 'WRONG_TYPE'",
        "--tx-pool-max-prioritized-by-type",
        "WRONG_TYPE=1");
  }

  @Test
  public void minScoreWorks() {
    final byte minScore = -10;
    internalTestSuccess(
        config -> assertThat(config.getMinScore()).isEqualTo(minScore),
        "--tx-pool-min-score",
        Byte.toString(minScore));
  }

  @Test
  public void minScoreWorksConfigFile() throws IOException {
    final byte minScore = -10;
    final Path tempConfigFilePath =
        createTempFile(
            "config",
            String.format(
                """
              tx-pool-min-score=%s
              """,
                minScore));

    internalTestSuccess(
        config -> assertThat(config.getMinScore()).isEqualTo(minScore),
        "--config-file",
        tempConfigFilePath.toString());
  }

  @Test
  public void minScoreNonByteValueReturnError() {
    final var overflowMinScore = Integer.toString(-300);
    internalTestFailure(
        "Invalid value for option '--tx-pool-min-score': '" + overflowMinScore + "' is not a byte",
        "--tx-pool-min-score",
        overflowMinScore);
  }

  @Test
  public void balanceCheckWorks() {
    final boolean balanceCheck = true;
    internalTestSuccess(
        config -> assertThat(config.getEnableBalanceCheck()).isEqualTo(balanceCheck),
        "--tx-pool-enable-balance-check",
        Boolean.toString(balanceCheck));
  }

  @Test
  public void balanceCheckWorksConfigFile() throws IOException {
    final boolean balanceCheck = true;
    final Path tempConfigFilePath =
        createTempFile(
            "config",
            String.format(
                """
              tx-pool-enable-balance-check=%s
              """,
                balanceCheck));

    internalTestSuccess(
        config -> assertThat(config.getEnableBalanceCheck()).isEqualTo(balanceCheck),
        "--config-file",
        tempConfigFilePath.toString());
  }

  @Test
  public void defaultForgetEvictedTxsWithSequencedIsTrue() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isTrue(),
        "--tx-pool",
        SEQUENCED.name().toLowerCase(Locale.ROOT));
  }

  @Test
  public void explicitlyDisablingForgetEvictedTxsWithSequenced() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isFalse(),
        "--tx-pool",
        SEQUENCED.name().toLowerCase(Locale.ROOT),
        "--Xpeer-tracker-forget-evicted-txs=false");
  }

  @Test
  public void fallbackValueForgetEvictedTxsWithSequenced() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isTrue(),
        "--tx-pool",
        SEQUENCED.name().toLowerCase(Locale.ROOT),
        "--Xpeer-tracker-forget-evicted-txs");
  }

  @Test
  public void defaultForgetEvictedTxsWithLayeredIsFalse() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isFalse(),
        "--tx-pool",
        LAYERED.name().toLowerCase(Locale.ROOT));
  }

  @Test
  public void explicitlyEnablingForgetEvictedTxsWithLayered() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isTrue(),
        "--tx-pool",
        LAYERED.name().toLowerCase(Locale.ROOT),
        "--Xpeer-tracker-forget-evicted-txs=true");
  }

  @Test
  public void fallbackValueForgetEvictedTxsWithLayered() {
    internalTestSuccess(
        config -> assertThat(config.getUnstable().getPeerTrackerForgetEvictedTxs()).isTrue(),
        "--tx-pool",
        LAYERED.name().toLowerCase(Locale.ROOT),
        "--Xpeer-tracker-forget-evicted-txs");
  }

  @Override
  protected TransactionPoolConfiguration createDefaultDomainObject() {
    return TransactionPoolConfiguration.DEFAULT;
  }

  @Override
  protected TransactionPoolConfiguration createCustomizedDomainObject() {
    return ImmutableTransactionPoolConfiguration.builder()
        .strictTransactionReplayProtectionEnabled(true)
        .txPoolImplementation(LAYERED)
        .pendingTransactionsLayerMaxCapacityBytes(1_000_000L)
        .maxPrioritizedTransactions(1000)
        .maxFutureBySender(10)
        .build();
  }

  @Override
  protected TransactionPoolOptions optionsFromDomainObject(
      final TransactionPoolConfiguration domainObject) {
    return TransactionPoolOptions.fromConfig(domainObject);
  }

  @Override
  protected TransactionPoolOptions getOptionsFromBesuCommand(final TestBesuCommand besuCommand) {
    return besuCommand.getTransactionPoolOptions();
  }

  @Override
  protected String[] getNonOptionFields() {
    return new String[] {"transactionPoolValidatorService"};
  }
}
