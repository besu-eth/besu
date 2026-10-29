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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO5;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.EXPERIMENTAL_EIPS;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.FUTURE_EIPS;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.TransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSpecs.BerlinTransactionReceiptFactory;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSpecs.BogotaTransactionReceiptFactory;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult.FrameTransactionOutcome;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class BogotaTransactionReceiptFactoryTest {

  private static final Address PAYER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Log LOG =
      new Log(
          Address.fromHexString("0x2222222222222222222222222222222222222222"),
          Bytes.fromHexString("0x0badf00d"),
          List.of(LogTopic.fromHexString("0x" + "05".repeat(32))));
  private static final Bytes REVERT_REASON = Bytes.fromHexString("0x08c379a0");
  private static final long CUMULATIVE_GAS_USED = 123_456L;

  private static final long BPO5_TIME = 10L;
  private static final long BOGOTA_TIME = 20L;
  private static final long FUTURE_EIPS_TIME = 30L;
  private static final long EXPERIMENTAL_EIPS_TIME = 40L;

  @ParameterizedTest(name = "revert reason enabled: {0}")
  @ValueSource(booleans = {false, true})
  void frameTransactionReceiptCarriesThePayerAndTheFrameReceipts(
      final boolean revertReasonEnabled) {
    final List<FrameReceipt> frameReceipts =
        List.of(
            new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 100L, 0L, List.of()),
            new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 3_000L, 183_600L, List.of(LOG)));
    final TransactionProcessingResult result = successful();
    result.setFrameTransactionOutcome(new FrameTransactionOutcome(PAYER, frameReceipts));

    final TransactionReceipt receipt =
        new BogotaTransactionReceiptFactory(revertReasonEnabled)
            .create(TransactionType.FRAME, result, CUMULATIVE_GAS_USED);

    assertThat(receipt.getTransactionType()).isEqualTo(TransactionType.FRAME);
    assertThat(receipt.getCumulativeGasUsed()).isEqualTo(CUMULATIVE_GAS_USED);
    assertThat(receipt.getPayer()).contains(PAYER);
    assertThat(receipt.getFrameReceipts()).contains(frameReceipts);
    assertThat(receipt.getLogsList()).containsExactly(LOG);
    assertThat(receipt.getStatus()).isEqualTo(1);
    // The frame receipt payload has no revert reason, whatever the node's setting.
    assertThat(receipt.getRevertReason()).isEmpty();
    assertThat(receipt)
        .isEqualTo(new TransactionReceipt(PAYER, CUMULATIVE_GAS_USED, frameReceipts));
  }

  @Test
  void frameTransactionReceiptStatusIsZeroWhenAFrameDidNotSucceed() {
    final TransactionProcessingResult result = successful();
    result.setFrameTransactionOutcome(
        new FrameTransactionOutcome(
            PAYER,
            List.of(
                new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 100L, 0L, List.of()),
                new FrameReceipt(FrameReceipt.STATUS_FAILURE, 200L, 0L, List.of()))));

    final TransactionReceipt receipt =
        new BogotaTransactionReceiptFactory(false)
            .create(TransactionType.FRAME, result, CUMULATIVE_GAS_USED);

    assertThat(receipt.getStatus()).isZero();
  }

  @Test
  void frameTransactionWithoutAFrameOutcomeIsRejected() {
    final TransactionReceiptFactory factory = new BogotaTransactionReceiptFactory(false);
    final TransactionProcessingResult result = successful();

    assertThatThrownBy(() -> factory.create(TransactionType.FRAME, result, CUMULATIVE_GAS_USED))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Frame transaction result is missing its frame outcome");
  }

  @ParameterizedTest(name = "{0}, revert reason enabled: {1}, successful: {2}")
  @MethodSource("nonFrameCases")
  void nonFrameTypesProduceTheBerlinReceipt(
      final TransactionType type, final boolean revertReasonEnabled, final boolean successful) {
    final TransactionProcessingResult result = successful ? successful() : failed();
    // An outcome attached to a non-frame result is ignored: the receipt shape follows the type.
    result.setFrameTransactionOutcome(
        new FrameTransactionOutcome(
            PAYER, List.of(new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 1L, 0L, List.of()))));

    final TransactionReceipt bogota =
        new BogotaTransactionReceiptFactory(revertReasonEnabled)
            .create(type, result, CUMULATIVE_GAS_USED);
    final TransactionReceipt berlin =
        new BerlinTransactionReceiptFactory(revertReasonEnabled)
            .create(type, result, CUMULATIVE_GAS_USED);

    assertThat(bogota).isEqualTo(berlin);
    assertThat(bogota.getTransactionType()).isEqualTo(type);
    assertThat(bogota.getRevertReason()).isEqualTo(berlin.getRevertReason());
    assertThat(bogota.getPayer()).isEmpty();
    assertThat(bogota.getFrameReceipts()).isEmpty();
    // The storage encoding with a bloom and a revert reason covers every field of the receipt.
    assertThat(encode(bogota)).isEqualTo(encode(berlin));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("forksFromBogota")
  void bogotaAndTheForksBuiltOnItUseTheBogotaFactory(final HardforkId fork, final long timestamp) {
    final ProtocolSpec spec = specAt(timestamp);

    assertThat(spec.getHardforkId()).isEqualTo(fork);
    assertThat(spec.getTransactionReceiptFactory())
        .isExactlyInstanceOf(BogotaTransactionReceiptFactory.class);
  }

  @Test
  void forksBeforeBogotaKeepTheBerlinFactory() {
    final ProtocolSpec spec = specAt(BPO5_TIME);

    assertThat(spec.getHardforkId()).isEqualTo(BPO5);
    assertThat(spec.getTransactionReceiptFactory())
        .isExactlyInstanceOf(BerlinTransactionReceiptFactory.class);
  }

  private static Stream<Arguments> forksFromBogota() {
    return Stream.of(
        Arguments.of(BOGOTA, BOGOTA_TIME),
        Arguments.of(FUTURE_EIPS, FUTURE_EIPS_TIME),
        Arguments.of(EXPERIMENTAL_EIPS, EXPERIMENTAL_EIPS_TIME));
  }

  private static Stream<Arguments> nonFrameCases() {
    return Stream.of(TransactionType.values())
        .filter(type -> !type.supportsFrames())
        .flatMap(
            type ->
                Stream.of(
                    Arguments.of(type, false, true),
                    Arguments.of(type, false, false),
                    Arguments.of(type, true, true),
                    Arguments.of(type, true, false)));
  }

  private static ProtocolSpec specAt(final long timestamp) {
    final StubGenesisConfigOptions config =
        new StubGenesisConfigOptions()
            .chainId(BigInteger.ONE)
            .bpo5Time(BPO5_TIME)
            .bogotaTime(BOGOTA_TIME)
            .futureEipsTime(FUTURE_EIPS_TIME)
            .experimentalEipsTime(EXPERIMENTAL_EIPS_TIME);
    final ProtocolSchedule schedule =
        MainnetProtocolSchedule.fromConfig(
            config,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    return schedule.getByBlockHeader(
        new BlockHeaderTestFixture().number(1L).timestamp(timestamp).buildHeader());
  }

  private static TransactionProcessingResult successful() {
    return TransactionProcessingResult.successful(
        List.of(LOG), 21_000L, 0L, Bytes.EMPTY, Optional.empty(), ValidationResult.valid());
  }

  private static TransactionProcessingResult failed() {
    return TransactionProcessingResult.failed(
        21_000L,
        0L,
        ValidationResult.valid(),
        Optional.of(REVERT_REASON),
        Optional.empty(),
        Optional.empty());
  }

  private static Bytes encode(final TransactionReceipt receipt) {
    return RLP.encode(
        out ->
            TransactionReceiptEncoder.writeTo(
                receipt, out, TransactionReceiptEncodingConfiguration.STORAGE_WITHOUT_COMPACTION));
  }
}
