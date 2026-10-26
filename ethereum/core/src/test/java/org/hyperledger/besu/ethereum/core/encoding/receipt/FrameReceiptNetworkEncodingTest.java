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
package org.hyperledger.besu.ethereum.core.encoding.receipt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The EIP-8141 frame receipt forms exchanged in {@code Receipts} messages, checked against receipts
 * of bogota-devnet-1 blocks: the eth/69 form of the EIP-8141 Networking section, which geth and
 * ethrex send, and the typed form, which eth/68 uses and reth sends.
 */
class FrameReceiptNetworkEncodingTest {

  private static final Address PAYER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  private final SyncTransactionReceiptDecoder syncDecoder = new SyncTransactionReceiptDecoder();
  private final SyncTransactionReceiptEncoder syncEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());

  static List<BogotaDevnet1FrameReceipts> blocks() {
    return BogotaDevnet1FrameReceipts.all();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("blocks")
  void eth69FrameReceiptsSyncToTheHeaderReceiptsRoot(final BogotaDevnet1FrameReceipts block) {
    final List<SyncTransactionReceipt> synced =
        block.eth69Receipts().stream().map(syncDecoder::decode).toList();

    assertThat(synced).allMatch(SyncTransactionReceipt::isFormattedForRootCalculation);
    assertThat(synced)
        .map(SyncTransactionReceipt::getRlpBytes)
        .containsExactlyElementsOf(block.typedReceipts());
    assertThat(receiptsRoot(synced)).isEqualTo(block.receiptsRoot());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("blocks")
  void typedFrameReceiptsSyncUnchangedToTheHeaderReceiptsRoot(
      final BogotaDevnet1FrameReceipts block) {
    final List<SyncTransactionReceipt> synced =
        block.typedReceipts().stream().map(syncDecoder::decode).toList();

    assertThat(synced).allMatch(SyncTransactionReceipt::isFormattedForRootCalculation);
    assertThat(synced)
        .map(SyncTransactionReceipt::getRlpBytes)
        .containsExactlyElementsOf(block.typedReceipts());
    assertThat(receiptsRoot(synced)).isEqualTo(block.receiptsRoot());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("blocks")
  void frameReceiptsAreServedOnEth69InTheNetworkFormOfGeth(final BogotaDevnet1FrameReceipts block) {
    final List<TransactionReceipt> receipts = block.receipts();
    assertThat(BodyValidation.receiptsRoot(receipts)).isEqualTo(block.receiptsRoot());

    assertThat(receipts)
        .map(
            receipt ->
                encode(
                    receipt, TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION))
        .containsExactlyElementsOf(block.eth69Receipts());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("blocks")
  void frameReceiptsAreServedOnEth68AsTypedReceipts(final BogotaDevnet1FrameReceipts block) {
    assertThat(block.receipts())
        .map(
            receipt ->
                encode(
                    receipt, TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION))
        .containsExactlyElementsOf(block.typedReceipts().stream().map(RLP::encodeOne).toList());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("blocks")
  void eth69FrameReceiptsDecodeToTheSameReceiptsAsTypedOnes(
      final BogotaDevnet1FrameReceipts block) {
    assertThat(
            block.eth69Receipts().stream()
                .map(receipt -> TransactionReceiptDecoder.readFrom(RLP.input(receipt), false))
                .toList())
        .containsExactlyElementsOf(block.receipts());
  }

  @ParameterizedTest(name = "receipt compaction {0}")
  @ValueSource(booleans = {false, true})
  void syncedEth69FrameReceiptsAreStoredAsTypedReceipts(final boolean receiptCompaction) {
    final BogotaDevnet1FrameReceipts block = BogotaDevnet1FrameReceipts.BLOCK_0X190;
    final BlockchainStorage storage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            receiptCompaction);
    final Hash blockHash = Hash.hash(Bytes.of(1));

    final BlockchainStorage.Updater updater = storage.updater();
    updater.putSyncTransactionReceipts(
        blockHash, block.eth69Receipts().stream().map(syncDecoder::decode).toList());
    updater.commit();

    assertThat(storage.getTransactionReceipts(blockHash)).contains(block.receipts());
  }

  /**
   * A block mixing frame receipts with every other receipt type, as each protocol version sends it,
   * syncs to the receipts root computed from the receipts themselves.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("networkConfigurations")
  void mixedBlockSyncsToTheReceiptsRoot(
      final String protocol, final TransactionReceiptEncodingConfiguration configuration) {
    final Log log =
        new Log(
            Address.fromHexString("0x2222222222222222222222222222222222222222"),
            Bytes.fromHexString("0x0badf00d"),
            List.of(LogTopic.wrap(Bytes32.leftPad(Bytes.of(1)))));
    final List<TransactionReceipt> receipts =
        List.of(
            new TransactionReceipt(Hash.hash(Bytes.of(1)), 21_000L, List.of(log), Optional.empty()),
            new TransactionReceipt(
                TransactionType.FRONTIER, 1, 42_000L, List.of(), Optional.empty()),
            new TransactionReceipt(
                PAYER,
                100_000L,
                List.of(
                    new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 20_000L, 0L, List.of()),
                    new FrameReceipt(FrameReceipt.STATUS_FAILURE, 30_000L, 0L, List.of()))),
            new TransactionReceipt(
                TransactionType.ACCESS_LIST, 0, 121_000L, List.of(log), Optional.empty()),
            new TransactionReceipt(
                TransactionType.EIP1559, 1, 142_000L, List.of(log, log), Optional.empty()),
            new TransactionReceipt(TransactionType.BLOB, 1, 163_000L, List.of(), Optional.empty()),
            new TransactionReceipt(
                TransactionType.DELEGATE_CODE, 1, 184_000L, List.of(log), Optional.empty()),
            new TransactionReceipt(
                PAYER,
                300_000L,
                List.of(
                    new FrameReceipt(FrameReceipt.STATUS_SUCCESS, 50_000L, 25_000L, List.of(log)),
                    new FrameReceipt(FrameReceipt.STATUS_SKIPPED, 0L, 0L, List.of()))));

    final List<SyncTransactionReceipt> synced =
        receipts.stream()
            .map(receipt -> syncDecoder.decode(asReceivedFromPeer(encode(receipt, configuration))))
            .toList();

    assertThat(receiptsRoot(synced)).isEqualTo(BodyValidation.receiptsRoot(receipts));
  }

  static Stream<Arguments> networkConfigurations() {
    return Stream.of(
        Arguments.of(
            "eth/68", TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION),
        Arguments.of(
            "eth/69", TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));
  }

  @Test
  void eth69FrameReceiptInTheShapeOfOtherEth69ReceiptsIsRejected() {
    // [type, status, cumulative gas used, logs]: the status is not a cumulative gas used followed
    // by a payer
    final Bytes receipt =
        eth69FrameReceipt(
            out -> {
              out.writeLongScalar(1);
              out.writeLongScalar(21_000L);
              out.writeEmptyList();
            });

    assertThatThrownBy(() -> syncDecoder.decode(receipt)).isInstanceOf(RLPException.class);
  }

  static Stream<Arguments> malformedEth69FrameReceiptFields() {
    return Stream.of(
        Arguments.of(
            "short payer",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes().slice(1));
                  out.writeEmptyList();
                }),
        Arguments.of(
            "payer is a list",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeList(
                      List.of(PAYER.getBytes()), (payer, payerOut) -> payerOut.writeBytes(payer));
                  out.writeEmptyList();
                }),
        Arguments.of(
            "no frame receipts",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes());
                }),
        Arguments.of(
            "frame receipts are not a list",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes());
                  out.writeLongScalar(1);
                }),
        Arguments.of(
            "trailing field",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes());
                  out.writeEmptyList();
                  out.writeLongScalar(1);
                }),
        Arguments.of(
            "frame gas used without the state dimension",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes());
                  out.startList();
                  out.startList();
                  out.writeLongScalar(FrameReceipt.STATUS_SUCCESS);
                  out.startList();
                  out.writeLongScalar(21_000L);
                  out.endList();
                  out.writeEmptyList();
                  out.endList();
                  out.endList();
                }),
        Arguments.of(
            "frame log topic is not 32 bytes",
            (Consumer<RLPOutput>)
                out -> {
                  out.writeLongScalar(21_000L);
                  out.writeBytes(PAYER.getBytes());
                  out.startList();
                  out.startList();
                  out.writeLongScalar(FrameReceipt.STATUS_SUCCESS);
                  out.startList();
                  out.writeLongScalar(21_000L);
                  out.writeLongScalar(0L);
                  out.endList();
                  out.startList();
                  out.startList();
                  out.writeBytes(PAYER.getBytes());
                  out.startList();
                  out.writeBytes(Bytes.of(1));
                  out.endList();
                  out.writeBytes(Bytes.EMPTY);
                  out.endList();
                  out.endList();
                  out.endList();
                  out.endList();
                }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedEth69FrameReceiptFields")
  void malformedEth69FrameReceiptIsRejected(
      final String malformation, final Consumer<RLPOutput> fieldsAfterType) {
    final Bytes receipt = eth69FrameReceipt(fieldsAfterType);

    assertThatThrownBy(() -> syncDecoder.decode(receipt)).isInstanceOf(RLPException.class);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("malformedEth69FrameReceiptFields")
  void malformedTypedFrameReceiptIsRejected(
      final String malformation, final Consumer<RLPOutput> payloadFields) {
    final Bytes receipt =
        Bytes.concatenate(
            Bytes.of(TransactionType.FRAME.getSerializedType()),
            RLP.encode(
                out -> {
                  out.startList();
                  payloadFields.accept(out);
                  out.endList();
                }));

    assertThatThrownBy(() -> syncDecoder.decode(receipt)).isInstanceOf(RLPException.class);
  }

  private static Bytes eth69FrameReceipt(final Consumer<RLPOutput> fieldsAfterType) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeLongScalar(TransactionType.FRAME.getEthSerializedType());
          fieldsAfterType.accept(out);
          out.endList();
        });
  }

  private static Bytes encode(
      final TransactionReceipt receipt,
      final TransactionReceiptEncodingConfiguration configuration) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(receipt, out, configuration);
    return out.encoded();
  }

  /**
   * The bytes of a receipt as {@code ReceiptsMessage} hands them to the decoder: a list as is, the
   * content of a string otherwise.
   */
  private static Bytes asReceivedFromPeer(final Bytes encodedReceipt) {
    final RLPInput input = RLP.input(encodedReceipt);
    return input.nextIsList() ? input.currentListAsBytes() : input.readBytes();
  }

  private Hash receiptsRoot(final List<SyncTransactionReceipt> receipts) {
    return Util.getRootFromListOfBytes(
        receipts.stream()
            .map(
                receipt ->
                    receipt.isFormattedForRootCalculation()
                        ? receipt.getRlpBytes()
                        : syncEncoder.encodeForRootCalculation(receipt))
            .toList());
  }
}
