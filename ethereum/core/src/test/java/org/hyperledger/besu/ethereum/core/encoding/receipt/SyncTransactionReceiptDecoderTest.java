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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.FrameReceipt;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SyncTransactionReceiptDecoderTest {

  private SyncTransactionReceiptDecoder syncTransactionReceiptDecoder;

  @BeforeEach
  public void beforeTest() {
    syncTransactionReceiptDecoder = new SyncTransactionReceiptDecoder();
  }

  @Test
  public void testDecodeLegacyReceipt() {
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    final LogsBloomFilter bloomFilter = LogsBloomFilter.fromHexString("0x" + "deadbeef".repeat(64));
    final Optional<Bytes> revertReason = Optional.of(Bytes.fromHexString("06"));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            TransactionType.FRONTIER,
            stateRoot,
            cumulativeGasUsed,
            logs,
            bloomFilter,
            revertReason);

    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());
    Assertions.assertTrue(syncTransactionReceipt.isFormattedForRootCalculation());
    Assertions.assertNull(syncTransactionReceipt.getTransactionTypeCode());
    Assertions.assertNull(syncTransactionReceipt.getStatusOrStateRoot());
    Assertions.assertNull(syncTransactionReceipt.getCumulativeGasUsed());
    Assertions.assertNull(syncTransactionReceipt.getBloomFilter());
  }

  @Test
  public void testDecodeEth69Receipt() {
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(stateRoot, cumulativeGasUsed, logs, Optional.empty());

    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());
    Assertions.assertFalse(syncTransactionReceipt.isFormattedForRootCalculation());
    Assertions.assertEquals(
        Bytes.of(TransactionType.FRONTIER.getEthSerializedType()),
        syncTransactionReceipt.getTransactionTypeCode());
    Assertions.assertEquals(stateRoot.getBytes(), syncTransactionReceipt.getStatusOrStateRoot());
    Assertions.assertEquals(
        Bytes.of((byte) cumulativeGasUsed), syncTransactionReceipt.getCumulativeGasUsed());
    Assertions.assertEquals(1, syncTransactionReceipt.getLogs().size());
    Assertions.assertEquals(3, syncTransactionReceipt.getLogs().getFirst().size());
    String expectedBloomFilterHex =
        "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000010800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000020";
    Assertions.assertEquals(
        expectedBloomFilterHex, syncTransactionReceipt.getBloomFilter().getBytes().toHexString());
  }

  @Test
  public void testDecodeTypedReceipt() {
    final TransactionType transactionType = TransactionType.EIP1559;
    final Hash stateRoot = Hash.hash(Bytes.random(32));
    final long cumulativeGasUsed = 2;
    final List<Log> logs =
        List.of(
            new Log(
                Address.fromHexString("03"),
                Bytes.fromHexStringLenient("04"),
                List.of(LogTopic.fromHexString("05"))));
    final LogsBloomFilter bloomFilter = LogsBloomFilter.fromHexString("0x" + "deadbeef".repeat(64));
    final Optional<Bytes> revertReason = Optional.of(Bytes.fromHexString("06"));
    TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            transactionType, stateRoot, cumulativeGasUsed, logs, bloomFilter, revertReason);

    Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT));

    SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());
    Assertions.assertTrue(syncTransactionReceipt.isFormattedForRootCalculation());
    Assertions.assertNull(syncTransactionReceipt.getTransactionTypeCode());
    Assertions.assertNull(syncTransactionReceipt.getStatusOrStateRoot());
    Assertions.assertNull(syncTransactionReceipt.getCumulativeGasUsed());
    Assertions.assertNull(syncTransactionReceipt.getBloomFilter());
  }

  /**
   * EIP-8141 frame receipts have their own payload, with no status and no bloom, so they are not
   * split into components. A typed frame receipt, as eth/68 sends it, is already the trie-root
   * form, so it is returned raw.
   */
  @Test
  public void typedFrameReceiptIsReturnedRaw() {
    final Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    frameReceipt(),
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));

    final SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(encodedReceipt, syncTransactionReceipt.getRlpBytes());
    assertFrameReceiptIsNotSplit(syncTransactionReceipt);
  }

  /**
   * An eth/69 frame receipt, {@code [0x06, cumulative_gas_used, payer, frame_receipts]}, is not
   * split into components either. It is returned in the trie-root form, {@code 0x06 ||
   * rlp([cumulative_gas_used, payer, frame_receipts])}.
   */
  @Test
  public void eth69FrameReceiptIsReturnedInTheTrieRootForm() {
    final TransactionReceipt frameReceipt = frameReceipt();
    final Bytes encodedReceipt =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    frameReceipt,
                    rlpOut,
                    TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION));
    final BytesValueRLPOutput trieRootForm = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(
        frameReceipt, trieRootForm, TransactionReceiptEncodingConfiguration.TRIE_ROOT);

    final SyncTransactionReceipt syncTransactionReceipt =
        syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(trieRootForm.encoded(), syncTransactionReceipt.getRlpBytes());
    assertFrameReceiptIsNotSplit(syncTransactionReceipt);
  }

  private static TransactionReceipt frameReceipt() {
    return new TransactionReceipt(
        Address.fromHexString("0x1111111111111111111111111111111111111111"),
        2,
        List.of(
            new FrameReceipt(
                FrameReceipt.STATUS_SUCCESS,
                1,
                0,
                List.of(
                    new Log(
                        Address.fromHexString("03"),
                        Bytes.fromHexStringLenient("04"),
                        List.of(LogTopic.fromHexString("05")))))));
  }

  private static void assertFrameReceiptIsNotSplit(
      final SyncTransactionReceipt syncTransactionReceipt) {
    Assertions.assertTrue(syncTransactionReceipt.isFormattedForRootCalculation());
    Assertions.assertNull(syncTransactionReceipt.getTransactionTypeCode());
    Assertions.assertNull(syncTransactionReceipt.getStatusOrStateRoot());
    Assertions.assertNull(syncTransactionReceipt.getCumulativeGasUsed());
    Assertions.assertNull(syncTransactionReceipt.getBloomFilter());
  }

  /**
   * Builds a valid EIP-2718 typed receipt and replaces its leading type byte, keeping the payload
   * intact. The DEFAULT configuration writes a typed receipt as an RLP byte string holding {@code
   * type || rlp(payload)}, which is the shape decodeTypedReceipt handles.
   */
  private Bytes typedReceiptWithTypeByte(final int typeByte) {
    final TransactionReceipt transactionReceipt =
        new TransactionReceipt(
            TransactionType.EIP1559,
            Hash.hash(Bytes.random(32)),
            2,
            List.of(
                new Log(
                    Address.fromHexString("03"),
                    Bytes.fromHexStringLenient("04"),
                    List.of(LogTopic.fromHexString("05")))),
            LogsBloomFilter.fromHexString("0x" + "deadbeef".repeat(64)),
            Optional.of(Bytes.fromHexString("06")));

    final Bytes encoded =
        RLP.encode(
            (rlpOut) ->
                TransactionReceiptEncoder.writeTo(
                    transactionReceipt, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT));

    final Bytes opaque = RLP.input(encoded).readBytes();
    final Bytes retyped = Bytes.concatenate(Bytes.of((byte) typeByte), opaque.slice(1));
    return RLP.encode((rlpOut) -> rlpOut.writeBytes(retyped));
  }

  /**
   * EIP-2718 reserves 0x00 so that legacy and typed encodings cannot be confused, and defines no
   * type above the highest value in {@link TransactionType}. A peer-supplied byte outside that set
   * must be rejected at the decode boundary rather than carried into the receipt.
   *
   * <p>Note the asymmetry with the flat paths: {@code decodeEth69Receipt} and {@code
   * decodeLegacyReceipt} both *synthesise* a 0x00 type code as Besu's internal marker for "legacy,
   * no prefix" — see {@link #testDecodeEth69Receipt()}, which asserts exactly that. 0x00 is invalid
   * only as a value read off the wire in the EIP-2718 envelope, which is what this rejects.
   */
  @ParameterizedTest(name = "type byte {0}")
  // 0x06 is defined: it is the EIP-8141 frame transaction type.
  @ValueSource(ints = {0x00, 0x05, 0x07, 0x7f, 0xc0, 0xf8, 0xfe, 0xff})
  public void typedReceiptWithUndefinedTypeByteIsRejected(final int typeByte) {
    final Bytes encodedReceipt = typedReceiptWithTypeByte(typeByte);

    Assertions.assertThrows(
        RLPException.class, () -> syncTransactionReceiptDecoder.decode(encodedReceipt));
  }

  /**
   * Every type byte that {@link TransactionType} defines must still decode.
   *
   * <p>FRAME is excluded: a frame receipt has its own payload, so the EIP-1559 payload used here is
   * rejected with the frame type, see {@link #typedReceiptWithFrameTypeByteNeedsTheFramePayload()}.
   */
  @ParameterizedTest(name = "type byte {0}")
  @EnumSource(
      value = TransactionType.class,
      names = {"FRONTIER", "FRAME"},
      mode = EnumSource.Mode.EXCLUDE)
  public void typedReceiptWithDefinedTypeByteIsAccepted(final TransactionType txType) {
    final Bytes encodedReceipt = typedReceiptWithTypeByte(txType.getSerializedType());

    final SyncTransactionReceipt decoded = syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(encodedReceipt, decoded.getRlpBytes());
  }

  /** A typed frame receipt must carry the frame receipt payload, not that of the other types. */
  @Test
  public void typedReceiptWithFrameTypeByteNeedsTheFramePayload() {
    final Bytes encodedReceipt =
        typedReceiptWithTypeByte(TransactionType.FRAME.getSerializedType());

    Assertions.assertThrows(
        RLPException.class, () -> syncTransactionReceiptDecoder.decode(encodedReceipt));
  }

  /**
   * A typed frame receipt whose payload is short a field is rejected, even when an item follows the
   * payload list that would otherwise be read in place of the missing field.
   */
  @Test
  public void typedFrameReceiptMissingAPayloadFieldIsRejected() {
    final Bytes payload =
        RLP.encode(
            (out) -> {
              out.startList();
              out.writeLongScalar(21_000);
              out.writeBytes(Address.fromHexString("03").getBytes());
              out.endList();
            });
    final Bytes trailingFrameReceipts = RLP.encode((out) -> out.writeEmptyList());
    final Bytes encodedReceipt =
        Bytes.concatenate(
            Bytes.of(TransactionType.FRAME.getSerializedType()), payload, trailingFrameReceipts);

    Assertions.assertThrows(
        RLPException.class, () -> syncTransactionReceiptDecoder.decode(encodedReceipt));
  }

  /**
   * Hand-builds the eth/69 flat shape {@code [type, statusOrStateRoot, cumulativeGasUsed, logs]}
   * with an arbitrary type field, so the field can be set to values the encoder would never emit.
   * The encoder writes it as an RLP scalar, so a legacy receipt's type arrives as the empty string.
   */
  private Bytes eth69ReceiptWithTypeField(final Bytes typeField) {
    return RLP.encode(
        (out) -> {
          out.startList();
          out.writeBytes(typeField);
          out.writeBytes(Hash.hash(Bytes.random(32)).getBytes());
          out.writeLongScalar(2);
          out.startList(); // logs
          out.startList(); // one log: address, topics, data
          out.writeBytes(Address.fromHexString("03").getBytes());
          out.startList();
          out.writeBytes(LogTopic.fromHexString("05").getBytes());
          out.endList();
          out.writeBytes(Bytes.fromHexStringLenient("04"));
          out.endList();
          out.endList();
          out.endList();
        });
  }

  /**
   * The eth/69 type field is peer-supplied and was taken verbatim. That matters because {@code
   * SyncTransactionReceiptEncoder.encodeForRootCalculation} treats both {@code 0x00} and {@code
   * 0xf8} as "legacy" and drops the prefix — so a value like {@code 0xf8}, which is not a
   * transaction type at all but the RLP long-list prefix, would make a receipt hash as legacy for
   * the receipts-root check while being stored with the bogus byte.
   */
  @ParameterizedTest(name = "eth/69 type field {0}")
  // 0x06 is defined: it is the EIP-8141 frame transaction type.
  @ValueSource(ints = {0x05, 0x07, 0x7f, 0xc0, 0xf8, 0xfe, 0xff})
  public void eth69ReceiptWithUndefinedTypeFieldIsRejected(final int typeByte) {
    final Bytes encodedReceipt = eth69ReceiptWithTypeField(Bytes.of((byte) typeByte));

    Assertions.assertThrows(
        RLPException.class, () -> syncTransactionReceiptDecoder.decode(encodedReceipt));
  }

  /** An empty type field is how eth/69 encodes a legacy receipt, and must still be accepted. */
  @Test
  public void eth69ReceiptWithEmptyTypeFieldIsAcceptedAsLegacy() {
    final Bytes encodedReceipt = eth69ReceiptWithTypeField(Bytes.EMPTY);

    final SyncTransactionReceipt decoded = syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(
        Bytes.of(TransactionType.FRONTIER.getEthSerializedType()),
        decoded.getTransactionTypeCode());
  }

  /**
   * Every type the enum defines must still be accepted on the eth/69 path, including a literal
   * {@code 0x00}. That is a non-canonical encoding of the scalar zero rather than an invalid type,
   * and it means the same thing as the empty field, so it is tolerated rather than rejected.
   *
   * <p>FRAME is excluded: an eth/69 frame receipt has its own shape after the type, see {@link
   * #eth69ReceiptWithFrameTypeFieldNeedsTheFrameReceiptShape()}.
   */
  @ParameterizedTest(name = "eth/69 type field {0}")
  @EnumSource(
      value = TransactionType.class,
      names = {"FRAME"},
      mode = EnumSource.Mode.EXCLUDE)
  public void eth69ReceiptWithDefinedTypeFieldIsAccepted(final TransactionType txType) {
    final Bytes encodedReceipt = eth69ReceiptWithTypeField(Bytes.of(txType.getEthSerializedType()));

    final SyncTransactionReceipt decoded = syncTransactionReceiptDecoder.decode(encodedReceipt);

    Assertions.assertEquals(
        Bytes.of(txType.getEthSerializedType()), decoded.getTransactionTypeCode());
  }

  /**
   * An eth/69 frame receipt is {@code [0x06, cumulative_gas_used, payer, frame_receipts]}, so the
   * {@code [type, statusOrStateRoot, cumulativeGasUsed, logs]} shape of the other types is rejected
   * with the frame type.
   */
  @Test
  public void eth69ReceiptWithFrameTypeFieldNeedsTheFrameReceiptShape() {
    final Bytes encodedReceipt =
        eth69ReceiptWithTypeField(Bytes.of(TransactionType.FRAME.getEthSerializedType()));

    Assertions.assertThrows(
        RLPException.class, () -> syncTransactionReceiptDecoder.decode(encodedReceipt));
  }
}
