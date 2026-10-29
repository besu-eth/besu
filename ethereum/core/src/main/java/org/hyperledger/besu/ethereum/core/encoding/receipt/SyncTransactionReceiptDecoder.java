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
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class SyncTransactionReceiptDecoder {

  private static final SimpleNoCopyRlpEncoder RLP_ENCODER = new SimpleNoCopyRlpEncoder();

  public SyncTransactionReceipt decode(final Bytes rawRlp) {
    RLPInput rlpInput = RLP.input(rawRlp);
    // The first byte indicates whether the receipt is typed (eth/68) or flat (eth/69).
    SyncTransactionReceipt result;
    if (!rlpInput.nextIsList()) {
      result = decodeTypedReceipt(rawRlp, rlpInput);
    } else {
      result = decodeFlatReceipt(rawRlp, rlpInput);
    }
    return result;
  }

  private SyncTransactionReceipt decodeTypedReceipt(final Bytes rawRlp, final RLPInput rlpInput) {
    RLPInput receiptInput = rlpInput;
    Bytes transactionTypeCode = receiptInput.readBytes();
    if (transactionTypeCode.size() > 1) {
      receiptInput = new BytesValueRLPInput(transactionTypeCode.slice(1), false);
      transactionTypeCode = transactionTypeCode.slice(0, 1);
    }
    requireDefinedTransactionType(transactionTypeCode);

    if (isFrameTransactionType(transactionTypeCode)) {
      return decodeTypedFrameReceipt(rawRlp, receiptInput);
    }

    receiptInput.enterList();
    Bytes statusOrStateRoot = receiptInput.readBytes();
    Bytes cumulativeGasUsed = receiptInput.readBytes();
    final boolean isCompacted = isNextNotBloomFilter(receiptInput);
    SyncTransactionReceipt syncTransactionReceipt;
    if (!isCompacted) {
      syncTransactionReceipt = new SyncTransactionReceipt(rawRlp);
    } else {
      List<List<Bytes>> logs = parseLogs(receiptInput);
      LogsBloomFilter bloomFilter = LogsBloomFilter.builder().insertRawLogs(logs).build();
      receiptInput.leaveList();
      syncTransactionReceipt =
          new SyncTransactionReceipt(
              rawRlp, transactionTypeCode, statusOrStateRoot, cumulativeGasUsed, bloomFilter, logs);
    }
    return syncTransactionReceipt;
  }

  /**
   * Whether a type field is the type of an EIP-8141 frame transaction. In a flat receipt this tells
   * an eth/69 frame receipt apart from all others: a legacy receipt cannot start with that value,
   * its first element being a status of 0 or 1 or a 32-byte state root.
   *
   * @param transactionTypeCode the type field, or the first element of a flat receipt
   * @return true if it is the frame transaction type
   */
  private boolean isFrameTransactionType(final Bytes transactionTypeCode) {
    return transactionTypeCode.size() == 1
        && TransactionType.fromEthSerializedType(transactionTypeCode.get(0))
            .map(TransactionType::supportsFrames)
            .orElse(false);
  }

  /**
   * Decodes an EIP-8141 frame receipt in the typed form, {@code 0x06 || rlp([cumulative_gas_used,
   * payer, frame_receipts])}. This is its consensus encoding, so it is kept as is for the receipts
   * root. eth/68 carries typed receipts this way, and some eth/69 peers (reth) send frame receipts
   * this way too rather than in the form of {@link #decodeEth69FrameReceipt}; either way the bytes
   * are checked against the receipts root of the block header.
   *
   * @param rawRlp the typed receipt
   * @param payloadInput the RLP input positioned at the payload list
   * @return the receipt, in its consensus encoding
   */
  private SyncTransactionReceipt decodeTypedFrameReceipt(
      final Bytes rawRlp, final RLPInput payloadInput) {
    payloadInput.enterList();
    readFrameReceiptFields(payloadInput);
    payloadInput.leaveList();
    return new SyncTransactionReceipt(rawRlp);
  }

  /**
   * Decodes an EIP-8141 frame receipt in the eth/69 form of the EIP-8141 Networking section, {@code
   * [0x06, cumulative_gas_used, payer, frame_receipts]}, which geth and ethrex send. Moving the
   * fields after the type into a list of their own gives the consensus encoding, {@code 0x06 ||
   * rlp([cumulative_gas_used, payer, frame_receipts])}, which is what the receipts root is computed
   * over and what is stored, as for a typed frame receipt.
   *
   * @param input the RLP input positioned after the type
   * @param transactionTypeCode the type field
   * @return the receipt, in its consensus encoding
   */
  private SyncTransactionReceipt decodeEth69FrameReceipt(
      final RLPInput input, final Bytes transactionTypeCode) {
    final List<Bytes> payloadFields = readFrameReceiptFields(input);
    input.leaveList();
    return new SyncTransactionReceipt(
        Bytes.wrap(transactionTypeCode, RLP_ENCODER.encodeList(payloadFields)));
  }

  /**
   * Reads the fields of an EIP-8141 frame receipt payload, {@code cumulative_gas_used, payer,
   * [[status, [execution, state], logs], ...]}, from the current list, rejecting a receipt whose
   * fields do not have that shape so that what is stored can be decoded later.
   *
   * @param input the RLP input positioned at the cumulative gas used
   * @return the RLP encoding of each field
   */
  private List<Bytes> readFrameReceiptFields(final RLPInput input) {
    final RLPInput cumulativeGasUsed = readFrameReceiptField(input);
    cumulativeGasUsed.readLongScalar();
    final RLPInput payer = readFrameReceiptField(input);
    if (payer.readBytes().size() != Address.SIZE) {
      throw new RLPException("Frame receipt payer is not " + Address.SIZE + " bytes long");
    }
    final RLPInput frameReceipts = readFrameReceiptField(input);
    frameReceipts.enterList();
    while (!frameReceipts.isEndOfCurrentList()) {
      frameReceipts.enterList();
      frameReceipts.readIntScalar(); // status
      frameReceipts.enterList();
      frameReceipts.readLongScalar(); // execution gas used
      frameReceipts.readLongScalar(); // state gas used
      frameReceipts.leaveList();
      parseLogs(frameReceipts);
      frameReceipts.leaveList();
    }
    frameReceipts.leaveList();
    return List.of(cumulativeGasUsed.raw(), payer.raw(), frameReceipts.raw());
  }

  /**
   * Reads the next field of a frame receipt payload. {@link RLPInput#readAsRlp()} only bounds the
   * read by the end of the whole input, so a payload list that is short a field would otherwise
   * read whatever follows it.
   */
  private static RLPInput readFrameReceiptField(final RLPInput input) {
    if (input.isEndOfCurrentList()) {
      throw new RLPException("Frame receipt payload is missing fields");
    }
    return input.readAsRlp();
  }

  /**
   * Rejects an EIP-2718 wire type byte that no transaction type defines.
   *
   * <p>{@code 0x00} is reserved by EIP-2718 precisely so that legacy and typed encodings cannot be
   * confused, and {@code 0xc0}-{@code 0xfe} cannot be type bytes at all because they collide with
   * RLP list prefixes. Carrying an undefined byte past this point puts it in {@link
   * SyncTransactionReceipt#getTransactionTypeCode()}, where it is indistinguishable from the {@code
   * 0x00} that {@link #decodeEth69Receipt} and {@link #decodeLegacyReceipt} synthesise to mean
   * "legacy, no type prefix" — so a peer could make a typed receipt re-encode as a legacy one for
   * the receipts-root calculation.
   *
   * <p>The accepted set is derived from {@link TransactionType} rather than written out, so a new
   * transaction type widens it without this method needing to change. {@code FRONTIER} is excluded
   * deliberately: it is not a wire type, it is the absence of one.
   *
   * @param transactionTypeCode the single type byte read from the wire
   */
  private void requireDefinedTransactionType(final Bytes transactionTypeCode) {
    if (transactionTypeCode.size() != 1) {
      throw new RLPException(
          "Typed receipt type prefix is "
              + transactionTypeCode.size()
              + " bytes, expected exactly 1");
    }
    final byte typeByte = transactionTypeCode.get(0);
    final boolean isDefinedWireType =
        TransactionType.fromEthSerializedType(typeByte)
            .filter(transactionType -> transactionType != TransactionType.FRONTIER)
            .isPresent();
    if (!isDefinedWireType) {
      throw new RLPException(
          String.format("Invalid transaction type 0x%02x in typed receipt", typeByte));
    }
  }

  private SyncTransactionReceipt decodeFlatReceipt(final Bytes rawRlp, final RLPInput rlpInput) {
    rlpInput.enterList();
    // Flat receipts can be either legacy or eth/69 receipts.
    // To determine the type, we need to examine the logs' position, as the bloom filter cannot be
    // used. This is because compacted legacy receipts also lack a bloom filter.
    // The first element can be either the transaction type (eth/69) or stateRootOrStatus (eth/68)
    final Bytes firstElement = rlpInput.readBytes();
    // An eth/69 frame receipt continues with the fields of the frame receipt payload.
    if (isFrameTransactionType(firstElement)) {
      return decodeEth69FrameReceipt(rlpInput, firstElement);
    }
    // The second element can be either the state root or status (eth/68) or stateRootOrStatus
    // (eth/69)
    final Bytes secondElement = rlpInput.readBytes();
    final boolean isCompacted = isNextNotBloomFilter(rlpInput);
    LogsBloomFilter bloomFilter = null;
    if (!isCompacted) {
      bloomFilter = new LogsBloomFilter(rlpInput.readBytes());
    }
    boolean isEth69Receipt = isCompacted && !rlpInput.nextIsList();
    SyncTransactionReceipt result;
    if (isEth69Receipt) {
      result = decodeEth69Receipt(rawRlp, rlpInput, firstElement, secondElement);
    } else {
      result = decodeLegacyReceipt(rawRlp, rlpInput, firstElement, secondElement, bloomFilter);
    }
    return result;
  }

  private SyncTransactionReceipt decodeEth69Receipt(
      final Bytes rawRlp,
      final RLPInput input,
      final Bytes transactionByteRlp,
      final Bytes statusOrStateRoot) {
    requireDefinedEth69TransactionType(transactionByteRlp);
    Bytes transactionTypeCode =
        transactionByteRlp.isEmpty()
            ? Bytes.of(TransactionType.FRONTIER.getEthSerializedType())
            : transactionByteRlp;
    Bytes cumulativeGasUsed = input.readBytes();
    List<List<Bytes>> logs = parseLogs(input);
    LogsBloomFilter bloomFilter = LogsBloomFilter.builder().insertRawLogs(logs).build();
    return new SyncTransactionReceipt(
        rawRlp, transactionTypeCode, statusOrStateRoot, cumulativeGasUsed, bloomFilter, logs);
  }

  /**
   * Rejects an eth/69 type field that no transaction type defines.
   *
   * <p>The rule differs from the EIP-2718 envelope deliberately. eth/69 carries the type as an RLP
   * scalar (see {@code TransactionReceiptEncoder.writeEth69Receipt}), so a legacy receipt arrives
   * as the empty string; a literal {@code 0x00} is a non-canonical spelling of the same zero and
   * means the same thing, so both are accepted here even though {@code 0x00} is invalid inside the
   * EIP-2718 envelope.
   *
   * <p>What must not be accepted is a byte that is no type at all. {@code
   * SyncTransactionReceiptEncoder.encodeForRootCalculation} treats {@code 0xf8} as "legacy" exactly
   * as it treats {@code 0x00} — {@code 0xf8} being the RLP long-list prefix that {@code
   * TransactionType.FRONTIER} carries as its enum value, not a wire type — so an undefined byte
   * here would let a receipt hash as legacy for the receipts-root check while being stored with the
   * bogus value.
   *
   * @param transactionTypeField the type field as read from the wire, empty for a legacy receipt
   */
  private void requireDefinedEth69TransactionType(final Bytes transactionTypeField) {
    if (transactionTypeField.isEmpty()) {
      return;
    }
    if (transactionTypeField.size() != 1
        || TransactionType.fromEthSerializedType(transactionTypeField.get(0)).isEmpty()) {
      throw new RLPException(
          "Invalid transaction type " + transactionTypeField + " in eth/69 receipt");
    }
  }

  private SyncTransactionReceipt decodeLegacyReceipt(
      final Bytes rawRlp,
      final RLPInput input,
      final Bytes statusOrStateRoot,
      final Bytes cumulativeGasUsed,
      final LogsBloomFilter bloomFilter) {
    SyncTransactionReceipt syncTransactionReceipt;
    if (bloomFilter != null) {
      syncTransactionReceipt = new SyncTransactionReceipt(rawRlp);
    } else {
      Bytes transactionTypeCode = Bytes.of(TransactionType.FRONTIER.getEthSerializedType());
      List<List<Bytes>> logs = parseLogs(input);
      syncTransactionReceipt =
          new SyncTransactionReceipt(
              rawRlp,
              transactionTypeCode,
              statusOrStateRoot,
              cumulativeGasUsed,
              LogsBloomFilter.builder().insertRawLogs(logs).build(),
              logs);
    }
    return syncTransactionReceipt;
  }

  private List<List<Bytes>> parseLogs(final RLPInput input) {
    return input.readList(
        logInput -> {
          logInput.enterList();

          final Bytes logger = logInput.readBytes();
          final List<Bytes> topics = logInput.readList(RLPInput::readBytes32);
          final Bytes data = logInput.readBytes();

          logInput.leaveList();
          List<Bytes> result = new ArrayList<>(topics.size() + 2);
          result.add(logger);
          result.addAll(topics);
          result.add(data);
          return result;
        });
  }

  private boolean isNextNotBloomFilter(final RLPInput input) {
    return input.nextIsList() || input.nextSize() != LogsBloomFilter.BYTE_SIZE;
  }
}
