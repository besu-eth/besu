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
package org.hyperledger.besu.ethereum.eth.core;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class Utils {
  private static final SyncTransactionReceiptEncoder SYNC_RECEIPT_ENCODER =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());

  public static SyncTransactionReceipt receiptToSyncReceipt(
      final TransactionReceipt receipt,
      final TransactionReceiptEncodingConfiguration receiptEncodingConfiguration) {
    BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    TransactionReceiptEncoder.writeTo(receipt, rlpOutput, receiptEncodingConfiguration);
    // Read back the encoded item so the bytes match what the decoder extracts from wire messages:
    // list items (FRONTIER / eth69) → currentListAsBytes(); bytes items (typed eth68) →
    // readBytes().
    final BytesValueRLPInput rlpInput = new BytesValueRLPInput(rlpOutput.encoded(), false);
    final Bytes rawBytes =
        rlpInput.nextIsList() ? rlpInput.currentListAsBytes() : rlpInput.readBytes();
    return new SyncTransactionReceipt(rawBytes);
  }

  public static List<SyncTransactionReceipt> receiptsToSyncReceipts(
      final List<TransactionReceipt> receipts,
      final TransactionReceiptEncodingConfiguration receiptEncodingConfiguration) {

    return receipts.stream()
        .map(receipt -> Utils.receiptToSyncReceipt(receipt, receiptEncodingConfiguration))
        .toList();
  }

  public static TransactionReceipt syncReceiptToReceipt(final SyncTransactionReceipt syncReceipt) {
    final Bytes rlpBytes = syncReceipt.getRlpBytes();
    // Flat receipts (Frontier / eth69): rlpBytes is a complete RLP list, usable directly.
    // Typed receipts (EIP-2718+): rlpBytes is type || RLP(fields) and must be re-wrapped
    // as an RLP bytes item so that TransactionReceiptDecoder.decodeTypedReceipt can call
    // readBytes() to obtain the full typed-receipt payload.
    if ((rlpBytes.get(0) & 0xFF) >= 0xC0) {
      return TransactionReceiptDecoder.readFrom(new BytesValueRLPInput(rlpBytes, false), true);
    } else {
      final BytesValueRLPOutput wrapper = new BytesValueRLPOutput();
      wrapper.writeBytes(rlpBytes);
      return TransactionReceiptDecoder.readFrom(
          new BytesValueRLPInput(wrapper.encoded(), false), true);
    }
  }

  public static List<TransactionReceipt> syncReceiptsToReceipts(
      final List<SyncTransactionReceipt> syncReceipts) {
    return syncReceipts.stream().map(Utils::syncReceiptToReceipt).toList();
  }

  public static SyncBlock blockToSyncBlock(final Block block) {
    BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(rlpOutput);
    final BytesValueRLPInput input = new BytesValueRLPInput(rlpOutput.encoded(), false);
    final SyncBlockBody syncBlockBody =
        SyncBlockBody.readWrappedBodyFrom(
            input, false, new DefaultProtocolSchedule(Optional.of(BigInteger.ONE)));
    return new SyncBlock(block.getHeader(), syncBlockBody);
  }

  public static List<SyncBlock> blocksToSyncBlocks(final List<Block> blocks) {
    return blocks.stream().map(Utils::blockToSyncBlock).toList();
  }

  public static int compareSyncReceipts(
      final SyncTransactionReceipt receipt1, final SyncTransactionReceipt receipt2) {
    if (receipt1.isFormattedForRootCalculation()) {
      if (receipt2.isFormattedForRootCalculation()) {
        return receipt1.getRlpBytes().compareTo(receipt2.getRlpBytes());
      }
      return receipt1
          .getRlpBytes()
          .compareTo(SYNC_RECEIPT_ENCODER.encodeForRootCalculation(receipt2));
    } else {
      if (receipt2.isFormattedForRootCalculation()) {
        return SYNC_RECEIPT_ENCODER
            .encodeForRootCalculation(receipt1)
            .compareTo(receipt2.getRlpBytes());
      }
      return SYNC_RECEIPT_ENCODER
          .encodeForRootCalculation(receipt1)
          .compareTo(SYNC_RECEIPT_ENCODER.encodeForRootCalculation(receipt2));
    }
  }

  public static Bytes serializeReceiptsList(
      final List<List<TransactionReceipt>> receipts,
      final TransactionReceiptEncodingConfiguration encodingConfiguration) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    receipts.forEach(
        (receiptSet) -> {
          tmp.startList();
          receiptSet.forEach(r -> TransactionReceiptEncoder.writeTo(r, tmp, encodingConfiguration));
          tmp.endList();
        });
    tmp.endList();
    return tmp.encoded();
  }
}
