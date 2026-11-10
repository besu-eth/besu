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

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class SyncTransactionReceiptEncoder {

  private final SimpleNoCopyRlpEncoder rlpEncoder;

  public SyncTransactionReceiptEncoder(final SimpleNoCopyRlpEncoder rlpEncoder) {
    this.rlpEncoder = rlpEncoder;
  }

  public Bytes encodeForRootCalculation(final SyncTransactionReceipt receipt) {
    final Bytes typeCode = receipt.getTransactionTypeCode();
    final boolean isFrontier = isFrontier(typeCode);

    List<Bytes> encodedLogs =
        receipt.getLogs().stream()
            .map(
                (List<Bytes> log) -> {
                  Bytes encodedLogAddress = rlpEncoder.encode(log.getFirst());
                  List<Bytes> encodedLogTopics = new ArrayList<>();
                  for (int i = 1; i < log.size() - 1; i++) {
                    encodedLogTopics.add(rlpEncoder.encode(log.get(i)));
                  }
                  Bytes encodedLogData = rlpEncoder.encode(log.getLast());
                  return rlpEncoder.encodeList(
                      List.of(
                          Bytes.concatenate(
                              encodedLogAddress,
                              rlpEncoder.encodeList(encodedLogTopics),
                              encodedLogData)));
                })
            .toList();
    List<Bytes> mainList =
        List.of(
            rlpEncoder.encode(receipt.getStatusOrStateRoot()),
            rlpEncoder.encode(receipt.getCumulativeGasUsed()),
            rlpEncoder.encode(receipt.getBloomFilter().getBytes()),
            rlpEncoder.encodeList(encodedLogs));

    return !isFrontier
        ? Bytes.concatenate(
            rlpEncoder.encode(receipt.getTransactionTypeCode()), rlpEncoder.encodeList(mainList))
        : rlpEncoder.encodeList(mainList);
  }

  /**
   * Encodes a receipt without its bloom, in the eth/69 shape {@code [type, statusOrStateRoot,
   * cumulativeGasUsed, logs]}, from the same field values {@link #encodeForRootCalculation} hashes.
   *
   * <p>This is what a compacted receipt from a peer is stored as. Storing these bytes rather than
   * the ones the peer sent means storage only ever holds what the receipts root covered: the bloom
   * left out here is a function of the logs. For a canonically encoded receipt the result equals
   * {@code TransactionReceiptEncoder}'s eth/69 encoding. The type field is written as an RLP
   * scalar, so a legacy receipt's type is the empty string.
   *
   * @param transactionTypeCode the type byte, or {@code 0x00} for a legacy receipt
   * @param statusOrStateRoot the status or post-state root, as hashed for the receipts root
   * @param cumulativeGasUsed the cumulative gas used, as hashed for the receipts root
   * @param logs the logs, each as address, topics, then data
   * @return the RLP list
   */
  public static Bytes encodeCompacted(
      final Bytes transactionTypeCode,
      final Bytes statusOrStateRoot,
      final Bytes cumulativeGasUsed,
      final List<List<Bytes>> logs) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(isFrontier(transactionTypeCode) ? Bytes.EMPTY : transactionTypeCode);
    out.writeBytes(statusOrStateRoot);
    out.writeBytes(cumulativeGasUsed);
    out.startList();
    for (final List<Bytes> log : logs) {
      out.startList();
      out.writeBytes(log.getFirst());
      out.startList();
      for (int i = 1; i < log.size() - 1; i++) {
        out.writeBytes(log.get(i));
      }
      out.endList();
      out.writeBytes(log.getLast());
      out.endList();
    }
    out.endList();
    out.endList();
    return out.encoded();
  }

  private static boolean isFrontier(final Bytes typeCode) {
    return typeCode.isEmpty()
        || typeCode.get(0) == TransactionType.FRONTIER.getEthSerializedType()
        || typeCode.get(0) == TransactionType.FRONTIER.getSerializedType();
  }
}
