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
    final boolean isFrontier =
        typeCode.isEmpty()
            || typeCode.get(0) == TransactionType.FRONTIER.getEthSerializedType()
            || typeCode.get(0) == TransactionType.FRONTIER.getSerializedType();

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
}
