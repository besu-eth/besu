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

import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class SyncTransactionReceiptDecoder {

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

  private SyncTransactionReceipt decodeFlatReceipt(final Bytes rawRlp, final RLPInput rlpInput) {
    rlpInput.enterList();
    // Flat receipts can be either legacy or eth/69 receipts.
    // To determine the type, we need to examine the logs' position, as the bloom filter cannot be
    // used. This is because compacted legacy receipts also lack a bloom filter.
    // The first element can be either the transaction type (eth/69) or stateRootOrStatus (eth/68)
    final Bytes firstElement = rlpInput.readBytes();
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
