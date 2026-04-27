/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BytesHolder;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;

@JsonPropertyOrder({
  "blockHash",
  "blockNumber",
  "contractAddress",
  "cumulativeGasUsed",
  "from",
  "gasUsed",
  "effectiveGasPrice",
  "logs",
  "logsBloom",
  "root",
  "status",
  "to",
  "transactionHash",
  "transactionIndex",
  "revertReason",
  "type",
  "blobGasUsed",
  "blobGasPrice"
})
public abstract class TransactionReceiptResult {

  private final String blockHash;
  private final String blockNumber;
  private final String contractAddress;
  private final String cumulativeGasUsed;
  private final String from;
  private final String gasUsed;
  private final String effectiveGasPrice;
  private final List<TransactionReceiptLogResult> logs;
  private final String logsBloom;
  private final String to;
  private final String transactionHash;
  private final String transactionIndex;
  private final String revertReason;

  protected final TransactionReceipt receipt;
  protected final String type;

  private final String blobGasUsed;
  private final String blobGasPrice;

  protected TransactionReceiptResult(final TransactionReceiptWithMetadata receiptWithMetadata) {
    final Transaction txn = receiptWithMetadata.getTransaction();
    this.receipt = receiptWithMetadata.getReceipt();
    this.blockHash = receiptWithMetadata.getBlockHash().toString();
    this.blockNumber = Quantity.create(receiptWithMetadata.getBlockNumber());
    this.contractAddress = txn.contractAddress().map(Address::toString).orElse(null);
    this.cumulativeGasUsed = Quantity.create(receipt.getCumulativeGasUsed());
    this.from = txn.getSender().toString();
    this.gasUsed = Quantity.create(receiptWithMetadata.getGasUsed());
    this.blobGasUsed = receiptWithMetadata.getBlobGasUsed().map(Quantity::create).orElse(null);
    this.blobGasPrice = receiptWithMetadata.getBlobGasPrice().map(Quantity::create).orElse(null);
    this.effectiveGasPrice =
        Quantity.create(txn.getEffectiveGasPrice(receiptWithMetadata.getBaseFee()));

    this.logs =
        logReceipts(
            receipt.getLogsList(),
            receiptWithMetadata.getBlockNumber(),
            txn.getHash(),
            receiptWithMetadata.getBlockHash(),
            receiptWithMetadata.getBlockTimestamp(),
            receiptWithMetadata.getTransactionIndex(),
            receiptWithMetadata.getLogIndexOffset());
    this.logsBloom = receipt.getBloomFilter().toString();
    this.to = txn.getTo().map(BytesHolder::getBytes).map(Bytes::toHexString).orElse(null);
    this.transactionHash = txn.getHash().toString();
    this.transactionIndex = Quantity.create(receiptWithMetadata.getTransactionIndex());
    this.revertReason = receipt.getRevertReason().map(Bytes::toString).orElse(null);
    this.type =
        txn.getType().equals(TransactionType.FRONTIER)
            ? Quantity.create(0)
            : Quantity.create(txn.getType().getSerializedType());
  }

  @JsonGetter(value = "blockHash")
  public String getBlockHash() {
    return blockHash;
  }

  @JsonGetter(value = "blockNumber")
  public String getBlockNumber() {
    return blockNumber;
  }

  @JsonGetter(value = "contractAddress")
  public String getContractAddress() {
    return contractAddress;
  }

  @JsonGetter(value = "cumulativeGasUsed")
  public String getCumulativeGasUsed() {
    return cumulativeGasUsed;
  }

  @JsonGetter(value = "from")
  public String getFrom() {
    return from;
  }

  @JsonGetter(value = "gasUsed")
  public String getGasUsed() {
    return gasUsed;
  }

  @JsonGetter(value = "blobGasUsed")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getBlobGasUsed() {
    return blobGasUsed;
  }

  @JsonGetter(value = "blobGasPrice")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getBlobGasPrice() {
    return blobGasPrice;
  }

  @JsonGetter(value = "effectiveGasPrice")
  public String getEffectiveGasPrice() {
    return effectiveGasPrice;
  }

  @JsonGetter(value = "logs")
  public List<TransactionReceiptLogResult> getLogs() {
    return logs;
  }

  @JsonGetter(value = "logsBloom")
  public String getLogsBloom() {
    return logsBloom;
  }

  @JsonGetter(value = "to")
  public String getTo() {
    return to;
  }

  @JsonGetter(value = "transactionHash")
  public String getTransactionHash() {
    return transactionHash;
  }

  @JsonGetter(value = "transactionIndex")
  public String getTransactionIndex() {
    return transactionIndex;
  }

  @JsonGetter(value = "type")
  public String getType() {
    return type;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonGetter(value = "revertReason")
  public String getRevertReason() {
    return revertReason;
  }

  private List<TransactionReceiptLogResult> logReceipts(
      final List<Log> logs,
      final long blockNumber,
      final Hash transactionHash,
      final Hash blockHash,
      final long blockTimestamp,
      final int transactionIndex,
      final int logIndexOffset) {
    final List<TransactionReceiptLogResult> logResults = new ArrayList<>(logs.size());

    for (int i = 0; i < logs.size(); i++) {
      final Log log = logs.get(i);
      logResults.add(
          new TransactionReceiptLogResult(
              log,
              blockNumber,
              transactionHash,
              blockHash,
              blockTimestamp,
              transactionIndex,
              i + logIndexOffset));
    }

    return logResults;
  }
}
