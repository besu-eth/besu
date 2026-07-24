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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.json.QuantityJson;
import org.hyperledger.besu.ethereum.core.json.TransactionJson;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "parentHash",
  "feeRecipient",
  "stateRoot",
  "receiptsRoot",
  "logsBloom",
  "prevRandao",
  "blockNumber",
  "gasLimit",
  "gasUsed",
  "timestamp",
  "extraData",
  "baseFeePerGas",
  "blockHash",
  "transactions"
})
public sealed class ExecutionPayloadV1 permits ExecutionPayloadV2 {
  private Hash blockHash;
  private Hash parentHash;
  private Address feeRecipient;
  private Hash stateRoot;
  private long blockNumber;
  private Bytes32 prevRandao;
  private Wei baseFeePerGas;
  private long gasLimit;
  private long gasUsed;
  private long timestamp;
  private Bytes extraData;
  private Hash receiptsRoot;
  private LogsBloomFilter logsBloom;
  private List<Transaction> transactions;

  public ExecutionPayloadV1() {}

  public ExecutionPayloadV1(final BlockHeader header, final List<Transaction> transactions) {
    setFieldsFromBlock(header, transactions);
  }

  protected void setFieldsFromBlock(
      final BlockHeader header, final List<Transaction> transactions) {
    this.blockNumber = header.getNumber();
    this.blockHash = header.getHash();
    this.parentHash = header.getParentHash();
    this.logsBloom = header.getLogsBloom();
    this.stateRoot = header.getStateRoot();
    this.receiptsRoot = header.getReceiptsRoot();
    this.extraData = header.getExtraData();
    this.baseFeePerGas = header.getBaseFee().orElse(null);
    this.gasLimit = header.getGasLimit();
    this.gasUsed = header.getGasUsed();
    this.timestamp = header.getTimestamp();
    this.transactions = transactions;
    this.feeRecipient = header.getCoinbase();
    this.prevRandao = header.getPrevRandao().orElse(null);
  }

  @JsonSetter("blockHash")
  public void setBlockHash(final Hash blockHash) {
    this.blockHash = blockHash;
  }

  @JsonSetter("parentHash")
  public void setParentHash(final Hash parentHash) {
    this.parentHash = parentHash;
  }

  @JsonSetter("feeRecipient")
  public void setFeeRecipient(final Address feeRecipient) {
    this.feeRecipient = feeRecipient;
  }

  @JsonSetter("stateRoot")
  public void setStateRoot(final Hash stateRoot) {
    this.stateRoot = stateRoot;
  }

  @JsonSetter("blockNumber")
  @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
  public void setBlockNumber(final long blockNumber) {
    this.blockNumber = blockNumber;
  }

  @JsonSetter("baseFeePerGas")
  public void setBaseFeePerGas(final Wei baseFeePerGas) {
    this.baseFeePerGas = baseFeePerGas;
  }

  @JsonSetter("gasLimit")
  @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
  public void setGasLimit(final long gasLimit) {
    this.gasLimit = gasLimit;
  }

  @JsonSetter("gasUsed")
  @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
  public void setGasUsed(final long gasUsed) {
    this.gasUsed = gasUsed;
  }

  @JsonSetter("timestamp")
  @JsonDeserialize(using = QuantityJson.LongDeserializer.class)
  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }

  @JsonSetter("extraData")
  public void setExtraData(final Bytes extraData) {
    if (extraData == null) {
      throw new IllegalArgumentException("extraData must be present");
    }
    this.extraData = extraData;
  }

  @JsonSetter("receiptsRoot")
  public void setReceiptsRoot(final Hash receiptsRoot) {
    this.receiptsRoot = receiptsRoot;
  }

  @JsonSetter("logsBloom")
  public void setLogsBloom(final LogsBloomFilter logsBloom) {
    this.logsBloom = logsBloom;
  }

  @JsonSetter("prevRandao")
  public void setPrevRandao(final Bytes32 prevRandao) {
    this.prevRandao = prevRandao;
  }

  @JsonSetter("transactions")
  @JsonDeserialize(contentUsing = TransactionJson.BlockBodyDeserializer.class)
  public void setTransactions(final List<Transaction> transactions) {
    this.transactions = transactions;
  }

  public Hash getBlockHash() {
    return blockHash;
  }

  public Hash getParentHash() {
    return parentHash;
  }

  public Address getFeeRecipient() {
    return feeRecipient;
  }

  public Hash getStateRoot() {
    return stateRoot;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public long getBlockNumber() {
    return blockNumber;
  }

  public Wei getBaseFeePerGas() {
    return baseFeePerGas;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public long getGasLimit() {
    return gasLimit;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public long getGasUsed() {
    return gasUsed;
  }

  @JsonSerialize(using = QuantityJson.LongSerializer.class)
  public long getTimestamp() {
    return timestamp;
  }

  public Bytes getExtraData() {
    return extraData;
  }

  public Hash getReceiptsRoot() {
    return receiptsRoot;
  }

  public LogsBloomFilter getLogsBloom() {
    return logsBloom;
  }

  public Bytes32 getPrevRandao() {
    return prevRandao;
  }

  @JsonSerialize(contentUsing = TransactionJson.BlockBodySerializer.class)
  public List<Transaction> getTransactions() {
    return transactions;
  }
}
