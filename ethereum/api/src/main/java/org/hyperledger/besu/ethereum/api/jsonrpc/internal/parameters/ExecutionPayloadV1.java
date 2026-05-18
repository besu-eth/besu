/*
 * Copyright contributors to Hyperledger Besu.
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
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.tuweni.bytes.Bytes32;

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
  private String extraData;
  private Hash receiptsRoot;
  private LogsBloomFilter logsBloom;
  private List<String> transactions;

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
  public void setBlockNumber(final UnsignedLongParameter blockNumber) {
    this.blockNumber = blockNumber.getValue();
  }

  @JsonSetter("baseFeePerGas")
  public void setBaseFeePerGas(final String baseFeePerGas) {
    this.baseFeePerGas = Wei.fromHexString(baseFeePerGas);
  }

  @JsonSetter("gasLimit")
  public void setGasLimit(final UnsignedLongParameter gasLimit) {
    this.gasLimit = gasLimit.getValue();
  }

  @JsonSetter("gasUsed")
  public void setGasUsed(final UnsignedLongParameter gasUsed) {
    this.gasUsed = gasUsed.getValue();
  }

  @JsonSetter("timestamp")
  public void setTimestamp(final UnsignedLongParameter timestamp) {
    this.timestamp = timestamp.getValue();
  }

  @JsonSetter("extraData")
  public void setExtraData(final String extraData) {
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
  public void setPrevRandao(final String prevRandao) {
    this.prevRandao = Bytes32.fromHexString(prevRandao);
  }

  @JsonSetter("transactions")
  public void setTransactions(final List<String> transactions) {
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

  public long getBlockNumber() {
    return blockNumber;
  }

  public Wei getBaseFeePerGas() {
    return baseFeePerGas;
  }

  public long getGasLimit() {
    return gasLimit;
  }

  public long getGasUsed() {
    return gasUsed;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getExtraData() {
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

  public List<String> getTransactions() {
    return transactions;
  }
}
