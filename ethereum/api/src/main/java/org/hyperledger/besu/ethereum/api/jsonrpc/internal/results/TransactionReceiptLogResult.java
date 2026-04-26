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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
  "address",
  "topics",
  "data",
  "blockNumber",
  "transactionHash",
  "transactionIndex",
  "blockHash",
  "blockTimestamp",
  "logIndex",
  "removed"
})
public class TransactionReceiptLogResult {

  private final String address;
  private final List<String> topics;
  private final String data;
  private final String blockNumber;
  private final String transactionHash;
  private final String transactionIndex;
  private final String blockHash;
  private final String blockTimestamp;
  private final String logIndex;
  private final boolean removed;

  public TransactionReceiptLogResult(
      final Log log,
      final long blockNumber,
      final Hash transactionHash,
      final Hash blockHash,
      final long blockTimestamp,
      final int transactionIndex,
      final int logIndex) {
    this.address = log.getLogger().toChecksumString();
    this.topics = new ArrayList<>(log.getTopics().size());

    for (final LogTopic topic : log.getTopics()) {
      topics.add(topic.toString());
    }

    this.data = log.getData().toString();
    this.blockNumber = Quantity.create(blockNumber);
    this.transactionHash = transactionHash.toString();
    this.transactionIndex = Quantity.create(transactionIndex);
    this.blockHash = blockHash.toString();
    this.blockTimestamp = Quantity.create(blockTimestamp);
    this.logIndex = Quantity.create(logIndex);

    // TODO: Handle chain reorgs, i.e. return `true` if log is removed
    this.removed = false;
  }

  @JsonGetter(value = "address")
  public String getAddress() {
    return address;
  }

  @JsonGetter(value = "topics")
  public List<String> getTopics() {
    return topics;
  }

  @JsonGetter(value = "data")
  public String getData() {
    return data;
  }

  @JsonGetter(value = "blockNumber")
  public String getBlockNumber() {
    return blockNumber;
  }

  @JsonGetter(value = "transactionHash")
  public String getTransactionHash() {
    return transactionHash;
  }

  @JsonGetter(value = "transactionIndex")
  public String getTransactionIndex() {
    return transactionIndex;
  }

  @JsonGetter(value = "blockHash")
  public String getBlockHash() {
    return blockHash;
  }

  @JsonGetter(value = "logIndex")
  public String getLogIndex() {
    return logIndex;
  }

  @JsonGetter(value = "removed")
  public boolean isRemoved() {
    return removed;
  }

  @JsonGetter(value = "blockTimestamp")
  public String getBlockTimestamp() {
    return blockTimestamp;
  }
}
