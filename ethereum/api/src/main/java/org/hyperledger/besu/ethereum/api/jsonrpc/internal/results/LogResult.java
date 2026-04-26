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

import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** A single log result. */
@JsonPropertyOrder({
  "logIndex",
  "removed",
  "blockNumber",
  "blockHash",
  "blockTimestamp",
  "transactionHash",
  "transactionIndex",
  "address",
  "data",
  "topics"
})
public class LogResult implements JsonRpcResult {

  private final String logIndex;
  private final String blockNumber;
  private final String blockHash;
  private final String blockTimestamp;
  private final String transactionHash;
  private final String transactionIndex;
  private final String address;
  private final String data;
  private final List<String> topics;
  private final boolean removed;

  public LogResult(final LogWithMetadata logWithMetadata) {
    this.logIndex = Quantity.create(logWithMetadata.getLogIndex());
    this.blockNumber = Quantity.create(logWithMetadata.getBlockNumber());
    this.blockHash = logWithMetadata.getBlockHash().toString();
    this.blockTimestamp = Quantity.create(logWithMetadata.getBlockTimestamp());
    this.transactionHash = logWithMetadata.getTransactionHash().toString();
    this.transactionIndex = Quantity.create(logWithMetadata.getTransactionIndex());
    this.address = logWithMetadata.getLogger().toChecksumString();
    this.data = logWithMetadata.getData().toString();
    this.topics = new ArrayList<>(logWithMetadata.getTopics().size());
    this.removed = logWithMetadata.isRemoved();

    for (final LogTopic topic : logWithMetadata.getTopics()) {
      topics.add(topic.toString());
    }
  }

  @JsonGetter(value = "logIndex")
  public String getLogIndex() {
    return logIndex;
  }

  @JsonGetter(value = "blockNumber")
  public String getBlockNumber() {
    return blockNumber;
  }

  @JsonGetter(value = "blockHash")
  public String getBlockHash() {
    return blockHash;
  }

  @JsonGetter(value = "blockTimestamp")
  public String getBlockTimestamp() {
    return blockTimestamp;
  }

  @JsonGetter(value = "transactionHash")
  public String getTransactionHash() {
    return transactionHash;
  }

  @JsonGetter(value = "transactionIndex")
  public String getTransactionIndex() {
    return transactionIndex;
  }

  @JsonGetter(value = "address")
  public String getAddress() {
    return address;
  }

  @JsonGetter(value = "data")
  public String getData() {
    return data;
  }

  @JsonGetter(value = "topics")
  public List<String> getTopics() {
    return topics;
  }

  @JsonGetter(value = "removed")
  public boolean isRemoved() {
    return removed;
  }
}
