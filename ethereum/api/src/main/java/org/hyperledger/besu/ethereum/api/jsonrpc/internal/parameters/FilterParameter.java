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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static java.util.Collections.emptyList;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class FilterParameter {

  private final BlockParameter fromBlock;
  private final BlockParameter toBlock;
  private final List<Address> fromAddress;
  private final List<Address> toAddress;
  private final List<Address> addresses;
  private final List<List<LogTopic>> topics;
  private final Optional<Hash> maybeBlockHash;
  private final LogsQuery logsQuery;
  private final Optional<Integer> after, count;
  private final boolean isValid;

  @JsonCreator
  public FilterParameter(
      @JsonProperty("fromBlock") final BlockParameter fromBlock,
      @JsonProperty("toBlock") final BlockParameter toBlock,
      @JsonProperty("fromAddress") final List<Address> fromAddress,
      @JsonProperty("toAddress") final List<Address> toAddress,
      @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) @JsonProperty("address")
          final List<Address> address,
      @JsonDeserialize(using = TopicsDeserializer.class) @JsonProperty("topics")
          final List<List<LogTopic>> topics,
      @JsonProperty("blockHash") @JsonAlias({"blockhash"}) final Hash blockHash,
      @JsonProperty("after") final Integer after,
      @JsonProperty("count") final Integer count) {
    this.isValid = blockHash == null || (fromBlock == null && toBlock == null);
    // trace_filter pagination counts traces, so a negative offset or count is malformed
    if ((after != null && after < 0) || (count != null && count < 0)) {
      throw new IllegalArgumentException("after and count must not be negative");
    }
    this.fromBlock = fromBlock != null ? fromBlock : BlockParameter.LATEST;
    this.toBlock = toBlock != null ? toBlock : BlockParameter.LATEST;
    this.fromAddress = fromAddress != null ? fromAddress : emptyList();
    this.toAddress = toAddress != null ? toAddress : emptyList();
    this.addresses = address != null ? address : emptyList();
    this.topics = topics != null ? topics : emptyList();
    this.logsQuery = new LogsQuery(addresses, topics);
    this.maybeBlockHash = Optional.ofNullable(blockHash);
    this.after = Optional.ofNullable(after);
    this.count = Optional.ofNullable(count);
  }

  public BlockParameter getFromBlock() {
    return fromBlock;
  }

  public BlockParameter getToBlock() {
    return toBlock;
  }

  public List<Address> getFromAddress() {
    return fromAddress;
  }

  public List<Address> getToAddress() {
    return toAddress;
  }

  public List<Address> getAddresses() {
    return addresses;
  }

  public List<List<LogTopic>> getTopics() {
    return topics;
  }

  public Optional<Hash> getBlockHash() {
    return maybeBlockHash;
  }

  public LogsQuery getLogsQuery() {
    return logsQuery;
  }

  public Optional<Integer> getAfter() {
    return after;
  }

  public Optional<Integer> getCount() {
    return count;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FilterParameter that = (FilterParameter) o;
    return isValid == that.isValid
        && Objects.equals(fromBlock, that.fromBlock)
        && Objects.equals(toBlock, that.toBlock)
        && Objects.equals(fromAddress, that.fromAddress)
        && Objects.equals(toAddress, that.toAddress)
        && Objects.equals(addresses, that.addresses)
        && Objects.equals(topics, that.topics)
        && Objects.equals(maybeBlockHash, that.maybeBlockHash)
        && Objects.equals(logsQuery, that.logsQuery)
        && Objects.equals(after, that.after)
        && Objects.equals(count, that.count);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fromBlock,
        toBlock,
        fromAddress,
        toAddress,
        addresses,
        topics,
        maybeBlockHash,
        logsQuery,
        after,
        count,
        isValid);
  }

  @Override
  public String toString() {
    return "FilterParameter{"
        + "fromBlock="
        + fromBlock
        + ", toBlock="
        + toBlock
        + ", fromAddress="
        + fromAddress
        + ", toAddress="
        + toAddress
        + ", addresses="
        + addresses
        + ", topics="
        + topics
        + ", maybeBlockHash="
        + maybeBlockHash
        + ", logsQuery="
        + logsQuery
        + ", after="
        + after
        + ", count="
        + count
        + ", isValid="
        + isValid
        + '}';
  }

  public boolean isValid() {
    return isValid;
  }

  /**
   * Validates that fromBlock <= toBlock and toBlock <= latestBlock for already resolved block
   * numbers.
   *
   * @param fromBlockNumber the resolved from block number
   * @param toBlockNumber the resolved to block number
   * @param latestBlockNumber the latest block number in the chain
   * @throws InvalidJsonRpcParameters if fromBlock > toBlock or toBlock > latestBlock
   */
  public static void validateBlockRange(
      final long fromBlockNumber, final long toBlockNumber, final long latestBlockNumber) {
    if (fromBlockNumber > toBlockNumber) {
      throw new InvalidJsonRpcParameters(
          "fromBlock (" + fromBlockNumber + ") is greater than toBlock (" + toBlockNumber + ")",
          RpcErrorType.INVALID_PARAMS);
    }
    if (toBlockNumber > latestBlockNumber) {
      throw new InvalidJsonRpcParameters(
          "toBlock ("
              + toBlockNumber
              + ") is greater than latest block ("
              + latestBlockNumber
              + ")",
          RpcErrorType.INVALID_PARAMS);
    }
  }
}
