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

  /** How {@code trace_filter} combines {@code fromAddress} and {@code toAddress}. */
  public enum TraceFilterMode {
    /** A trace matches every populated address list. */
    INTERSECTION,
    /** A trace matches either populated address list. */
    UNION;

    /**
     * Parses a mode from its exact name, so ordinals and other spellings are rejected.
     *
     * @param name {@code intersection} or {@code union}
     * @return the mode
     */
    @JsonCreator
    public static TraceFilterMode fromName(final String name) {
      return switch (name) {
        case "intersection" -> INTERSECTION;
        case "union" -> UNION;
        default -> throw new IllegalArgumentException("Unknown trace filter mode: " + name);
      };
    }
  }

  private final BlockParameter fromBlock;
  private final BlockParameter toBlock;
  private final List<Address> fromAddress;
  private final List<Address> toAddress;
  private final List<Address> addresses;
  private final List<List<LogTopic>> topics;
  private final Optional<Hash> maybeBlockHash;
  private final LogsQuery logsQuery;
  private final Optional<Integer> after, count;
  private final TraceFilterMode mode;
  private final boolean isValid;

  public FilterParameter(
      final BlockParameter fromBlock,
      final BlockParameter toBlock,
      final List<Address> fromAddress,
      final List<Address> toAddress,
      final List<Address> address,
      final List<List<LogTopic>> topics,
      final Hash blockHash,
      final Integer after,
      final Integer count) {
    this(
        fromBlock, toBlock, fromAddress, toAddress, address, topics, blockHash, after, count, null);
  }

  @JsonCreator
  public FilterParameter(
      @JsonProperty("fromBlock") final BlockParameter fromBlock,
      @JsonProperty("toBlock") final BlockParameter toBlock,
      @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          @JsonProperty("fromAddress")
          final List<Address> fromAddress,
      @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) @JsonProperty("toAddress")
          final List<Address> toAddress,
      @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) @JsonProperty("address")
          final List<Address> address,
      @JsonDeserialize(using = TopicsDeserializer.class) @JsonProperty("topics")
          final List<List<LogTopic>> topics,
      @JsonProperty("blockHash") @JsonAlias({"blockhash"}) final Hash blockHash,
      @JsonProperty("after") final Integer after,
      @JsonProperty("count") final Integer count,
      @JsonProperty("mode") final TraceFilterMode mode) {
    this.isValid = blockHash == null || (fromBlock == null && toBlock == null);
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
    this.mode = mode != null ? mode : TraceFilterMode.INTERSECTION;
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

  public TraceFilterMode getMode() {
    return mode;
  }

  /**
   * Whether a trace with the given sender and recipient matches {@code fromAddress} and {@code
   * toAddress}. An empty list imposes no restriction. In intersection mode a trace matches every
   * populated list; in union mode it matches either populated list.
   *
   * @param from the trace's sender, if it has one
   * @param to the trace's recipient, if it has one
   * @return whether the trace matches the address filter
   */
  public boolean matchesTraceAddresses(final Optional<Address> from, final Optional<Address> to) {
    final boolean fromMatches = from.map(fromAddress::contains).orElse(false);
    final boolean toMatches = to.map(toAddress::contains).orElse(false);
    return switch (mode) {
      case INTERSECTION ->
          (fromAddress.isEmpty() || fromMatches) && (toAddress.isEmpty() || toMatches);
      case UNION -> (fromAddress.isEmpty() && toAddress.isEmpty()) || fromMatches || toMatches;
    };
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
        && Objects.equals(count, that.count)
        && mode == that.mode;
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
        mode,
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
        + ", mode="
        + mode
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
