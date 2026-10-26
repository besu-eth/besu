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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.LogWithMetadata;
import org.hyperledger.besu.plugin.services.BlockchainQueryService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes32;

/**
 * The BlockchainQueryService implementation. It delegates to the node's {@link BlockchainQueries},
 * so fee suggestions come from the same gas price oracle and range log queries use the same log
 * bloom cache as the JSON-RPC APIs.
 */
@Unstable
public class BlockchainQueryServiceImpl implements BlockchainQueryService {

  private final BlockchainQueries blockchainQueries;

  /**
   * Instantiates a new BlockchainQueryServiceImpl.
   *
   * @param blockchainQueries the blockchain queries to delegate to
   */
  public BlockchainQueryServiceImpl(final BlockchainQueries blockchainQueries) {
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public Wei getOracleGasPrice() {
    return blockchainQueries.gasPrice();
  }

  @Override
  public Wei getOracleMaxPriorityFeePerGas() {
    return blockchainQueries.gasPriorityFee();
  }

  @Override
  public List<LogWithMetadata> getLogs(
      final long fromBlockNumber,
      final long toBlockNumber,
      final List<Address> addresses,
      final List<List<Bytes32>> topics,
      final BooleanSupplier isQueryAlive) {
    Objects.requireNonNull(addresses, "addresses");
    Objects.requireNonNull(topics, "topics");
    Objects.requireNonNull(isQueryAlive, "isQueryAlive");
    if (fromBlockNumber < 0) {
      throw new IllegalArgumentException("fromBlockNumber must be >= 0 but was " + fromBlockNumber);
    }
    // Clamp to the head before delegating: matchingLogs walks every bloom-cache segment up to
    // toBlockNumber, so an unclamped Long.MAX_VALUE would spin until isQueryAlive turns false.
    // Results are unchanged, because blocks past the head have no logs to return.
    final long clampedToBlockNumber = Math.min(toBlockNumber, blockchainQueries.headBlockNumber());
    if (fromBlockNumber > clampedToBlockNumber) {
      return List.of();
    }
    // matchingLogs wraps any failure, including an aborted query, in
    // IllegalStateException("Error retrieving matching logs").
    return Collections.unmodifiableList(
        blockchainQueries.matchingLogs(
            fromBlockNumber,
            clampedToBlockNumber,
            buildQuery(addresses, topics),
            isQueryAlive::getAsBoolean));
  }

  @Override
  public List<LogWithMetadata> getLogs(
      final Hash blockHash, final List<Address> addresses, final List<List<Bytes32>> topics) {
    Objects.requireNonNull(blockHash, "blockHash");
    Objects.requireNonNull(addresses, "addresses");
    Objects.requireNonNull(topics, "topics");
    final LogsQuery query = buildQuery(addresses, topics);
    // Any block (canonical or not, with removed set when it is not canonical) and log indexes
    // relative to the block. matchingLogs wraps a failure to read the block in a RuntimeException.
    try {
      return Collections.unmodifiableList(
          blockchainQueries.matchingLogs(blockHash, query, () -> true));
    } catch (final RuntimeException e) {
      throw new IllegalStateException("Error retrieving the logs of block " + blockHash, e);
    }
  }

  /**
   * Builds the log query. An empty topic position becomes a single {@code null} wildcard. A {@code
   * null} position, or one containing a {@code null} entry, is a wildcard too (as in {@code
   * BlockchainService.subscribeLogs}). Such a position is reduced to the single {@code null}
   * because {@link LogsQuery}'s bloom prefilter ignores {@code null} entries, so keeping the other
   * values would skip blocks whose bloom lacks them.
   */
  private static LogsQuery buildQuery(
      final List<Address> addresses, final List<List<Bytes32>> topics) {
    final List<List<LogTopic>> transformedTopics = new ArrayList<>();
    for (final List<Bytes32> position : topics) {
      // Not position.contains(null): immutable lists such as List.of throw on it.
      if (position == null || position.isEmpty() || position.stream().anyMatch(Objects::isNull)) {
        transformedTopics.add(Collections.singletonList(null));
      } else {
        transformedTopics.add(position.stream().map(LogTopic::wrap).toList());
      }
    }
    return new LogsQuery.Builder().addresses(addresses).topics(transformedTopics).build();
  }
}
