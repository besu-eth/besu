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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.LogWithMetadata;

import java.util.List;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes32;

/**
 * This service provides queries over the chain that are served by the node's API layer: fee
 * suggestions from the node's gas price oracle and log queries over block ranges. It is separate
 * from {@link BlockchainService} because it is backed by that API layer (the gas price oracle and
 * the log bloom cache), which exists only once the node is built.
 *
 * <p>The service is available from the {@code start} callback onward. During the {@code register}
 * and {@code beforeExternalServices} callbacks it is not yet registered.
 */
@Unstable
public interface BlockchainQueryService extends BesuService {

  /**
   * Get the gas price suggested by the node's gas price oracle.
   *
   * <p>The suggestion samples the recent blocks configured by the --api-gas-price-blocks,
   * --api-gas-price-percentile and --api-gas-price-max options, and is never lower than the minimum
   * gas price (--min-gas-price), or than the next block's base fee on a base fee market. When
   * sampling is disabled (--api-gas-price-blocks=0) or there is nothing to sample, that lower bound
   * is returned.
   *
   * <p>The value is not limited by --api-gas-and-priority-fee-limiting-enabled, so it can differ
   * from what {@code eth_gasPrice} returns when that option is enabled.
   *
   * @return the suggested gas price
   */
  Wei getOracleGasPrice();

  /**
   * Get the max priority fee per gas suggested by the node's gas price oracle.
   *
   * <p>The suggestion samples the same recent blocks as {@link #getOracleGasPrice()} and is never
   * lower than the minimum priority fee (--min-priority-fee). When sampling is disabled
   * (--api-gas-price-blocks=0) or there is nothing to sample, the minimum priority fee is returned.
   *
   * <p>No further limiting applies, so the value is the same as what {@code
   * eth_maxPriorityFeePerGas} returns.
   *
   * @return the suggested max priority fee per gas
   */
  Wei getOracleMaxPriorityFeePerGas();

  /**
   * Get the logs on the canonical chain in the given block range that match the given filter. The
   * range stops at the chain head. An empty address list matches any address. Topics are matched by
   * position, where an empty or {@code null} inner list, or one with a {@code null} entry, matches
   * any value at that position. A log must have at least {@code topics.size()} topics to match,
   * even when every position is a wildcard. No limits on the range or the number of addresses are
   * applied; callers enforce their own. A range that starts after the chain head, or whose end is
   * before its start, is empty.
   *
   * <p>The log index of each returned log is its position among all the logs of its block, and
   * {@link LogWithMetadata#isRemoved()} is false, as every block in the range is canonical.
   *
   * @param fromBlockNumber the first block of the range, inclusive; must not be negative
   * @param toBlockNumber the last block of the range, inclusive; values past the chain head are
   *     treated as the chain head
   * @param addresses the contract addresses to match, empty for any
   * @param topics the topics to match by position, empty for any
   * @param isQueryAlive checked while the query runs; the query is aborted when it returns false
   * @return the matching logs, in block order
   * @throws IllegalArgumentException if {@code fromBlockNumber} is negative
   * @throws NullPointerException if {@code addresses}, {@code topics} or {@code isQueryAlive} is
   *     null
   * @throws IllegalStateException if the query is aborted or the logs cannot be retrieved
   */
  List<LogWithMetadata> getLogs(
      long fromBlockNumber,
      long toBlockNumber,
      List<Address> addresses,
      List<List<Bytes32>> topics,
      BooleanSupplier isQueryAlive);

  /**
   * Get the logs of the block with the given hash that match the given filter. The block does not
   * need to be on the canonical chain. Matching follows the same rules as {@link #getLogs(long,
   * long, List, List, BooleanSupplier)}.
   *
   * <p>The log index of each returned log is its position among all the logs of the block. {@link
   * LogWithMetadata#isRemoved()} is true when the block is not on the canonical chain.
   *
   * @param blockHash the block hash
   * @param addresses the contract addresses to match, empty for any
   * @param topics the topics to match by position, empty for any
   * @return the matching logs, empty if the block is unknown
   * @throws NullPointerException if {@code blockHash}, {@code addresses} or {@code topics} is null
   * @throws IllegalStateException if the block's receipts or transactions cannot be read from
   *     storage
   */
  List<LogWithMetadata> getLogs(
      Hash blockHash, List<Address> addresses, List<List<Bytes32>> topics);
}
