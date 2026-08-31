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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toUnmodifiableList;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.AbstractVerticle;

/** Manages JSON-RPC filter events. */
public class FilterManager extends AbstractVerticle {

  private static final int FILTER_TIMEOUT_CHECK_TIMER = 10000;

  private final FilterIdGenerator filterIdGenerator;
  private final FilterRepository filterRepository;
  private final BlockchainQueries blockchainQueries;
  private final long maxLogRange;
  private final Duration filterTimeout;

  FilterManager(
      final BlockchainQueries blockchainQueries,
      final TransactionPool transactionPool,
      final FilterIdGenerator filterIdGenerator,
      final FilterRepository filterRepository,
      final Duration filterTimeout,
      final long maxLogRange) {
    this.filterIdGenerator = filterIdGenerator;
    this.filterRepository = filterRepository;
    this.filterTimeout = filterTimeout;
    this.maxLogRange = maxLogRange;
    checkNotNull(blockchainQueries.getBlockchain());
    blockchainQueries.getBlockchain().observeBlockAdded(this::recordBlockEvent);
    transactionPool.subscribePendingTransactions(this::recordPendingTransactionEvent);
    this.blockchainQueries = blockchainQueries;
  }

  @Override
  public void start() {
    startFilterTimeoutTimer();
  }

  @Override
  public void stop() {
    filterRepository.deleteAll();
  }

  private void startFilterTimeoutTimer() {
    vertx.setPeriodic(
        FILTER_TIMEOUT_CHECK_TIMER,
        timerId -> new FilterTimeoutMonitor(filterRepository).checkFilters());
  }

  /**
   * Installs a new block filter
   *
   * @return the block filter id
   */
  public String installBlockFilter() {
    final String filterId = filterIdGenerator.nextId();
    filterRepository.save(new BlockFilter(filterId, filterTimeout));
    return filterId;
  }

  /**
   * Installs a pending transaction filter
   *
   * @return the transaction filter id
   */
  public String installPendingTransactionFilter() {
    final String filterId = filterIdGenerator.nextId();
    filterRepository.save(new PendingTransactionFilter(filterId, filterTimeout));
    return filterId;
  }

  /**
   * Installs a new log filter
   *
   * @param fromBlock {@link BlockParameter} Integer block number, or latest/pending/earliest.
   * @param toBlock {@link BlockParameter} Integer block number, or latest/pending/earliest.
   * @param logsQuery {@link LogsQuery} Addresses and/or topics to filter by
   * @return the log filter id
   */
  public String installLogFilter(
      final BlockParameter fromBlock, final BlockParameter toBlock, final LogsQuery logsQuery) {
    final String filterId = filterIdGenerator.nextId();
    filterRepository.save(new LogFilter(filterId, fromBlock, toBlock, logsQuery, filterTimeout));
    return filterId;
  }

  /**
   * Uninstalls the specified filter.
   *
   * @param filterId the id of the filter to remove
   * @return {@code true} if the filter was successfully removed; otherwise {@code false}
   */
  public boolean uninstallFilter(final String filterId) {
    if (filterRepository.exists(filterId)) {
      filterRepository.delete(filterId);
      return true;
    } else {
      return false;
    }
  }

  public void recordBlockEvent(final BlockAddedEvent event) {
    final Hash blockHash = event.getHeader().getHash();
    final Collection<BlockFilter> blockFilters =
        filterRepository.getFiltersOfType(BlockFilter.class);
    blockFilters.forEach(
        filter -> {
          synchronized (filter) {
            filter.addBlockHash(blockHash);
          }
        });

    final List<LogWithMetadata> logsWithMetadata = event.getLogsWithMetadata();
    filterRepository.getFiltersOfType(LogFilter.class).stream()
        .filter(
            // Only keep filters where the "to" block could include the block in the event
            filter -> {
              final Optional<Long> maybeToBlockNumber = filter.getToBlock().getNumber();
              return maybeToBlockNumber.isEmpty()
                  || maybeToBlockNumber.get() >= event.getHeader().getNumber();
            })
        .forEach(
            filter -> {
              final LogsQuery logsQuery = filter.getLogsQuery();
              filter.addLogs(
                  // We need to use privacy queries for private log filters but for regular
                  // log filters we already have all the info in the event
                  logsWithMetadata.stream()
                      .filter(logsQuery::matches)
                      .collect(toUnmodifiableList()));
            });
  }

  @VisibleForTesting
  void recordPendingTransactionEvent(final Transaction transaction) {
    final Collection<PendingTransactionFilter> pendingTransactionFilters =
        filterRepository.getFiltersOfType(PendingTransactionFilter.class);
    if (pendingTransactionFilters.isEmpty()) {
      return;
    }

    pendingTransactionFilters.forEach(
        filter -> {
          synchronized (filter) {
            filter.addTransactionHash(transaction.getHash());
          }
        });
  }

  /**
   * Gets the new block hashes that have occurred since the filter was last checked.
   *
   * @param filterId the id of the filter to get the new blocks for
   * @return the new block hashes that have occurred since the filter was last checked
   */
  public List<Hash> blockChanges(final String filterId) {
    final BlockFilter filter = filterRepository.getFilter(filterId, BlockFilter.class).orElse(null);
    if (filter == null) {
      return null;
    }

    final List<Hash> hashes;
    synchronized (filter) {
      hashes = new ArrayList<>(filter.blockHashes());
      filter.clearBlockHashes();
      filter.resetExpireTime();
    }
    return hashes;
  }

  /**
   * Gets the pending transactions that have occurred since the filter was last checked.
   *
   * @param filterId the id of the filter to get the pending transactions for
   * @return the new pending transaction hashes that have occurred since the filter was last checked
   */
  public List<Hash> pendingTransactionChanges(final String filterId) {
    final PendingTransactionFilter filter =
        filterRepository.getFilter(filterId, PendingTransactionFilter.class).orElse(null);
    if (filter == null) {
      return null;
    }

    final List<Hash> hashes;
    synchronized (filter) {
      hashes = new ArrayList<>(filter.transactionHashes());
      filter.clearTransactionHashes();
      filter.resetExpireTime();
    }
    return hashes;
  }

  public List<LogWithMetadata> logsChanges(final String filterId) {
    final LogFilter filter = filterRepository.getFilter(filterId, LogFilter.class).orElse(null);
    if (filter == null) {
      return null;
    }

    final List<LogWithMetadata> logs;
    synchronized (filter) {
      logs = new ArrayList<>(filter.logs());
      filter.clearLogs();
      filter.resetExpireTime();
    }
    return logs;
  }

  public List<LogWithMetadata> logs(final String filterId, final Supplier<Boolean> isAlive) {
    final LogFilter filter = filterRepository.getFilter(filterId, LogFilter.class).orElse(null);
    if (filter == null) {
      return null;
    } else {
      filter.resetExpireTime();
    }

    // Read head exactly once so that LATEST..LATEST filters always refer to the same block,
    // avoiding a race where a new block lands between the two reads and shifts the range.
    final long headBlockNumber = blockchainQueries.headBlockNumber();
    final long fromBlockNumber = resolveFilterBlockNumber(filter.getFromBlock(), headBlockNumber);
    final long toBlockNumber = resolveFilterBlockNumber(filter.getToBlock(), headBlockNumber);

    if (maxLogRange > 0 && (toBlockNumber - fromBlockNumber) > maxLogRange) {
      throw new InvalidJsonRpcParameters(
          "Requested range exceeds maximum range limit", RpcErrorType.EXCEEDS_RPC_MAX_BLOCK_RANGE);
    }

    return findLogsWithinRange(filter, fromBlockNumber, toBlockNumber, isAlive);
  }

  // Resolves a filter block parameter to a concrete block number without calling headBlockNumber()
  // again. FINALIZED and SAFE are looked up via the chain; everything else (LATEST, PENDING,
  // NUMERIC, EARLIEST) either returns its stored number or falls back to the already-read head.
  private long resolveFilterBlockNumber(final BlockParameter param, final long headBlockNumber) {
    if (param.isFinalized() || param.isSafe()) {
      return param.getBlockNumber(blockchainQueries).orElse(headBlockNumber);
    }
    return param.getNumber().orElse(headBlockNumber);
  }

  private List<LogWithMetadata> findLogsWithinRange(
      final LogFilter filter,
      final long fromBlockNumber,
      final long toBlockNumber,
      final Supplier<Boolean> isAlive) {
    return blockchainQueries.matchingLogs(
        fromBlockNumber, toBlockNumber, filter.getLogsQuery(), isAlive);
  }
}
