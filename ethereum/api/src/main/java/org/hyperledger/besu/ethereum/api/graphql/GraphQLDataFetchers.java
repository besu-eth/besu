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
package org.hyperledger.besu.ethereum.api.graphql;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.AccountAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.EmptyAccountAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.LogAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.NormalBlockAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.PendingStateAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.SyncStateAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter.TransactionAdapter;
import org.hyperledger.besu.ethereum.api.graphql.internal.response.GraphQLError;
import org.hyperledger.besu.ethereum.api.query.BackendQuery;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.SyncStatus;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import graphql.GraphQLContext;
import graphql.schema.DataFetcher;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * This class contains data fetchers for GraphQL queries.
 *
 * <p>Data fetchers are responsible for fetching data for a specific field. Each field in the schema
 * is associated with a data fetcher. When the field is being processed during a query, the
 * associated data fetcher is invoked to get the data for that field.
 *
 * <p>This class contains data fetchers for various fields such as protocol version, syncing state,
 * pending state, gas price, chain ID, max priority fee per gas, range block, block, account, logs,
 * and transaction.
 *
 * <p>Each data fetcher is a method that returns a `DataFetcher` object. The `DataFetcher` object
 * defines how to fetch the data for the field. It takes a `DataFetchingEnvironment` object as input
 * which contains all the context needed to fetch the data.
 */
public class GraphQLDataFetchers {

  /**
   * Default maximum number of blocks a single {@code blocks(from,to)} query may span, used when no
   * explicit limit is supplied (e.g. by test call sites). Mirrors {@link
   * GraphQLConfiguration#DEFAULT_MAX_BLOCK_RANGE}.
   */
  public static final long DEFAULT_MAX_BLOCK_RANGE = GraphQLConfiguration.DEFAULT_MAX_BLOCK_RANGE;

  private final Integer highestEthVersion;
  private final long maxBlockRange;

  /**
   * Constructs a new GraphQLDataFetchers instance.
   *
   * <p>This constructor takes a set of supported capabilities and determines the highest Ethereum
   * protocol version supported by these capabilities. This version is then stored and can be
   * fetched using the getProtocolVersionDataFetcher method.
   *
   * @param supportedCapabilities a set of capabilities supported by the Ethereum node
   */
  public GraphQLDataFetchers(final Set<Capability> supportedCapabilities) {
    this(supportedCapabilities, DEFAULT_MAX_BLOCK_RANGE);
  }

  /**
   * Constructs a new GraphQLDataFetchers instance with an explicit cap on the span of a single
   * {@code blocks(from,to)} range query.
   *
   * @param supportedCapabilities a set of capabilities supported by the Ethereum node
   * @param maxBlockRange the maximum number of blocks a single {@code blocks(from,to)} query may
   *     span; 0 means no limit
   */
  public GraphQLDataFetchers(
      final Set<Capability> supportedCapabilities, final long maxBlockRange) {
    final OptionalInt version =
        supportedCapabilities.stream()
            .filter(cap -> EthProtocol.NAME.equals(cap.getName()))
            .mapToInt(Capability::getVersion)
            .max();
    highestEthVersion = version.isPresent() ? version.getAsInt() : null;
    this.maxBlockRange = maxBlockRange;
  }

  /**
   * Returns a DataFetcher that fetches the highest Ethereum protocol version supported by the node.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input and returns the highest Ethereum protocol version as an
   * Optional<Integer>.
   *
   * @return a DataFetcher that fetches the highest Ethereum protocol version
   */
  DataFetcher<Optional<Integer>> getProtocolVersionDataFetcher() {
    return dataFetchingEnvironment -> Optional.of(highestEthVersion);
  }

  /**
   * Returns a DataFetcher that fetches the result of sending a raw transaction.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input and returns the hash of the transaction if it is valid
   * and added to the transaction pool. If the transaction is invalid, it throws a GraphQLException
   * with the invalid reason. If the raw transaction data cannot be read, it throws a
   * GraphQLException with INVALID_PARAMS error.
   *
   * @return a DataFetcher that fetches the result of sending a raw transaction
   */
  DataFetcher<Optional<Bytes32>> getSendRawTransactionDataFetcher() {
    return dataFetchingEnvironment -> {
      try {
        final TransactionPool transactionPool =
            dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.TRANSACTION_POOL);
        final Bytes rawTran = dataFetchingEnvironment.getArgument("data");

        final Transaction transaction = Transaction.readFrom(RLP.input(rawTran));
        final ValidationResult<TransactionInvalidReason> validationResult =
            transactionPool.addTransactionViaApi(transaction);
        if (validationResult.isValid()) {
          return Optional.of(Bytes32.wrap(transaction.getHash().getBytes()));
        } else {
          throw new GraphQLException(GraphQLError.of(validationResult.getInvalidReason()));
        }
      } catch (final IllegalArgumentException | RLPException e) {
        throw new GraphQLException(GraphQLError.INVALID_PARAMS);
      }
    };
  }

  /**
   * Returns a DataFetcher that fetches the syncing state of the Ethereum node.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input and returns the syncing state as an
   * Optional<SyncStateAdapter>.
   *
   * <p>The SyncStateAdapter is a wrapper around the SyncStatus of the Ethereum node. It provides
   * information about the current syncing state of the node such as the current block, highest
   * block, and starting block.
   *
   * @return a DataFetcher that fetches the syncing state of the Ethereum node
   */
  DataFetcher<Optional<SyncStateAdapter>> getSyncingDataFetcher() {
    return dataFetchingEnvironment -> {
      final Synchronizer synchronizer =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.SYNCHRONIZER);
      final Optional<SyncStatus> syncStatus = synchronizer.getSyncStatus();
      return syncStatus.map(SyncStateAdapter::new);
    };
  }

  DataFetcher<Optional<PendingStateAdapter>> getPendingStateDataFetcher() {
    return dataFetchingEnvironment -> {
      final TransactionPool txPool =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.TRANSACTION_POOL);
      return Optional.of(new PendingStateAdapter(txPool));
    };
  }

  DataFetcher<Wei> getGasPriceDataFetcher() {
    return dataFetchingEnvironment -> {
      final GraphQLContext graphQLContext = dataFetchingEnvironment.getGraphQlContext();
      final BlockchainQueries blockchainQueries =
          graphQLContext.get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      return blockchainQueries.gasPrice();
    };
  }

  /**
   * Returns a DataFetcher that fetches the chain ID of the Ethereum node.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input and returns the chain ID as an {@code
   * Optional<BigInteger>}.
   *
   * @return a DataFetcher that fetches the chain ID of the Ethereum node
   */
  public DataFetcher<Optional<BigInteger>> getChainIdDataFetcher() {
    return dataFetchingEnvironment -> {
      final GraphQLContext graphQLContext = dataFetchingEnvironment.getGraphQlContext();
      return graphQLContext.get(GraphQLContextType.CHAIN_ID);
    };
  }

  /**
   * Returns a DataFetcher that fetches the maximum priority fee per gas of the Ethereum node.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input and returns the maximum priority fee per gas as a Wei
   * object.
   *
   * @return a DataFetcher that fetches the maximum priority fee per gas of the Ethereum node
   */
  public DataFetcher<Wei> getMaxPriorityFeePerGasDataFetcher() {
    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchainQuery =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      return blockchainQuery.gasPriorityFee();
    };
  }

  DataFetcher<List<NormalBlockAdapter>> getRangeBlockDataFetcher() {

    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchainQuery =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      final Supplier<Boolean> isAlive =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.IS_ALIVE_HANDLER);

      final long chainHeadBlockNumber = blockchainQuery.getBlockchain().getChainHeadBlockNumber();
      long from;
      if (dataFetchingEnvironment.containsArgument("from")) {
        from = dataFetchingEnvironment.getArgument("from");
      } else {
        from = chainHeadBlockNumber;
      }
      long to;
      if (dataFetchingEnvironment.containsArgument("to")) {
        to = dataFetchingEnvironment.getArgument("to");
      } else {
        to = chainHeadBlockNumber;
      }
      if (from < 0 || from > to) {
        throw new GraphQLException(GraphQLError.INVALID_PARAMS);
      }
      // Checked on the caller-supplied (pre-clamp) span so an attacker can't sidestep the cap by
      // supplying a `to` far beyond the chain head, relying on the clamp below to shrink it first.
      if (maxBlockRange > 0 && (to - from) > maxBlockRange) {
        throw new GraphQLException(GraphQLError.INVALID_PARAMS);
      }
      // A supplied `to` beyond the chain head only ever shrinks the loop below, so clamping here
      // (after the span check above) can only reduce work, never let more through.
      to = Math.min(to, chainHeadBlockNumber);

      final List<NormalBlockAdapter> results = new ArrayList<>();
      for (long i = from; i <= to; i++) {
        // IS_ALIVE_HANDLER is always populated for real HTTP requests (GraphQLHttpService), but
        // any call site that builds a GraphQlContext without it (tests, future callers) should
        // fail open here rather than NPE inside the loop.
        if (isAlive != null) {
          BackendQuery.stopIfExpired(isAlive);
        }
        final Optional<BlockWithMetadata<TransactionWithMetadata, Hash>> block =
            blockchainQuery.blockByNumber(i);
        block.ifPresent(e -> results.add(new NormalBlockAdapter(e)));
      }
      return results;
    };
  }

  /**
   * Returns a DataFetcher that fetches a specific block in the Ethereum blockchain.
   *
   * <p>The DataFetcher is a functional interface. It has a single method that takes a
   * DataFetchingEnvironment object as input. This method fetches a block based on either a block
   * number or a block hash. If both a block number and a block hash are provided, it throws a
   * GraphQLException with INVALID_PARAMS error. If neither a block number nor a block hash is
   * provided, it fetches the latest block.
   *
   * <p>The fetched block is then wrapped in a {@link NormalBlockAdapter} and returned as an {@code
   * Optional<NormalBlockAdapter>}.
   *
   * @return a DataFetcher that fetches a specific block in the Ethereum blockchain
   */
  public DataFetcher<Optional<NormalBlockAdapter>> getBlockDataFetcher() {

    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchain =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      final Long number = dataFetchingEnvironment.getArgument("number");
      final Hash hash = dataFetchingEnvironment.getArgument("hash");
      if ((number != null) && (hash != null)) {
        throw new GraphQLException(GraphQLError.INVALID_PARAMS);
      }

      final Optional<BlockWithMetadata<TransactionWithMetadata, Hash>> block;
      if (number != null) {
        block = blockchain.blockByNumber(number);
        checkArgument(block.isPresent(), "Block number %s was not found", number);
      } else if (hash != null) {
        block = blockchain.blockByHash(hash);
        Preconditions.checkArgument(block.isPresent(), "Block hash %s was not found", hash);
      } else {
        block = blockchain.latestBlock();
      }
      return block.map(NormalBlockAdapter::new);
    };
  }

  DataFetcher<Optional<AccountAdapter>> getAccountDataFetcher() {
    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchainQuery =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      final Address addr = dataFetchingEnvironment.getArgument("address");
      final Long bn = dataFetchingEnvironment.getArgument("blockNumber");
      if (bn != null) {
        return blockchainQuery
            .getAndMapWorldState(
                bn,
                ws -> {
                  final Account account = ws.get(addr);
                  if (account == null) {
                    return Optional.of(new EmptyAccountAdapter(addr));
                  }
                  return Optional.of(new AccountAdapter(account));
                })
            .or(
                () -> {
                  if (bn > blockchainQuery.getBlockchain().getChainHeadBlockNumber()) {
                    // block is past chainhead
                    throw new GraphQLException(GraphQLError.INVALID_PARAMS);
                  } else {
                    // we don't have that block
                    throw new GraphQLException(GraphQLError.CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE);
                  }
                });
      } else {
        // return account on latest block
        final long latestBn = blockchainQuery.latestBlock().orElseThrow().getHeader().getNumber();
        return blockchainQuery.getAndMapWorldState(
            latestBn,
            ws -> {
              final Account account = ws.get(addr);
              if (account == null) {
                return Optional.of(new EmptyAccountAdapter(addr));
              }
              return Optional.of(new AccountAdapter(account));
            });
      }
    };
  }

  DataFetcher<Optional<List<LogAdapter>>> getLogsDataFetcher() {
    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchainQuery =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);

      final Map<String, Object> filter = dataFetchingEnvironment.getArgument("filter");

      final long currentBlock = blockchainQuery.getBlockchain().getChainHeadBlockNumber();
      final long fromBlock = (Long) filter.getOrDefault("fromBlock", currentBlock);
      final long toBlock = (Long) filter.getOrDefault("toBlock", currentBlock);

      if (fromBlock > toBlock) {
        throw new GraphQLException(GraphQLError.INVALID_PARAMS);
      }

      @SuppressWarnings("unchecked")
      final List<Address> addrs = (List<Address>) filter.get("addresses");
      // `topics` is nullable in the schema, and the schema's own documentation says "[] or nil
      // matches any topic list", so an omitted value has to behave like an empty one rather than
      // being dereferenced. graphql-java puts the key in the map with a null value when a client
      // writes `topics: null` explicitly, so getOrDefault is not enough on its own.
      // (`addrs` may stay null: LogsQuery.Builder.addresses tolerates it.)
      @SuppressWarnings("unchecked")
      final List<List<LogTopic>> topics =
          Optional.ofNullable((List<List<LogTopic>>) filter.get("topics")).orElse(List.of());

      final List<List<LogTopic>> transformedTopics = new ArrayList<>();
      for (final List<LogTopic> topic : topics) {
        if (topic.isEmpty()) {
          transformedTopics.add(Collections.singletonList(null));
        } else {
          transformedTopics.add(topic);
        }
      }

      final LogsQuery query =
          new LogsQuery.Builder().addresses(addrs).topics(transformedTopics).build();

      final List<LogWithMetadata> logs =
          blockchainQuery.matchingLogs(
              fromBlock,
              toBlock,
              query,
              dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.IS_ALIVE_HANDLER));
      final List<LogAdapter> results = new ArrayList<>();
      for (final LogWithMetadata log : logs) {
        results.add(new LogAdapter(log));
      }
      return Optional.of(results);
    };
  }

  DataFetcher<Optional<TransactionAdapter>> getTransactionDataFetcher() {
    return dataFetchingEnvironment -> {
      final BlockchainQueries blockchain =
          dataFetchingEnvironment.getGraphQlContext().get(GraphQLContextType.BLOCKCHAIN_QUERIES);
      final Hash hash = dataFetchingEnvironment.getArgument("hash");
      final Optional<TransactionWithMetadata> tran = blockchain.transactionByHash(hash);
      return tran.map(this::getTransactionAdapter);
    };
  }

  private TransactionAdapter getTransactionAdapter(
      final TransactionWithMetadata transactionWithMetadata) {
    return new TransactionAdapter(transactionWithMetadata);
  }
}
