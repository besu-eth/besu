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
package org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLContextType;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.common.primitives.Longs;
import graphql.schema.DataFetchingEnvironment;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The BlockAdapterBase class extends the AdapterBase class. It provides methods to get and
 * manipulate block data.
 */
@SuppressWarnings("unused") // reflected by GraphQL
public class BlockAdapterBase extends AdapterBase {

  private final BlockHeader header;

  /**
   * Constructs a new BlockAdapterBase with the given BlockHeader.
   *
   * @param header the BlockHeader to be adapted
   */
  BlockAdapterBase(final BlockHeader header) {
    this.header = header;
  }

  /**
   * Returns the parent block of the current block.
   *
   * @param environment the DataFetchingEnvironment
   * @return an Optional containing the parent block if it exists, otherwise an empty Optional
   */
  public Optional<NormalBlockAdapter> getParent(final DataFetchingEnvironment environment) {
    final BlockchainQueries query = getBlockchainQueries(environment);
    final Hash parentHash = header.getParentHash();
    final Optional<BlockWithMetadata<TransactionWithMetadata, Hash>> block =
        query.blockByHash(parentHash);
    return block.map(NormalBlockAdapter::new);
  }

  /**
   * Returns the hash of the current block.
   *
   * @return the hash of the block
   */
  public Hash getHash() {
    return header.getHash();
  }

  /**
   * Returns the nonce of the current block.
   *
   * @return the nonce of the block
   */
  public Bytes getNonce() {
    final long nonce = header.getNonce();
    final byte[] bytes = Longs.toByteArray(nonce);
    return Bytes.wrap(bytes);
  }

  /**
   * Returns the transactions root of the current block.
   *
   * @return the transactions root of the block
   */
  public Hash getTransactionsRoot() {
    return header.getTransactionsRoot();
  }

  /**
   * Returns the state root of the current block.
   *
   * @return the state root of the block
   */
  public Hash getStateRoot() {
    return header.getStateRoot();
  }

  /**
   * Returns the receipts root of the current block.
   *
   * @return the receipts root of the block
   */
  public Hash getReceiptsRoot() {
    return header.getReceiptsRoot();
  }

  /**
   * Returns the miner of the current block.
   *
   * @param environment the DataFetchingEnvironment
   * @return an AccountAdapter instance representing the miner of the block
   */
  public AccountAdapter getMiner(final DataFetchingEnvironment environment) {

    final BlockchainQueries query = getBlockchainQueries(environment);
    long blockNumber = header.getNumber();
    final Long bn = environment.getArgument("block");
    if (bn != null) {
      blockNumber = bn;
    }

    return query
        .getAndMapWorldState(blockNumber, ws -> Optional.ofNullable(ws.get(header.getCoinbase())))
        .map(AccountAdapter::new)
        .orElseGet(() -> new EmptyAccountAdapter(header.getCoinbase()));
  }

  /**
   * Returns the extra data of the current block.
   *
   * @return the extra data of the block
   */
  public Bytes getExtraData() {
    return header.getExtraData();
  }

  /**
   * Returns the base fee per gas of the current block.
   *
   * @return the base fee per gas of the block
   */
  public Optional<Wei> getBaseFeePerGas() {
    return header.getBaseFee();
  }

  /**
   * Returns the gas limit of the current block.
   *
   * @return the gas limit of the block
   */
  public Long getGasLimit() {
    return header.getGasLimit();
  }

  /**
   * Returns the gas used by the current block.
   *
   * @return the gas used by the block
   */
  public Long getGasUsed() {
    return header.getGasUsed();
  }

  /**
   * Returns the timestamp of the current block.
   *
   * @return the timestamp of the block
   */
  public Long getTimestamp() {
    return header.getTimestamp();
  }

  /**
   * Returns the logs bloom of the current block.
   *
   * @return the logs bloom of the block
   */
  public Bytes getLogsBloom() {
    return header.getLogsBloom().getBytes();
  }

  /**
   * Returns the mix hash of the current block.
   *
   * @return the mix hash of the block
   */
  public Hash getMixHash() {
    return header.getMixHash();
  }

  /**
   * Returns the difficulty of the current block.
   *
   * @return the difficulty of the block
   */
  public Difficulty getDifficulty() {
    return header.getDifficulty();
  }

  /**
   * Returns the ommer hash of the current block.
   *
   * @return the ommer hash of the block
   */
  public Hash getOmmerHash() {
    return header.getOmmersHash();
  }

  /**
   * Returns the number of the current block.
   *
   * @return the number of the block
   */
  public Long getNumber() {
    return header.getNumber();
  }

  /**
   * Returns an AccountAdapter instance for a given address at the current block.
   *
   * @param environment the DataFetchingEnvironment
   * @return an AccountAdapter instance representing the account of the given address at the current
   *     block
   */
  public AccountAdapter getAccount(final DataFetchingEnvironment environment) {
    final BlockchainQueries query = getBlockchainQueries(environment);
    final long bn = header.getNumber();
    final Address address = environment.getArgument("address");
    return query
        .getAndMapWorldState(
            bn, ws -> Optional.of(new AccountAdapter(ws.get(address), Optional.of(bn))))
        .get();
  }

  /**
   * Returns a list of logs for the current block that match a given filter.
   *
   * @param environment the DataFetchingEnvironment
   * @return a list of LogAdapter instances representing the logs of the current block that match
   *     the filter
   */
  public List<LogAdapter> getLogs(final DataFetchingEnvironment environment) {

    final Map<String, Object> filter = environment.getArgument("filter");

    @SuppressWarnings("unchecked")
    final List<Address> addresses = (List<Address>) filter.get("addresses");
    // `topics` is nullable in the schema, and the schema's own documentation says "[] or nil
    // matches any topic list", so an omitted value has to behave like an empty one rather than
    // being dereferenced. graphql-java puts the key in the map with a null value when a client
    // writes `topics: null` explicitly, so getOrDefault is not enough on its own.
    // (`addresses` may stay null: LogsQuery.Builder.addresses tolerates it.)
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
        new LogsQuery.Builder().addresses(addresses).topics(transformedTopics).build();

    final BlockchainQueries blockchain = getBlockchainQueries(environment);

    final Hash hash = header.getHash();
    final List<LogWithMetadata> logs = blockchain.matchingLogs(hash, query, () -> true);
    final List<LogAdapter> results = new ArrayList<>();
    for (final LogWithMetadata log : logs) {
      results.add(new LogAdapter(log));
    }
    return results;
  }

  /**
   * Estimates the gas used for a call execution.
   *
   * @param environment the DataFetchingEnvironment
   * @return the estimated gas used for the call execution
   */
  public Long getEstimateGas(final DataFetchingEnvironment environment) {
    final Optional<CallResult> result = executeCall(environment);
    return result.map(CallResult::getGasUsed).orElse(0L);
  }

  /**
   * Executes a call and returns the result.
   *
   * @param environment the DataFetchingEnvironment
   * @return an Optional containing the result of the call execution if it exists, otherwise an
   *     empty Optional
   */
  public Optional<CallResult> getCall(final DataFetchingEnvironment environment) {
    return executeCall(environment);
  }

  private Optional<CallResult> executeCall(final DataFetchingEnvironment environment) {
    final Map<String, Object> callData = environment.getArgument("data");
    final Optional<Address> from = Optional.ofNullable((Address) callData.get("from"));
    final Optional<Address> to = Optional.ofNullable((Address) callData.get("to"));
    final var gasParam = callData.get("gas");
    final OptionalLong gas =
        gasParam != null ? OptionalLong.of((Long) gasParam) : OptionalLong.empty();
    final Optional<Wei> gasPrice =
        Optional.ofNullable((UInt256) callData.get("gasPrice")).map(Wei::of);
    final Optional<Wei> value = Optional.ofNullable((UInt256) callData.get("value")).map(Wei::of);
    final Optional<Bytes> data = Optional.ofNullable((Bytes) callData.get("data"));
    final Optional<Wei> maxFeePerGas =
        Optional.ofNullable((UInt256) callData.get("maxFeePerGas")).map(Wei::of);
    final Optional<Wei> maxPriorityFeePerGas =
        Optional.ofNullable((UInt256) callData.get("maxPriorityFeePerGas")).map(Wei::of);

    final BlockchainQueries query = getBlockchainQueries(environment);
    final ProtocolSchedule protocolSchedule =
        environment.getGraphQlContext().get(GraphQLContextType.PROTOCOL_SCHEDULE);
    final long bn = header.getNumber();
    final TransactionSimulator transactionSimulator =
        environment.getGraphQlContext().get(GraphQLContextType.TRANSACTION_SIMULATOR);

    final CallParameter callParameters =
        ImmutableCallParameter.builder()
            .sender(from)
            .to(to)
            .gas(gas)
            .gasPrice(gasPrice)
            .value(value)
            .maxPriorityFeePerGas(maxPriorityFeePerGas)
            .maxFeePerGas(maxFeePerGas)
            .input(data)
            .build();

    return transactionSimulator.process(
        callParameters,
        TransactionValidationParams.transactionSimulatorAllowExceedingBalance(),
        OperationTracer.NO_TRACING,
        (mutableWorldState, transactionSimulatorResult) ->
            transactionSimulatorResult.map(
                result -> {
                  long status = 0;
                  if (result.isSuccessful()) {
                    status = 1;
                  }
                  return new CallResult(status, result.getGasEstimate(), result.getOutput());
                }),
        header);
  }

  /**
   * Returns the raw header of the current block.
   *
   * @return the raw header of the block
   */
  Bytes getRawHeader() {
    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    header.writeTo(rlpOutput);
    return rlpOutput.encoded();
  }

  /**
   * Returns the raw data of the current block.
   *
   * @param environment the DataFetchingEnvironment
   * @return the raw data of the block
   */
  Bytes getRaw(final DataFetchingEnvironment environment) {
    final BlockchainQueries query = getBlockchainQueries(environment);
    return query
        .getBlockchain()
        .getBlockBody(header.getBlockHash())
        .map(
            blockBody -> {
              final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
              blockBody.writeWrappedBodyTo(rlpOutput);
              return rlpOutput.encoded();
            })
        .orElse(Bytes.EMPTY);
  }

  /**
   * Returns the withdrawals root of the current block.
   *
   * @return an Optional containing the withdrawals root if it exists, otherwise an empty Optional
   */
  Optional<Hash> getWithdrawalsRoot() {
    return header.getWithdrawalsRoot();
  }

  /**
   * Returns a list of withdrawals for the current block.
   *
   * @param environment the DataFetchingEnvironment
   * @return an Optional containing a list of WithdrawalAdapter instances representing the
   *     withdrawals of the current block if they exist, otherwise an empty Optional
   */
  Optional<List<WithdrawalAdapter>> getWithdrawals(final DataFetchingEnvironment environment) {
    final BlockchainQueries query = getBlockchainQueries(environment);
    return query
        .getBlockchain()
        .getBlockBody(header.getBlockHash())
        .flatMap(
            blockBody ->
                blockBody
                    .getWithdrawals()
                    .map(wl -> wl.stream().map(WithdrawalAdapter::new).toList()));
  }

  /**
   * Returns the blob gas used by the current block.
   *
   * @return an Optional containing the blob gas used by the current block if it exists, otherwise
   *     an empty Optional
   */
  public Optional<Long> getBlobGasUsed() {
    return header.getBlobGasUsed();
  }

  /**
   * Returns the excess blob gas of the current block.
   *
   * @return an Optional containing the excess blob gas of the current block if it exists, otherwise
   *     an empty Optional
   */
  public Optional<Long> getExcessBlobGas() {
    return header.getExcessBlobGas().map(BlobGas::toLong);
  }
}
