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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.BlockSimulationResult;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;

@JsonPropertyOrder(alphabetic = true)
public class BlockStateCallResult extends BlockResult {
  private final List<CallProcessingResult> callProcessingResults;
  private final Optional<String> trieLog;

  private BlockStateCallResult(
      final Block block,
      final List<TransactionResult> transactions,
      final List<CallProcessingResult> callProcessingResults,
      final Optional<Bytes> trieLog) {
    super(
        block.getHeader(),
        transactions,
        List.of(),
        null,
        block.getSize(),
        false,
        block.getBody().getWithdrawals());
    this.callProcessingResults = callProcessingResults;
    this.trieLog = trieLog.map(Bytes::toHexString);
  }

  @JsonGetter(value = "calls")
  public List<CallProcessingResult> getTransactionProcessingResults() {
    return callProcessingResults;
  }

  @JsonGetter(value = "totalDifficulty")
  @Override
  public String getTotalDifficulty() {
    return null; // Not applicable for this result type.
  }

  @JsonGetter(value = "trieLog")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Optional<String> getTrieLog() {
    return trieLog;
  }

  public static BlockStateCallResult create(
      final BlockSimulationResult simulationResult, final boolean isFullTransactionReturn) {
    Block block = simulationResult.getBlock();
    List<TransactionResult> transactionResults =
        isFullTransactionReturn
            ? createFullTransactionResults(block)
            : createHashTransactionResults(block);

    List<LogWithMetadata> logs = simulationResult.getLogsWithMetadata();

    var callProcessingResults =
        simulationResult.getTransactionSimulations().stream()
            .map(simulatorResult -> createTransactionProcessingResult(simulatorResult, logs))
            .collect(Collectors.toList());

    return new BlockStateCallResult(
        block, transactionResults, callProcessingResults, simulationResult.getSerializedTrieLog());
  }

  /**
   * Generates a list of {@link TransactionResult} objects with full transaction details for each
   * transaction in the block.
   *
   * @param block the block containing the transactions
   * @return a list of {@link TransactionResult} objects with full transaction details
   */
  private static List<TransactionResult> createFullTransactionResults(final Block block) {
    final var txs = block.getBody().getTransactions();
    return IntStream.range(0, txs.size())
        .mapToObj(
            i ->
                new TransactionWithMetadata(
                    txs.get(i),
                    block.getHeader().getNumber(),
                    block.getHeader().getBaseFee(),
                    block.getHash(),
                    i,
                    block.getHeader().getTimestamp()))
        .map(TransactionWithMetadataResult::new)
        .collect(Collectors.toList());
  }

  /**
   * Generates a list of {@link TransactionResult} objects containing only the transaction hashes
   * for each transaction in the block.
   *
   * @param block the block containing the transactions
   * @return a list of {@link TransactionResult} objects with only transaction hashes
   */
  private static List<TransactionResult> createHashTransactionResults(final Block block) {
    return block.getBody().getTransactions().stream()
        .map(transaction -> new TransactionHashResult(transaction.getHash().toString()))
        .collect(Collectors.toList());
  }

  /**
   * Constructs a {@link CallProcessingResult} from a transaction simulation result and associated
   * logs.
   *
   * @param simulatorResult the result of simulating a single transaction
   * @param logs a list of logs associated with the transaction
   * @return a {@link CallProcessingResult} representing the outcome of the transaction simulation
   */
  private static CallProcessingResult createTransactionProcessingResult(
      final TransactionSimulatorResult simulatorResult, final List<LogWithMetadata> logs) {
    Hash transactionHash = simulatorResult.transaction().getHash();
    List<LogWithMetadata> transactionLogs =
        logs.stream()
            .filter(log -> log.getTransactionHash().equals(transactionHash))
            .collect(Collectors.toList());
    TransactionProcessingResult result = simulatorResult.result();
    return new CallProcessingResult(
        result.isSuccessful() ? 1 : 0,
        result.getOutput(),
        result.getGasSpent(),
        result.getEstimateGasUsedByTransaction(),
        getError(result),
        new LogsResult(transactionLogs));
  }

  /**
   * Determines the appropriate {@link JsonRpcError} based on the transaction processing result.
   *
   * <p>This method checks for revert reasons and exceptional halt reasons to construct an
   * appropriate error message.
   *
   * @param result the result of processing a transaction
   * @return a {@link JsonRpcError} representing the error encountered during transaction
   *     processing, or {@code null} if no error occurred
   */
  private static JsonRpcError getError(final TransactionProcessingResult result) {
    if (result.getRevertReason().isPresent()) {
      return new JsonRpcError(
          RpcErrorType.REVERT_ERROR, result.getRevertReason().get().toHexString());
    } else if (result.getExceptionalHaltReason().isPresent()) {
      return new JsonRpcError(
          -32015, result.getExceptionalHaltReason().get().getDescription(), null);
    }
    return null;
  }
}
