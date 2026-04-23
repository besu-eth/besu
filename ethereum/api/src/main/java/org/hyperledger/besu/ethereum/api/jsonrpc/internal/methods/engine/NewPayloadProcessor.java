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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.ENGINE_API_LOGGING_THRESHOLD;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.ACCEPTED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.RequestValidatorProvider.getRequestsValidator;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.WithdrawalsValidatorProvider.getWithdrawalsValidator;
import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCK_PROCESSING;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.vertx.core.json.Json;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the {@code engine_newPayload} request/response pipeline for any version, driven by a
 * {@link NewPayloadRules} spec.
 *
 * <p>This service orchestrates the pipeline and owns the stateful and dependency-bound steps
 * (provider-based validation, block execution, metrics). Stateless helpers live in {@link
 * NewPayloadParamsParser} and {@link NewPayloadValidators}.
 *
 * <p>Intended to be instantiated once and shared across every {@code EngineNewPayloadVN} method.
 * Each version class supplies its own {@code NewPayloadRules} constant when delegating to {@link
 * #process(JsonRpcRequestContext, NewPayloadRules)}.
 */
public class NewPayloadProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(NewPayloadProcessor.class);

  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final MergeMiningCoordinator mergeCoordinator;
  private final EthPeers ethPeers;
  private final Supplier<MergeContext> mergeContext;

  private long lastExecutionTimeInNs = 0L;
  // engine api calls are synchronous, no need for volatile
  private long lastInvalidWarn = 0;

  public NewPayloadProcessor(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final MetricsSystem metricsSystem) {
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.mergeCoordinator = mergeCoordinator;
    this.ethPeers = ethPeers;
    this.mergeContext = protocolContext.safeConsensusContext(MergeContext.class)::orElseThrow;
    metricsSystem.createLongGauge(
        BLOCK_PROCESSING,
        "execution_time_head",
        "The execution time of the last block (head)",
        () -> lastExecutionTimeInNs);
  }

  public JsonRpcResponse process(
      final JsonRpcRequestContext requestContext, final NewPayloadRules rules) {
    final EnginePayloadParameter blockParam =
        NewPayloadParamsParser.parseBlockParam(requestContext);
    final Optional<List<String>> versionedHashParam =
        NewPayloadParamsParser.parseVersionedHashParam(requestContext);
    final Optional<String> parentBeaconRootParam =
        NewPayloadParamsParser.parseParentBeaconRootParam(requestContext);
    final Optional<List<String>> requestsParam =
        NewPayloadParamsParser.parseRequestsParam(requestContext);
    final Object reqId = requestContext.getRequest().getId();

    final ValidationResult<RpcErrorType> forkCheck =
        rules.forkWindow().validate(blockParam.getTimestamp(), protocolSchedule);
    if (!forkCheck.isValid()) {
      return new JsonRpcErrorResponse(reqId, forkCheck);
    }

    final ValidationResult<RpcErrorType> presenceCheck =
        NewPayloadValidators.validateFieldPresence(
            rules, blockParam, versionedHashParam, parentBeaconRootParam, requestsParam);
    if (!presenceCheck.isValid()) {
      return new JsonRpcErrorResponse(reqId, presenceCheck);
    }

    final Optional<List<VersionedHash>> versionedHashes;
    try {
      versionedHashes = NewPayloadParamsParser.decodeVersionedHashes(versionedHashParam);
    } catch (final RuntimeException e) {
      return invalidPayload(reqId, blockParam, "Invalid versionedHash");
    }

    final Optional<BlockHeader> parentHeader =
        protocolContext.getBlockchain().getBlockHeader(blockParam.getParentHash());

    LOG.atTrace()
        .setMessage("blockparam: {}")
        .addArgument(() -> Json.encodePrettily(blockParam))
        .log();

    final Optional<List<Withdrawal>> withdrawals =
        NewPayloadParamsParser.extractWithdrawals(blockParam);
    if (!validateWithdrawals(blockParam, withdrawals)) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INVALID_WITHDRAWALS_PARAMS);
    }

    final Optional<List<Request>> requests;
    try {
      requests = NewPayloadParamsParser.decodeRequests(requestsParam);
    } catch (final RequestType.InvalidRequestTypeException e) {
      return invalidPayload(reqId, blockParam, "Invalid execution requests");
    } catch (final Exception e) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
    }
    if (!validateRequests(blockParam, requests)) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
    }

    final Optional<BlockAccessList> blockAccessList;
    try {
      blockAccessList = NewPayloadParamsParser.extractBlockAccessList(rules, blockParam);
    } catch (final InvalidBlockAccessListException e) {
      return invalidPayload(reqId, blockParam, e.getMessage());
    }

    if (mergeContext.get().isSyncing()) {
      LOG.debug("We are syncing");
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    final List<Transaction> transactions;
    try {
      transactions = NewPayloadParamsParser.decodeTransactions(blockParam);
      precomputeSenders(transactions);
    } catch (final RLPException | IllegalArgumentException e) {
      return invalidPayload(
          reqId, blockParam, "Failed to decode transactions from block parameter");
    }

    if (blockParam.getExtraData() == null) {
      return invalidPayload(reqId, blockParam, "Field extraData must not be null");
    }

    final Optional<Bytes32> parentBeaconRoot = parentBeaconRootParam.map(Bytes32::fromHexString);
    final BlockHeader newHeader =
        NewPayloadParamsParser.buildHeader(
            blockParam, transactions, withdrawals, requests, blockAccessList, parentBeaconRoot);

    if (!newHeader.getHash().equals(blockParam.getBlockHash())) {
      final String errorMessage =
          String.format(
              "Computed block hash %s does not match block hash parameter %s",
              newHeader.getBlockHash(), blockParam.getBlockHash());
      LOG.debug(errorMessage);
      return respondWithInvalid(reqId, blockParam, null, INVALID, errorMessage);
    }

    final List<Transaction> blobTransactions =
        transactions.stream().filter(tx -> tx.getType().supportsBlob()).toList();

    final ValidationResult<RpcErrorType> blobCheck =
        NewPayloadValidators.validateBlobs(
            rules,
            blobTransactions,
            newHeader,
            parentHeader,
            versionedHashes,
            protocolSchedule.getByBlockHeader(newHeader));
    if (!blobCheck.isValid()) {
      return invalidPayload(reqId, blockParam, blobCheck.getErrorMessage());
    }

    final Optional<JsonRpcResponse> earlyReturn =
        checkKnownOrBadBlock(reqId, blockParam, newHeader, parentHeader);
    if (earlyReturn.isPresent()) {
      return earlyReturn.get();
    }

    return assembleAndExecute(
        reqId,
        blockParam,
        newHeader,
        transactions,
        withdrawals,
        blockAccessList,
        parentHeader,
        blobTransactions);
  }

  private boolean validateWithdrawals(
      final EnginePayloadParameter blockParam, final Optional<List<Withdrawal>> withdrawals) {
    return getWithdrawalsValidator(
            protocolSchedule, blockParam.getTimestamp(), blockParam.getBlockNumber())
        .validateWithdrawals(withdrawals);
  }

  private boolean validateRequests(
      final EnginePayloadParameter blockParam, final Optional<List<Request>> requests) {
    return getRequestsValidator(
            protocolSchedule, blockParam.getTimestamp(), blockParam.getBlockNumber())
        .validate(requests);
  }

  private void precomputeSenders(final List<Transaction> transactions) {
    transactions.forEach(
        transaction -> {
          mergeCoordinator
              .getEthScheduler()
              .scheduleComputationTask(
                  () -> {
                    final var sender = transaction.getSender();
                    LOG.atTrace()
                        .setMessage("The sender for transaction {} is calculated : {}")
                        .addArgument(transaction::getHash)
                        .addArgument(sender)
                        .log();
                    return sender;
                  });
          if (transaction.getType().supportsDelegateCode()) {
            precomputeAuthorities(transaction);
          }
        });
  }

  private void precomputeAuthorities(final Transaction transaction) {
    final var codeDelegations = transaction.getCodeDelegationList().get();
    int index = 0;
    for (final var codeDelegation : codeDelegations) {
      final var constIndex = index++;
      mergeCoordinator
          .getEthScheduler()
          .scheduleComputationTask(
              () -> {
                final var authority = codeDelegation.authorizer();
                LOG.atTrace()
                    .setMessage(
                        "The code delegation authority at index {} for transaction {} is calculated : {}")
                    .addArgument(constIndex)
                    .addArgument(transaction::getHash)
                    .addArgument(authority)
                    .log();
                return authority;
              });
    }
  }

  private Optional<JsonRpcResponse> checkKnownOrBadBlock(
      final Object reqId,
      final EnginePayloadParameter blockParam,
      final BlockHeader newHeader,
      final Optional<BlockHeader> parentHeader) {
    if (protocolContext.getBlockchain().getBlockByHash(newHeader.getBlockHash()).isPresent()) {
      LOG.debug("block already present");
      return Optional.of(respondWith(reqId, blockParam, blockParam.getBlockHash(), VALID));
    }
    if (mergeCoordinator.isBadBlock(blockParam.getBlockHash())) {
      return Optional.of(
          respondWithInvalid(
              reqId,
              blockParam,
              mergeCoordinator
                  .getLatestValidHashOfBadBlock(blockParam.getBlockHash())
                  .orElse(Hash.ZERO),
              INVALID,
              "Block already present in bad block manager."));
    }
    if (parentHeader.isPresent()
        && Long.compareUnsigned(parentHeader.get().getTimestamp(), blockParam.getTimestamp())
            >= 0) {
      return Optional.of(
          invalidPayload(reqId, blockParam, "block timestamp not greater than parent"));
    }
    return Optional.empty();
  }

  private JsonRpcResponse assembleAndExecute(
      final Object reqId,
      final EnginePayloadParameter blockParam,
      final BlockHeader newHeader,
      final List<Transaction> transactions,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<BlockHeader> parentHeader,
      final List<Transaction> blobTransactions) {
    final Block block =
        new Block(newHeader, new BlockBody(transactions, Collections.emptyList(), withdrawals));

    if (parentHeader.isEmpty()) {
      LOG.atDebug()
          .setMessage("Parent of block {} is not present, append it to backward sync")
          .addArgument(block::toLogString)
          .log();
      mergeCoordinator.appendNewPayloadToSync(block);
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    final var latestValidAncestor = mergeCoordinator.getLatestValidAncestor(newHeader);
    if (latestValidAncestor.isEmpty()) {
      return respondWith(reqId, blockParam, null, ACCEPTED);
    }

    final long startTimeNs = System.nanoTime();
    final BlockProcessingResult executionResult =
        mergeCoordinator.rememberBlock(block, blockAccessList);
    if (executionResult.isSuccessful()) {
      lastExecutionTimeInNs = System.nanoTime() - startTimeNs;
      logImportedBlockInfo(
          block,
          blobTransactions.stream()
              .map(Transaction::getVersionedHashes)
              .flatMap(Optional::stream)
              .mapToInt(List::size)
              .sum(),
          lastExecutionTimeInNs,
          executionResult.getNbParallelizedTransactions());
      return respondWith(reqId, blockParam, newHeader.getHash(), VALID);
    }

    if (executionResult.causedBy().isPresent()) {
      final Throwable causedBy = executionResult.causedBy().get();
      if (causedBy instanceof StorageException || causedBy instanceof MerkleTrieException) {
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
    }
    LOG.debug("New payload is invalid: {}", executionResult.errorMessage.get());
    return respondWithInvalid(
        reqId, blockParam, latestValidAncestor.get(), INVALID, executionResult.errorMessage.get());
  }

  /** Shortcut for the common "INVALID with latest valid ancestor" response pattern. */
  private JsonRpcResponse invalidPayload(
      final Object reqId, final EnginePayloadParameter blockParam, final String message) {
    return respondWithInvalid(
        reqId,
        blockParam,
        mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
        INVALID,
        message);
  }

  private JsonRpcResponse respondWith(
      final Object requestId,
      final EnginePayloadParameter param,
      final Hash latestValidHash,
      final EngineStatus status) {
    if (INVALID.equals(status) || INVALID_BLOCK_HASH.equals(status)) {
      throw new IllegalArgumentException(
          "Don't call respondWith() with invalid status of " + status);
    }
    LOG.atDebug()
        .setMessage(
            "New payload: number: {}, hash: {}, parentHash: {}, latestValidHash: {}, status: {}")
        .addArgument(param::getBlockNumber)
        .addArgument(param::getBlockHash)
        .addArgument(param::getParentHash)
        .addArgument(
            () -> latestValidHash == null ? null : latestValidHash.getBytes().toHexString())
        .addArgument(status::name)
        .log();
    return new JsonRpcSuccessResponse(
        requestId, new EnginePayloadStatusResult(status, latestValidHash, Optional.empty()));
  }

  private JsonRpcResponse respondWithInvalid(
      final Object requestId,
      final EnginePayloadParameter param,
      final Hash latestValidHash,
      final EngineStatus invalidStatus,
      final String validationError) {
    if (!INVALID.equals(invalidStatus) && !INVALID_BLOCK_HASH.equals(invalidStatus)) {
      throw new IllegalArgumentException(
          "Don't call respondWithInvalid() with non-invalid status of " + invalidStatus);
    }
    final String invalidBlockLogMessage =
        String.format(
            "Invalid new payload: number: %s, hash: %s, parentHash: %s, latestValidHash: %s, status: %s, validationError: %s",
            param.getBlockNumber(),
            param.getBlockHash(),
            param.getParentHash(),
            latestValidHash == null ? null : latestValidHash.getBytes().toHexString(),
            invalidStatus.name(),
            validationError);
    LOG.debug(invalidBlockLogMessage);
    if (lastInvalidWarn + ENGINE_API_LOGGING_THRESHOLD < System.currentTimeMillis()) {
      lastInvalidWarn = System.currentTimeMillis();
      LOG.warn(invalidBlockLogMessage);
    }
    return new JsonRpcSuccessResponse(
        requestId,
        new EnginePayloadStatusResult(
            invalidStatus, latestValidHash, Optional.of(validationError)));
  }

  private void logImportedBlockInfo(
      final Block block,
      final int blobCount,
      final long timeInNs,
      final Optional<Integer> nbParallelizedTransactions) {
    final StringBuilder message = new StringBuilder();
    final int nbTransactions = block.getBody().getTransactions().size();
    message.append("Imported #%,d  (%s)| %4d tx");
    final List<Object> messageArgs =
        new ArrayList<>(
            List.of(
                block.getHeader().getNumber(), block.getHash().toShortLogString(), nbTransactions));
    if (nbParallelizedTransactions.isPresent()) {
      final double parallelizedTxPercentage =
          (double) (nbParallelizedTransactions.get() * 100) / nbTransactions;
      message.append(" (%5.1f%% parallel)");
      messageArgs.add(parallelizedTxPercentage);
    }
    if (block.getBody().getWithdrawals().isPresent()) {
      message.append("| %2d ws");
      messageArgs.add(block.getBody().getWithdrawals().get().size());
    }
    final double mgasPerSec =
        (timeInNs != 0) ? (double) (block.getHeader().getGasUsed() * 1_000) / timeInNs : 0;
    final double timeInMs = (double) timeInNs / 1_000_000;
    final boolean timeOverOrEq1second = timeInMs >= 1_000;
    if (timeOverOrEq1second) {
      message.append(
          "| %2d blobs| %s bfee| %,11d (%5.1f%%) gas used| %01.3fs exec| %6.2f Mgas/s| %2d peers");
    } else {
      message.append(
          "| %2d blobs| %s bfee| %,11d (%5.1f%%) gas used| %03.1fms exec| %6.2f Mgas/s| %2d peers");
    }
    messageArgs.addAll(
        List.of(
            blobCount,
            block.getHeader().getBaseFee().map(Wei::toHumanReadablePaddedString).orElse("N/A"),
            block.getHeader().getGasUsed(),
            (block.getHeader().getGasUsed() * 100.0) / block.getHeader().getGasLimit(),
            timeOverOrEq1second ? timeInMs / 1_000 : timeInMs,
            mgasPerSec,
            ethPeers.peerCount()));
    LOG.info(String.format(message.toString(), messageArgs.toArray()));
  }
}
