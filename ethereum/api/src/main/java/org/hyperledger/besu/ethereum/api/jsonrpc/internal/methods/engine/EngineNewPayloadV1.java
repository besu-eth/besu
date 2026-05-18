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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.ACCEPTED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCK_PROCESSING;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class EngineNewPayloadV1 extends ExecutionEngineJsonRpcMethod
    permits EngineNewPayloadV2 {

  private static final Hash OMMERS_HASH_CONSTANT = Hash.EMPTY_LIST_HASH;
  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadV1.class);
  private static final BlockHeaderFunctions headerFunctions = new MainnetBlockHeaderFunctions();
  private final MergeMiningCoordinator mergeCoordinator;
  private final EthPeers ethPeers;
  private long lastExecutionTimeInNs = 0L;

  private final Optional<Long> minForkTimestamp;
  private final Optional<Long> maxForkTimestamp;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  public EngineNewPayloadV1(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final EngineCallListener engineCallListener,
      final MetricsSystem metricsSystem,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(vertx, protocolSchedule, protocolContext, engineCallListener);
    this.mergeCoordinator = mergeCoordinator;
    this.ethPeers = ethPeers;
    this.minSupportedFork = minSupportedFork;
    this.firstUnsupportedFork = firstUnsupportedFork;
    this.minForkTimestamp =
        minSupportedFork != null
            ? protocolSchedule.milestoneFor(minSupportedFork)
            : Optional.empty();
    this.maxForkTimestamp =
        firstUnsupportedFork != null
            ? protocolSchedule.milestoneFor(firstUnsupportedFork)
            : Optional.empty();
    metricsSystem.createLongGauge(
        BLOCK_PROCESSING,
        "execution_time_head",
        "The execution time of the last block (head)",
        this::getLastExecutionTime);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final Object reqId = requestContext.getRequest().getId();

    final NewPayloadRequestParametersV1 requestParameters = readRequestParameters(requestContext);

    final ExecutionPayloadV1 blockParam = requestParameters.payloadParameter();
    final ValidationResult<RpcErrorType> parameterValidationResult =
        validateParameters(requestParameters);
    if (!parameterValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, parameterValidationResult);
    }

    final ValidationResult<RpcErrorType> forkValidationResult =
        validateForkSupported(blockParam.getTimestamp());
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, forkValidationResult);
    }

    final VersionSpecificPayloadData versionSpecificPayloadData;
    try {
      versionSpecificPayloadData = createVersionSpecificPayloadData(requestParameters);
    } catch (final InvalidVersionSpecificPayloadException e) {
      return respondToInvalidVersionSpecificPayload(reqId, blockParam, e);
    }

    final ValidationResult<RpcErrorType> versionSpecificPayloadValidationResult =
        validateVersionSpecificPayloadData(requestParameters, versionSpecificPayloadData);
    if (!versionSpecificPayloadValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, versionSpecificPayloadValidationResult);
    }

    final Optional<BlockHeader> maybeParentHeader =
        protocolContext.getBlockchain().getBlockHeader(blockParam.getParentHash());

    LOG.atTrace()
        .setMessage("blockparam: {}")
        .addArgument(() -> Json.encodePrettily(blockParam))
        .log();

    if (mergeContext.get().isSyncing()) {
      LOG.debug("We are syncing");
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    final List<Transaction> transactions;
    try {
      transactions =
          blockParam.getTransactions().stream()
              .map(Bytes::fromHexString)
              .map(in -> TransactionDecoder.decodeOpaqueBytes(in, EncodingContext.BLOCK_BODY))
              .toList();
      precomputeSenders(transactions);
    } catch (final RLPException | IllegalArgumentException e) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          INVALID,
          "Failed to decode transactions from block parameter");
    }

    if (blockParam.getExtraData() == null) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          INVALID,
          "Field extraData must not be null");
    }

    final BlockHeaderBuilder blockHeaderBuilder =
        createBlockHeaderBuilder(blockParam, transactions);
    setVersionSpecificBlockHeaderFields(
        blockHeaderBuilder, requestParameters, versionSpecificPayloadData);
    final BlockHeader newBlockHeader = blockHeaderBuilder.buildBlockHeader();

    // ensure the block hash matches the blockParam hash
    // this must be done before any other check
    if (!newBlockHeader.getHash().equals(blockParam.getBlockHash())) {
      String errorMessage =
          String.format(
              "Computed block hash %s does not match block hash parameter %s",
              newBlockHeader.getBlockHash(), blockParam.getBlockHash());
      LOG.debug(errorMessage);
      return respondWithInvalid(reqId, blockParam, null, getInvalidBlockHashStatus(), errorMessage);
    }

    final ValidationResult<RpcErrorType> versionSpecificBlockValidationResult =
        validateVersionSpecificBlockData(
            transactions,
            newBlockHeader,
            maybeParentHeader,
            protocolSchedule.get().getByBlockHeader(newBlockHeader),
            versionSpecificPayloadData);
    if (!versionSpecificBlockValidationResult.isValid()) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          getInvalidBlockHashStatus(),
          versionSpecificBlockValidationResult.getErrorMessage());
    }

    // do we already have this payload
    if (protocolContext.getBlockchain().getBlockByHash(newBlockHeader.getBlockHash()).isPresent()) {
      LOG.debug("block already present");
      return respondWith(reqId, blockParam, blockParam.getBlockHash(), VALID);
    }
    if (mergeCoordinator.isBadBlock(blockParam.getBlockHash())) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator
              .getLatestValidHashOfBadBlock(blockParam.getBlockHash())
              .orElse(Hash.ZERO),
          INVALID,
          "Block already present in bad block manager.");
    }

    if (maybeParentHeader.isPresent()
        && (Long.compareUnsigned(maybeParentHeader.get().getTimestamp(), blockParam.getTimestamp())
            >= 0)) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          INVALID,
          "block timestamp not greater than parent");
    }

    final var block =
        new Block(newBlockHeader, createBlockBody(transactions, versionSpecificPayloadData));

    if (maybeParentHeader.isEmpty()) {
      LOG.atDebug()
          .setMessage("Parent of block {} is not present, append it to backward sync")
          .addArgument(block::toLogString)
          .log();
      mergeCoordinator.appendNewPayloadToSync(block);
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    final var latestValidAncestor = mergeCoordinator.getLatestValidAncestor(newBlockHeader);

    if (latestValidAncestor.isEmpty()) {
      return respondWith(reqId, blockParam, null, ACCEPTED);
    }

    // execute block and return result response
    final long startTimeNs = System.nanoTime();
    final BlockProcessingResult executionResult = rememberBlock(block, versionSpecificPayloadData);
    if (executionResult.isSuccessful()) {
      lastExecutionTimeInNs = System.nanoTime() - startTimeNs;
      logImportedBlockInfo(
          block,
          transactions,
          versionSpecificPayloadData,
          lastExecutionTimeInNs,
          executionResult.getNbParallelizedTransactions());
      return respondWith(reqId, blockParam, newBlockHeader.getHash(), VALID);
    } else {
      if (executionResult.causedBy().isPresent()) {
        Throwable causedBy = executionResult.causedBy().get();
        if (causedBy instanceof StorageException || causedBy instanceof MerkleTrieException) {
          RpcErrorType error = RpcErrorType.INTERNAL_ERROR;
          JsonRpcErrorResponse response = new JsonRpcErrorResponse(reqId, error);
          return response;
        }
      }
      LOG.debug("New payload is invalid: {}", executionResult.errorMessage.get());
      return respondWithInvalid(
          reqId,
          blockParam,
          latestValidAncestor.get(),
          INVALID,
          executionResult.errorMessage.get());
    }
  }

  protected NewPayloadRequestParametersV1 readRequestParameters(
      final JsonRpcRequestContext requestContext) {
    final ExecutionPayloadV1 blockParam;
    try {
      blockParam = requestContext.getRequiredParameter(0, getPayloadParameterClass());
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid engine payload parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
          e);
    }
    return new NewPayloadRequestParametersV1(blockParam);
  }

  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV1.class;
  }

  private BlockHeaderBuilder createBlockHeaderBuilder(
      final ExecutionPayloadV1 blockParam, final List<Transaction> transactions) {
    return BlockHeaderBuilder.create()
        .parentHash(blockParam.getParentHash())
        .ommersHash(OMMERS_HASH_CONSTANT)
        .coinbase(blockParam.getFeeRecipient())
        .stateRoot(blockParam.getStateRoot())
        .transactionsRoot(BodyValidation.transactionsRoot(transactions))
        .receiptsRoot(blockParam.getReceiptsRoot())
        .logsBloom(blockParam.getLogsBloom())
        .difficulty(Difficulty.ZERO)
        .number(blockParam.getBlockNumber())
        .gasLimit(blockParam.getGasLimit())
        .gasUsed(blockParam.getGasUsed())
        .timestamp(blockParam.getTimestamp())
        .extraData(Bytes.fromHexString(blockParam.getExtraData()))
        .baseFee(blockParam.getBaseFeePerGas())
        .prevRandao(blockParam.getPrevRandao())
        .nonce(0)
        .blockHeaderFunctions(headerFunctions);
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

  JsonRpcResponse respondWith(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final Hash latestValidHash,
      final EngineStatus status) {
    if (INVALID.equals(status) || INVALID_BLOCK_HASH.equals(status)) {
      throw new IllegalArgumentException(
          "Don't call respondWith() with invalid status of " + status.toString());
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
        requestId, new PayloadStatusV1(status, latestValidHash, Optional.empty()));
  }

  // engine api calls are synchronous, no need for volatile
  private long lastInvalidWarn = 0;

  JsonRpcResponse respondWithInvalid(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final Hash latestValidHash,
      final EngineStatus invalidStatus,
      final String validationError) {
    if (!INVALID.equals(invalidStatus) && !INVALID_BLOCK_HASH.equals(invalidStatus)) {
      throw new IllegalArgumentException(
          "Don't call respondWithInvalid() with non-invalid status of " + invalidStatus.toString());
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
    // always log invalid at DEBUG
    LOG.debug(invalidBlockLogMessage);
    // periodically log at WARN
    if (lastInvalidWarn + ENGINE_API_LOGGING_THRESHOLD < System.currentTimeMillis()) {
      lastInvalidWarn = System.currentTimeMillis();
      LOG.warn(invalidBlockLogMessage);
    }
    return new JsonRpcSuccessResponse(
        requestId,
        new PayloadStatusV1(invalidStatus, latestValidHash, Optional.of(validationError)));
  }

  protected EngineStatus getInvalidBlockHashStatus() {
    return INVALID_BLOCK_HASH;
  }

  protected ValidationResult<RpcErrorType> validateParameters(
      final NewPayloadRequestParametersV1 requestParameters) {
    return ValidationResult.valid();
  }

  protected VersionSpecificPayloadData createVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    return new VersionSpecificPayloadData();
  }

  protected ValidationResult<RpcErrorType> validateVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    return ValidationResult.valid();
  }

  protected void setVersionSpecificBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder,
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {}

  protected BlockBody createBlockBody(
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    return new BlockBody(transactions, Collections.emptyList());
  }

  protected ValidationResult<RpcErrorType> validateVersionSpecificBlockData(
      final List<Transaction> transactions,
      final BlockHeader header,
      final Optional<BlockHeader> maybeParentHeader,
      final ProtocolSpec protocolSpec,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    return ValidationResult.valid();
  }

  protected BlockProcessingResult rememberBlock(
      final Block block, final VersionSpecificPayloadData versionSpecificPayloadData) {
    return mergeCoordinator.rememberBlock(block, Optional.empty());
  }

  protected MergeMiningCoordinator getMergeCoordinator() {
    return mergeCoordinator;
  }

  private void logImportedBlockInfo(
      final Block block,
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData,
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
      double parallelizedTxPercentage =
          (double) (nbParallelizedTransactions.get() * 100) / nbTransactions;
      message.append(" (%5.1f%% parallel)");
      messageArgs.add(parallelizedTxPercentage);
    }
    appendVersionSpecificLogInfo(
        message, messageArgs, block, transactions, versionSpecificPayloadData);
    double mgasPerSec =
        (timeInNs != 0) ? (double) (block.getHeader().getGasUsed() * 1_000) / timeInNs : 0;
    double timeInMs = (double) timeInNs / 1_000_000;
    boolean timeOverOrEq1second = timeInMs >= 1_000;
    if (timeOverOrEq1second) {
      message.append("| %s bfee| %,11d (%5.1f%%) gas used| %01.3fs exec| %6.2f Mgas/s| %2d peers");
    } else {
      message.append("| %s bfee| %,11d (%5.1f%%) gas used| %03.1fms exec| %6.2f Mgas/s| %2d peers");
    }
    messageArgs.addAll(
        List.of(
            block.getHeader().getBaseFee().map(Wei::toHumanReadablePaddedString).orElse("N/A"),
            block.getHeader().getGasUsed(),
            (block.getHeader().getGasUsed() * 100.0) / block.getHeader().getGasLimit(),
            timeOverOrEq1second ? timeInMs / 1_000 : timeInMs,
            mgasPerSec,
            ethPeers.peerCount()));
    LOG.info(String.format(message.toString(), messageArgs.toArray()));
  }

  protected void appendVersionSpecificLogInfo(
      final StringBuilder message,
      final List<Object> messageArgs,
      final Block block,
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData) {}

  protected static class VersionSpecificPayloadData {}

  protected static class InvalidVersionSpecificPayloadException extends Exception {
    private final Optional<RpcErrorType> rpcErrorType;

    private InvalidVersionSpecificPayloadException(
        final String message, final Optional<RpcErrorType> rpcErrorType) {
      super(message);
      this.rpcErrorType = rpcErrorType;
    }

    protected static InvalidVersionSpecificPayloadException invalidPayload(final String message) {
      return new InvalidVersionSpecificPayloadException(message, Optional.empty());
    }

    protected static InvalidVersionSpecificPayloadException jsonRpcError(
        final RpcErrorType rpcErrorType) {
      return new InvalidVersionSpecificPayloadException(
          rpcErrorType.name(), Optional.of(rpcErrorType));
    }

    Optional<RpcErrorType> getRpcErrorType() {
      return rpcErrorType;
    }
  }

  private JsonRpcResponse respondToInvalidVersionSpecificPayload(
      final Object reqId,
      final ExecutionPayloadV1 blockParam,
      final InvalidVersionSpecificPayloadException exception) {
    if (exception.getRpcErrorType().isPresent()) {
      return new JsonRpcErrorResponse(reqId, exception.getRpcErrorType().get());
    }
    return respondWithInvalid(
        reqId,
        blockParam,
        mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
        INVALID,
        exception.getMessage());
  }

  private long getLastExecutionTime() {
    return this.lastExecutionTimeInNs;
  }

  @Override
  protected final ValidationResult<RpcErrorType> validateForkSupported(final long blockTimestamp) {
    return ForkSupportHelper.validateForkSupported(
        minSupportedFork, minForkTimestamp, firstUnsupportedFork, maxForkTimestamp, blockTimestamp);
  }
}
