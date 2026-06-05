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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.FAIL_ON_UNKNOWN_BUT_NULL;
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
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;
import io.vertx.core.Vertx;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class EngineNewPayloadV1<
        EP extends ExecutionPayloadV1, NPRP extends NewPayloadRequestParametersV1<? extends EP>>
    extends ExecutionEngineJsonRpcMethod permits EngineNewPayloadV2 {

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadV1.class);
  private static final Hash OMMERS_HASH_CONSTANT = Hash.EMPTY_LIST_HASH;
  private static final BlockHeaderFunctions HEADER_FUNCTIONS = new MainnetBlockHeaderFunctions();
  private final EthPeers ethPeers;
  private long lastExecutionTimeInNs = 0L;
  private long lastInvalidWarn = 0L;
  protected final MergeMiningCoordinator mergeCoordinator;

  private final Optional<Long> minForkTimestamp;
  private final Optional<Long> maxForkTimestamp;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  public EngineNewPayloadV1(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final MetricsSystem metricsSystem,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(protocolSchedule, protocolContext, vertx, engineCallListener);
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

    final NPRP requestParameters;
    try {
      // 1. Client software MUST validate that all transactions have non-zero length (at least 1
      // byte). Client software MUST run this validation in all cases even if this branch or any
      // other branches of the block tree are in an active sync process.
      // the validation is done during the deserialization process
      requestParameters = readRequestParameters(requestContext);
    } catch (final InvalidRequestParametersException e) {
      // 6. Client software MUST respond to this method call in the following way:
      // {status: INVALID, latestValidHash: null, validationError: errorMessage | null} if
      // transactions contain zero length or invalid entries
      return processParametersParsingException(reqId, e);
    }

    final EP blockParam = requestParameters.payloadParameter();

    final ValidationResult<RpcErrorType> forkValidationResult =
        validateForkSupported(blockParam.getTimestamp());
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, forkValidationResult);
    }

    final ValidationResult<RpcErrorType> parameterValidationResult =
        validateParameters(requestParameters);

    if (!parameterValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, parameterValidationResult.getInvalidReason());
    }

    if (mergeContext.get().isSyncing()) {
      LOG.debug("We are syncing");
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    // 2. Client software MUST validate blockHash value as being equivalent to
    // Keccak256(RLP(ExecutionBlockHeader)), where ExecutionBlockHeader is the execution layer block
    // header (the former PoW block header structure). Fields of this object are set to the
    // corresponding payload values and constant values according to the Block structure section of
    // EIP-3675, extended with the corresponding section of EIP-4399. Client software MUST run this
    // validation in all cases even if this branch or any other branches of the block tree are in an
    // active sync process.
    final BlockHeaderBuilder blockHeaderBuilder = BlockHeaderBuilder.create();
    setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    final BlockHeader newBlockHeader = blockHeaderBuilder.buildBlockHeader();

    // ensure the block hash matches the blockParam hash
    // this must be done before any other check
    if (!newBlockHeader.getHash().equals(blockParam.getBlockHash())) {
      String errorMessage =
          String.format(
              "Computed block hash %s does not match block hash parameter %s",
              newBlockHeader.getBlockHash(), blockParam.getBlockHash());
      LOG.debug(errorMessage);
      // 6. Client software MUST respond to this method call in the following way:
      // {status: INVALID_BLOCK_HASH, latestValidHash: null, validationError: errorMessage | null}
      // if the blockHash validation has failed
      return respondWithInvalid(reqId, blockParam, null, getInvalidBlockHashStatus(), errorMessage);
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

    final Optional<BlockHeader> maybeParentHeader =
        protocolContext.getBlockchain().getBlockHeader(blockParam.getParentHash());

    final var unvalidatedBlock = new Block(newBlockHeader, createBlockBody(blockParam));

    // 3. Client software MAY initiate a sync process if requisite data for payload validation is
    // missing. Sync process is specified in the Sync section.
    final boolean needsSync = maybeParentHeader.isEmpty();
    if (needsSync) {
      LOG.atDebug()
          .setMessage("Parent of block {} is not present, append it to backward sync")
          .addArgument(unvalidatedBlock::toLogString)
          .log();
      mergeCoordinator.appendNewPayloadToSync(unvalidatedBlock);
    }

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(newBlockHeader);
    final var maybeLatestValidAncestor = mergeCoordinator.getLatestValidAncestor(newBlockHeader);

    // 4. Client software MUST validate the payload if it extends the canonical chain, and requisite
    // data for the validation is locally available. The validation process is specified in the
    // Payload validation section.
    // 5. Client software MAY NOT validate the payload if the payload doesn't belong to the
    // canonical chain.
    final ValidationResult<RpcErrorType> versionSpecificBlockValidationResult =
        validateNewBlock(unvalidatedBlock, protocolSpec, maybeParentHeader, requestParameters);
    if (!versionSpecificBlockValidationResult.isValid()) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          INVALID,
          versionSpecificBlockValidationResult.getErrorMessage());
    }

    // block is now valid and can be processed
    final Block block = unvalidatedBlock;

    // do we already have this payload?
    if (protocolContext.getBlockchain().getBlockByHash(block.getHash()).isPresent()) {
      LOG.atDebug()
          .setMessage("block {} already present")
          .addArgument(newBlockHeader::toLogString)
          .log();
      return respondWith(reqId, blockParam, block.getHash(), VALID);
    }

    if (needsSync) {
      // 6. Client software MUST respond to this method call in the following way:
      // {status: SYNCING, latestValidHash: null, validationError: null} if requisite data for the
      // payload's acceptance or validation is missing
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    // 6. Client software MUST respond to this method call in the following way:
    // {status: ACCEPTED, latestValidHash: null, validationError: null} if the following conditions
    // are met:
    //    all transactions have non-zero length
    //    the blockHash of the payload is valid
    //    the payload doesn't extend the canonical chain
    //    the payload hasn't been fully validated
    //    ancestors of a payload are known and comprise a well-formed chain.
    if (maybeLatestValidAncestor.isEmpty()) {
      return respondWith(reqId, blockParam, null, ACCEPTED);
    }

    final Hash latestValidAncestor = maybeLatestValidAncestor.get();

    // async precompute sender to improve performance during transaction processing
    asyncPrecomputeSenders(blockParam.getTransactions());

    // execute block and return result response
    final long startTimeNs = System.nanoTime();
    final BlockProcessingResult executionResult = rememberBlock(block, blockParam);
    if (executionResult.isSuccessful()) {
      lastExecutionTimeInNs = System.nanoTime() - startTimeNs;
      logImportedBlockInfo(
          block, lastExecutionTimeInNs, executionResult.getNbParallelizedTransactions());
      return respondWith(reqId, blockParam, newBlockHeader.getHash(), VALID);
    } else {
      LOG.debug("New payload is invalid: {}", executionResult);
      if (executionResult.causedBy().isPresent()) {
        Throwable causedBy = executionResult.causedBy().get();
        if (causedBy instanceof StorageException || causedBy instanceof MerkleTrieException) {
          return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
        }
      }
      return respondWithInvalid(
          reqId,
          blockParam,
          latestValidAncestor,
          INVALID,
          executionResult.errorMessage.orElse("N/A"));
    }
  }

  protected ExecutionPayloadV1 readPayloadParameter(final JsonRpcRequestContext requestContext) {
    final ExecutionPayloadV1 blockParam;
    try {
      blockParam =
          requestContext.getRequiredParameter(
              0, getPayloadParameterClass(), FAIL_ON_UNKNOWN_BUT_NULL);
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          "Invalid engine payload parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
          e);
    }
    return blockParam;
  }

  @SuppressWarnings("unchecked")
  protected NPRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final int requestNumOfParams = requestContext.getRequest().getParamLength();
    if (requestNumOfParams != getNumberOfParameters()) {
      throw new InvalidRequestParametersException(
          "Expected %d parameters but got %d"
              .formatted(getNumberOfParameters(), requestNumOfParams),
          RpcErrorType.INVALID_PARAM_COUNT);
    }
    return (NPRP) new NewPayloadRequestParametersV1<>(readPayloadParameter(requestContext));
  }

  protected int getNumberOfParameters() {
    return 1;
  }

  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV1.class;
  }

  private void asyncPrecomputeSenders(final List<Transaction> transactions) {
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
            asyncPrecomputeAuthorities(transaction);
          }
        });
  }

  private void asyncPrecomputeAuthorities(final Transaction transaction) {
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
        requestId, new PayloadStatusV1(status, latestValidHash, Optional.empty()));
  }

  JsonRpcResponse respondWithInvalid(final Object requestId, final String validationError) {
    return respondWithInvalid(requestId, null, null, INVALID, validationError);
  }

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
            param == null ? null : param.getBlockNumber(),
            param == null ? null : param.getBlockHash(),
            param == null ? null : param.getParentHash(),
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

  protected ValidationResult<RpcErrorType> validateParameters(final NPRP requestParameters) {
    return ValidationResult.valid();
  }

  protected ValidationResult<RpcErrorType> validateNewBlock(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final Optional<BlockHeader> maybeParentHeader,
      final NPRP requestParameters) {
    final BlockHeader newBlockHeader = newBlock.getHeader();
    if (newBlockHeader.getExtraData().size() > 32) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXTRA_DATA_PARAMS, "extra data field larger than 32 bytes");
    }
    if (maybeParentHeader.isPresent()
        && Long.compareUnsigned(
                maybeParentHeader.get().getTimestamp(), newBlock.getHeader().getTimestamp())
            >= 0) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_TIMESTAMP_PARAMS, "block timestamp not greater than parent");
    }
    return ValidationResult.valid();
  }

  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    final ExecutionPayloadV1 blockParam = requestParameters.payloadParameter();
    blockHeaderBuilder
        .parentHash(blockParam.getParentHash())
        .ommersHash(OMMERS_HASH_CONSTANT)
        .coinbase(blockParam.getFeeRecipient())
        .stateRoot(blockParam.getStateRoot())
        .transactionsRoot(BodyValidation.transactionsRoot(blockParam.getTransactions()))
        .receiptsRoot(blockParam.getReceiptsRoot())
        .logsBloom(blockParam.getLogsBloom())
        .difficulty(Difficulty.ZERO)
        .number(blockParam.getBlockNumber())
        .gasLimit(blockParam.getGasLimit())
        .gasUsed(blockParam.getGasUsed())
        .timestamp(blockParam.getTimestamp())
        .extraData(blockParam.getExtraData())
        .baseFee(blockParam.getBaseFeePerGas())
        .prevRandao(blockParam.getPrevRandao())
        .nonce(0)
        .blockHeaderFunctions(HEADER_FUNCTIONS);
  }

  protected BlockBody createBlockBody(final EP executionPayload) {
    return new BlockBody(executionPayload.getTransactions(), Collections.emptyList());
  }

  protected BlockProcessingResult rememberBlock(final Block block, final EP executionPayload) {
    return mergeCoordinator.rememberBlock(block, Optional.empty());
  }

  private void logImportedBlockInfo(
      final Block block, final long timeInNs, final Optional<Integer> nbParallelizedTransactions) {
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
    appendVersionSpecificLogInfo(message, messageArgs, block);
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
      final StringBuilder message, final List<Object> messageArgs, final Block block) {}

  private long getLastExecutionTime() {
    return this.lastExecutionTimeInNs;
  }

  @Override
  protected final ValidationResult<RpcErrorType> validateForkSupported(final long blockTimestamp) {
    return ForkSupportHelper.validateForkSupported(
        minSupportedFork, minForkTimestamp, firstUnsupportedFork, maxForkTimestamp, blockTimestamp);
  }

  protected JsonRpcResponse processParametersParsingException(
      final Object reqId, final InvalidRequestParametersException e) {
    final Optional<JsonMappingException> maybeFieldEx =
        extractCauseByType(e, JsonMappingException.class);

    // specific invalid field with custom error response
    String customMessage = null;
    if (maybeFieldEx.isPresent()) {
      final JsonMappingException fieldEx = maybeFieldEx.get();
      final Optional<String> maybeJsonPath = extractJsonPath(fieldEx);
      if (maybeJsonPath.isPresent()) {
        final String jsonPath = maybeJsonPath.get();
        if (jsonPath.equals("transactions")) {
          return respondWithInvalid(
              reqId,
              "Failed to decode transactions from block parameter ("
                  + fieldEx.getOriginalMessage()
                  + ")");
        } else if (jsonPath.equals("extraData")) {
          customMessage =
              "Failed to decode extraData from block parameter ("
                  + fieldEx.getOriginalMessage()
                  + ")";
        }
      }
    }

    return new JsonRpcErrorResponse(
        reqId,
        ValidationResult.invalid(
            RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
            Objects.requireNonNullElse(
                customMessage, "Failed to decode block parameter (" + e.getMessage() + ")")));
  }

  protected static class InvalidRequestParametersException extends InvalidJsonRpcRequestException {
    private final ExecutionPayloadV1 payloadParameter;

    InvalidRequestParametersException(final String message, final RpcErrorType rpcErrorType) {
      super(message, rpcErrorType);
      this.payloadParameter = null;
    }

    InvalidRequestParametersException(
        final String message, final RpcErrorType rpcErrorType, final Throwable cause) {
      this(null, message, rpcErrorType, cause);
    }

    InvalidRequestParametersException(
        final @Nullable ExecutionPayloadV1 payloadParameter,
        final String message,
        final RpcErrorType rpcErrorType,
        final Throwable cause) {
      super(message, rpcErrorType, cause);
      this.payloadParameter = payloadParameter;
    }

    boolean hasPayloadParameter() {
      return payloadParameter != null;
    }

    @NonNull ExecutionPayloadV1 getPayloadParameter() {
      Preconditions.checkNotNull(payloadParameter, "Payload parameter not present");
      return payloadParameter;
    }

    @Override
    public String getMessage() {
      return super.getMessage()
          + " (payloadParameter "
          + (payloadParameter != null ? " present)" : " absent)");
    }
  }
}
