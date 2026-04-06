/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.CallParameterUtil.validateAndGetCallParams;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcErrorConverter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

public abstract class AbstractEstimateGas extends AbstractBlockParameterMethod {
  protected final TransactionSimulator transactionSimulator;

  public AbstractEstimateGas(
      final BlockchainQueries blockchainQueries, final TransactionSimulator transactionSimulator) {
    super(blockchainQueries);
    this.transactionSimulator = transactionSimulator;
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    try {
      return request.getOptionalParameter(1, BlockParameter.class).orElse(BlockParameter.PENDING);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  protected abstract Object simulate(
      final JsonRpcRequestContext requestContext,
      final CallParameter callParams,
      final ProcessableBlockHeader blockHeader,
      final TransactionSimulationFunction simulationFunction,
      final long gasLimitUpperBound,
      final long minTxCost);

  @Override
  protected Object pendingResult(final JsonRpcRequestContext requestContext) {
    final CallParameter callParameter = validateAndGetCallParams(requestContext);
    final var validationParams = getTransactionValidationParams(callParameter);
    final var maybeStateOverrides = getAddressStateOverrideMap(requestContext);
    final var pendingBlockHeader = transactionSimulator.simulatePendingBlockHeader();
    final var minTxCost = getBlockchainQueries().getMinimumTransactionCost(pendingBlockHeader);
    final var gasLimitUpperBound = calculateGasLimitUpperBound(callParameter, pendingBlockHeader);
    if (gasLimitUpperBound < minTxCost) {
      return errorResponse(requestContext, RpcErrorType.TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE);
    }
    final TransactionSimulationFunction simulationFunction =
        (cp, op) ->
            transactionSimulator.processOnPending(
                cp, maybeStateOverrides, validationParams, op, pendingBlockHeader);
    return simulate(
        requestContext,
        callParameter,
        pendingBlockHeader,
        simulationFunction,
        gasLimitUpperBound,
        minTxCost);
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext requestContext, final long blockNumber) {
    final CallParameter callParameter = validateAndGetCallParams(requestContext);
    final Optional<BlockHeader> maybeBlockHeader = blockHeader(blockNumber);
    final Optional<RpcErrorType> jsonRpcError = validateBlockHeader(maybeBlockHeader);
    if (jsonRpcError.isPresent()) {
      return errorResponse(requestContext, jsonRpcError.get());
    }
    return resultByBlockHeader(requestContext, callParameter, maybeBlockHeader.get());
  }

  private Object resultByBlockHeader(
      final JsonRpcRequestContext requestContext,
      final CallParameter callParameter,
      final BlockHeader blockHeader) {
    final var validationParams = getTransactionValidationParams(callParameter);
    final var maybeStateOverrides = getAddressStateOverrideMap(requestContext);
    final var minTxCost = getBlockchainQueries().getMinimumTransactionCost(blockHeader);
    final var gasLimitUpperBound = calculateGasLimitUpperBound(callParameter, blockHeader);
    if (gasLimitUpperBound < minTxCost) {
      return errorResponse(requestContext, RpcErrorType.TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE);
    }
    final TransactionSimulationFunction simulationFunction =
        (cp, op) ->
            transactionSimulator.process(
                cp, maybeStateOverrides, validationParams, op, blockHeader);
    return simulate(
        requestContext,
        callParameter,
        blockHeader,
        simulationFunction,
        gasLimitUpperBound,
        minTxCost);
  }

  private Optional<BlockHeader> blockHeader(final long blockNumber) {
    if (getBlockchainQueries().headBlockNumber() == blockNumber) {
      // chain head header if cached, and we can return it form memory
      return Optional.of(getBlockchainQueries().getBlockchain().getChainHeadHeader());
    }
    return getBlockchainQueries().getBlockHeaderByNumber(blockNumber);
  }

  private Optional<RpcErrorType> validateBlockHeader(final Optional<BlockHeader> maybeBlockHeader) {
    if (maybeBlockHeader.isEmpty()) {
      return Optional.of(RpcErrorType.BLOCK_NOT_FOUND);
    }

    final var blockHeader = maybeBlockHeader.get();
    if (!getBlockchainQueries()
        .getWorldStateArchive()
        .isWorldStateAvailable(blockHeader.getStateRoot(), blockHeader.getHash())) {
      return Optional.of(RpcErrorType.WORLD_STATE_UNAVAILABLE);
    }
    return Optional.empty();
  }

  protected CallParameter overrideGasLimit(final CallParameter callParams, final long gasLimit) {
    return ImmutableCallParameter.builder().from(callParams).gas(gasLimit).build();
  }

  protected JsonRpcErrorResponse errorResponse(
      final JsonRpcRequestContext request, final TransactionSimulatorResult result) {

    final ValidationResult<TransactionInvalidReason> validationResult =
        result.getValidationResult();
    if (validationResult != null && !validationResult.isValid()) {
      if (!validationResult.getErrorMessage().isEmpty()) {
        return errorResponse(request, JsonRpcError.from(validationResult));
      }
      return errorResponse(
          request,
          JsonRpcErrorConverter.convertTransactionInvalidReason(
              validationResult.getInvalidReason()));
    } else {
      final TransactionProcessingResult resultTrx = result.result();
      if (resultTrx != null && resultTrx.getRevertReason().isPresent()) {
        return errorResponse(
            request,
            new JsonRpcError(
                RpcErrorType.REVERT_ERROR, resultTrx.getRevertReason().get().toHexString()));
      }
      return errorResponse(request, RpcErrorType.INTERNAL_ERROR);
    }
  }

  protected JsonRpcErrorResponse errorResponse(
      final JsonRpcRequestContext request, final RpcErrorType rpcErrorType) {
    return errorResponse(request, new JsonRpcError(rpcErrorType));
  }

  protected JsonRpcErrorResponse errorResponse(
      final JsonRpcRequestContext request, final JsonRpcError jsonRpcError) {
    return new JsonRpcErrorResponse(request.getRequest().getId(), jsonRpcError);
  }

  protected static TransactionValidationParams getTransactionValidationParams(
      final CallParameter callParams) {
    final boolean isAllowExceedingBalance = !callParams.getStrict().orElse(Boolean.TRUE);

    return isAllowExceedingBalance
        ? TransactionValidationParams.transactionSimulatorAllowExceedingBalanceAndFutureNonce()
        : TransactionValidationParams.transactionSimulatorAllowUnderpricedAndFutureNonce();
  }

  @VisibleForTesting
  protected Optional<StateOverrideMap> getAddressStateOverrideMap(
      final JsonRpcRequestContext request) {
    try {
      return request.getOptionalParameter(2, StateOverrideMap.class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid account overrides parameter (index 2)", RpcErrorType.INVALID_CALL_PARAMS, e);
    }
  }

  protected boolean attemptOptimisticSimulationWithMinimumBlockGasUsed(
      final long minTxCost,
      final CallParameter callParams,
      final TransactionSimulationFunction simulationFunction,
      final OperationTracer operationTracer) {

    // If the transaction is a plain value transfer, try minTxCost. It is likely to succeed.
    if (callParams.getPayload().isEmpty() || callParams.getPayload().get().equals(Bytes.EMPTY)) {
      var maybeSimpleTransferResult =
          simulationFunction.simulate(overrideGasLimit(callParams, minTxCost), operationTracer);
      return maybeSimpleTransferResult.isPresent()
          && maybeSimpleTransferResult.get().isSuccessful();
    }
    return false;
  }

  private long calculateGasLimitUpperBound(
      final CallParameter callParameters, final ProcessableBlockHeader blockHeader) {

    final var maxTxGasLimit =
        Math.min(
            getBlockchainQueries().getTransactionGasLimitCap(blockHeader),
            blockHeader.getGasLimit());

    if (callParameters.getSender().isPresent() && callParameters.getStrict().orElse(Boolean.TRUE)) {
      final var sender = callParameters.getSender().get();
      final var maxGasPrice = calculateTxMaxGasPrice(callParameters);
      if (!maxGasPrice.equals(Wei.ZERO)) {
        // For a concrete BlockHeader (historical or "latest"), look up the balance at that
        // block's own state.  For a synthetic ProcessableBlockHeader (pending), the block
        // does not exist yet, so the correct state is the parent's (chain-head) state.
        final Hash balanceBlockHash =
            (blockHeader instanceof BlockHeader bh) ? bh.getHash() : blockHeader.getParentHash();
        final var maybeBalance = getBlockchainQueries().accountBalance(sender, balanceBlockHash);
        if (maybeBalance.isEmpty() || maybeBalance.get().equals(Wei.ZERO)) {
          return 0;
        }
        final var balance = maybeBalance.get();
        final var balanceForGas = callParameters.getValue().map(balance::subtract).orElse(balance);
        final var gasLimitForBalance = balanceForGas.divide(maxGasPrice);
        return gasLimitForBalance.fitsLong()
            ? Math.min(gasLimitForBalance.toLong(), maxTxGasLimit)
            : maxTxGasLimit;
      }
    }

    return maxTxGasLimit;
  }

  private Wei calculateTxMaxGasPrice(final CallParameter callParameters) {
    return callParameters
        .getMaxFeePerGas()
        .orElseGet(() -> callParameters.getGasPrice().orElse(Wei.ZERO));
  }

  protected interface TransactionSimulationFunction {
    Optional<TransactionSimulatorResult> simulate(
        CallParameter callParams, OperationTracer operationTracer);
  }
}
