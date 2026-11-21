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

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.CreateAccessListResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.tracing.AccessListOperationTracer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class EthCreateAccessList extends AbstractEstimateGas {

  private static final String DEFAULT_FAILURE_MESSAGE = "execution reverted";

  public EthCreateAccessList(
      final BlockchainQueries blockchainQueries, final TransactionSimulator transactionSimulator) {
    super(blockchainQueries, transactionSimulator);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_CREATE_ACCESS_LIST.getMethodName();
  }

  @Override
  protected Object simulate(
      final JsonRpcRequestContext requestContext,
      final CallParameter callParams,
      final ProcessableBlockHeader blockHeader,
      final TransactionSimulationFunction simulationFunction,
      final long gasLimitUpperBound,
      final long minTxCost) {

    final CallParameter accessListCallParams = withAccessList(callParams);
    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    if (attemptOptimisticSimulationWithMinimumBlockGasUsed(
        minTxCost, accessListCallParams, simulationFunction, tracer)) {
      return new CreateAccessListResult(tracer.getAccessList(), minTxCost);
    }
    // Otherwise, do the calculation with the provided gasLimit
    final Optional<TransactionSimulatorResult> firstResult =
        simulationFunction.simulate(
            overrideGasLimit(accessListCallParams, gasLimitUpperBound), tracer);

    // if the call accessList is different from the simulation result, calculate gas and return
    if (shouldProcessWithAccessListOverride(callParams, tracer)) {
      final AccessListSimulatorResult result =
          processTransactionWithAccessListOverride(
              accessListCallParams, gasLimitUpperBound, tracer.getAccessList(), simulationFunction);
      return createResponse(requestContext, result);
    } else {
      return createResponse(requestContext, new AccessListSimulatorResult(firstResult, tracer));
    }
  }

  @Override
  protected long adjustGasLimitUpperBound(
      final CallParameter callParameter, final long gasLimitUpperBound, final long minTxCost) {
    // honour the gas limit provided by the caller: a call that fails within
    // the provided limit must be reported as failed instead of being
    // simulated with a larger allowance, see
    // https://github.com/besu-eth/besu/issues/11848
    // a gas limit of 0 means "not specified" (the same convention the other
    // execution clients use for JSON-RPC calls)
    if (callParameter.getGas().isEmpty() || callParameter.getGas().getAsLong() == 0) {
      return gasLimitUpperBound;
    }
    return Math.min(callParameter.getGas().getAsLong(), gasLimitUpperBound);
  }

  private CallParameter withAccessList(final CallParameter callParams) {
    if (callParams.getAccessList().isPresent()) {
      return callParams;
    }
    return ImmutableCallParameter.builder().from(callParams).accessList(List.of()).build();
  }

  private Object createResponse(
      final JsonRpcRequestContext requestContext, final AccessListSimulatorResult result) {
    if (result.result().isPresent() && result.result().get().isInvalid()) {
      return errorResponse(requestContext, result.result().get());
    }
    return result
        .result()
        .map(createResponse(result.tracer()))
        .orElseGet(() -> errorResponse(requestContext, RpcErrorType.INTERNAL_ERROR));
  }

  private boolean shouldProcessWithAccessListOverride(
      final CallParameter parameters, final AccessListOperationTracer tracer) {

    // if empty, transaction did not access any storage, does not need to reprocess
    if (tracer.getAccessList().isEmpty()) {
      return false;
    }

    // if empty, call did not include accessList, should reprocess
    if (parameters.getAccessList().isEmpty()) {
      return true;
    }

    // If call included access list, compare it with tracer result and return true if different
    return !Objects.equals(tracer.getAccessList(), parameters.getAccessList().get());
  }

  private Function<TransactionSimulatorResult, Object> createResponse(
      final AccessListOperationTracer operationTracer) {
    // access list is created regardless, but include error message if result was not successful
    return result ->
        new CreateAccessListResult(
            operationTracer.getAccessList(),
            result.getGasEstimate(),
            result.isSuccessful() ? Optional.empty() : Optional.of(failureMessage(result)));
  }

  private String failureMessage(final TransactionSimulatorResult result) {
    if (result.result().getRevertReason().isPresent()) {
      return DEFAULT_FAILURE_MESSAGE;
    }
    return result
        .result()
        .getExceptionalHaltReason()
        .map(ExceptionalHaltReason::getDescription)
        .orElse(DEFAULT_FAILURE_MESSAGE);
  }

  private AccessListSimulatorResult processTransactionWithAccessListOverride(
      final CallParameter callParameter,
      final long gasLimit,
      final List<AccessListEntry> accessList,
      final TransactionSimulationFunction simulationFunction) {

    final AccessListOperationTracer tracer = AccessListOperationTracer.create();
    final CallParameter modifiedCallParameter =
        overrideAccessList(callParameter, gasLimit, accessList);

    final Optional<TransactionSimulatorResult> result =
        simulationFunction.simulate(modifiedCallParameter, tracer);
    return new AccessListSimulatorResult(result, tracer);
  }

  private CallParameter overrideAccessList(
      final CallParameter callParams,
      final long gasLimit,
      final List<AccessListEntry> accessListEntries) {

    return ImmutableCallParameter.builder()
        .from(callParams)
        .gas(gasLimit)
        .accessList(accessListEntries)
        .build();
  }

  private record AccessListSimulatorResult(
      Optional<TransactionSimulatorResult> result, AccessListOperationTracer tracer) {}
}
