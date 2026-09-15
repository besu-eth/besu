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

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulatorResult;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EthEstimateGas extends AbstractEstimateGas {
  private static final Logger LOG = LoggerFactory.getLogger(EthEstimateGas.class);
  private static final double SUB_CALL_REMAINING_GAS_RATIO = 64D / 63D;
  // zero tolerance means there is no tolerance,
  // which means keep looping until the estimate is exact (previous behavior)
  protected double estimateGasToleranceRatio;
  private static final long CALL_STIPEND = 2_300L;

  public EthEstimateGas(
      final BlockchainQueries blockchainQueries,
      final TransactionSimulator transactionSimulator,
      final ApiConfiguration apiConfiguration) {
    super(blockchainQueries, transactionSimulator);
    this.estimateGasToleranceRatio = apiConfiguration.getEstimateGasToleranceRatio();
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_ESTIMATE_GAS.getMethodName();
  }

  @Override
  protected Object simulate(
      final JsonRpcRequestContext requestContext,
      final CallParameter callParams,
      final ProcessableBlockHeader blockHeader,
      final TransactionSimulationFunction simulationFunction,
      final long gasLimitUpperBound,
      final long minTxCost) {

    LOG.debug(
        "Processing transaction with tolerance {}; callParams: {}",
        estimateGasToleranceRatio,
        callParams);

    if (attemptOptimisticSimulationWithMinimumBlockGasUsed(
        minTxCost, callParams, simulationFunction, OperationTracer.NO_TRACING)) {
      return Quantity.create(minTxCost);
    }

    final var maybeResult =
        simulationFunction.simulate(
            overrideGasLimit(callParams, gasLimitUpperBound), OperationTracer.NO_TRACING);

    final Optional<JsonRpcErrorResponse> maybeErrorResponse =
        validateSimulationResult(requestContext, maybeResult);
    if (maybeErrorResponse.isPresent()) {
      return maybeErrorResponse.get();
    }

    final var result = maybeResult.get();
    final long estimateGasUsed = result.result().getEstimateGasUsedByTransaction();
    long high = gasLimitUpperBound;
    long mid;

    long low = estimateGasUsed - 1;

    if (isPlainValueTransfer(callParams)) {
      // gas used at a high limit is not always a sufficient limit (63/64 rule), so confirm it
      final var exactResult =
          simulationFunction.simulate(
              overrideGasLimit(callParams, estimateGasUsed), OperationTracer.NO_TRACING);
      if (exactResult.isPresent() && exactResult.get().isSuccessful()) {
        return Quantity.create(estimateGasUsed);
      }
      low = estimateGasUsed;
    }

    var optimisticGasLimit = processEstimateGas(result);

    final var optimisticResult =
        simulationFunction.simulate(
            overrideGasLimit(callParams, optimisticGasLimit), OperationTracer.NO_TRACING);

    if (optimisticResult.isPresent() && optimisticResult.get().isSuccessful()) {
      high = optimisticGasLimit;
    } else {
      low = optimisticGasLimit;
    }

    while (low + 1 < high) {
      // check if we are close enough
      if (estimateGasToleranceRatio > 0
          && (double) (high - low) / high < estimateGasToleranceRatio) {
        break;
      }
      mid = (low + high) / 2;
      var binarySearchResult =
          simulationFunction.simulate(
              overrideGasLimit(callParams, mid), OperationTracer.NO_TRACING);

      if (binarySearchResult.isEmpty() || !binarySearchResult.get().isSuccessful()) {
        low = mid;
      } else {
        high = mid;
      }
    }

    return Quantity.create(high);
  }

  private Optional<JsonRpcErrorResponse> validateSimulationResult(
      final JsonRpcRequestContext requestContext,
      final Optional<TransactionSimulatorResult> maybeResult) {
    if (maybeResult.isEmpty()) {
      LOG.error("No result after simulating transaction.");
      return Optional.of(
          new JsonRpcErrorResponse(
              requestContext.getRequest().getId(), RpcErrorType.INTERNAL_ERROR));
    }

    // if the transaction is invalid or doesn't have enough gas with the max it never will!
    if (maybeResult.get().isInvalid() || !maybeResult.get().isSuccessful()) {
      return Optional.of(errorResponse(requestContext, maybeResult.get()));
    }
    return Optional.empty();
  }

  /**
   * Estimate gas by adding call stipend and compute the necessary gas for sub calls
   *
   * @param result transaction simulator result
   * @return estimate gas
   */
  protected long processEstimateGas(final TransactionSimulatorResult result) {
    final long gasUsedByTransaction = result.result().getEstimateGasUsedByTransaction();

    // no more than 64/63 of the remaining gas can be passed to the sub calls
    return ((long) ((gasUsedByTransaction + CALL_STIPEND) * SUB_CALL_REMAINING_GAS_RATIO));
  }
}
