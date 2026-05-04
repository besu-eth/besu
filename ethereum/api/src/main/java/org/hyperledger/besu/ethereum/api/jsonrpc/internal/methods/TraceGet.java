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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceGet extends AbstractTraceByHash implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(TraceGet.class);

  public TraceGet(
      final Supplier<BlockTracer> blockTracerSupplier,
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule) {
    super(blockTracerSupplier, blockchainQueries, protocolSchedule);
  }

  @Override
  public String getName() {
    return RpcMethod.TRACE_GET.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    if (requestContext.getRequest().getParamLength() != 2) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PARAM_COUNT);
    }

    final Hash transactionHash;
    try {
      transactionHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction hash parameter (index 0)",
          RpcErrorType.INVALID_TRANSACTION_HASH_PARAMS,
          e);
    }
    final List<?> traceNumbersAsStrings;
    try {
      traceNumbersAsStrings = requestContext.getRequiredParameter(1, List.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid trace numbers parameters (index 1)",
          RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS,
          e);
    }
    final List<Integer> traceAddress;
    try {
      traceAddress =
          traceNumbersAsStrings.stream()
              .map(
                  t -> {
                    if (t == null) {
                      throw new NumberFormatException("Trace number cannot be null");
                    }
                    final String traceNumber = (String) t;
                    if (!traceNumber.startsWith("0x") && !traceNumber.startsWith("0X")) {
                      throw new NumberFormatException(
                          "Trace number missing 0x prefix: " + traceNumber);
                    }
                    return Integer.parseInt(traceNumber.substring(2), 16);
                  })
              .collect(Collectors.toList());
    } catch (final NumberFormatException
        | ClassCastException
        | NullPointerException
        | StringIndexOutOfBoundsException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid trace numbers parameters (index 1)",
          RpcErrorType.INVALID_TRACE_NUMBERS_PARAMS,
          e);
    }

    LOG.trace(
        "Received RPC rpcName={} txHash={} traceAddress={}",
        getName(),
        transactionHash,
        traceAddress);

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        resultByTransactionHash(transactionHash)
            .filter(trace -> trace.getTraceAddress().equals(traceAddress))
            .findFirst()
            .orElse(null));
  }
}
