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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DebugTraceBlockByNumber extends AbstractBlockParameterMethod
    implements StreamingJsonRpcMethod {

  protected final ProtocolSchedule protocolSchedule;
  private final BlockchainQueries blockchainQueriesRef;

  public DebugTraceBlockByNumber(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    super(blockchainQueries);
    this.protocolSchedule = protocolSchedule;
    this.blockchainQueriesRef = blockchainQueries;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_TRACE_BLOCK_BY_NUMBER.getMethodName();
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext request, final long blockNumber) {
    final TraceOptions traceOptions = getTraceOptions(request);
    final Optional<Block> maybeBlock =
        getBlockchainQueries().getBlockchain().getBlockByNumber(blockNumber);

    if (blockNumber == 0L) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.GENESIS_BLOCK_NOT_TRACEABLE);
    }

    return maybeBlock
        .map(
            block ->
                new DebugTraceBlockStreamer(
                    block, traceOptions, protocolSchedule, blockchainQueriesRef))
        .orElse(null);
  }

  @Override
  public void streamResponse(
      final JsonRpcRequestContext requestContext, final OutputStream out, final ObjectMapper mapper)
      throws IOException {
    final Object result = findResultByParamType(requestContext);

    if (result instanceof JsonRpcErrorResponse errorResponse) {
      mapper.writeValue(out, errorResponse);
      return;
    }

    final DebugTraceBlockStreamer streamer = result instanceof DebugTraceBlockStreamer s ? s : null;
    AbstractDebugTraceBlock.writeStreamingResponse(
        requestContext.getRequest().getId(), streamer, out, mapper);
  }

  /**
   * Synchronous response path used by batch JSON-RPC requests. Accumulates all transaction traces
   * in memory rather than streaming.
   */
  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Object result = findResultByParamType(requestContext);
    if (result instanceof JsonRpcErrorResponse errorResponse) {
      return errorResponse;
    }
    if (!(result instanceof DebugTraceBlockStreamer streamer)) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }
    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), streamer.accumulateAll());
  }

  private TraceOptions getTraceOptions(final JsonRpcRequestContext request) {
    final TraceOptions traceOptions;
    try {
      traceOptions =
          request
              .getOptionalParameter(1, TransactionTraceParams.class)
              .map(TransactionTraceParams::traceOptions)
              .orElse(TraceOptions.DEFAULT);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction trace parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS,
          e);
    } catch (IllegalArgumentException e) {
      throw new InvalidJsonRpcParameters(
          e.getMessage(), RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS, e);
    }
    return traceOptions;
  }
}
