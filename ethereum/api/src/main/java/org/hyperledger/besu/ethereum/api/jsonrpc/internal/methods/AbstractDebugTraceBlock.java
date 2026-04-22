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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.debug.TraceOptions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;

public abstract class AbstractDebugTraceBlock implements StreamingJsonRpcMethod {

  private final ProtocolSchedule protocolSchedule;
  private final Supplier<BlockchainQueries> blockchainQueriesSupplier;

  public AbstractDebugTraceBlock(
      final ProtocolSchedule protocolSchedule, final BlockchainQueries blockchainQueries) {
    this.blockchainQueriesSupplier = Suppliers.ofInstance(blockchainQueries);
    this.protocolSchedule = protocolSchedule;
  }

  protected BlockchainQueries getBlockchainQueries() {
    return blockchainQueriesSupplier.get();
  }

  protected TraceOptions getTraceOptions(final JsonRpcRequestContext requestContext) {
    final TraceOptions traceOptions;
    try {
      traceOptions =
          requestContext
              .getOptionalParameter(1, TransactionTraceParams.class)
              .map(TransactionTraceParams::traceOptions)
              .orElse(TraceOptions.DEFAULT);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction trace parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS,
          e);
    } catch (IllegalArgumentException e) {
      // Handle invalid tracer type from TracerType.fromString()
      throw new InvalidJsonRpcParameters(
          e.getMessage(), RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS, e);
    }
    return traceOptions;
  }

  /**
   * Finds the block to be traced from the request parameters.
   *
   * @param request the RPC request context
   * @return the block to trace, or empty if it cannot be found (not found, invalid params, etc.)
   */
  protected abstract Optional<Block> findBlock(JsonRpcRequestContext request);

  /**
   * Synchronous response path used by batch JSON-RPC requests. Accumulates all transaction traces
   * in memory rather than streaming, so that the result can be returned as a single object.
   */
  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext request) {
    final Optional<Block> maybeBlock = findBlock(request);
    if (maybeBlock.isEmpty()) {
      return new JsonRpcSuccessResponse(request.getRequest().getId(), null);
    }
    final TraceOptions traceOptions = getTraceOptions(request);
    final DebugTraceBlockStreamer streamer = createStreamer(traceOptions, maybeBlock);
    return new JsonRpcSuccessResponse(request.getRequest().getId(), streamer.accumulateAll());
  }

  protected DebugTraceBlockStreamer createStreamer(
      final TraceOptions traceOptions, final Optional<Block> maybeBlock) {
    return maybeBlock
        .map(
            block ->
                new DebugTraceBlockStreamer(
                    block, traceOptions, protocolSchedule, getBlockchainQueries()))
        .orElse(null);
  }

  protected static void writeStreamingResponse(
      final Object id,
      final DebugTraceBlockStreamer streamer,
      final OutputStream out,
      final ObjectMapper mapper)
      throws IOException {
    if (streamer == null) {
      out.write(
          ("{\"jsonrpc\":\"2.0\",\"id\":" + mapper.writeValueAsString(id) + ",\"result\":null}")
              .getBytes(StandardCharsets.UTF_8));
      return;
    }
    final byte[] prefix =
        ("{\"jsonrpc\":\"2.0\",\"id\":" + mapper.writeValueAsString(id) + ",\"result\":")
            .getBytes(StandardCharsets.UTF_8);
    out.write(prefix);
    streamer.streamTo(out, mapper);
    out.write("}".getBytes(StandardCharsets.UTF_8));
  }
}
