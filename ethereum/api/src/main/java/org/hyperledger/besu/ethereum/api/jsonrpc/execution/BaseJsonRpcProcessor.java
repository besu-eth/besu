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
package org.hyperledger.besu.ethereum.api.jsonrpc.execution;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.StreamingJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseJsonRpcProcessor implements JsonRpcProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(BaseJsonRpcProcessor.class);

  @Override
  public JsonRpcResponse process(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final Span metricSpan,
      final JsonRpcRequestContext request) {
    try {
      return method.response(request);
    } catch (final InvalidJsonRpcParameters e) {
      LOG.debug(
          "Invalid Params {} for method: {}, error: {}",
          Arrays.toString(request.getRequest().getParams()),
          method.getName(),
          e.getRpcErrorType().getMessage(),
          e);
      return new JsonRpcErrorResponse(id, e.getRpcErrorType());
    } catch (final RuntimeException e) {
      final JsonArray params = JsonObject.mapFrom(request.getRequest()).getJsonArray("params");
      LOG.error(String.format("Error processing method: %s %s", method.getName(), params), e);
      return new JsonRpcErrorResponse(id, RpcErrorType.INTERNAL_ERROR);
    }
  }

  @Override
  public void streamProcess(
      final JsonRpcRequestId id,
      final JsonRpcMethod method,
      final Span metricSpan,
      final JsonRpcRequestContext request,
      final OutputStream out,
      final ObjectMapper mapper)
      throws IOException {
    // Let exceptions propagate — the caller decides how to handle them based on
    // whether the response headers have already been flushed.
    ((StreamingJsonRpcMethod) method).streamResponse(request, out, mapper);
  }
}
