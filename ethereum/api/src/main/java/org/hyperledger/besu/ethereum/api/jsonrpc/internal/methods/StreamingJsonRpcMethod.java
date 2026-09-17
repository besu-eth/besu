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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface StreamingJsonRpcMethod extends JsonRpcMethod {

  void streamResponse(JsonRpcRequestContext request, OutputStream out, ObjectMapper mapper)
      throws IOException;

  @Override
  default boolean isStreaming() {
    return true;
  }

  /**
   * Default synchronous response path for batch JSON-RPC requests. Returns an error since streaming
   * cannot be used within a batch response. Must use the non-streaming JsonRp instead
   */
  @Override
  default JsonRpcResponse response(final JsonRpcRequestContext request) {
    return new JsonRpcErrorResponse(
        new JsonRpcRequestId(request.getRequest().getId()), RpcErrorType.INVALID_REQUEST);
  }
}
