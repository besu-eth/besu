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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.plugin.services.InProcessRpcService;
import org.hyperledger.besu.plugin.services.exception.InProcessRpcDisabledException;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves Besu's own JSON-RPC methods to plugins in-process. */
public class InProcessRpcServiceImpl implements InProcessRpcService {
  private static final Logger LOG = LoggerFactory.getLogger(InProcessRpcServiceImpl.class);

  private final boolean enabled;
  private final Map<String, JsonRpcMethod> inProcessRpcMethods;

  /**
   * Creates the service.
   *
   * @param enabled whether in-process RPC is enabled on this node
   * @param inProcessRpcMethods the methods that can be called, empty when disabled
   */
  public InProcessRpcServiceImpl(
      final boolean enabled, final Map<String, JsonRpcMethod> inProcessRpcMethods) {
    this.enabled = enabled;
    this.inProcessRpcMethods = inProcessRpcMethods;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public PluginRpcResponse call(final String methodName, final Object[] params) {
    if (!enabled) {
      throw new InProcessRpcDisabledException();
    }

    LOG.atTrace()
        .setMessage("Calling method:{} with params:{}")
        .addArgument(methodName)
        .addArgument(() -> Arrays.toString(params))
        .log();

    final var method = inProcessRpcMethods.get(methodName);

    if (method == null) {
      throw new NoSuchElementException("Unknown or not enabled method: " + methodName);
    }

    final var requestContext =
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", methodName, params));
    final var response = method.response(requestContext);
    return new PluginRpcResponse() {
      @Override
      public Object getResult() {
        return switch (response.getType()) {
          case NONE, UNAUTHORIZED -> null;
          case SUCCESS -> ((JsonRpcSuccessResponse) response).getResult();
          case ERROR -> ((JsonRpcErrorResponse) response).getError();
        };
      }

      @Override
      public RpcResponseType getType() {
        return response.getType();
      }
    };
  }
}
