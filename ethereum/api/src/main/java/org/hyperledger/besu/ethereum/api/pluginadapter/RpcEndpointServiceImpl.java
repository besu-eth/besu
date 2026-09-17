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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.PluginJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.plugin.services.RpcEndpointService;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RPC endpoint service implementation. */
public class RpcEndpointServiceImpl implements RpcEndpointService {
  private static final Logger LOG = LoggerFactory.getLogger(RpcEndpointServiceImpl.class);

  private final Map<String, Function<PluginRpcRequest, ?>> rpcMethods = new HashMap<>();
  private Map<String, JsonRpcMethod> inProcessRpcMethods;

  /** Default Constructor. */
  public RpcEndpointServiceImpl() {}

  /**
   * Init the service
   *
   * @param inProcessRpcMethods set of RPC methods that can be called
   */
  public void init(final Map<String, JsonRpcMethod> inProcessRpcMethods) {
    this.inProcessRpcMethods = inProcessRpcMethods;
  }

  @Override
  public <T> void registerRPCEndpoint(
      final String namespace,
      final String functionName,
      final Function<PluginRpcRequest, T> function) {
    checkArgument(namespace.matches("\\p{Alnum}+"), "Namespace must be only alpha numeric");
    checkArgument(functionName.matches("\\p{Alnum}+"), "Function Name must be only alpha numeric");
    checkNotNull(function);

    rpcMethods.put(namespace + "_" + functionName, function);
  }

  @Override
  public PluginRpcResponse call(final String methodName, final Object[] params) {
    checkNotNull(
        inProcessRpcMethods,
        "Service not initialized yet, this method must be called after plugin 'beforeExternalServices' call completes");

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

  /**
   * Gets plugin methods.
   *
   * @param namespaces the namespaces collection
   * @return the Json Rpc Methods from plugins
   */
  public Map<String, ? extends JsonRpcMethod> getPluginMethods(
      final Collection<String> namespaces) {
    return rpcMethods.entrySet().stream()
        .filter(
            entry ->
                namespaces.stream()
                    .anyMatch(
                        namespace ->
                            entry
                                .getKey()
                                .toUpperCase(Locale.ROOT)
                                .startsWith(namespace.toUpperCase(Locale.ROOT))))
        .map(entry -> new PluginJsonRpcMethod(entry.getKey(), entry.getValue()))
        .collect(Collectors.toMap(PluginJsonRpcMethod::getName, e -> e));
  }

  /**
   * Checks if RPC methods belongs to a namespace
   *
   * @param namespace the namespace to check against
   * @return true if any of the RPC method starts with given namespace, false otherwise.
   */
  public boolean hasNamespace(final String namespace) {
    return rpcMethods.keySet().stream()
        .anyMatch(
            key -> key.toUpperCase(Locale.ROOT).startsWith(namespace.toUpperCase(Locale.ROOT)));
  }
}
