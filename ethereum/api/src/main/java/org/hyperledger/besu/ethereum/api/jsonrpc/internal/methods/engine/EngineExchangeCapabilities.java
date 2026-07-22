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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ENGINE_EXCHANGE_CAPABILITIES;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ENGINE_PREPARE_PAYLOAD_DEBUG;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineExchangeCapabilities extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineExchangeCapabilities.class);

  public EngineExchangeCapabilities(final ConstructorArguments constructorArguments) {
    super(constructorArguments, null, null);
  }

  @Override
  public String getName() {
    return ENGINE_EXCHANGE_CAPABILITIES.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final Object reqId = requestContext.getRequest().getId();

    LOG.atTrace()
        .setMessage("received remote capabilities: {}")
        .addArgument(
            () -> {
              try {
                return requestContext.getRequiredParameter(0, String[].class);
              } catch (JsonRpcParameterException e) {
                throw new InvalidJsonRpcParameters(
                    "Invalid remote capabilities parameters (index 0)",
                    RpcErrorType.INVALID_REMOTE_CAPABILITIES_PARAMS,
                    e);
              }
            })
        .log();

    final List<String> localCapabilities =
        Stream.of(RpcMethod.values())
            .filter(e -> e.getMethodName().startsWith("engine_"))
            .filter(e -> !e.equals(ENGINE_EXCHANGE_CAPABILITIES))
            .filter(e -> !e.equals(ENGINE_PREPARE_PAYLOAD_DEBUG))
            .map(RpcMethod::getMethodName)
            .collect(Collectors.toList());

    return respondWith(reqId, localCapabilities);
  }

  private JsonRpcResponse respondWith(
      final Object requestId, final List<String> localCapabilities) {
    return new JsonRpcSuccessResponse(requestId, localCapabilities);
  }
}
