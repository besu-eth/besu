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

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterCountExceededException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

public class EthNewFilter implements JsonRpcMethod {

  private final FilterManager filterManager;

  public EthNewFilter(final FilterManager filterManager) {
    this.filterManager = filterManager;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_NEW_FILTER.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final FilterParameter filter;
    try {
      filter = requestContext.getRequiredParameter(0, FilterParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid filter paramters (index 0)", RpcErrorType.INVALID_FILTER_PARAMS, e);
    }

    if (!filter.isValid()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_FILTER_PARAMS);
    }

    final String logFilterId;
    try {
      logFilterId =
          filterManager.installLogFilter(
              filter.getFromBlock(), filter.getToBlock(), filter.getLogsQuery());
    } catch (final FilterCountExceededException e) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.EXCEEDS_RPC_MAX_ACTIVE_FILTERS);
    }

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), logFilterId);
  }
}
