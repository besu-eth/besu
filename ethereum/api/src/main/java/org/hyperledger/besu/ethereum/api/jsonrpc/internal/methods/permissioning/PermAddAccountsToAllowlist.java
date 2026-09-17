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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.permissioning;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.AllowlistOperationResult;

import java.util.List;
import java.util.Optional;

public class PermAddAccountsToAllowlist implements JsonRpcMethod {

  private final Optional<AccountLocalConfigPermissioningController> allowlistController;

  public PermAddAccountsToAllowlist(
      final Optional<AccountLocalConfigPermissioningController> allowlistController) {
    this.allowlistController = allowlistController;
  }

  @Override
  public String getName() {
    return RpcMethod.PERM_ADD_ACCOUNTS_TO_ALLOWLIST.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final List<String> accountsList;
    try {
      accountsList = requestContext.getRequiredParameter(0, List.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid accounts list parameter (index 0)", RpcErrorType.INVALID_ACCOUNT_PARAMS, e);
    }

    if (allowlistController.isPresent()) {
      final AllowlistOperationResult addResult =
          allowlistController.get().addAccounts(accountsList);

      return switch (addResult) {
        case ERROR_EMPTY_ENTRY ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.ACCOUNT_ALLOWLIST_EMPTY_ENTRY);
        case ERROR_INVALID_ENTRY ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.ACCOUNT_ALLOWLIST_INVALID_ENTRY);
        case ERROR_EXISTING_ENTRY ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.ACCOUNT_ALLOWLIST_EXISTING_ENTRY);
        case ERROR_DUPLICATED_ENTRY ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(),
                RpcErrorType.ACCOUNT_ALLOWLIST_DUPLICATED_ENTRY);
        case ERROR_ALLOWLIST_PERSIST_FAIL ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.ALLOWLIST_PERSIST_FAILURE);
        case ERROR_ALLOWLIST_FILE_SYNC ->
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.ALLOWLIST_FILE_SYNC);
        case SUCCESS -> new JsonRpcSuccessResponse(requestContext.getRequest().getId());
        default ->
            throw new IllegalStateException(
                "Unmapped result from AccountLocalConfigPermissioningController");
      };
    } else {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.ACCOUNT_ALLOWLIST_NOT_ENABLED);
    }
  }
}
