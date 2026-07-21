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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import com.google.common.base.Suppliers;

public class DebugGetRawBlockAccessList extends AbstractBlockParameterMethod {

  public DebugGetRawBlockAccessList(final BlockchainQueries blockchain) {
    super(Suppliers.ofInstance(blockchain));
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_GET_RAW_BLOCK_ACCESS_LIST.getMethodName();
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
    return getBlockchainQueries()
        .getBlockHeaderByNumber(blockNumber)
        .map(header -> getRawBlockAccessList(request, header))
        .orElseGet(
            () ->
                new JsonRpcErrorResponse(
                    request.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND));
  }

  private Object getRawBlockAccessList(
      final JsonRpcRequestContext request, final BlockHeader header) {
    if (!getBlockchainQueries().isBlockAccessListSupported(header)) {
      return new JsonRpcErrorResponse(
          request.getRequest().getId(), RpcErrorType.RESOURCE_NOT_FOUND);
    }

    return getBlockchainQueries()
        .getBlockchain()
        .getBlockAccessList(header.getHash())
        .<Object>map(
            blockAccessList -> {
              final BytesValueRLPOutput output = new BytesValueRLPOutput();
              blockAccessList.writeTo(output);
              return output.encoded().toHexString();
            })
        .orElseGet(
            () ->
                new JsonRpcErrorResponse(
                    request.getRequest().getId(), RpcErrorType.PRUNED_HISTORY_UNAVAILABLE));
  }
}
