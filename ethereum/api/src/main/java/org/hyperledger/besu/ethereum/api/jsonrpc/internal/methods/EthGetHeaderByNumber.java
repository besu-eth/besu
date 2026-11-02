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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockHeaderResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

public class EthGetHeaderByNumber implements JsonRpcMethod {

  private final Supplier<BlockchainQueries> blockchain;

  public EthGetHeaderByNumber(final BlockchainQueries blockchain) {
    this(Suppliers.ofInstance(blockchain));
  }

  public EthGetHeaderByNumber(final Supplier<BlockchainQueries> blockchain) {
    this.blockchain = blockchain;
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_HEADER_BY_NUMBER.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final BlockParameter blockParameter;
    try {
      blockParameter = requestContext.getRequiredParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 0)", RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS, e);
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(),
        headerResult(blockParameter).map(BlockHeaderResult::new).orElse(null));
  }

  private Optional<BlockHeader> headerResult(final BlockParameter blockParameter) {
    final BlockchainQueries queries = blockchain.get();
    if (blockParameter.isPending()) {
      // The spec returns null for the pending tag and for unresolvable safe/finalized tags.
      return Optional.empty();
    }
    if (blockParameter.isLatest()) {
      return Optional.of(queries.headBlockHeader());
    }
    if (blockParameter.isSafe()) {
      return queries.safeBlockHeader();
    }
    if (blockParameter.isFinalized()) {
      return queries.finalizedBlockHeader();
    }
    if (blockParameter.isEarliest()) {
      return queries.getBlockHeaderByNumber(0);
    }
    return blockParameter.getNumber().flatMap(queries::getBlockHeaderByNumber);
  }
}
