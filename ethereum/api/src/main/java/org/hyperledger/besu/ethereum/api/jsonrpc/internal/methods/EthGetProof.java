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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.GetProofResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.units.bigints.UInt256;

public class EthGetProof extends AbstractBlockParameterOrBlockHashMethod {
  public EthGetProof(final BlockchainQueries blockchain) {
    super(blockchain);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_PROOF.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    return blockParameterOrBlockHashWithLatestDefault(request, 2);
  }

  @Override
  protected Object resultByBlockHash(
      final JsonRpcRequestContext requestContext, final Hash blockHash) {

    final Address address;
    try {
      address = requestContext.getRequiredParameter(0, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 0)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }
    final List<UInt256> storageKeys = getStorageKeys(requestContext);

    final Blockchain blockchain = getBlockchainQueries().getBlockchain();
    final WorldStateArchive worldStateArchive = getBlockchainQueries().getWorldStateArchive();
    return blockchain
        .getBlockHeader(blockHash)
        .flatMap(
            blockHeader -> {
              return worldStateArchive.getAccountProof(
                  blockHeader,
                  address,
                  storageKeys,
                  maybeWorldStateProof ->
                      maybeWorldStateProof
                          .map(
                              proof ->
                                  (JsonRpcResponse)
                                      new JsonRpcSuccessResponse(
                                          requestContext.getRequest().getId(),
                                          GetProofResult.build(address, proof)))
                          .or(
                              () ->
                                  Optional.of(
                                      new JsonRpcErrorResponse(
                                          requestContext.getRequest().getId(),
                                          RpcErrorType.INTERNAL_ERROR))));
            })
        .orElse(
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE));
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return (JsonRpcResponse) handleParamTypes(requestContext);
  }

  private List<UInt256> getStorageKeys(final JsonRpcRequestContext request) {
    try {
      return Arrays.stream(request.getRequiredParameter(1, String[].class))
          .map(UInt256::fromHexString)
          .collect(Collectors.toList());
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid storage keys parameters (index 1)", RpcErrorType.INVALID_STORAGE_KEYS_PARAMS, e);
    }
  }
}
