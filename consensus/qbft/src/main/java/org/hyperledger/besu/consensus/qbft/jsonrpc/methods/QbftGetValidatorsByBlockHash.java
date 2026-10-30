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
package org.hyperledger.besu.consensus.qbft.jsonrpc.methods;

import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft get validators by block hash. */
public class QbftGetValidatorsByBlockHash implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(QbftGetValidatorsByBlockHash.class);

  private final Blockchain blockchain;
  private final ValidatorProvider validatorProvider;

  /**
   * Instantiates a new Qbft get validators by block hash.
   *
   * @param blockchain the blockchain
   * @param validatorProvider the validator provider
   */
  public QbftGetValidatorsByBlockHash(
      final Blockchain blockchain, final ValidatorProvider validatorProvider) {
    this.blockchain = blockchain;
    this.validatorProvider = validatorProvider;
  }

  @Override
  public String getName() {
    return RpcMethod.QBFT_GET_VALIDATORS_BY_BLOCK_HASH.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), blockResult(requestContext));
  }

  private @Nullable Object blockResult(final JsonRpcRequestContext request) {
    final Hash hash;
    try {
      hash = request.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }
    LOG.trace("Received RPC rpcName={} blockHash={}", getName(), hash);
    final Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(hash);
    return blockHeader
        .map(
            header ->
                validatorProvider.getValidatorsForBlock(header).stream()
                    .map(Address::toString)
                    .collect(Collectors.toList()))
        .orElse(null);
  }
}
