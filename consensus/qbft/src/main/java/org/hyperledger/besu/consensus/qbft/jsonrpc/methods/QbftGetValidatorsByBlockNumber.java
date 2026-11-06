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
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.AbstractBlockParameterMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft get validators by block number. */
public class QbftGetValidatorsByBlockNumber extends AbstractBlockParameterMethod
    implements JsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(QbftGetValidatorsByBlockNumber.class);
  private final ValidatorProvider validatorProvider;

  /**
   * Instantiates a new Qbft get validators by block number.
   *
   * @param blockchainQueries the blockchain queries
   * @param validatorProvider the validator provider
   */
  public QbftGetValidatorsByBlockNumber(
      final BlockchainQueries blockchainQueries, final ValidatorProvider validatorProvider) {
    super(blockchainQueries);
    this.validatorProvider = validatorProvider;
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
  protected Object pendingResult(final JsonRpcRequestContext request) {
    final BlockHeader blockHeader = getBlockchainQueries().headBlockHeader();
    LOG.trace("Received RPC rpcName={} block={}", getName(), blockHeader.getNumber());
    return validatorProvider.getValidatorsAfterBlock(blockHeader).stream()
        .map(Address::toString)
        .collect(Collectors.toList());
  }

  @Override
  protected @Nullable Object resultByBlockNumber(
      final JsonRpcRequestContext request, final long blockNumber) {
    final Optional<BlockHeader> blockHeader =
        getBlockchainQueries().getBlockHeaderByNumber(blockNumber);
    LOG.trace("Received RPC rpcName={} block={}", getName(), blockNumber);
    return blockHeader
        .map(
            header ->
                validatorProvider.getValidatorsForBlock(header).stream()
                    .map(Address::toString)
                    .collect(Collectors.toList()))
        .orElse(null);
  }

  @Override
  public String getName() {
    return RpcMethod.QBFT_GET_VALIDATORS_BY_BLOCK_NUMBER.getMethodName();
  }
}
