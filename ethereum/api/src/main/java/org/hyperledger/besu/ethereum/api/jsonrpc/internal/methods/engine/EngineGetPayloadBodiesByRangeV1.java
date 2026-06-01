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

import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadBodiesResultV1;
import org.hyperledger.besu.ethereum.chain.Blockchain;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineGetPayloadBodiesByRangeV1 extends AbstractEngineGetPayloadBodies {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetPayloadBodiesByRangeV1.class);

  public EngineGetPayloadBodiesByRangeV1(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final BlockResultFactory blockResultFactory,
      final EngineCallListener engineCallListener) {
    super(vertx, protocolContext, blockResultFactory, engineCallListener);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    engineCallListener.executionEngineCalled();

    final long startBlockNumber;
    try {
      startBlockNumber = request.getRequiredParameter(0, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid start block number parameter (index 0)",
          RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS,
          e);
    }
    final long count;
    try {
      count = request.getRequiredParameter(1, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block count params (index 1)", RpcErrorType.INVALID_BLOCK_COUNT_PARAMS, e);
    }
    final Object reqId = request.getRequest().getId();

    LOG.atTrace()
        .setMessage("{} parameters: start block number {} count {}")
        .addArgument(this::getName)
        .addArgument(startBlockNumber)
        .addArgument(count)
        .log();

    if (startBlockNumber < 1 || count < 1) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
    }

    if (count > getMaxRequestBlocks()) {
      return new JsonRpcErrorResponse(reqId, RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE);
    }

    final Blockchain blockchain = protocolContext.getBlockchain();
    final long chainHeadBlockNumber = blockchain.getChainHeadBlockNumber();

    // request startBlockNumber is beyond head of chain
    if (chainHeadBlockNumber < startBlockNumber) {
      // Empty List of payloadBodies
      return new JsonRpcSuccessResponse(reqId, new EngineGetPayloadBodiesResultV1());
    }

    final long upperBound = startBlockNumber + count;

    // if we've received request from blocks beyond the head we exclude those from the query
    final long endExclusiveBlockNumber =
        chainHeadBlockNumber < upperBound ? chainHeadBlockNumber + 1 : upperBound;

    EngineGetPayloadBodiesResultV1 engineGetPayloadBodiesResultV1 =
        blockResultFactory.payloadBodiesCompleteV1(
            LongStream.range(startBlockNumber, endExclusiveBlockNumber)
                .parallel()
                .mapToObj(
                    blockNumber ->
                        blockchain
                            .getBlockHashByNumber(blockNumber)
                            .flatMap(blockchain::getBlockBody))
                .collect(Collectors.toList()));

    return new JsonRpcSuccessResponse(reqId, engineGetPayloadBodiesResultV1);
  }
}
