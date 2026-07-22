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

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV1;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;

import java.util.List;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class EngineGetPayloadBodiesByRangeV1<EPB extends ExecutionPayloadBodiesV1>
    extends ExecutionEngineJsonRpcMethod permits EngineGetPayloadBodiesByRangeV2 {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetPayloadBodiesByRangeV1.class);
  private final int maxRequestBlocks;
  protected final Blockchain blockchain;

  public EngineGetPayloadBodiesByRangeV1(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.maxRequestBlocks = constructorArguments.maxRequestBlocks();
    this.blockchain = protocolContext.getBlockchain();
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V1.getMethodName();
  }

  @Override
  public final JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    engineCallListener.executionEngineCalled();
    final Object reqId = request.getRequest().getId();
    try {
      return new JsonRpcSuccessResponse(reqId, collectExecutionPayloadBodiesByRange(request));
    } catch (final InvalidJsonRpcParameters e) {
      return new JsonRpcErrorResponse(reqId, e.getRpcErrorType());
    }
  }

  private List<EPB> collectExecutionPayloadBodiesByRange(final JsonRpcRequestContext request) {
    final long startBlockNumber = getStartBlockNumber(request);
    final long count = getCount(request);

    LOG.atTrace()
        .setMessage("{} parameters: start block number {} count {}")
        .addArgument(this::getName)
        .addArgument(startBlockNumber)
        .addArgument(count)
        .log();

    if (startBlockNumber < 1) {
      throw new InvalidJsonRpcParameters(
          "start block number %d < 1".formatted(startBlockNumber),
          RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
    }

    if (count < 1) {
      throw new InvalidJsonRpcParameters(
          "count %d < 1".formatted(startBlockNumber), RpcErrorType.INVALID_BLOCK_COUNT_PARAMS);
    }

    if (count > maxRequestBlocks) {
      throw new InvalidJsonRpcParameters(
          "count %d > %d ".formatted(count, maxRequestBlocks),
          RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE);
    }

    final long chainHeadBlockNumber = blockchain.getChainHeadBlockNumber();

    if (chainHeadBlockNumber < startBlockNumber) {
      return List.of();
    }

    final long upperBound = startBlockNumber + count;
    final long endExclusiveBlockNumber =
        chainHeadBlockNumber < upperBound ? chainHeadBlockNumber + 1 : upperBound;

    return LongStream.range(startBlockNumber, endExclusiveBlockNumber)
        .parallel()
        .mapToObj(
            blockNumber ->
                blockchain
                    .getBlockByNumber(blockNumber)
                    .map(this::fetchExecutionPayloadBody)
                    .orElse(null))
        .toList();
  }

  @SuppressWarnings("unchecked")
  protected EPB fetchExecutionPayloadBody(final Block block) {
    final BlockBody body = block.getBody();
    return (EPB)
        new ExecutionPayloadBodiesV1(body.getTransactions(), body.getWithdrawals().orElse(null));
  }

  private long getStartBlockNumber(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid start block number parameter (index 0)",
          RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS,
          e);
    }
  }

  private long getCount(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(1, UnsignedLongParameter.class).getValue();
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block count params (index 1)", RpcErrorType.INVALID_BLOCK_COUNT_PARAMS, e);
    }
  }
}
