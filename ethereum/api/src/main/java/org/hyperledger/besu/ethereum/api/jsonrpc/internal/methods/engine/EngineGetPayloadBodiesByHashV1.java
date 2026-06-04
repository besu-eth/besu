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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
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
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public sealed class EngineGetPayloadBodiesByHashV1<EPB extends ExecutionPayloadBodiesV1>
    extends ExecutionEngineJsonRpcMethod permits EngineGetPayloadBodiesByHashV2 {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetPayloadBodiesByHashV1.class);
  private final int maxRequestBlocks;
  protected final Blockchain blockchain;

  public EngineGetPayloadBodiesByHashV1(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final int maxRequestBlocks) {
    super(protocolSchedule, protocolContext, vertx, engineCallListener);
    this.maxRequestBlocks = maxRequestBlocks;
    this.blockchain = protocolContext.getBlockchain();
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_HASH_V1.getMethodName();
  }

  @Override
  public final JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    engineCallListener.executionEngineCalled();
    final Object reqId = request.getRequest().getId();

    try {
      return new JsonRpcSuccessResponse(reqId, collectExecutionPayloadBodiesByHash(request));
    } catch (final InvalidJsonRpcParameters e) {
      return new JsonRpcErrorResponse(reqId, e.getRpcErrorType());
    }
  }

  private List<EPB> collectExecutionPayloadBodiesByHash(final JsonRpcRequestContext request) {
    final Hash[] blockHashes = getBlockHashes(request);

    LOG.atTrace()
        .setMessage("{} parameters: blockHashes {}")
        .addArgument(this::getName)
        .addArgument(blockHashes)
        .log();

    if (blockHashes.length > maxRequestBlocks) {
      throw new InvalidJsonRpcParameters(
          "Requested %d while max is %d".formatted(blockHashes.length, maxRequestBlocks),
          RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE);
    }

    return Arrays.stream(blockHashes).parallel().map(this::fetchExecutionPayloadBody).toList();
  }

  private Hash[] getBlockHashes(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, Hash[].class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameters (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }
  }

  @SuppressWarnings("unchecked")
  protected EPB fetchExecutionPayloadBody(final Hash blockHash) {
    final Optional<BlockBody> maybeBody = blockchain.getBlockBody(blockHash);
    return maybeBody
        .map(
            blockBody ->
                (EPB)
                    new ExecutionPayloadBodiesV1(
                        blockBody.getTransactions(), blockBody.getWithdrawals().orElse(null)))
        .orElse(null);
  }
}
