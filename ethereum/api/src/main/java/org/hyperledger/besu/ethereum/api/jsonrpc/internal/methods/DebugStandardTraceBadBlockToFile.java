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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransactionTraceParams;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

public class DebugStandardTraceBadBlockToFile extends DebugStandardTraceBlockToFile
    implements JsonRpcMethod {

  private final ProtocolContext protocolContext;

  public DebugStandardTraceBadBlockToFile(
      final Supplier<TransactionTracer> transactionTracerSupplier,
      final BlockchainQueries blockchainQueries,
      final ProtocolContext protocolContext,
      final Path dataDir) {
    this(transactionTracerSupplier, blockchainQueries, protocolContext, dataDir, 0L);
  }

  public DebugStandardTraceBadBlockToFile(
      final Supplier<TransactionTracer> transactionTracerSupplier,
      final BlockchainQueries blockchainQueries,
      final ProtocolContext protocolContext,
      final Path dataDir,
      final long serverStepLimit) {
    super(transactionTracerSupplier, blockchainQueries, dataDir, serverStepLimit);
    this.protocolContext = protocolContext;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_STANDARD_TRACE_BAD_BLOCK_TO_FILE.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash blockHash;
    try {
      blockHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }
    final Optional<TransactionTraceParams> transactionTraceParams;
    try {
      transactionTraceParams = requestContext.getOptionalParameter(1, TransactionTraceParams.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction trace parameters (index 1)",
          RpcErrorType.INVALID_TRANSACTION_TRACE_PARAMS,
          e);
    }

    final BadBlockManager badBlockManager = protocolContext.getBadBlockManager();

    return badBlockManager
        .getBadBlock(blockHash)
        .map(
            block ->
                (JsonRpcResponse)
                    new JsonRpcSuccessResponse(
                        requestContext.getRequest().getId(),
                        traceBlock(block, transactionTraceParams)))
        .orElse(
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND));
  }
}
