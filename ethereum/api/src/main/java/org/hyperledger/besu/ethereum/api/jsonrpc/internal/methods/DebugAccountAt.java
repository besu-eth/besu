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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ImmutableDebugAccountAtResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

public class DebugAccountAt extends AbstractBlockParameterOrBlockHashMethod {
  private final Supplier<BlockTracer> blockTracerSupplier;

  public DebugAccountAt(
      final BlockchainQueries blockchainQueries, final Supplier<BlockTracer> blockTracerSupplier) {
    super(blockchainQueries);
    this.blockTracerSupplier = blockTracerSupplier;
  }

  public DebugAccountAt(
      final Supplier<BlockchainQueries> blockchainQueries,
      final Supplier<BlockTracer> blockTracerSupplier) {
    super(blockchainQueries);
    this.blockTracerSupplier = blockTracerSupplier;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_ACCOUNT_AT.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext requestContext) {
    try {
      return requestContext.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockHash(
      final JsonRpcRequestContext requestContext, final Hash blockHash) {
    final Integer txIndex;
    try {
      txIndex = requestContext.getRequiredParameter(1, Integer.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction index parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_INDEX_PARAMS,
          e);
    }
    final Address address;
    try {
      address = requestContext.getRequiredParameter(2, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 2)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }

    Optional<BlockWithMetadata<TransactionWithMetadata, Hash>> block =
        blockchainQueries.get().blockByHash(blockHash);
    if (block.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
    }

    List<TransactionWithMetadata> transactions = block.get().getTransactions();
    if (transactions.isEmpty() || txIndex < 0 || txIndex >= transactions.size()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_TRANSACTION_PARAMS);
    }

    return Tracer.processTracing(
            blockchainQueries.get(),
            Optional.of(block.get().getHeader()),
            mutableWorldState -> {
              final Optional<TransactionTrace> transactionTrace =
                  blockTracerSupplier
                      .get()
                      .trace(
                          mutableWorldState,
                          blockHash,
                          new DebugOperationTracer(
                              OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                                  .traceStorage(false)
                                  .traceMemory(true)
                                  .traceStack(true)
                                  .build(),
                              false))
                      .map(BlockTrace::getTransactionTraces)
                      .orElse(Collections.emptyList())
                      .stream()
                      .filter(
                          trxTrace ->
                              trxTrace
                                  .getTransaction()
                                  .getHash()
                                  .equals(transactions.get(txIndex).getTransaction().getHash()))
                      .findFirst();

              if (transactionTrace.isEmpty()) {
                return Optional.of(
                    new JsonRpcErrorResponse(
                        requestContext.getRequest().getId(), RpcErrorType.TRANSACTION_NOT_FOUND));
              }

              Optional<Account> account =
                  transactionTrace.get().getTraceFrames().stream()
                      .map(traceFrame -> traceFrame.getWorldUpdater().get(address))
                      .filter(Objects::nonNull)
                      .filter(a -> a.getAddress().equals(address))
                      .findFirst();
              if (account.isEmpty()) {
                return Optional.of(
                    new JsonRpcErrorResponse(
                        requestContext.getRequest().getId(), RpcErrorType.NO_ACCOUNT_FOUND));
              }

              return Optional.of(
                  debugAccountAtResult(
                      account.get().getCode(),
                      Quantity.create(account.get().getNonce()),
                      Quantity.create(account.get().getBalance()),
                      Quantity.create(account.get().getCodeHash().getBytes())));
            })
        .orElse(
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE));
  }

  protected ImmutableDebugAccountAtResult debugAccountAtResult(
      final Bytes code, final String nonce, final String balance, final String codeHash) {
    return ImmutableDebugAccountAtResult.builder()
        .code(code.isEmpty() ? "0x0" : code.toHexString())
        .nonce(nonce)
        .balance(balance)
        .codehash(codeHash)
        .build();
  }
}
