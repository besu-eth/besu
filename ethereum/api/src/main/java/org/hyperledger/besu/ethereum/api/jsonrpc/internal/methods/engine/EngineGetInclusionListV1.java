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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListTransactionSelector;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineGetInclusionListV1 extends ExecutionEngineJsonRpcMethod {

  private static final Logger LOG = LoggerFactory.getLogger(EngineGetInclusionListV1.class);

  private final TransactionPool transactionPool;
  private final InclusionListTransactionSelector selector;
  private final Counter transactionsGeneratedCounter;
  private final Counter selectorDurationMsCounter;

  public EngineGetInclusionListV1(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem,
      final InclusionListTransactionSelector selector) {
    super(vertx, protocolContext, engineCallListener);
    this.transactionPool = transactionPool;
    this.selector = selector;
    this.transactionsGeneratedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "engine_inclusion_list_transactions_generated",
            "Total number of transactions generated for inclusion lists");
    this.selectorDurationMsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "engine_inclusion_list_selector_duration_ms",
            "Total time spent selecting inclusion list transactions in milliseconds");
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_INCLUSION_LIST_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext request) {
    engineCallListener.executionEngineCalled();

    final Hash parentHash;
    try {
      parentHash = request.getRequiredParameter(0, Hash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid parent hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }

    final Optional<BlockHeader> parentHeader =
        protocolContext.getBlockchain().getBlockHeader(parentHash);
    if (parentHeader.isEmpty()) {
      LOG.debug("Unknown parent block hash: {}", parentHash);
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.UNKNOWN_PARENT);
    }

    final long startTimeNanos = System.nanoTime();
    final List<PendingTransaction> selectedPendingTransactions =
        transactionPool.getInclusionListPendingTransactions(parentHeader.get());
    final long durationMs = Duration.ofNanos(System.nanoTime() - startTimeNanos).toMillis();

    final List<String> result =
        selectedPendingTransactions.stream()
            .map(PendingTransaction::getTransaction)
            .map(Transaction::encoded)
            .map(Bytes::toHexString)
            .toList();

    transactionsGeneratedCounter.inc(result.size());
    selectorDurationMsCounter.inc(durationMs);

    LOG.atInfo()
        .setMessage(
            "engine_getInclusionListV1: parentHash={}, selected {} transactions, selector took {}ms")
        .addArgument(parentHash)
        .addArgument(result.size())
        .addArgument(durationMs)
        .log();
    LOG.atDebug()
        .setMessage("IL generation details: mempool size={}, strategy={}")
        .addArgument(selectedPendingTransactions.size())
        .addArgument(selector.getClass().getSimpleName())
        .log();

    return new JsonRpcSuccessResponse(request.getRequest().getId(), result);
  }
}
