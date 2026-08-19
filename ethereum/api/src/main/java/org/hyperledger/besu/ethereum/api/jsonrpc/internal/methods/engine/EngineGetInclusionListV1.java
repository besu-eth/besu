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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.time.Duration;
import java.util.List;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineGetInclusionListV1 extends ExecutionEngineJsonRpcMethod {

  private static final Logger LOG = LoggerFactory.getLogger(EngineGetInclusionListV1.class);

  private final TransactionPool transactionPool;
  private final Counter transactionsGeneratedCounter;
  private final Counter selectorDurationMsCounter;

  public EngineGetInclusionListV1(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem) {
    super(vertx, protocolContext, engineCallListener);
    this.transactionPool = transactionPool;
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

    final long startTimeNanos = System.nanoTime();
    final List<PendingTransaction> selectedPendingTransactions =
        transactionPool.getInclusionListPendingTransactions();
    final long durationMs = Duration.ofNanos(System.nanoTime() - startTimeNanos).toMillis();

    final List<Transaction> result =
        selectedPendingTransactions.stream().map(PendingTransaction::getTransaction).toList();

    transactionsGeneratedCounter.inc(result.size());
    selectorDurationMsCounter.inc(durationMs);

    LOG.atInfo()
        .setMessage("engine_getInclusionListV1: selected {} transactions, selector took {}ms")
        .addArgument(result.size())
        .addArgument(durationMs)
        .log();

    return new JsonRpcSuccessResponse(request.getRequest().getId(), result);
  }
}
