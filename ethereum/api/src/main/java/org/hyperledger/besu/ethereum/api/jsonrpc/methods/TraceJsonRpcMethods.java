/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceCall;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceCallMany;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceFilter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceGet;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceRawTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceReplayBlockTransactions;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Map;

public class TraceJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final BlockchainQueries blockchainQueries;
  private final ProtocolSchedule protocolSchedule;
  private final ApiConfiguration apiConfiguration;
  private final ProtocolContext protocolContext;
  private final TransactionSimulator transactionSimulator;
  private final MetricsSystem metricsSystem;
  private final EthScheduler ethScheduler;

  TraceJsonRpcMethods(
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final ApiConfiguration apiConfiguration,
      final TransactionSimulator transactionSimulator,
      final MetricsSystem metricsSystem,
      final EthScheduler ethScheduler) {
    this.blockchainQueries = blockchainQueries;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.apiConfiguration = apiConfiguration;
    this.transactionSimulator = transactionSimulator;
    this.metricsSystem = metricsSystem;
    this.ethScheduler = ethScheduler;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.TRACE.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    final BlockReplay blockReplay =
        new BlockReplay(protocolSchedule, protocolContext, blockchainQueries.getBlockchain());
    return mapOf(
        new TraceReplayBlockTransactions(
            protocolSchedule, blockchainQueries, metricsSystem, ethScheduler),
        new TraceFilter(
            protocolSchedule,
            blockchainQueries,
            apiConfiguration.getMaxTraceFilterRange(),
            metricsSystem,
            ethScheduler),
        new TraceGet(() -> new BlockTracer(blockReplay), blockchainQueries, protocolSchedule),
        new TraceTransaction(
            () -> new BlockTracer(blockReplay), protocolSchedule, blockchainQueries),
        new TraceBlock(protocolSchedule, blockchainQueries, metricsSystem, ethScheduler),
        new TraceCall(blockchainQueries, protocolSchedule, transactionSimulator, apiConfiguration),
        new TraceCallMany(blockchainQueries, protocolSchedule, transactionSimulator),
        new TraceRawTransaction(protocolSchedule, blockchainQueries, transactionSimulator));
  }
}
