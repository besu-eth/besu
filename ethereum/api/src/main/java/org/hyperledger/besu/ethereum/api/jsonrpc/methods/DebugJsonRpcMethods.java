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
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.DebugReplayBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugAccountAt;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugAccountRange;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugBatchSendRawTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugGetBadBlocks;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugGetRawBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugGetRawHeader;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugGetRawReceipts;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugGetRawTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugMetrics;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugResyncWorldstate;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugSetHead;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugStandardTraceBadBlockToFile;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugStandardTraceBlockToFile;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugStorageRangeAt;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceBlockByHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceBlockByNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceCall;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.DebugTraceTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

import java.nio.file.Path;
import java.util.Map;

public class DebugJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final BlockResultFactory blockResult = new BlockResultFactory();

  private final BlockchainQueries blockchainQueries;
  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final ObservableMetricsSystem metricsSystem;
  private final TransactionPool transactionPool;
  private final Synchronizer synchronizer;
  private final Path dataDir;
  private final TransactionSimulator transactionSimulator;

  DebugJsonRpcMethods(
      final BlockchainQueries blockchainQueries,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final ObservableMetricsSystem metricsSystem,
      final TransactionPool transactionPool,
      final Synchronizer synchronizer,
      final Path dataDir,
      final TransactionSimulator transactionSimulator) {
    this.blockchainQueries = blockchainQueries;
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.metricsSystem = metricsSystem;
    this.transactionPool = transactionPool;
    this.synchronizer = synchronizer;
    this.dataDir = dataDir;
    this.transactionSimulator = transactionSimulator;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.DEBUG.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    final BlockReplay blockReplay =
        new BlockReplay(protocolSchedule, protocolContext, blockchainQueries.getBlockchain());

    return mapOf(
        new DebugTraceTransaction(
            blockchainQueries, new TransactionTracer(blockReplay), protocolSchedule),
        new DebugAccountRange(blockchainQueries),
        new DebugStorageRangeAt(blockchainQueries, blockReplay),
        new DebugMetrics(metricsSystem),
        new DebugResyncWorldstate(protocolContext, synchronizer),
        new DebugTraceBlock(protocolSchedule, blockchainQueries),
        new DebugSetHead(blockchainQueries, protocolContext),
        new DebugReplayBlock(blockchainQueries, protocolContext, protocolSchedule),
        new DebugTraceBlockByNumber(protocolSchedule, blockchainQueries),
        new DebugTraceBlockByHash(protocolSchedule, blockchainQueries),
        new DebugBatchSendRawTransaction(transactionPool),
        new DebugGetBadBlocks(protocolContext, blockResult),
        new DebugStandardTraceBlockToFile(
            () -> new TransactionTracer(blockReplay), blockchainQueries, dataDir),
        new DebugStandardTraceBadBlockToFile(
            () -> new TransactionTracer(blockReplay), blockchainQueries, protocolContext, dataDir),
        new DebugAccountAt(blockchainQueries, () -> new BlockTracer(blockReplay)),
        new DebugGetRawHeader(blockchainQueries),
        new DebugGetRawBlock(blockchainQueries),
        new DebugGetRawReceipts(blockchainQueries),
        new DebugGetRawTransaction(blockchainQueries),
        new DebugTraceCall(blockchainQueries, protocolSchedule, transactionSimulator));
  }
}
