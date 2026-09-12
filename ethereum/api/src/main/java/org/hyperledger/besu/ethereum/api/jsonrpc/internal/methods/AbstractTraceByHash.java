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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockTracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.FlatTraceGenerator;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public abstract class AbstractTraceByHash implements JsonRpcMethod {
  protected final Supplier<BlockTracer> blockTracerSupplier;
  protected final BlockchainQueries blockchainQueries;
  protected final ProtocolSchedule protocolSchedule;

  protected AbstractTraceByHash(
      final Supplier<BlockTracer> blockTracerSupplier,
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule) {
    this.blockTracerSupplier = blockTracerSupplier;
    this.blockchainQueries = blockchainQueries;
    this.protocolSchedule = protocolSchedule;
  }

  public Stream<FlatTrace> resultByTransactionHash(final Hash transactionHash) {
    return blockchainQueries
        .transactionByHash(transactionHash)
        .flatMap(TransactionWithMetadata::getBlockNumber)
        .flatMap(blockNumber -> blockchainQueries.getBlockchain().getBlockByNumber(blockNumber))
        .map(block -> getTraceBlock(block, transactionHash))
        .orElse(Stream.empty());
  }

  private Stream<FlatTrace> getTraceBlock(final Block block, final Hash transactionHash) {
    if (block == null || block.getBody().getTransactions().isEmpty()) {
      return Stream.empty();
    }
    return Tracer.processTracing(
            blockchainQueries,
            Optional.of(block.getHeader()),
            traceableState -> {
              final TransactionTrace transactionTrace =
                  getTransactionTrace(traceableState, block, transactionHash);
              return Optional.ofNullable(getTraceStream(transactionTrace, block));
            })
        .orElse(Stream.empty());
  }

  private TransactionTrace getTransactionTrace(
      final Tracer.TraceableState traceableState, final Block block, final Hash transactionHash) {
    return blockTracerSupplier
        .get()
        .trace(
            traceableState,
            block,
            new DebugOperationTracer(
                OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                    .traceStorage(false)
                    .traceMemory(false)
                    .traceStack(true)
                    .build(),
                false))
        .map(BlockTrace::getTransactionTraces)
        .orElse(Collections.emptyList())
        .stream()
        .filter(trxTrace -> trxTrace.getTransaction().getHash().equals(transactionHash))
        .findFirst()
        .orElseThrow();
  }

  private Stream<FlatTrace> getTraceStream(
      final TransactionTrace transactionTrace, final Block block) {
    return FlatTraceGenerator.generateFromTransactionTraceAndBlock(
            this.protocolSchedule, transactionTrace, block)
        .map(FlatTrace.class::cast);
  }

  protected JsonNode arrayNodeFromTraceStream(final Stream<FlatTrace> traceStream) {
    final ObjectMapper mapper = new ObjectMapper();
    final ArrayNode resultArrayNode = mapper.createArrayNode();
    traceStream.forEachOrdered(resultArrayNode::addPOJO);
    return resultArrayNode;
  }
}
