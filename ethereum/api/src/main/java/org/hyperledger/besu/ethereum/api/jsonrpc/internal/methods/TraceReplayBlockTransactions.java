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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.services.pipeline.PipelineBuilder.createPipelineFrom;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TraceTypeParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TraceReplayResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.util.ArrayNodeWrapper;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.vm.DebugOperationTracer;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceReplayBlockTransactions extends AbstractBlockParameterMethod {
  private static final Logger LOG = LoggerFactory.getLogger(TraceReplayBlockTransactions.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final ProtocolSchedule protocolSchedule;
  private final LabelledMetric<Counter> outputCounter;
  private final EthScheduler ethScheduler;

  public TraceReplayBlockTransactions(
      final ProtocolSchedule protocolSchedule,
      final BlockchainQueries queries,
      final MetricsSystem metricsSystem,
      final EthScheduler ethScheduler) {
    super(queries);
    this.protocolSchedule = protocolSchedule;
    this.outputCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "transactions_tracereplayblock_pipeline_processed_total",
            "Number of transactions processed for each block",
            "step",
            "action");
    this.ethScheduler = ethScheduler;
  }

  @Override
  public String getName() {
    return RpcMethod.TRACE_REPLAY_BLOCK_TRANSACTIONS.getMethodName();
  }

  @Override
  protected BlockParameter blockParameter(final JsonRpcRequestContext request) {
    try {
      return request.getRequiredParameter(0, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockNumber(
      final JsonRpcRequestContext request, final long blockNumber) {
    final TraceTypeParameter traceTypeParameter;
    try {
      traceTypeParameter = request.getRequiredParameter(1, TraceTypeParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid trace type parameter (index 1)", RpcErrorType.INVALID_TRACE_TYPE_PARAMS, e);
    }

    LOG.trace(
        "Received RPC rpcName={} block={} traceType={}",
        getName(),
        blockNumber,
        traceTypeParameter);

    if (blockNumber == BlockHeader.GENESIS_BLOCK_NUMBER) {
      // Nothing to trace for the genesis block
      return emptyResult();
    }

    final Optional<Block> maybeBlock =
        getBlockchainQueries().getBlockchain().getBlockByNumber(blockNumber);
    if (maybeBlock.isEmpty()) {
      return new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
    }
    return traceBlock(maybeBlock.get(), traceTypeParameter);
  }

  private ArrayNode traceBlock(final Block block, final TraceTypeParameter traceTypeParameter) {

    if (block == null) {
      return emptyResult();
    }

    final Set<TraceTypeParameter.TraceType> traceTypes = traceTypeParameter.getTraceTypes();

    final BlockHeader header = block.getHeader();
    return Tracer.processTracing(
            getBlockchainQueries(),
            Optional.of(header),
            traceableState -> {
              ArrayNodeWrapper resultArrayNode = new ArrayNodeWrapper(MAPPER.createArrayNode());

              final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(header);
              final MainnetTransactionProcessor transactionProcessor =
                  protocolSpec.getTransactionProcessor();
              final ChainUpdater chainUpdater = new ChainUpdater(traceableState);

              final TransactionSource transactionSource = new TransactionSource(block);
              final DebugOperationTracer debugOperationTracer =
                  new DebugOperationTracer(
                      OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                          .traceStorage(false)
                          .traceMemory(false)
                          .traceStack(true)
                          .build(),
                      false);
              final ExecuteTransactionStep executeTransactionStep =
                  new ExecuteTransactionStep(
                      chainUpdater,
                      transactionProcessor,
                      getBlockchainQueries().getBlockchain(),
                      debugOperationTracer,
                      protocolSpec,
                      block);

              final Function<TransactionTrace, CompletableFuture<TraceReplayResult>>
                  traceReplayTransactionStep =
                      new TraceReplayTransactionStep(protocolSchedule, block, traceTypes);

              final BuildArrayNodeCompleterStep buildArrayNodeStep =
                  new BuildArrayNodeCompleterStep(resultArrayNode);

              final Pipeline<TransactionTrace> traceBlockPipeline =
                  createPipelineFrom(
                          "getTransactions",
                          transactionSource,
                          4,
                          outputCounter,
                          false,
                          "trace_replay_block_transactions")
                      .thenProcess("executeTransaction", executeTransactionStep)
                      .thenProcessAsyncOrdered(
                          "traceReplayTransaction", traceReplayTransactionStep, 4)
                      .andFinishWith("buildArrayNode", buildArrayNodeStep::accept);
              try {
                ethScheduler.startPipeline(traceBlockPipeline).get();
              } catch (final InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
              resultArrayNode = buildArrayNodeStep.getResultArrayNode();
              return Optional.of(resultArrayNode.getArrayNode());
            })
        .orElse(emptyResult());
  }

  private ArrayNode emptyResult() {
    return MAPPER.createArrayNode();
  }
}
