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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.FilterParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.TransactionTrace;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.tracing.flat.RewardTraceGenerator;
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
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceBlock extends AbstractBlockParameterMethod {
  private static final Logger LOG = LoggerFactory.getLogger(TraceBlock.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  protected final ProtocolSchedule protocolSchedule;
  private final LabelledMetric<Counter> outputCounter;
  protected final EthScheduler ethScheduler;

  public TraceBlock(
      final ProtocolSchedule protocolSchedule,
      final BlockchainQueries queries,
      final MetricsSystem metricsSystem,
      final EthScheduler ethScheduler) {
    super(queries);
    this.protocolSchedule = protocolSchedule;
    this.outputCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "transactions_traceblock_pipeline_processed_total",
            "Number of transactions processed for each block",
            "step",
            "action");
    this.ethScheduler = ethScheduler;
  }

  @Override
  public String getName() {
    return RpcMethod.TRACE_BLOCK.getMethodName();
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
    if (blockNumber == BlockHeader.GENESIS_BLOCK_NUMBER) {
      // Nothing to trace for the genesis block
      return emptyResult().getArrayNode();
    }
    LOG.trace("Received RPC rpcName={} block={}", getName(), blockNumber);

    return getBlockchainQueries()
        .getBlockchain()
        .getBlockByNumber(blockNumber)
        .map(block -> traceBlock(block, Optional.empty()))
        .map(ArrayNodeWrapper::getArrayNode)
        .orElse(null);
  }

  protected ArrayNodeWrapper traceBlock(
      final Block block, final Optional<FilterParameter> filterParameter) {

    if (block == null) {
      return emptyResult();
    }
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

              TransactionSource transactionSource = new TransactionSource(block);
              DebugOperationTracer debugOperationTracer =
                  new DebugOperationTracer(
                      OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                          .traceStorage(false)
                          .traceMemory(false)
                          .traceStack(true)
                          .build(),
                      false);
              ExecuteTransactionStep executeTransactionStep =
                  new ExecuteTransactionStep(
                      chainUpdater,
                      transactionProcessor,
                      getBlockchainQueries().getBlockchain(),
                      debugOperationTracer,
                      protocolSpec,
                      block);
              TraceFlatTransactionStep traceFlatTransactionStep =
                  new TraceFlatTransactionStep(protocolSchedule, block, filterParameter);
              BuildArrayNodeCompleterStep buildArrayNodeStep =
                  new BuildArrayNodeCompleterStep(resultArrayNode);
              Pipeline<TransactionTrace> traceBlockPipeline =
                  createPipelineFrom(
                          "getTransactions",
                          transactionSource,
                          4,
                          outputCounter,
                          false,
                          "trace_block_transactions")
                      .thenProcess("executeTransaction", executeTransactionStep)
                      .thenProcessAsyncOrdered("traceFlatTransaction", traceFlatTransactionStep, 4)
                      .andFinishWith(
                          "buildArrayNode",
                          traceStream -> traceStream.forEachOrdered(buildArrayNodeStep));

              try {
                ethScheduler.startPipeline(traceBlockPipeline).get();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } catch (ExecutionException e) {
                throw new RuntimeException(e);
              }

              resultArrayNode = buildArrayNodeStep.getResultArrayNode();
              generateRewardsFromBlock(filterParameter, block, resultArrayNode);
              return Optional.of(resultArrayNode);
            })
        .orElse(emptyResult());
  }

  protected void generateRewardsFromBlock(
      final Optional<FilterParameter> maybeFilterParameter,
      final Block block,
      final ArrayNodeWrapper resultArrayNode) {
    RewardTraceGenerator.generateFromBlock(protocolSchedule, block)
        .forEachOrdered(resultArrayNode::addPOJO);
  }

  ArrayNodeWrapper emptyResult() {
    return new ArrayNodeWrapper(MAPPER.createArrayNode());
  }

  public static class ChainUpdater {

    private final MutableWorldState worldState;
    private WorldUpdater updater;

    public ChainUpdater(final MutableWorldState worldState) {
      this.worldState = worldState;
    }

    public ChainUpdater(final MutableWorldState worldState, final WorldUpdater updater) {
      this.worldState = worldState;
      this.updater = updater;
    }

    public WorldUpdater getNextUpdater() {
      // if we have no prior updater, it must be the first TX, so use the block's initial state
      if (updater == null) {
        updater = worldState.updater();
      } else {
        updater.markTransactionBoundary();
      }
      updater = updater.updater();
      return updater;
    }
  }
}
