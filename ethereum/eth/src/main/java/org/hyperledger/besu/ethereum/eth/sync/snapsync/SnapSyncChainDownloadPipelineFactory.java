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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.DownloadSyncBodiesStep;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardBlockNumberSource;
import org.hyperledger.besu.ethereum.eth.sync.common.BlockHeaderSource;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.DownloadBackwardHeadersStep;
import org.hyperledger.besu.ethereum.eth.sync.common.DownloadSyncReceiptsStep;
import org.hyperledger.besu.ethereum.eth.sync.common.ImportHeadersStep;
import org.hyperledger.besu.ethereum.eth.sync.common.ImportSyncBlocksStep;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapSyncChainDownloadPipelineFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapSyncChainDownloadPipelineFactory.class);

  protected final SynchronizerConfiguration syncConfig;
  protected final ProtocolSchedule protocolSchedule;
  protected final ProtocolContext protocolContext;
  protected final EthContext ethContext;
  protected final PivotSyncState fastSyncState;
  protected final MetricsSystem metricsSystem;

  public SnapSyncChainDownloadPipelineFactory(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final PivotSyncState fastSyncState,
      final MetricsSystem metricsSystem) {
    this.syncConfig = syncConfig;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.fastSyncState = fastSyncState;
    this.metricsSystem = metricsSystem;
  }

  /**
   * Creates Pipeline 1: Backward header download from pivot block to checkpoint block. Downloads
   * headers in reverse direction, validates boundaries, and stores in database. Supports
   * out-of-order parallel execution with resume capability.
   *
   * @param chainState chain sync state containing pivot and progress
   * @return the backward header download pipeline
   */
  public Pipeline<Long> createBackwardHeaderDownloadPipeline(final ChainSyncState chainState) {
    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int headerDownloadParallelismFactor = syncConfig.getHeaderDownloadParallelismFactor();
    final int headerRequestSize = syncConfig.getDownloaderHeaderRequestSize();

    BlockHeader anchorForHeaderDownload =
        chainState.headerDownloadAnchor() == null
            ? chainState.blockDownloadAnchor()
            : chainState.headerDownloadAnchor();

    final long pivotBlockNumber = chainState.pivotBlockHeader().getNumber();
    LOG.info(
        "Creating backward header download pipeline from pivot={} down to lowest block={}, parallelism={}, batchSize={}, peers={}",
        pivotBlockNumber,
        anchorForHeaderDownload.getNumber(),
        downloaderParallelism,
        headerRequestSize,
        ethContext.getEthPeers().peerCount());

    final BackwardBlockNumberSource headerSource =
        new BackwardBlockNumberSource(
            headerRequestSize, anchorForHeaderDownload.getNumber() + 1L, pivotBlockNumber - 1L);

    final DownloadBackwardHeadersStep downloadStep =
        new DownloadBackwardHeadersStep(
            protocolSchedule,
            ethContext,
            headerRequestSize,
            anchorForHeaderDownload.getNumber(),
            Duration.ofMillis(syncConfig.getBackwardHeadersDownloadStepTimeoutMillis()));

    final ImportHeadersStep importHeadersStep =
        new ImportHeadersStep(
            protocolContext.getBlockchain(),
            anchorForHeaderDownload,
            chainState.pivotBlockHeader());

    return PipelineBuilder.createPipelineFrom(
            "backwardHeaderSource",
            headerSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "backward_header_download_pipeline_processed_total",
                "Number of entries processed by each backward header download pipeline stage",
                "step",
                "action"),
            true,
            "backwardHeaderSync")
        .thenProcessAsyncOrdered(
            "downloadBackwardHeaders",
            downloadStep,
            downloaderParallelism * headerDownloadParallelismFactor)
        .andFinishWith("importHeadersStep", importHeadersStep);
  }

  /**
   * Creates Pipeline 2 with custom start and end blocks: Forward bodies and receipts download from
   * start block to end block. Used for continuing sync to an updated pivot.
   *
   * @param anchorBlock the block to start from
   * @param pivotHeader the block to end at
   * @param syncState the sync state for reporting progress
   * @return the forward bodies and receipts download pipeline
   */
  public Pipeline<List<BlockHeader>> createForwardBodiesAndReceiptsDownloadPipeline(
      final long anchorBlock, final BlockHeader pivotHeader, final SyncState syncState) {

    long pivotHeaderNumber = pivotHeader.getNumber();

    final int downloaderParallelism = syncConfig.getDownloaderParallelism();
    final int bodiesRequestSize = syncConfig.getDownloaderBodiesRequestSize();

    final MutableBlockchain blockchain = protocolContext.getBlockchain();

    LOG.trace(
        "Creating forward bodies and receipts download pipeline: anchorBlock={}, pivotHeaderNumber={}, parallelism={}, batchSize={}",
        anchorBlock,
        pivotHeaderNumber,
        downloaderParallelism,
        bodiesRequestSize);

    final BlockHeaderSource headerSource =
        new BlockHeaderSource(blockchain, anchorBlock, pivotHeaderNumber, bodiesRequestSize);

    final DownloadSyncBodiesStep downloadBodiesStep =
        new DownloadSyncBodiesStep(
            protocolSchedule,
            ethContext,
            Duration.ofMillis(syncConfig.getBodiesDownloadStepTimeoutMillis()));

    final DownloadSyncReceiptsStep downloadReceiptsStep =
        new DownloadSyncReceiptsStep(
            protocolSchedule,
            ethContext,
            new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder()),
            Duration.ofMillis(syncConfig.getReceiptsDownloadStepTimeoutMillis()));

    final ImportSyncBlocksStep importBlocksStep =
        new ImportSyncBlocksStep(
            protocolContext,
            ethContext,
            syncState,
            anchorBlock,
            pivotHeader.getNumber(),
            syncConfig.getSnapSyncConfiguration().isSnapSyncTransactionIndexingEnabled());

    return PipelineBuilder.createPipelineFrom(
            "forwardHeaderSource",
            headerSource,
            downloaderParallelism,
            metricsSystem.createLabelledCounter(
                BesuMetricCategory.SYNCHRONIZER,
                "forward_bodies_receipts_pipeline_processed_total",
                "Number of entries processed by each forward bodies/receipts pipeline stage",
                "step",
                "action"),
            true,
            "forwardBodiesReceipts")
        .thenProcessAsyncOrdered("downloadBodies", downloadBodiesStep, downloaderParallelism)
        .thenProcessAsyncOrdered("downloadReceipts", downloadReceiptsStep, downloaderParallelism)
        .andFinishWith("importBlocks", importBlocksStep);
  }
}
