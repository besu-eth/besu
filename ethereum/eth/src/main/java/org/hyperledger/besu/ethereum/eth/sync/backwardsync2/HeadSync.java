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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BadChainListener;
import org.hyperledger.besu.ethereum.eth.sync.common.DownloadBackwardHeadersStep;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.util.Subscribers;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Syncs the chain to one head, on two pipelines: the headers from the head down to the chain, then
 * the blocks above the chain, the next batches downloading while one imports. Finalized blocks are
 * imported from their block access list instead of being executed.
 */
class HeadSync {

  private static final Logger LOG = LoggerFactory.getLogger(HeadSync.class);

  // enough batches downloaded ahead of the import to hide the download latency, few enough to
  // bound the memory their bodies and block access lists take
  static final int IMPORT_BATCH_SIZE = 64;
  static final int DOWNLOADS_AHEAD = 4;
  // the blocks of one request, so that the scheduler spreads a batch over several peers
  static final int REQUEST_SIZE = 16;
  // the import starts once the blocks of one request arrived
  static final int FIRST_BATCH_SIZE = REQUEST_SIZE;

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final SynchronizerConfiguration syncConfig;
  private final EthContext ethContext;
  private final HeaderStore headerStore;
  private final BalImporter balImporter;
  private final Supplier<Hash> finalizedBlockHash;
  private final Subscribers<BadChainListener> badChainListeners;
  private final LabelledMetric<Counter> headersPipelineCounter;
  private final LabelledMetric<Counter> importPipelineCounter;

  /**
   * @param finalizedBlockHash the last finalized block the consensus client sent
   * @param badChainListeners told of the chain above an invalid block
   */
  HeadSync(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration syncConfig,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final HeaderStore headerStore,
      final Supplier<Hash> finalizedBlockHash,
      final Subscribers<BadChainListener> badChainListeners) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.syncConfig = syncConfig;
    this.ethContext = ethContext;
    this.headerStore = headerStore;
    this.balImporter = new BalImporter(protocolContext, protocolSchedule);
    this.finalizedBlockHash = finalizedBlockHash;
    this.badChainListeners = badChainListeners;
    this.headersPipelineCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "backward_sync_headers_pipeline_processed_total",
            "Number of entries processed by each backward sync headers pipeline stage",
            "step",
            "action");
    this.importPipelineCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "backward_sync_import_pipeline_processed_total",
            "Number of entries processed by each backward sync import pipeline stage",
            "step",
            "action");
  }

  /** Imports {@code head} and the blocks between it and the chain, or throws if it cannot. */
  void syncTo(final Head head) {
    final BlockHeader target =
        head.block().map(Block::getHeader).orElseGet(() -> downloadHeader(head.hash()));
    LOG.info("Backward sync to {}", target.toLogString());
    headerStore.clear();
    try {
      final StoreHeadersStep storeHeaders =
          new StoreHeadersStep(headerStore, target, finalizedBlockHash);
      final BlockHeader anchor = downloadHeaders(target, storeHeaders);
      importBlocks(head, target, anchor, storeHeaders.finalizedNumber());
    } finally {
      headerStore.clear();
    }
  }

  /**
   * The finalized block of the consensus client if the chain holds it, else the finalized block of
   * the chain: no fork reaches below it.
   */
  private BlockHeader finalizedBlock() {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    return blockchain
        .getBlockHeader(finalizedBlockHash.get())
        .or(() -> blockchain.getFinalized().flatMap(blockchain::getBlockHeader))
        .filter(header -> header.getNumber() <= blockchain.getChainHeadBlockNumber())
        .orElseGet(blockchain::getChainHeadHeader);
  }

  private BlockHeader downloadHeader(final Hash hash) {
    final int maxHeaders = 1;
    final int skip = 0;
    final int retries = Math.max(1, ethContext.getEthPeers().peerCount());
    final PeerTaskExecutorResult<List<BlockHeader>> result =
        ethContext
            .getPeerTaskExecutor()
            .execute(
                new GetHeadersFromPeerTask(
                    hash, maxHeaders, skip, Direction.FORWARD, retries, protocolSchedule));
    final List<BlockHeader> headers = result.result().orElse(List.of());
    checkState(
        result.responseCode() == PeerTaskExecutorResponseCode.SUCCESS && !headers.isEmpty(),
        "Cannot download the header of head %s: %s",
        hash,
        result.responseCode());
    return headers.getFirst();
  }

  /**
   * Stores the headers from {@code target} down to the block of the chain they descend from: the
   * chain head, which a new head almost always extends, else the finalized block. A sync that
   * stopped halfway so resumes above the blocks it imported.
   *
   * @return the block of the chain the stored headers descend from
   */
  private BlockHeader downloadHeaders(
      final BlockHeader target, final StoreHeadersStep storeHeaders) {
    headerStore.putAll(List.of(target));
    final BlockHeader finalized = finalizedBlock();
    for (final BlockHeader anchor :
        List.of(protocolContext.getBlockchain().getChainHeadHeader(), finalized)) {
      walkDownTo(anchor, storeHeaders);
      if (storeHeaders.lowest().getParentHash().equals(anchor.getHash())) {
        return anchor;
      }
    }
    throw new IllegalStateException(
        "Head " + target.toLogString() + " does not descend from " + finalized.toLogString());
  }

  // stores the headers below the lowest one stored, down to the anchor excluded
  private void walkDownTo(final BlockHeader anchor, final StoreHeadersStep storeHeaders) {
    final long top = storeHeaders.lowest().getNumber() - 1;
    if (top > anchor.getNumber()) {
      runToEnd(headersPipeline(top, anchor.getNumber(), storeHeaders));
    }
  }

  /** Imports the stored blocks the chain does not hold, up to {@code head}. */
  private void importBlocks(
      final Head head,
      final BlockHeader target,
      final BlockHeader anchor,
      final long finalizedNumber) {
    final ImportBlocksStep importBlocks =
        new ImportBlocksStep(
            protocolContext,
            protocolSchedule,
            balImporter,
            finalizedNumber,
            target.getNumber(),
            badBlock -> notifyBadChain(badBlock, target.getNumber()));
    final long first = firstBlockToImport(anchor.getNumber() + 1, target.getNumber());
    // the head that came in a new payload is imported from it, peers may not serve it yet
    final long last = head.block().isPresent() ? target.getNumber() - 1 : target.getNumber();
    if (first <= last) {
      final RequestSender sender = ethContext.getRequestScheduler().scope();
      try {
        runToEnd(importPipeline(first, last, finalizedNumber, importBlocks, sender));
      } finally {
        sender.stop();
      }
    }
    head.block()
        .ifPresent(
            block -> importBlocks.importBlock(block, head.blockAccessList(), Optional.empty()));
  }

  // the blocks above the anchor the chain already holds are not imported again
  private long firstBlockToImport(final long from, final long to) {
    final Blockchain blockchain = protocolContext.getBlockchain();
    long number = from;
    while (number <= to
        && headerStore
            .get(number)
            .map(BlockHeader::getHash)
            .equals(blockchain.getBlockHashByNumber(number))) {
      number++;
    }
    return number;
  }

  private Pipeline<Long> headersPipeline(
      final long top, final long anchorNumber, final StoreHeadersStep storeHeaders) {
    final int requestSize = syncConfig.getDownloaderHeaderRequestSize();
    final int parallelism = syncConfig.getDownloaderParallelism();
    final Iterator<Long> batchTops =
        LongStream.iterate(top, number -> number > anchorNumber, number -> number - requestSize)
            .boxed()
            .iterator();
    final DownloadBackwardHeadersStep downloadHeaders =
        new DownloadBackwardHeadersStep(
            protocolSchedule,
            ethContext,
            requestSize,
            anchorNumber,
            anchorNumber,
            Duration.ofMillis(syncConfig.getBackwardHeadersDownloadStepTimeoutMillis()));
    return PipelineBuilder.createPipelineFrom(
            "headerSource",
            batchTops,
            parallelism,
            headersPipelineCounter,
            true,
            "backwardSyncHeaders")
        .thenProcessAsyncOrdered("downloadHeaders", downloadHeaders, parallelism)
        .andFinishWith("storeHeaders", storeHeaders);
  }

  private Pipeline<Long> importPipeline(
      final long first,
      final long last,
      final long finalizedNumber,
      final ImportBlocksStep importBlocks,
      final RequestSender sender) {
    final Iterator<Long> batchStarts =
        LongStream.iterate(first, number -> number <= last, number -> batchEnd(number, first) + 1)
            .boxed()
            .iterator();
    final DownloadBlockDataStep downloadBlockData =
        new DownloadBlockDataStep(
            sender,
            ethContext.getScheduler(),
            protocolSchedule,
            new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder()),
            REQUEST_SIZE,
            Duration.ofMillis(syncConfig.getBodiesDownloadStepTimeoutMillis()),
            finalizedNumber,
            first);
    return PipelineBuilder.createPipelineFrom(
            "blockSource",
            batchStarts,
            DOWNLOADS_AHEAD,
            importPipelineCounter,
            true,
            "backwardSyncImport")
        .thenProcess(
            "loadHeaders",
            start -> headerStore.getRange(start, Math.min(batchEnd(start, first), last)))
        .thenProcessAsyncOrdered("downloadBlockData", downloadBlockData, DOWNLOADS_AHEAD)
        .thenProcessAsyncOrdered(
            "recoverSenders", new RecoverSendersStep(ethContext.getScheduler()), DOWNLOADS_AHEAD)
        .andFinishWith("importBlocks", importBlocks);
  }

  // the last block of the batch from start, the first batch of the import being smaller
  private static long batchEnd(final long start, final long first) {
    return start + (start == first ? FIRST_BATCH_SIZE : IMPORT_BATCH_SIZE) - 1;
  }

  private void runToEnd(final Pipeline<?> pipeline) {
    try {
      ethContext.getScheduler().startPipeline(pipeline).get();
    } catch (final InterruptedException e) {
      pipeline.abort();
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Backward sync interrupted", e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e.getCause().getMessage(), e.getCause());
    }
  }

  private void notifyBadChain(final BlockHeader badBlock, final long targetNumber) {
    final long last =
        Math.min(targetNumber, badBlock.getNumber() + BadBlockManager.MAX_BAD_CHAIN_SIZE);
    final List<BlockHeader> descendants = headerStore.getRange(badBlock.getNumber() + 1, last);
    badChainListeners.forEach(listener -> listener.onBadChain(badBlock, List.of(), descendants));
  }
}
