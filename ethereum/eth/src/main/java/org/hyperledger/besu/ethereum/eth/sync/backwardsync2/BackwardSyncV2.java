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
import org.hyperledger.besu.ethereum.eth.sync.DownloadSyncBodiesStep;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSync;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BadChainListener;
import org.hyperledger.besu.ethereum.eth.sync.common.DownloadBackwardHeadersStep;
import org.hyperledger.besu.ethereum.eth.sync.common.DownloadSyncReceiptsStep;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.util.Subscribers;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backward sync on the Besu pipelines. To reach a head the chain does not hold, it downloads the
 * headers from that head down to the finalized block the chain holds, then downloads and imports
 * the blocks above the chain, the next batches downloading while one imports. Finalized blocks are
 * imported from their block access list instead of being executed.
 */
public class BackwardSyncV2 implements BackwardSync {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncV2.class);

  // enough batches downloaded ahead of the import to hide the download latency, few enough to
  // bound the memory their bodies and block access lists take
  static final int IMPORT_BATCH_SIZE = 64;
  static final int DOWNLOADS_AHEAD = 2;
  // the newest blocks may not be served by peers yet, they are imported from their new payload
  private static final int RECENT_PAYLOADS = 64;

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final SynchronizerConfiguration syncConfig;
  private final EthContext ethContext;
  private final HeaderStore headerStore;
  private final BalImporter balImporter;
  private final LabelledMetric<Counter> headersPipelineCounter;
  private final LabelledMetric<Counter> importPipelineCounter;
  private final Subscribers<BadChainListener> badChainListeners = Subscribers.create();
  private final AtomicReference<Hash> finalizedBlockHash = new AtomicReference<>(Hash.ZERO);

  // guarded by this
  private final Map<Hash, Head> recentPayloads =
      new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Hash, Head> eldest) {
          return size() > RECENT_PAYLOADS;
        }
      };
  private Head latestHead;
  private boolean running;
  private CompletableFuture<Void> sync = CompletableFuture.completedFuture(null);

  /** A head to sync to, with its block and block access list when they came in a new payload. */
  private record Head(
      Hash hash, Optional<Block> block, Optional<BlockAccessList> blockAccessList) {}

  public BackwardSyncV2(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration syncConfig,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final StorageProvider storageProvider) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.syncConfig = syncConfig;
    this.ethContext = ethContext;
    this.headerStore =
        new HeaderStore(
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.BACKWARD_SYNC_HEADERS),
            ScheduleBasedBlockHeaderFunctions.create(protocolSchedule));
    this.balImporter = new BalImporter(protocolContext, protocolSchedule);
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

  @Override
  public synchronized boolean isSyncing() {
    return running;
  }

  // the progress logs use the head of each sync
  @Override
  public void maybeUpdateTargetHeight(final Hash headHash) {}

  @Override
  public void updateFinalizedBlockHash(final Hash finalizedBlockHash) {
    this.finalizedBlockHash.set(finalizedBlockHash);
  }

  @Override
  public synchronized CompletableFuture<Void> syncBackwardsUntil(final Hash newBlockHash) {
    latestHead =
        recentPayloads.getOrDefault(
            newBlockHash, new Head(newBlockHash, Optional.empty(), Optional.empty()));
    return startIfIdle();
  }

  @Override
  public synchronized CompletableFuture<Void> syncBackwardsUntil(
      final Block newPivot, final Optional<BlockAccessList> blockAccessList) {
    latestHead = new Head(newPivot.getHash(), Optional.of(newPivot), blockAccessList);
    recentPayloads.put(newPivot.getHash(), latestHead);
    return startIfIdle();
  }

  // the headers it downloads are stored by number, only the new payloads are found by hash
  @Override
  public synchronized Optional<BlockHeader> getHeader(final Hash blockHash) {
    return Optional.ofNullable(recentPayloads.get(blockHash))
        .flatMap(Head::block)
        .map(Block::getHeader);
  }

  @Override
  public void subscribeBadChainListener(final BadChainListener badChainListener) {
    badChainListeners.subscribe(badChainListener);
  }

  private CompletableFuture<Void> startIfIdle() {
    if (!running) {
      running = true;
      LOG.info("Starting backward sync");
      sync = ethContext.getScheduler().scheduleServiceTask(this::syncToLatestHead);
    }
    return sync;
  }

  private void syncToLatestHead() {
    try {
      for (Optional<Head> next = nextHead(); next.isPresent(); next = nextHead()) {
        syncTo(next.get());
      }
      LOG.info("Backward sync done");
    } catch (final RuntimeException | Error e) {
      synchronized (this) {
        running = false;
      }
      LOG.info(
          "Backward sync stopped ({}), the next head from the consensus client restarts it",
          e.getMessage());
      LOG.debug("Backward sync failure", e);
      throw e;
    }
  }

  /** The head to sync to next, empty once the chain holds the latest one, the sync then done. */
  private synchronized Optional<Head> nextHead() {
    if (protocolContext.getBlockchain().contains(latestHead.hash())) {
      running = false;
      recentPayloads.clear();
      return Optional.empty();
    }
    return Optional.of(latestHead);
  }

  private void syncTo(final Head head) {
    final BlockHeader targetHeader =
        head.block().map(Block::getHeader).orElseGet(() -> downloadHeader(head.hash()));
    final BlockHeader anchor = anchor();
    if (targetHeader.getNumber() <= anchor.getNumber()) {
      throw new IllegalStateException(
          "Head " + targetHeader.toLogString() + " is not above " + anchor.toLogString());
    }
    LOG.info("Backward sync from {} to {}", anchor.toLogString(), targetHeader.toLogString());

    headerStore.clear();
    headerStore.putAll(List.of(targetHeader));
    final StoreHeadersStep storeHeaders =
        new StoreHeadersStep(
            headerStore, targetHeader, anchor.getNumber(), finalizedBlockHash::get);
    if (targetHeader.getNumber() - 1 > anchor.getNumber()) {
      run(headersPipeline(targetHeader.getNumber() - 1, anchor.getNumber(), storeHeaders));
    }
    if (!storeHeaders.lowest().getParentHash().equals(anchor.getHash())) {
      throw new IllegalStateException(
          "Head " + targetHeader.toLogString() + " does not descend from " + anchor.toLogString());
    }

    final ImportBlocksStep importBlocks =
        new ImportBlocksStep(
            protocolContext,
            protocolSchedule,
            balImporter,
            storeHeaders.finalizedNumber(),
            targetHeader.getNumber(),
            badBlock -> notifyBadChain(badBlock, targetHeader.getNumber()));
    final long first = firstBlockToImport(anchor.getNumber() + 1, targetHeader.getNumber());
    // the head that came in a new payload is imported from it, peers may not serve it yet
    final long last =
        head.block().isPresent() ? targetHeader.getNumber() - 1 : targetHeader.getNumber();
    if (first <= last) {
      run(importPipeline(first, last, storeHeaders.finalizedNumber(), importBlocks));
    }
    head.block()
        .ifPresent(
            block -> importBlocks.importBlock(block, head.blockAccessList(), Optional.empty()));
    headerStore.clear();
  }

  /**
   * The block the headers are downloaded down to: the finalized block of the consensus client if
   * the chain holds it, else the finalized block of the chain, so that no fork reaches below it.
   */
  private BlockHeader anchor() {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    return blockchain
        .getBlockHeader(finalizedBlockHash.get())
        .or(() -> blockchain.getFinalized().flatMap(blockchain::getBlockHeader))
        .filter(header -> header.getNumber() <= blockchain.getChainHeadBlockNumber())
        .orElseGet(blockchain::getChainHeadHeader);
  }

  private BlockHeader downloadHeader(final Hash hash) {
    final PeerTaskExecutorResult<List<BlockHeader>> result =
        ethContext
            .getPeerTaskExecutor()
            .execute(
                new GetHeadersFromPeerTask(
                    hash,
                    1,
                    0,
                    Direction.FORWARD,
                    Math.max(1, ethContext.getEthPeers().peerCount()),
                    protocolSchedule));
    if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
        || result.result().map(List::isEmpty).orElse(true)) {
      throw new IllegalStateException(
          "Cannot download the header of head " + hash + ": " + result.responseCode());
    }
    return result.result().get().getFirst();
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
    final Iterator<Long> starts =
        LongStream.iterate(top, number -> number > anchorNumber, number -> number - requestSize)
            .boxed()
            .iterator();
    return PipelineBuilder.createPipelineFrom(
            "headerSource",
            starts,
            parallelism,
            headersPipelineCounter,
            true,
            "backwardSyncHeaders")
        .thenProcessAsyncOrdered(
            "downloadHeaders",
            new DownloadBackwardHeadersStep(
                protocolSchedule,
                ethContext,
                requestSize,
                anchorNumber,
                anchorNumber,
                Duration.ofMillis(syncConfig.getBackwardHeadersDownloadStepTimeoutMillis())),
            parallelism)
        .andFinishWith("storeHeaders", storeHeaders);
  }

  private Pipeline<Long> importPipeline(
      final long first,
      final long last,
      final long finalizedNumber,
      final ImportBlocksStep importBlocks) {
    final Iterator<Long> starts =
        LongStream.iterate(first, number -> number <= last, number -> number + IMPORT_BATCH_SIZE)
            .boxed()
            .iterator();
    final DownloadSyncReceiptsStep downloadReceipts =
        new DownloadSyncReceiptsStep(
            protocolSchedule,
            ethContext,
            new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder()),
            Duration.ofMillis(syncConfig.getForwardDownloadStepTimeoutMillis()));
    return PipelineBuilder.createPipelineFrom(
            "blockSource",
            starts,
            DOWNLOADS_AHEAD,
            importPipelineCounter,
            true,
            "backwardSyncImport")
        .thenProcess(
            "loadHeaders",
            start -> headerStore.getRange(start, Math.min(start + IMPORT_BATCH_SIZE - 1, last)))
        .thenProcessAsyncOrdered(
            "downloadBodies",
            new DownloadSyncBodiesStep(
                protocolSchedule,
                ethContext,
                Duration.ofMillis(syncConfig.getBodiesDownloadStepTimeoutMillis())),
            DOWNLOADS_AHEAD)
        .thenProcessAsyncOrdered(
            "downloadBlockData",
            new DownloadBlockDataStep(ethContext, downloadReceipts, finalizedNumber),
            DOWNLOADS_AHEAD)
        .andFinishWith("importBlocks", importBlocks);
  }

  private void run(final Pipeline<?> pipeline) {
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
