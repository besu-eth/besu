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
package org.hyperledger.besu.ethereum.eth.sync;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.services.tasks.CachingTaskCollection;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import org.apache.tuweni.units.bigints.UInt256;

public class SynchronizerConfiguration {

  public static final int DEFAULT_PIVOT_DISTANCE_FROM_HEAD = 50;
  public static final float DEFAULT_FULL_VALIDATION_RATE = .1f;
  public static final int DEFAULT_SYNC_MINIMUM_PEERS = 5;
  public static final int DEFAULT_WORLD_STATE_HASH_COUNT_PER_REQUEST = 384;
  public static final int DEFAULT_WORLD_STATE_REQUEST_PARALLELISM = 10;
  public static final int DEFAULT_WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS = 1000;
  public static final long DEFAULT_WORLD_STATE_MIN_MILLIS_BEFORE_STALLING =
      TimeUnit.MINUTES.toMillis(5);
  public static final Range<Long> DEFAULT_BLOCK_PROPAGATION_RANGE = Range.closed(-10L, 30L);
  public static final long DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT = 200L;
  public static final UInt256 DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD =
      UInt256.valueOf(1_000_000_000_000_000_000L);
  public static final int DEFAULT_DOWNLOADER_HEADER_REQUEST_SIZE = 192;
  public static final int DEFAULT_DOWNLOADER_BODIES_REQUEST_SIZE = 128;
  public static final int DEFAULT_DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED = 5;
  public static final int DEFAULT_DOWNLOADER_CHAIN_SEGMENT_SIZE = 200;
  public static final int DEFAULT_DOWNLOADER_PARALLELISM = 8;
  public static final int DEFAULT_HEADER_DOWNLOAD_PARALLELISM_FACTOR = 20;
  public static final int DEFAULT_TRANSACTIONS_PARALLELISM = 4;
  public static final int DEFAULT_COMPUTATION_PARALLELISM = 2;
  public static final int DEFAULT_WORLD_STATE_TASK_CACHE_SIZE =
      CachingTaskCollection.DEFAULT_CACHE_SIZE;
  public static final long DEFAULT_RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS =
      TimeUnit.SECONDS.toMillis(60);
  public static final long DEFAULT_BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS =
      TimeUnit.SECONDS.toMillis(60);
  public static final long DEFAULT_BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS =
      TimeUnit.SECONDS.toMillis(60);
  public static final Boolean DEFAULT_ERA1_IMPORT_PREPIPELINE_ENABLED = Boolean.FALSE;
  public static final URI DEFAULT_ERA1_DATA_URI = URI.create("https://mainnet.era1.nimbus.team/");
  public static final Integer DEFAULT_ERA1_IMPORT_PREPIPELINE_CONCURRENCY = 1;

  // Fast sync config
  private final int syncPivotDistance;
  private final float fastSyncFullValidationRate;
  private final int syncMinimumPeerCount;
  private final int worldStateHashCountPerRequest;
  private final int worldStateRequestParallelism;
  private final int worldStateMaxRequestsWithoutProgress;
  private final int worldStateTaskCacheSize;

  // Snapsync
  private final SnapSyncConfiguration snapSyncConfiguration;

  // Block propagation config
  private final Range<Long> blockPropagationRange;

  // General config
  private final SyncMode syncMode;

  // Downloader config
  private final long downloaderChangeTargetThresholdByHeight;
  private final UInt256 downloaderChangeTargetThresholdByTd;
  private final int downloaderHeaderRequestSize;
  private final int downloaderBodiesRequestSize;
  private final int downloaderCheckpointRetries;
  private final int downloaderChainSegmentSize;
  private final int downloaderParallelism;
  private final int headerDownloadParallelismFactor;
  private final int transactionsParallelism;
  private final int computationParallelism;
  private final int maxTrailingPeers;
  private final long worldStateMinMillisBeforeStalling;
  private final long receiptsDownloadStepTimeoutMillis;
  private final long backwardHeadersDownloadStepTimeoutMillis;
  private final long bodiesDownloadStepTimeoutMillis;
  private final boolean isPeerTaskSystemEnabled;
  private final boolean snapSyncSavePreCheckpointHeadersOnlyEnabled;

  // ERA1 import prepipeline config
  private final boolean era1ImportPrepipelineEnabled;
  private final URI era1DataUri;
  private final int era1ImportPrepipelineConcurrency;

  private SynchronizerConfiguration(
      final int syncPivotDistance,
      final float fastSyncFullValidationRate,
      final int syncMinimumPeerCount,
      final int worldStateHashCountPerRequest,
      final int worldStateRequestParallelism,
      final int worldStateMaxRequestsWithoutProgress,
      final long worldStateMinMillisBeforeStalling,
      final int worldStateTaskCacheSize,
      final SnapSyncConfiguration snapSyncConfiguration,
      final Range<Long> blockPropagationRange,
      final SyncMode syncMode,
      final long downloaderChangeTargetThresholdByHeight,
      final UInt256 downloaderChangeTargetThresholdByTd,
      final int downloaderHeaderRequestSize,
      final int downloaderBodiesRequestSize,
      final int downloaderCheckpointRetries,
      final int downloaderChainSegmentSize,
      final int downloaderParallelism,
      final int headerDownloadParallelismFactor,
      final int transactionsParallelism,
      final int computationParallelism,
      final int maxTrailingPeers,
      final long receiptsDownloadStepTimeoutMillis,
      final long backwardHeadersDownloadStepTimeoutMillis,
      final long bodiesDownloadStepTimeoutMillis,
      final boolean isPeerTaskSystemEnabled,
      final boolean snapSyncSavePreCheckpointHeadersOnlyEnabled,
      final boolean era1ImportPrepipelineEnabled,
      final URI era1DataUri,
      final int era1ImportPrepipelineConcurrency) {
    this.syncPivotDistance = syncPivotDistance;
    this.fastSyncFullValidationRate = fastSyncFullValidationRate;
    this.syncMinimumPeerCount = syncMinimumPeerCount;
    this.worldStateHashCountPerRequest = worldStateHashCountPerRequest;
    this.worldStateRequestParallelism = worldStateRequestParallelism;
    this.worldStateMaxRequestsWithoutProgress = worldStateMaxRequestsWithoutProgress;
    this.worldStateMinMillisBeforeStalling = worldStateMinMillisBeforeStalling;
    this.worldStateTaskCacheSize = worldStateTaskCacheSize;
    this.snapSyncConfiguration = snapSyncConfiguration;
    this.blockPropagationRange = blockPropagationRange;
    this.syncMode = syncMode;
    this.downloaderChangeTargetThresholdByHeight = downloaderChangeTargetThresholdByHeight;
    this.downloaderChangeTargetThresholdByTd = downloaderChangeTargetThresholdByTd;
    this.downloaderHeaderRequestSize = downloaderHeaderRequestSize;
    this.downloaderBodiesRequestSize = downloaderBodiesRequestSize;
    this.downloaderCheckpointRetries = downloaderCheckpointRetries;
    this.downloaderChainSegmentSize = downloaderChainSegmentSize;
    this.downloaderParallelism = downloaderParallelism;
    this.headerDownloadParallelismFactor = headerDownloadParallelismFactor;
    this.transactionsParallelism = transactionsParallelism;
    this.computationParallelism = computationParallelism;
    this.maxTrailingPeers = maxTrailingPeers;
    this.receiptsDownloadStepTimeoutMillis = receiptsDownloadStepTimeoutMillis;
    this.backwardHeadersDownloadStepTimeoutMillis = backwardHeadersDownloadStepTimeoutMillis;
    this.bodiesDownloadStepTimeoutMillis = bodiesDownloadStepTimeoutMillis;
    this.isPeerTaskSystemEnabled = isPeerTaskSystemEnabled;
    this.snapSyncSavePreCheckpointHeadersOnlyEnabled = snapSyncSavePreCheckpointHeadersOnlyEnabled;
    this.era1ImportPrepipelineEnabled = era1ImportPrepipelineEnabled;
    this.era1DataUri = era1DataUri;
    this.era1ImportPrepipelineConcurrency = era1ImportPrepipelineConcurrency;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * The actual sync mode to be used.
   *
   * @return the sync mode
   */
  public SyncMode getSyncMode() {
    return syncMode;
  }

  /**
   * All the configuration related to snapsync
   *
   * @return snapsync configuration
   */
  public SnapSyncConfiguration getSnapSyncConfiguration() {
    return snapSyncConfiguration;
  }

  /**
   * The range of block numbers (relative to the current chain head and the best network block) that
   * are considered appropriate to import as new blocks are announced on the network.
   *
   * @return the range of blocks considered valid to import from the network, relative to the
   *     current chain head.
   */
  public Range<Long> getBlockPropagationRange() {
    return blockPropagationRange;
  }

  /**
   * The distance from the chain head at which we should switch from fast, snap, or checkpoint sync
   * to full sync.
   *
   * @return distance from the chain head at which we should switch from fast, snap or checkpoint
   *     sync to full sync.
   */
  public int getSyncPivotDistance() {
    return syncPivotDistance;
  }

  public long getDownloaderChangeTargetThresholdByHeight() {
    return downloaderChangeTargetThresholdByHeight;
  }

  public UInt256 getDownloaderChangeTargetThresholdByTd() {
    return downloaderChangeTargetThresholdByTd;
  }

  public int getDownloaderHeaderRequestSize() {
    return downloaderHeaderRequestSize;
  }

  public int getDownloaderBodiesRequestSize() {
    return downloaderBodiesRequestSize;
  }

  public int getDownloaderCheckpointRetries() {
    return downloaderCheckpointRetries;
  }

  public int getDownloaderChainSegmentSize() {
    return downloaderChainSegmentSize;
  }

  public int getDownloaderParallelism() {
    return downloaderParallelism;
  }

  public int getHeaderDownloadParallelismFactor() {
    return headerDownloadParallelismFactor;
  }

  public int getTransactionsParallelism() {
    return transactionsParallelism;
  }

  public int getComputationParallelism() {
    return computationParallelism;
  }

  /**
   * The rate at which blocks should be fully validated during fast sync. At a rate of 1f, all
   * blocks are fully validated. At rates less than 1f, a subset of blocks will undergo light-weight
   * validation.
   *
   * @return rate at which blocks should be fully validated during fast sync.
   */
  public float getFastSyncFullValidationRate() {
    return fastSyncFullValidationRate;
  }

  public int getSyncMinimumPeerCount() {
    return syncMinimumPeerCount;
  }

  public int getWorldStateHashCountPerRequest() {
    return worldStateHashCountPerRequest;
  }

  public int getWorldStateRequestParallelism() {
    return worldStateRequestParallelism;
  }

  public int getWorldStateMaxRequestsWithoutProgress() {
    return worldStateMaxRequestsWithoutProgress;
  }

  public long getWorldStateMinMillisBeforeStalling() {
    return worldStateMinMillisBeforeStalling;
  }

  public int getWorldStateTaskCacheSize() {
    return worldStateTaskCacheSize;
  }

  public int getMaxTrailingPeers() {
    return maxTrailingPeers;
  }

  public long getReceiptsDownloadStepTimeoutMillis() {
    return receiptsDownloadStepTimeoutMillis;
  }

  public long getBackwardHeadersDownloadStepTimeoutMillis() {
    return backwardHeadersDownloadStepTimeoutMillis;
  }

  public long getBodiesDownloadStepTimeoutMillis() {
    return bodiesDownloadStepTimeoutMillis;
  }

  public boolean isPeerTaskSystemEnabled() {
    return isPeerTaskSystemEnabled;
  }

  public boolean isSnapSyncSavePreCheckpointHeadersOnlyEnabled() {
    return snapSyncSavePreCheckpointHeadersOnlyEnabled;
  }

  public boolean era1ImportPrepipelineEnabled() {
    return era1ImportPrepipelineEnabled;
  }

  public URI era1DataUri() {
    return era1DataUri;
  }

  public int era1ImportPrepipelineConcurrency() {
    return era1ImportPrepipelineConcurrency;
  }

  public static class Builder {
    private SyncMode syncMode = SyncMode.FULL;
    private int syncMinimumPeerCount = DEFAULT_SYNC_MINIMUM_PEERS;
    private int maxTrailingPeers = Integer.MAX_VALUE;
    private Range<Long> blockPropagationRange = DEFAULT_BLOCK_PROPAGATION_RANGE;
    private long downloaderChangeTargetThresholdByHeight =
        DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT;
    private UInt256 downloaderChangeTargetThresholdByTd =
        DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD;
    private int downloaderHeaderRequestSize = DEFAULT_DOWNLOADER_HEADER_REQUEST_SIZE;
    private int downloaderBodiesRequestSize = DEFAULT_DOWNLOADER_BODIES_REQUEST_SIZE;
    private int downloaderCheckpointRetries = DEFAULT_DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED;
    private SnapSyncConfiguration snapSyncConfiguration = SnapSyncConfiguration.getDefault();
    private int downloaderChainSegmentSize = DEFAULT_DOWNLOADER_CHAIN_SEGMENT_SIZE;
    private int downloaderParallelism = DEFAULT_DOWNLOADER_PARALLELISM;
    private int headerDownloadParallelismFactor = DEFAULT_HEADER_DOWNLOAD_PARALLELISM_FACTOR;
    private int transactionsParallelism = DEFAULT_TRANSACTIONS_PARALLELISM;
    private int computationParallelism =
        Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
    private int syncPivotDistance = DEFAULT_PIVOT_DISTANCE_FROM_HEAD;
    private float fastSyncFullValidationRate = DEFAULT_FULL_VALIDATION_RATE;
    private int worldStateHashCountPerRequest = DEFAULT_WORLD_STATE_HASH_COUNT_PER_REQUEST;
    private int worldStateRequestParallelism = DEFAULT_WORLD_STATE_REQUEST_PARALLELISM;
    private int worldStateMaxRequestsWithoutProgress =
        DEFAULT_WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS;
    private long worldStateMinMillisBeforeStalling = DEFAULT_WORLD_STATE_MIN_MILLIS_BEFORE_STALLING;
    private int worldStateTaskCacheSize = DEFAULT_WORLD_STATE_TASK_CACHE_SIZE;
    private boolean isPeerTaskSystemEnabled = false;
    private boolean snapSyncSavePreCheckpointHeadersOnlyEnabled = true;
    private boolean era1ImportPrepipelineEnabled = DEFAULT_ERA1_IMPORT_PREPIPELINE_ENABLED;
    private URI era1DataUri = DEFAULT_ERA1_DATA_URI;
    private int era1ImportPrepipelineConcurrency = DEFAULT_ERA1_IMPORT_PREPIPELINE_CONCURRENCY;
    private long receiptsDownloadStepTimeoutMillis = DEFAULT_RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS;
    private long backwardHeadersDownloadStepTimeoutMillis =
        DEFAULT_BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS;
    private long bodiesDownloadStepTimeoutMillis = DEFAULT_BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS;

    public Builder syncPivotDistance(final int distance) {
      syncPivotDistance = distance;
      return this;
    }

    public Builder fastSyncFullValidationRate(final float rate) {
      this.fastSyncFullValidationRate = rate;
      return this;
    }

    public Builder snapSyncConfiguration(final SnapSyncConfiguration snapSyncConfiguration) {
      this.snapSyncConfiguration = snapSyncConfiguration;
      return this;
    }

    public Builder syncMode(final SyncMode mode) {
      this.syncMode = mode;
      return this;
    }

    public Builder blockPropagationRange(final Range<Long> blockPropagationRange) {
      checkNotNull(blockPropagationRange);
      this.blockPropagationRange = blockPropagationRange;
      return this;
    }

    public Builder downloaderChangeTargetThresholdByHeight(
        final long downloaderChangeTargetThresholdByHeight) {
      this.downloaderChangeTargetThresholdByHeight = downloaderChangeTargetThresholdByHeight;
      return this;
    }

    public Builder downloaderChangeTargetThresholdByTd(
        final UInt256 downloaderChangeTargetThresholdByTd) {
      this.downloaderChangeTargetThresholdByTd = downloaderChangeTargetThresholdByTd;
      return this;
    }

    public Builder downloaderHeadersRequestSize(final int downloaderHeaderRequestSize) {
      this.downloaderHeaderRequestSize = downloaderHeaderRequestSize;
      return this;
    }

    public Builder downloaderBodiesRequestSize(final int downloaderBodiesRequestSize) {
      this.downloaderBodiesRequestSize = downloaderBodiesRequestSize;
      return this;
    }

    public Builder downloaderCheckpointRetries(final int downloaderCheckpointRetries) {
      this.downloaderCheckpointRetries = downloaderCheckpointRetries;
      return this;
    }

    public Builder downloaderChainSegmentSize(final int downloaderChainSegmentSize) {
      this.downloaderChainSegmentSize = downloaderChainSegmentSize;
      return this;
    }

    public Builder blockPropagationRange(final long min, final long max) {
      checkArgument(min < max, "Invalid range: min must be less than max.");
      blockPropagationRange = Range.closed(min, max);
      return this;
    }

    public Builder downloaderParallelism(final int downloaderParallelism) {
      this.downloaderParallelism = downloaderParallelism;
      return this;
    }

    public Builder headerDownloadParallelismFactor(final int headerDownloadParallelismFactor) {
      this.headerDownloadParallelismFactor = headerDownloadParallelismFactor;
      return this;
    }

    public Builder transactionsParallelism(final int transactionsParallelism) {
      this.transactionsParallelism = transactionsParallelism;
      return this;
    }

    public Builder computationParallelism(final int computationParallelism) {
      this.computationParallelism = computationParallelism;
      return this;
    }

    public Builder syncMinimumPeerCount(final int syncMinimumPeerCount) {
      this.syncMinimumPeerCount = syncMinimumPeerCount;
      return this;
    }

    public Builder worldStateHashCountPerRequest(final int worldStateHashCountPerRequest) {
      this.worldStateHashCountPerRequest = worldStateHashCountPerRequest;
      return this;
    }

    public Builder worldStateRequestParallelism(final int worldStateRequestParallelism) {
      this.worldStateRequestParallelism = worldStateRequestParallelism;
      return this;
    }

    public Builder worldStateMaxRequestsWithoutProgress(
        final int worldStateMaxRequestsWithoutProgress) {
      this.worldStateMaxRequestsWithoutProgress = worldStateMaxRequestsWithoutProgress;
      return this;
    }

    public Builder worldStateMinMillisBeforeStalling(final long worldStateMinMillisBeforeStalling) {
      this.worldStateMinMillisBeforeStalling = worldStateMinMillisBeforeStalling;
      return this;
    }

    public Builder worldStateTaskCacheSize(final int worldStateTaskCacheSize) {
      this.worldStateTaskCacheSize = worldStateTaskCacheSize;
      return this;
    }

    public Builder maxTrailingPeers(final int maxTailingPeers) {
      this.maxTrailingPeers = maxTailingPeers;
      return this;
    }

    public Builder receiptsDownloadStepTimeoutMillis(final long receiptsDownloadStepTimeoutMillis) {
      this.receiptsDownloadStepTimeoutMillis = receiptsDownloadStepTimeoutMillis;
      return this;
    }

    public Builder backwardHeadersDownloadStepTimeoutMillis(
        final long backwardHeadersDownloadStepTimeoutMillis) {
      this.backwardHeadersDownloadStepTimeoutMillis = backwardHeadersDownloadStepTimeoutMillis;
      return this;
    }

    public Builder bodiesDownloadStepTimeoutMillis(final long bodiesDownloadStepTimeoutMillis) {
      this.bodiesDownloadStepTimeoutMillis = bodiesDownloadStepTimeoutMillis;
      return this;
    }

    public Builder isPeerTaskSystemEnabled(final boolean isPeerTaskSystemEnabled) {
      this.isPeerTaskSystemEnabled = isPeerTaskSystemEnabled;
      return this;
    }

    public Builder snapSyncSavePreCheckpointHeadersOnlyEnabled(
        final boolean snapSyncSavePreCheckpointHeadersOnlyEnabled) {
      this.snapSyncSavePreCheckpointHeadersOnlyEnabled =
          snapSyncSavePreCheckpointHeadersOnlyEnabled;
      return this;
    }

    public Builder era1ImportPrepipelineEnabled(final boolean era1ImportPrepipelineEnabled) {
      this.era1ImportPrepipelineEnabled = era1ImportPrepipelineEnabled;
      return this;
    }

    public Builder era1DataUri(final URI era1DataUri) {
      this.era1DataUri = era1DataUri;
      return this;
    }

    public Builder era1ImportPrepipelineConcurrency(final int era1ImportPrepipelineConcurrency) {
      this.era1ImportPrepipelineConcurrency = era1ImportPrepipelineConcurrency;
      return this;
    }

    public SynchronizerConfiguration build() {
      return new SynchronizerConfiguration(
          syncPivotDistance,
          fastSyncFullValidationRate,
          syncMinimumPeerCount,
          worldStateHashCountPerRequest,
          worldStateRequestParallelism,
          worldStateMaxRequestsWithoutProgress,
          worldStateMinMillisBeforeStalling,
          worldStateTaskCacheSize,
          snapSyncConfiguration,
          blockPropagationRange,
          syncMode,
          downloaderChangeTargetThresholdByHeight,
          downloaderChangeTargetThresholdByTd,
          downloaderHeaderRequestSize,
          downloaderBodiesRequestSize,
          downloaderCheckpointRetries,
          downloaderChainSegmentSize,
          downloaderParallelism,
          headerDownloadParallelismFactor,
          transactionsParallelism,
          computationParallelism,
          maxTrailingPeers,
          receiptsDownloadStepTimeoutMillis,
          backwardHeadersDownloadStepTimeoutMillis,
          bodiesDownloadStepTimeoutMillis,
          isPeerTaskSystemEnabled,
          snapSyncSavePreCheckpointHeadersOnlyEnabled,
          era1ImportPrepipelineEnabled,
          era1DataUri,
          era1ImportPrepipelineConcurrency);
    }
  }
}
