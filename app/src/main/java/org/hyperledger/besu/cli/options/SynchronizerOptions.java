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
package org.hyperledger.besu.cli.options;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration.DEFAULT_SNAP_SYNC_SAVE_PRE_MERGE_HEADERS_ONLY_ENABLED;

import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Range;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** The Synchronizer Cli options. */
public class SynchronizerOptions implements CLIOptions<SynchronizerConfiguration.Builder> {
  private static final Logger LOG = LoggerFactory.getLogger(SynchronizerOptions.class);
  private static final String BLOCK_PROPAGATION_RANGE_FLAG =
      "--Xsynchronizer-block-propagation-range";
  private static final String DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT_FLAG =
      "--Xsynchronizer-downloader-change-target-threshold-by-height";
  private static final String DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD_FLAG =
      "--Xsynchronizer-downloader-change-target-threshold-by-td";
  private static final String DOWNLOADER_HEADER_REQUEST_SIZE_FLAG =
      "--Xsynchronizer-downloader-header-request-size";
  private static final String DOWNLOADER_BODIES_REQUEST_SIZE_FLAG =
      "--Xsynchronizer-downloader-bodies-request-size";
  private static final String DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED_FLAG =
      "--Xsynchronizer-downloader-checkpoint-timeouts-permitted";
  private static final String DOWNLOADER_CHECKPOINT_RETRIES_FLAG =
      "--Xsynchronizer-downloader-checkpoint-RETRIES";
  private static final String DOWNLOADER_CHAIN_SEGMENT_SIZE_FLAG =
      "--Xsynchronizer-downloader-chain-segment-size";
  private static final String DOWNLOADER_PARALLELISM_FLAG =
      "--Xsynchronizer-downloader-parallelism";
  private static final String HEADER_DOWNLOAD_PARALLELISM_FACTOR_FLAG =
      "--Xsynchronizer-header-download-parallelism-factor";
  private static final String TRANSACTIONS_PARALLELISM_FLAG =
      "--Xsynchronizer-transactions-parallelism";
  private static final String COMPUTATION_PARALLELISM_FLAG =
      "--Xsynchronizer-computation-parallelism";
  private static final String PIVOT_DISTANCE_FROM_HEAD_FLAG = "--Xsynchronizer-pivot-distance";
  private static final String WORLD_STATE_HASH_COUNT_PER_REQUEST_FLAG =
      "--Xsynchronizer-world-state-hash-count-per-request";
  private static final String WORLD_STATE_REQUEST_PARALLELISM_FLAG =
      "--Xsynchronizer-world-state-request-parallelism";
  private static final String WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS_FLAG =
      "--Xsynchronizer-world-state-max-requests-without-progress";
  private static final String WORLD_STATE_MIN_MILLIS_BEFORE_STALLING_FLAG =
      "--Xsynchronizer-world-state-min-millis-before-stalling";
  private static final String WORLD_STATE_TASK_CACHE_SIZE_FLAG =
      "--Xsynchronizer-world-state-task-cache-size";
  private static final String RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG =
      "--Xsynchronizer-receipts-download-step-timeout-millis";
  private static final String BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG =
      "--Xsynchronizer-backward-headers-download-step-timeout-millis";
  private static final String BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG =
      "--Xsynchronizer-bodies-download-step-timeout-millis";

  // Regular (stable) flag
  private static final String SNAP_SERVER_ENABLED_FLAG = "--snapsync-server-enabled";

  private static final String SNAP_PIVOT_BLOCK_WINDOW_VALIDITY_FLAG =
      "--Xsnapsync-synchronizer-pivot-block-window-validity";
  private static final String SNAP_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING_FLAG =
      "--Xsnapsync-synchronizer-pivot-block-distance-before-caching";
  private static final String SNAP_STORAGE_COUNT_PER_REQUEST_FLAG =
      "--Xsnapsync-synchronizer-storage-count-per-request";
  private static final String SNAP_BYTECODE_COUNT_PER_REQUEST_FLAG =
      "--Xsnapsync-synchronizer-bytecode-count-per-request";
  private static final String SNAP_TRIENODE_COUNT_PER_REQUEST_FLAG =
      "--Xsnapsync-synchronizer-trienode-count-per-request";
  private static final String SNAP_TRANSACTION_INDEXING_ENABLED_FLAG =
      "--snapsync-synchronizer-transaction-indexing-enabled";

  private static final String SNAP_FLAT_ACCOUNT_HEALED_COUNT_PER_REQUEST_FLAG =
      "--Xsnapsync-synchronizer-flat-account-healed-count-per-request";

  private static final String SNAP_FLAT_STORAGE_HEALED_COUNT_PER_REQUEST_FLAG =
      "--Xsnapsync-synchronizer-flat-slot-healed-count-per-request";

  private static final String CHECKPOINT_POST_MERGE_FLAG = "--Xcheckpoint-post-merge-enabled";

  private static final String SNAP_SYNC_SAVE_PRE_CHECKPOINT_HEADERS_ONLY_FLAG =
      "--snapsync-synchronizer-pre-checkpoint-headers-only-enabled";

  private static final String ERA1_IMPORT_PREPIPELINE_ENABLED_FLAG =
      "--era1-import-prepipeline-enabled";
  private static final String ERA1_DATA_URI_FLAG = "--era1-data-uri";
  private static final String ERA1_IMPORT_PREPIPELINE_CONCURRENCY_FLAG =
      "--era1-import-prepipeline-concurrency";

  /**
   * Parse block propagation range.
   *
   * @param arg the range such as -10..30
   */
  @CommandLine.Option(
      names = BLOCK_PROPAGATION_RANGE_FLAG,
      hidden = true,
      defaultValue = "-10..30",
      paramLabel = "<LONG>..<LONG>",
      description =
          "Range around chain head where inbound blocks are propagated (default: ${DEFAULT-VALUE})")
  public void parseBlockPropagationRange(final String arg) {
    blockPropagationRange = OptionParser.parseLongRange(arg);
  }

  private Range<Long> blockPropagationRange =
      SynchronizerConfiguration.DEFAULT_BLOCK_PROPAGATION_RANGE;

  @CommandLine.Option(
      names = DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT_FLAG,
      hidden = true,
      paramLabel = "<LONG>",
      description =
          "Minimum height difference before switching fast sync download peers (default: ${DEFAULT-VALUE})")
  private long downloaderChangeTargetThresholdByHeight =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT;

  @CommandLine.Option(
      names = DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD_FLAG,
      hidden = true,
      paramLabel = "<UINT256>",
      description =
          "Minimum total difficulty difference before switching fast sync download peers (default: ${DEFAULT-VALUE})")
  private UInt256 downloaderChangeTargetThresholdByTd =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD;

  @CommandLine.Option(
      names = DOWNLOADER_HEADER_REQUEST_SIZE_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Number of headers to request per packet (default: ${DEFAULT-VALUE})")
  private int downloaderHeaderRequestSize =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_HEADER_REQUEST_SIZE;

  @CommandLine.Option(
      names = DOWNLOADER_BODIES_REQUEST_SIZE_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Number of bodies to request per packet (default: ${DEFAULT-VALUE})")
  private int downloaderBodiesRequestSize =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_BODIES_REQUEST_SIZE;

  @CommandLine.Option(
      names = {DOWNLOADER_CHECKPOINT_RETRIES_FLAG, DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED_FLAG},
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Number of tries to attempt to download checkpoints before stopping (default: ${DEFAULT-VALUE})")
  private int downloaderCheckpointRetries =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED;

  @CommandLine.Option(
      names = DOWNLOADER_CHAIN_SEGMENT_SIZE_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Distance between checkpoint headers (default: ${DEFAULT-VALUE})")
  private int downloaderChainSegmentSize =
      SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHAIN_SEGMENT_SIZE;

  @CommandLine.Option(
      names = DOWNLOADER_PARALLELISM_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Number of threads to provide to chain downloader (default: ${DEFAULT-VALUE})")
  private int downloaderParallelism = SynchronizerConfiguration.DEFAULT_DOWNLOADER_PARALLELISM;

  @CommandLine.Option(
      names = HEADER_DOWNLOAD_PARALLELISM_FACTOR_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Parallelism factor for header downloads relative to downloader parallelism (default: ${DEFAULT-VALUE})")
  private int headerDownloadParallelismFactor =
      SynchronizerConfiguration.DEFAULT_HEADER_DOWNLOAD_PARALLELISM_FACTOR;

  @CommandLine.Option(
      names = TRANSACTIONS_PARALLELISM_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Number of threads to commit to transaction processing (default: ${DEFAULT-VALUE})")
  private int transactionsParallelism = SynchronizerConfiguration.DEFAULT_TRANSACTIONS_PARALLELISM;

  @CommandLine.Option(
      names = COMPUTATION_PARALLELISM_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Number of threads to make available for bulk hash computations during downloads (default: # of processors)")
  private int computationParallelism = Runtime.getRuntime().availableProcessors();

  @CommandLine.Option(
      names = PIVOT_DISTANCE_FROM_HEAD_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Distance from initial chain head to sync target (default: ${DEFAULT-VALUE})")
  private int pivotDistance = SynchronizerConfiguration.DEFAULT_PIVOT_DISTANCE_FROM_HEAD;

  // Not exposed as CLI option anymore, but used internally
  private float fastSyncFullValidationRate = SynchronizerConfiguration.DEFAULT_FULL_VALIDATION_RATE;

  @CommandLine.Option(
      names = WORLD_STATE_HASH_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Fast sync world state hashes queried per request (default: ${DEFAULT-VALUE})")
  private int worldStateHashCountPerRequest =
      SynchronizerConfiguration.DEFAULT_WORLD_STATE_HASH_COUNT_PER_REQUEST;

  @CommandLine.Option(
      names = WORLD_STATE_REQUEST_PARALLELISM_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Number of concurrent requests to use when downloading fast sync world state (default: ${DEFAULT-VALUE})")
  private int worldStateRequestParallelism =
      SynchronizerConfiguration.DEFAULT_WORLD_STATE_REQUEST_PARALLELISM;

  @CommandLine.Option(
      names = WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Number of world state requests accepted without progress before considering the download stalled (default: ${DEFAULT-VALUE})")
  private int worldStateMaxRequestsWithoutProgress =
      SynchronizerConfiguration.DEFAULT_WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS;

  @CommandLine.Option(
      names = WORLD_STATE_MIN_MILLIS_BEFORE_STALLING_FLAG,
      hidden = true,
      paramLabel = "<LONG>",
      description =
          "Minimum time in ms without progress before considering a world state download as stalled (default: ${DEFAULT-VALUE})")
  private long worldStateMinMillisBeforeStalling =
      SynchronizerConfiguration.DEFAULT_WORLD_STATE_MIN_MILLIS_BEFORE_STALLING;

  @CommandLine.Option(
      names = WORLD_STATE_TASK_CACHE_SIZE_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The max number of pending node data requests cached in-memory during fast sync world state download. (default: ${DEFAULT-VALUE})")
  private int worldStateTaskCacheSize =
      SynchronizerConfiguration.DEFAULT_WORLD_STATE_TASK_CACHE_SIZE;

  @CommandLine.Option(
      names = RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
      hidden = true,
      paramLabel = "<LONG>",
      description =
          "Maximum time in milliseconds to wait for receipts download step including all retries (default: ${DEFAULT-VALUE})")
  private long receiptsDownloadStepTimeoutMillis =
      SynchronizerConfiguration.DEFAULT_RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS;

  @CommandLine.Option(
      names = BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
      hidden = true,
      paramLabel = "<LONG>",
      description =
          "Maximum time in milliseconds to wait for backward headers download step including all retries (default: ${DEFAULT-VALUE})")
  private long backwardHeadersDownloadStepTimeoutMillis =
      SynchronizerConfiguration.DEFAULT_BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS;

  @CommandLine.Option(
      names = BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
      hidden = true,
      paramLabel = "<LONG>",
      description =
          "Maximum time in milliseconds to wait for bodies download step including all retries (default: ${DEFAULT-VALUE})")
  private long bodiesDownloadStepTimeoutMillis =
      SynchronizerConfiguration.DEFAULT_BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS;

  @CommandLine.Option(
      names = SNAP_PIVOT_BLOCK_WINDOW_VALIDITY_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The size of the pivot block window before having to change it (default: ${DEFAULT-VALUE})")
  private int snapsyncPivotBlockWindowValidity =
      SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_WINDOW_VALIDITY;

  @CommandLine.Option(
      names = SNAP_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "The distance from the head before loading a pivot block into the cache to have a ready pivot block when the window is finished (default: ${DEFAULT-VALUE})")
  private int snapsyncPivotBlockDistanceBeforeCaching =
      SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING;

  @CommandLine.Option(
      names = SNAP_STORAGE_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Snap sync storage queried per request (default: ${DEFAULT-VALUE})")
  private int snapsyncStorageCountPerRequest =
      SnapSyncConfiguration.DEFAULT_STORAGE_COUNT_PER_REQUEST;

  @CommandLine.Option(
      names = SNAP_BYTECODE_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Snap sync bytecode queried per request (default: ${DEFAULT-VALUE})")
  private int snapsyncBytecodeCountPerRequest =
      SnapSyncConfiguration.DEFAULT_BYTECODE_COUNT_PER_REQUEST;

  @CommandLine.Option(
      names = SNAP_TRIENODE_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description = "Snap sync trie node queried per request (default: ${DEFAULT-VALUE})")
  private int snapsyncTrieNodeCountPerRequest =
      SnapSyncConfiguration.DEFAULT_TRIENODE_COUNT_PER_REQUEST;

  @CommandLine.Option(
      names = SNAP_FLAT_ACCOUNT_HEALED_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Snap sync flat accounts verified and healed per request (default: ${DEFAULT-VALUE})")
  private int snapsyncFlatAccountHealedCountPerRequest =
      SnapSyncConfiguration.DEFAULT_LOCAL_FLAT_ACCOUNT_COUNT_TO_HEAL_PER_REQUEST;

  @CommandLine.Option(
      names = SNAP_FLAT_STORAGE_HEALED_COUNT_PER_REQUEST_FLAG,
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Snap sync flat slots verified and healed per request (default: ${DEFAULT-VALUE})")
  private int snapsyncFlatStorageHealedCountPerRequest =
      SnapSyncConfiguration.DEFAULT_LOCAL_FLAT_STORAGE_COUNT_TO_HEAL_PER_REQUEST;

  @CommandLine.Option(
      names = {SNAP_SERVER_ENABLED_FLAG},
      paramLabel = "<Boolean>",
      arity = "0..1",
      fallbackValue = "true",
      description = "Enable snap sync server capability. (default: ${DEFAULT-VALUE})")
  private Boolean snapsyncServerEnabled = SnapSyncConfiguration.DEFAULT_SNAP_SERVER_ENABLED;

  @SuppressWarnings("unused")
  @CommandLine.Option(
      names = {CHECKPOINT_POST_MERGE_FLAG},
      hidden = true,
      paramLabel = "<Boolean>",
      arity = "0..1",
      fallbackValue = "true",
      description =
          "DEPRECATED: This option is deprecated as CHECKPOINT sync mode has been replaced by SNAP. This option no longer has any effect.")
  private void setCheckpointPostMergeSyncEnabled(final Boolean enabled) {
    LOG.warn(
        "{} is deprecated and has no effect. CHECKPOINT sync mode has been replaced by SNAP.",
        CHECKPOINT_POST_MERGE_FLAG);
  }

  @CommandLine.Option(
      names = {"--Xpeertask-system-enabled"},
      hidden = true,
      description =
          "Temporary feature toggle to enable using the new peertask system (default: ${DEFAULT-VALUE})")
  private final Boolean isPeerTaskSystemEnabled = false;

  @CommandLine.Option(
      names = SNAP_TRANSACTION_INDEXING_ENABLED_FLAG,
      paramLabel = "<Boolean>",
      arity = "0..1",
      description =
          "Enable transaction indexing during SNAP sync. Enabling will increase syncing time and disk usage. However, to support RPCs that use transaction hash for historical queries, you'll need to enable this. (default: ${DEFAULT-VALUE})")
  private Boolean snapTransactionIndexingEnabled =
      SnapSyncConfiguration.DEFAULT_SNAP_SYNC_TRANSACTION_INDEXING_ENABLED;

  @CommandLine.Option(
      names = {SNAP_SYNC_SAVE_PRE_CHECKPOINT_HEADERS_ONLY_FLAG},
      paramLabel = "<Boolean>",
      arity = "0..1",
      description =
          "Enable snap sync downloader to save only headers (not block bodies) for blocks before the checkpoint. (default: ${DEFAULT-VALUE})")
  private Boolean snapSyncSavePreCheckpointHeadersOnlyEnabled =
      DEFAULT_SNAP_SYNC_SAVE_PRE_MERGE_HEADERS_ONLY_ENABLED;

  @CommandLine.Option(
      names = ERA1_IMPORT_PREPIPELINE_ENABLED_FLAG,
      paramLabel = "<Boolean>",
      arity = "0..1",
      description =
          "Enable the ERA1 import prepipeline for FULL sync. ERA1 files will be imported from the URI specified by "
              + ERA1_DATA_URI_FLAG
              + " (currently only local files and http is supported). If a sync mode other than FULL is selected, this will have no affect. (default: ${DEFAULT-VALUE})")
  private Boolean era1ImportPrepipelineEnabled =
      SynchronizerConfiguration.DEFAULT_ERA1_IMPORT_PREPIPELINE_ENABLED;

  @CommandLine.Option(
      names = ERA1_DATA_URI_FLAG,
      paramLabel = "<URI>",
      description =
          "The URI to attempt to load ERA1 files from. For local files, a simple path may be used (e.g. /home/user/era1). (default: ${DEFAULT-VALUE})")
  private URI era1DataUri = SynchronizerConfiguration.DEFAULT_ERA1_DATA_URI;

  @CommandLine.Option(
      names = ERA1_IMPORT_PREPIPELINE_CONCURRENCY_FLAG,
      paramLabel = "<INTEGER>",
      description =
          "How many parallel pipelines to use in the ERA1 import prepipeline (default: ${DEFAULT-VALUE})")
  private Integer era1ImportPrepipelineConcurrency =
      SynchronizerConfiguration.DEFAULT_ERA1_IMPORT_PREPIPELINE_CONCURRENCY;

  private SynchronizerOptions() {}

  /**
   * Flag to know whether the Snap sync server feature is enabled or disabled.
   *
   * @return true if snap sync server is enabled
   */
  public boolean isSnapsyncServerEnabled() {
    return snapsyncServerEnabled;
  }

  /**
   * Flag to indicate whether the peer task system should be used where available
   *
   * @return true if the peer task system should be used where available
   */
  public boolean isPeerTaskSystemEnabled() {
    return isPeerTaskSystemEnabled;
  }

  /**
   * Create synchronizer options.
   *
   * @return the synchronizer options
   */
  public static SynchronizerOptions create() {
    return new SynchronizerOptions();
  }

  /**
   * Create synchronizer options from Synchronizer Configuration.
   *
   * @param config the Synchronizer Configuration
   * @return the synchronizer options
   */
  public static SynchronizerOptions fromConfig(final SynchronizerConfiguration config) {
    final SynchronizerOptions options = new SynchronizerOptions();
    options.blockPropagationRange = config.getBlockPropagationRange();
    options.downloaderChangeTargetThresholdByHeight =
        config.getDownloaderChangeTargetThresholdByHeight();
    options.downloaderChangeTargetThresholdByTd = config.getDownloaderChangeTargetThresholdByTd();
    options.downloaderHeaderRequestSize = config.getDownloaderHeaderRequestSize();
    options.downloaderBodiesRequestSize = config.getDownloaderBodiesRequestSize();
    options.downloaderCheckpointRetries = config.getDownloaderCheckpointRetries();
    options.downloaderChainSegmentSize = config.getDownloaderChainSegmentSize();
    options.downloaderParallelism = config.getDownloaderParallelism();
    options.headerDownloadParallelismFactor = config.getHeaderDownloadParallelismFactor();
    options.transactionsParallelism = config.getTransactionsParallelism();
    options.computationParallelism = config.getComputationParallelism();
    options.pivotDistance = config.getSyncPivotDistance();
    options.fastSyncFullValidationRate = config.getFastSyncFullValidationRate();
    options.worldStateHashCountPerRequest = config.getWorldStateHashCountPerRequest();
    options.worldStateRequestParallelism = config.getWorldStateRequestParallelism();
    options.worldStateMaxRequestsWithoutProgress = config.getWorldStateMaxRequestsWithoutProgress();
    options.worldStateMinMillisBeforeStalling = config.getWorldStateMinMillisBeforeStalling();
    options.worldStateTaskCacheSize = config.getWorldStateTaskCacheSize();
    options.snapsyncPivotBlockWindowValidity =
        config.getSnapSyncConfiguration().getPivotBlockWindowValidity();
    options.snapsyncPivotBlockDistanceBeforeCaching =
        config.getSnapSyncConfiguration().getPivotBlockDistanceBeforeCaching();
    options.snapsyncStorageCountPerRequest =
        config.getSnapSyncConfiguration().getStorageCountPerRequest();
    options.snapsyncBytecodeCountPerRequest =
        config.getSnapSyncConfiguration().getBytecodeCountPerRequest();
    options.snapsyncTrieNodeCountPerRequest =
        config.getSnapSyncConfiguration().getTrienodeCountPerRequest();
    options.snapsyncFlatAccountHealedCountPerRequest =
        config.getSnapSyncConfiguration().getLocalFlatAccountCountToHealPerRequest();
    options.snapsyncFlatStorageHealedCountPerRequest =
        config.getSnapSyncConfiguration().getLocalFlatStorageCountToHealPerRequest();
    options.snapsyncServerEnabled = config.getSnapSyncConfiguration().isSnapServerEnabled();
    options.snapTransactionIndexingEnabled =
        config.getSnapSyncConfiguration().isSnapSyncTransactionIndexingEnabled();
    options.snapSyncSavePreCheckpointHeadersOnlyEnabled =
        config.isSnapSyncSavePreCheckpointHeadersOnlyEnabled();
    options.era1ImportPrepipelineEnabled = config.era1ImportPrepipelineEnabled();
    options.era1DataUri = config.era1DataUri();
    options.era1ImportPrepipelineConcurrency = config.era1ImportPrepipelineConcurrency();
    options.receiptsDownloadStepTimeoutMillis = config.getReceiptsDownloadStepTimeoutMillis();
    options.backwardHeadersDownloadStepTimeoutMillis =
        config.getBackwardHeadersDownloadStepTimeoutMillis();
    options.bodiesDownloadStepTimeoutMillis = config.getBodiesDownloadStepTimeoutMillis();
    return options;
  }

  @Override
  public SynchronizerConfiguration.Builder toDomainObject() {
    final SynchronizerConfiguration.Builder builder = SynchronizerConfiguration.builder();
    builder.blockPropagationRange(blockPropagationRange);
    builder.downloaderChangeTargetThresholdByHeight(downloaderChangeTargetThresholdByHeight);
    builder.downloaderChangeTargetThresholdByTd(downloaderChangeTargetThresholdByTd);
    builder.downloaderHeadersRequestSize(downloaderHeaderRequestSize);
    builder.downloaderBodiesRequestSize(downloaderBodiesRequestSize);
    builder.downloaderCheckpointRetries(downloaderCheckpointRetries);
    builder.downloaderChainSegmentSize(downloaderChainSegmentSize);
    builder.downloaderParallelism(downloaderParallelism);
    builder.headerDownloadParallelismFactor(headerDownloadParallelismFactor);
    builder.transactionsParallelism(transactionsParallelism);
    builder.computationParallelism(computationParallelism);
    builder.syncPivotDistance(pivotDistance);
    builder.fastSyncFullValidationRate(fastSyncFullValidationRate);
    builder.worldStateHashCountPerRequest(worldStateHashCountPerRequest);
    builder.worldStateRequestParallelism(worldStateRequestParallelism);
    builder.worldStateMaxRequestsWithoutProgress(worldStateMaxRequestsWithoutProgress);
    builder.worldStateMinMillisBeforeStalling(worldStateMinMillisBeforeStalling);
    builder.worldStateTaskCacheSize(worldStateTaskCacheSize);
    builder.snapSyncConfiguration(
        ImmutableSnapSyncConfiguration.builder()
            .pivotBlockWindowValidity(snapsyncPivotBlockWindowValidity)
            .pivotBlockDistanceBeforeCaching(snapsyncPivotBlockDistanceBeforeCaching)
            .storageCountPerRequest(snapsyncStorageCountPerRequest)
            .bytecodeCountPerRequest(snapsyncBytecodeCountPerRequest)
            .trienodeCountPerRequest(snapsyncTrieNodeCountPerRequest)
            .localFlatAccountCountToHealPerRequest(snapsyncFlatAccountHealedCountPerRequest)
            .localFlatStorageCountToHealPerRequest(snapsyncFlatStorageHealedCountPerRequest)
            .isSnapServerEnabled(snapsyncServerEnabled)
            .isSnapSyncTransactionIndexingEnabled(snapTransactionIndexingEnabled)
            .build());
    builder.isPeerTaskSystemEnabled(isPeerTaskSystemEnabled);
    builder.snapSyncSavePreCheckpointHeadersOnlyEnabled(
        snapSyncSavePreCheckpointHeadersOnlyEnabled);
    builder.receiptsDownloadStepTimeoutMillis(receiptsDownloadStepTimeoutMillis);
    builder.backwardHeadersDownloadStepTimeoutMillis(backwardHeadersDownloadStepTimeoutMillis);
    builder.bodiesDownloadStepTimeoutMillis(bodiesDownloadStepTimeoutMillis);
    builder.era1ImportPrepipelineEnabled(era1ImportPrepipelineEnabled);
    builder.era1DataUri(era1DataUri);
    builder.era1ImportPrepipelineConcurrency(era1ImportPrepipelineConcurrency);
    return builder;
  }

  @Override
  public List<String> getCLIOptions() {
    List<String> value =
        Arrays.asList(
            BLOCK_PROPAGATION_RANGE_FLAG,
            OptionParser.format(blockPropagationRange),
            DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT_FLAG,
            OptionParser.format(downloaderChangeTargetThresholdByHeight),
            DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD_FLAG,
            OptionParser.format(downloaderChangeTargetThresholdByTd),
            DOWNLOADER_HEADER_REQUEST_SIZE_FLAG,
            OptionParser.format(downloaderHeaderRequestSize),
            DOWNLOADER_BODIES_REQUEST_SIZE_FLAG,
            OptionParser.format(downloaderBodiesRequestSize),
            DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED_FLAG,
            OptionParser.format(downloaderCheckpointRetries),
            DOWNLOADER_CHAIN_SEGMENT_SIZE_FLAG,
            OptionParser.format(downloaderChainSegmentSize),
            DOWNLOADER_PARALLELISM_FLAG,
            OptionParser.format(downloaderParallelism),
            HEADER_DOWNLOAD_PARALLELISM_FACTOR_FLAG,
            OptionParser.format(headerDownloadParallelismFactor),
            TRANSACTIONS_PARALLELISM_FLAG,
            OptionParser.format(transactionsParallelism),
            COMPUTATION_PARALLELISM_FLAG,
            OptionParser.format(computationParallelism),
            PIVOT_DISTANCE_FROM_HEAD_FLAG,
            OptionParser.format(pivotDistance),
            WORLD_STATE_HASH_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(worldStateHashCountPerRequest),
            WORLD_STATE_REQUEST_PARALLELISM_FLAG,
            OptionParser.format(worldStateRequestParallelism),
            WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS_FLAG,
            OptionParser.format(worldStateMaxRequestsWithoutProgress),
            WORLD_STATE_MIN_MILLIS_BEFORE_STALLING_FLAG,
            OptionParser.format(worldStateMinMillisBeforeStalling),
            WORLD_STATE_TASK_CACHE_SIZE_FLAG,
            OptionParser.format(worldStateTaskCacheSize),
            RECEIPTS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
            OptionParser.format(receiptsDownloadStepTimeoutMillis),
            BACKWARD_HEADERS_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
            OptionParser.format(backwardHeadersDownloadStepTimeoutMillis),
            BODIES_DOWNLOAD_STEP_TIMEOUT_MILLIS_FLAG,
            OptionParser.format(bodiesDownloadStepTimeoutMillis),
            SNAP_PIVOT_BLOCK_WINDOW_VALIDITY_FLAG,
            OptionParser.format(snapsyncPivotBlockWindowValidity),
            SNAP_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING_FLAG,
            OptionParser.format(snapsyncPivotBlockDistanceBeforeCaching),
            SNAP_STORAGE_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(snapsyncStorageCountPerRequest),
            SNAP_BYTECODE_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(snapsyncBytecodeCountPerRequest),
            SNAP_TRIENODE_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(snapsyncTrieNodeCountPerRequest),
            SNAP_FLAT_ACCOUNT_HEALED_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(snapsyncFlatAccountHealedCountPerRequest),
            SNAP_FLAT_STORAGE_HEALED_COUNT_PER_REQUEST_FLAG,
            OptionParser.format(snapsyncFlatStorageHealedCountPerRequest),
            SNAP_SERVER_ENABLED_FLAG,
            OptionParser.format(snapsyncServerEnabled),
            SNAP_TRANSACTION_INDEXING_ENABLED_FLAG,
            OptionParser.format(snapTransactionIndexingEnabled),
            SNAP_SYNC_SAVE_PRE_CHECKPOINT_HEADERS_ONLY_FLAG,
            OptionParser.format(snapSyncSavePreCheckpointHeadersOnlyEnabled),
            ERA1_IMPORT_PREPIPELINE_ENABLED_FLAG,
            OptionParser.format(era1ImportPrepipelineEnabled),
            ERA1_DATA_URI_FLAG,
            OptionParser.format(era1DataUri),
            ERA1_IMPORT_PREPIPELINE_CONCURRENCY_FLAG,
            OptionParser.format(era1ImportPrepipelineConcurrency));
    return value;
  }
}
