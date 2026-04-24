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

import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.ImmutableSnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;

import java.net.URI;
import java.util.List;

import com.google.common.collect.Range;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SynchronizerOptionsTest
    extends AbstractCLIOptionsTest<SynchronizerConfiguration.Builder, SynchronizerOptions> {

  @Override
  protected SynchronizerConfiguration.Builder createDefaultDomainObject() {
    return SynchronizerConfiguration.builder();
  }

  @Override
  protected SynchronizerConfiguration.Builder createCustomizedDomainObject() {
    return SynchronizerConfiguration.builder()
        .syncPivotDistance(SynchronizerConfiguration.DEFAULT_PIVOT_DISTANCE_FROM_HEAD + 10)
        .fastSyncFullValidationRate(SynchronizerConfiguration.DEFAULT_FULL_VALIDATION_RATE / 2)
        .syncMinimumPeerCount(SynchronizerConfiguration.DEFAULT_SYNC_MINIMUM_PEERS + 2)
        .worldStateHashCountPerRequest(
            SynchronizerConfiguration.DEFAULT_WORLD_STATE_HASH_COUNT_PER_REQUEST + 2)
        .worldStateRequestParallelism(
            SynchronizerConfiguration.DEFAULT_WORLD_STATE_REQUEST_PARALLELISM * 2)
        .worldStateMaxRequestsWithoutProgress(
            SynchronizerConfiguration.DEFAULT_WORLD_STATE_MAX_REQUESTS_WITHOUT_PROGRESS * 2)
        .worldStateMinMillisBeforeStalling(
            SynchronizerConfiguration.DEFAULT_WORLD_STATE_MIN_MILLIS_BEFORE_STALLING * 2)
        .worldStateTaskCacheSize(SynchronizerConfiguration.DEFAULT_WORLD_STATE_TASK_CACHE_SIZE + 1)
        .blockPropagationRange(
            Range.closed(
                SynchronizerConfiguration.DEFAULT_BLOCK_PROPAGATION_RANGE.lowerEndpoint() - 2,
                SynchronizerConfiguration.DEFAULT_BLOCK_PROPAGATION_RANGE.upperEndpoint() + 2))
        .downloaderChangeTargetThresholdByHeight(
            SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_HEIGHT + 2)
        .downloaderChangeTargetThresholdByTd(
            SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHANGE_TARGET_THRESHOLD_BY_TD.add(2L))
        .downloaderHeadersRequestSize(
            SynchronizerConfiguration.DEFAULT_DOWNLOADER_HEADER_REQUEST_SIZE + 2)
        .downloaderCheckpointRetries(
            SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHECKPOINT_TIMEOUTS_PERMITTED + 2)
        .downloaderChainSegmentSize(
            SynchronizerConfiguration.DEFAULT_DOWNLOADER_CHAIN_SEGMENT_SIZE + 2)
        .downloaderParallelism(SynchronizerConfiguration.DEFAULT_DOWNLOADER_PARALLELISM + 2)
        .transactionsParallelism(SynchronizerConfiguration.DEFAULT_TRANSACTIONS_PARALLELISM + 2)
        .computationParallelism(SynchronizerConfiguration.DEFAULT_COMPUTATION_PARALLELISM + 2)
        .snapSyncConfiguration(
            ImmutableSnapSyncConfiguration.builder()
                .pivotBlockWindowValidity(
                    SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_WINDOW_VALIDITY + 2)
                .pivotBlockDistanceBeforeCaching(
                    SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING - 2)
                .trienodeCountPerRequest(
                    SnapSyncConfiguration.DEFAULT_TRIENODE_COUNT_PER_REQUEST + 2)
                .storageCountPerRequest(SnapSyncConfiguration.DEFAULT_STORAGE_COUNT_PER_REQUEST + 2)
                .bytecodeCountPerRequest(
                    SnapSyncConfiguration.DEFAULT_BYTECODE_COUNT_PER_REQUEST + 2)
                .isSnapServerEnabled(Boolean.TRUE)
                .isSnapSyncTransactionIndexingEnabled(Boolean.TRUE)
                .build())
        .snapSyncSavePreCheckpointHeadersOnlyEnabled(
            SnapSyncConfiguration.DEFAULT_SNAP_SYNC_SAVE_PRE_MERGE_HEADERS_ONLY_ENABLED)
        .era1ImportPrepipelineEnabled(true)
        .era1DataUri(URI.create("sepolia.era1.nimbus.team/"));
  }

  @Override
  protected String[] getFieldsWithComputedDefaults() {
    return new String[] {"maxTrailingPeers", "computationParallelism"};
  }

  @Override
  protected String[] getNonOptionFields() {
    return new String[] {"fastSyncFullValidationRate", "checkpointPostMergeEnabled"};
  }

  @Override
  protected SynchronizerOptions getOptionsFromBesuCommand(final TestBesuCommand besuCommand) {
    return besuCommand.getSynchronizerOptions();
  }

  @Override
  protected List<String> getFieldsToIgnore() {
    return List.of("syncMinimumPeerCount");
  }

  @Override
  protected SynchronizerOptions optionsFromDomainObject(
      final SynchronizerConfiguration.Builder domainObject) {
    return SynchronizerOptions.fromConfig(domainObject.build());
  }
}
