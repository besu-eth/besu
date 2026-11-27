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
package org.hyperledger.besu.ethereum.worldstate;

import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public interface ExtraStorageConfiguration {

  ExtraStorageConfiguration DEFAULT = ImmutableExtraStorageConfiguration.builder().build();

  ExtraStorageConfiguration DISABLED =
      ImmutableExtraStorageConfiguration.builder()
          .limitTrieLogsEnabled(false)
          .unstable(Unstable.DISABLED)
          .parallelTxProcessingEnabled(false)
          .parallelStateRootComputationEnabled(false)
          .build();

  /**
   * EIP-8252 REORG_RETENTION_WINDOW: 8192 epochs * 32 slots = 2^18 = 262,144 blocks (~36.4 days),
   * the inactivity-leak-bounded span over which the chain may stay non-final. EIP-8252 requires an
   * EL to be able to reconstruct the post-state of every canonical block with number in [head -
   * REORG_RETENTION_WINDOW, head] without re-syncing. For Bonsai that depth is bounded by the
   * number of retained trie logs (see {@link #getMaxLayersToLoad()}), so the default retention is
   * set to this window. See https://eips.ethereum.org/EIPS/eip-8252
   */
  long EIP_8252_REORG_RETENTION_WINDOW = 262_144;

  /**
   * Default number of historical trie-log layers retained, and therefore the depth from head across
   * which Bonsai can reconstruct state without re-sync. Set to the EIP-8252 reorg retention window
   * so a default node is EIP-8252 compliant out of the box. Overridable with {@code
   * --bonsai-historical-block-limit}.
   */
  long DEFAULT_MAX_LAYERS_TO_LOAD = EIP_8252_REORG_RETENTION_WINDOW;

  boolean DEFAULT_LIMIT_TRIE_LOGS_ENABLED = true;

  /**
   * Lowest retention an operator may configure via {@code --bonsai-historical-block-limit}.
   * Deliberately decoupled from (and far below) {@link #DEFAULT_MAX_LAYERS_TO_LOAD}: raising the
   * EIP-8252 default must not raise this floor, so minimal / non-archive nodes can still trade
   * reorg-reconstruction depth for disk by opting below the window (compare erigon's
   * MinimalPruneDistance). This is the historical default retention value (512).
   */
  long MINIMUM_TRIE_LOG_RETENTION_LIMIT = 512;

  /**
   * Startup trie-log pruning batch size. Must be strictly greater than the configured retention
   * ({@link #getMaxLayersToLoad()}) or the pruner only loads still-retained layers and makes no
   * progress; the CLI enforces this. Sized above the EIP-8252 default retention with headroom so
   * the default configuration validates and catch-up pruning can reach beyond the window.
   */
  int DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE = (int) (EIP_8252_REORG_RETENTION_WINDOW + 30_000L);

  boolean DEFAULT_PARALLEL_TX_PROCESSING = true;
  boolean DEFAULT_PARALLEL_STATE_ROOT_COMPUTATION = true;

  @Value.Default
  default Long getMaxLayersToLoad() {
    return DEFAULT_MAX_LAYERS_TO_LOAD;
  }

  @Value.Default
  default boolean getLimitTrieLogsEnabled() {
    return DEFAULT_LIMIT_TRIE_LOGS_ENABLED;
  }

  @Value.Default
  default int getTrieLogPruningWindowSize() {
    return DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE;
  }

  @Value.Default
  default boolean getParallelTxProcessingEnabled() {
    return DEFAULT_PARALLEL_TX_PROCESSING;
  }

  @Value.Default
  default boolean getParallelStateRootComputationEnabled() {
    return DEFAULT_PARALLEL_STATE_ROOT_COMPUTATION;
  }

  @Value.Default
  default Unstable getUnstable() {
    return Unstable.DEFAULT;
  }

  @Value.Immutable
  interface Unstable {

    ExtraStorageConfiguration.Unstable DEFAULT =
        ImmutableExtraStorageConfiguration.Unstable.builder().build();

    ExtraStorageConfiguration.Unstable PARTIAL_MODE =
        ImmutableExtraStorageConfiguration.Unstable.builder().fullFlatDbEnabled(false).build();

    ExtraStorageConfiguration.Unstable DISABLED =
        ImmutableExtraStorageConfiguration.Unstable.builder()
            .fullFlatDbEnabled(false)
            .codeStoredByCodeHashEnabled(false)
            .build();

    boolean DEFAULT_FULL_FLAT_DB_ENABLED = true;
    boolean DEFAULT_CODE_USING_CODE_HASH_ENABLED = true;
    boolean DEFAULT_BONSAI_CROSS_BLOCK_CACHE_ENABLED = false;
    long DEFAULT_BONSAI_CROSS_BLOCK_CACHE_ACCOUNT_SIZE = 100_000L;
    long DEFAULT_BONSAI_CROSS_BLOCK_CACHE_STORAGE_SIZE = 500_000L;
    boolean DEFAULT_BONSAI_ARCHIVE_STATE_PROOFS_ENABLED = false;
    int DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL = 32;
    int DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL = 16;

    @Value.Default
    default boolean getFullFlatDbEnabled() {
      return DEFAULT_FULL_FLAT_DB_ENABLED;
    }

    @Value.Default
    default boolean getCodeStoredByCodeHashEnabled() {
      return DEFAULT_CODE_USING_CODE_HASH_ENABLED;
    }

    @Value.Default
    default boolean getBonsaiCrossBlockCacheEnabled() {
      return DEFAULT_BONSAI_CROSS_BLOCK_CACHE_ENABLED;
    }

    @Value.Default
    default long getBonsaiCrossBlockCacheAccountSize() {
      return DEFAULT_BONSAI_CROSS_BLOCK_CACHE_ACCOUNT_SIZE;
    }

    @Value.Default
    default long getBonsaiCrossBlockCacheStorageSize() {
      return DEFAULT_BONSAI_CROSS_BLOCK_CACHE_STORAGE_SIZE;
    }

    @Value.Default
    default boolean getBonsaiArchiveStateProofsEnabled() {
      return DEFAULT_BONSAI_ARCHIVE_STATE_PROOFS_ENABLED;
    }

    @Value.Default
    default int getBonsaiArchiveShallowCheckpointInterval() {
      return DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;
    }

    @Value.Default
    default int getBonsaiArchiveDeepCheckpointInterval() {
      return DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
    }
  }
}
