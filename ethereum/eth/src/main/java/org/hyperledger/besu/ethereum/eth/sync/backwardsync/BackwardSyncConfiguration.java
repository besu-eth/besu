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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.time.Duration;

/**
 * Tuning of the backward sync.
 *
 * @param headersPerRequest headers per header request
 * @param headerParallelism concurrent header segments
 * @param windowBytes memory budget of the block data window above the import cursor
 * @param maxWindowBlocks maximum number of blocks in the window
 * @param optionalDataWait how long the importer waits for a BAL or receipts before giving up on
 *     them
 * @param balPeerAttempts peers that may answer empty for a BAL before it is deemed unavailable
 * @param requestAttempts attempts of a body or receipts request
 * @param retryDelay delay before a failed request or import is tried again
 * @param maxImportAttempts consecutive failed attempts to import one block before giving up
 * @param pendingPruneInterval imports between prunings of the imported part of the pending chain
 * @param trustedPayloadBytes memory budget of the newPayload block cache
 * @param progressInterval interval of the progress log
 * @param balApplyEnabled whether finalized blocks may be imported from their BAL without execution
 */
record BackwardSyncConfiguration(
    int headersPerRequest,
    int headerParallelism,
    long windowBytes,
    int maxWindowBlocks,
    Duration optionalDataWait,
    int balPeerAttempts,
    int requestAttempts,
    Duration retryDelay,
    int maxImportAttempts,
    long pendingPruneInterval,
    long trustedPayloadBytes,
    Duration progressInterval,
    boolean balApplyEnabled) {

  static BackwardSyncConfiguration create(
      final SynchronizerConfiguration syncConfig, final WorldStateArchive archive) {
    return new BackwardSyncConfiguration(
        syncConfig.getDownloaderHeaderRequestSize(),
        64,
        Math.clamp(Runtime.getRuntime().maxMemory() / 8, 64L << 20, 1L << 30),
        2048,
        Duration.ofSeconds(10),
        3,
        10,
        Duration.ofSeconds(5),
        3,
        1024,
        64L << 20,
        Duration.ofSeconds(30),
        syncConfig.isBackwardSyncBalApplyEnabled()
            && syncConfig.getSyncMode() != SyncMode.FULL
            && archive instanceof PathBasedWorldStateProvider);
  }
}
