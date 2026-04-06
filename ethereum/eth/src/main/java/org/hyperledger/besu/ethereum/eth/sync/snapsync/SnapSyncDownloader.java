/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncDownloader;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class SnapSyncDownloader extends PivotSyncDownloader {

  public SnapSyncDownloader(
      final PivotSyncActions fastSyncActions,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final WorldStateDownloader worldStateDownloader,
      final Path fastSyncDataDirectory,
      final PivotSyncState initialPivotSyncState,
      final SyncDurationMetrics syncDurationMetrics) {
    super(
        fastSyncActions,
        worldStateStorageCoordinator,
        worldStateDownloader,
        fastSyncDataDirectory,
        initialPivotSyncState,
        syncDurationMetrics);
  }

  @Override
  protected CompletableFuture<PivotSyncState> start(final PivotSyncState fastSyncState) {
    LOG.debug("Start snap sync with initial sync state {}", fastSyncState);
    return findPivotBlock(fastSyncState, fss -> downloadChainAndWorldState(fastSyncActions, fss));
  }

  @Override
  protected PivotSyncState storeState(final PivotSyncState fastSyncState) {
    initialPivotSyncState = fastSyncState;
    return new SnapSyncProcessState(fastSyncState);
  }
}
