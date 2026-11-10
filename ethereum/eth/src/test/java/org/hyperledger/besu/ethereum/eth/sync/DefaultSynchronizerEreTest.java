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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.fullsync.FullSyncDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultSynchronizerEreTest {
  @TempDir Path directory;

  @Test
  void startsEreOnAlreadyPostMergeDatabaseWithoutCompletingInitialPhaseEarly() {
    final var config =
        SynchronizerConfiguration.builder()
            .ereImportPrepipelineEnabled(true)
            .ereDataUri(directory.toUri())
            .build();
    final var state = mock(SyncState.class);
    final var future = new CompletableFuture<Void>();
    try (var tracker = mockStatic(ChainHeadTracker.class);
        var downloads =
            mockConstruction(
                FullSyncDownloader.class,
                (mock, context) -> when(mock.start()).thenReturn(future))) {
      final var synchronizer = synchronizer(config, state);
      assertThat(downloads.constructed()).hasSize(1);
      final var result = synchronizer.start();
      verify(downloads.constructed().getFirst()).start();
      verify(state, never()).markInitialSyncPhaseAsDone();
      assertThat(result).isNotDone();
      future.complete(null);
      assertThat(result).isCompleted();
      verify(state).setReachedTerminalDifficulty(true);
    }
  }

  @Test
  void preservesExistingPostMergeStartupWithoutEre() {
    final var state = mock(SyncState.class);
    try (var tracker = mockStatic(ChainHeadTracker.class);
        var downloads = mockConstruction(FullSyncDownloader.class)) {
      final var synchronizer = synchronizer(SynchronizerConfiguration.builder().build(), state);
      assertThat(downloads.constructed()).isEmpty();
      assertThat(synchronizer.start()).isCompleted();
      verify(state).markInitialSyncPhaseAsDone();
    }
  }

  private DefaultSynchronizer synchronizer(
      final SynchronizerConfiguration config, final SyncState state) {
    return new DefaultSynchronizer(
        config,
        mock(ProtocolSchedule.class),
        mock(ProtocolContext.class),
        mock(WorldStateStorageCoordinator.class),
        mock(BlockBroadcaster.class),
        mock(EthContext.class),
        null,
        state,
        directory,
        mock(StorageProvider.class),
        Clock.systemUTC(),
        new NoOpMetricsSystem(),
        () -> true,
        mock(PivotBlockSelector.class),
        Optional.empty(),
        Optional.empty());
  }
}
