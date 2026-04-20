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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.ChainDownloader;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.nio.file.Path;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class PivotSyncDownloaderTest {

  @SuppressWarnings("unchecked")
  private final PivotSyncActions fastSyncActions = mock(PivotSyncActions.class);

  private final WorldStateDownloader worldStateDownloader = mock(WorldStateDownloader.class);

  private final ChainDownloader chainDownloader = mock(ChainDownloader.class);

  private final Path fastSyncDataDirectory = null;
  private WorldStateStorageCoordinator worldStateStorageCoordinator;
  private PivotSyncDownloader downloader;

  static class PivotSyncDownloaderTestArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageFormat.BONSAI), Arguments.of(DataStorageFormat.FOREST));
    }
  }

  public void setup(final DataStorageFormat dataStorageFormat) {
    final WorldStateKeyValueStorage worldStateKeyValueStorage;
    if (dataStorageFormat.equals(DataStorageFormat.BONSAI)) {
      worldStateKeyValueStorage = mock(BonsaiWorldStateKeyValueStorage.class);
      when(((BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage)
              .isWorldStateAvailable(any(), any()))
          .thenReturn(true);
    } else {
      worldStateKeyValueStorage = mock(ForestWorldStateKeyValueStorage.class);
      when(((ForestWorldStateKeyValueStorage) worldStateKeyValueStorage)
              .isWorldStateAvailable(any()))
          .thenReturn(true);
    }
    when(worldStateKeyValueStorage.getDataStorageFormat()).thenReturn(dataStorageFormat);
    worldStateStorageCoordinator = new WorldStateStorageCoordinator(worldStateKeyValueStorage);
    downloader =
        new PivotSyncDownloader(
            fastSyncActions,
            worldStateStorageCoordinator,
            worldStateDownloader,
            fastSyncDataDirectory,
            PivotSyncState.EMPTY_SYNC_STATE,
            SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldCompleteFastSyncSuccessfully(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);
    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(completedFuture(null));

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(chainDownloader).start();
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
    assertThat(result).isCompletedWithValue(downloadPivotBlockHeaderState);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldResumeFastSync(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState fastSyncState = new PivotSyncState(pivotBlockHeader, false);
    final CompletableFuture<PivotSyncState> complete = completedFuture(fastSyncState);
    when(fastSyncActions.selectPivotBlock(fastSyncState)).thenReturn(complete);
    when(fastSyncActions.downloadPivotBlockHeader(fastSyncState)).thenReturn(complete);
    when(fastSyncActions.createChainDownloader(
            fastSyncState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(completedFuture(null));

    final PivotSyncDownloader resumedDownloader =
        new PivotSyncDownloader(
            fastSyncActions,
            worldStateStorageCoordinator,
            worldStateDownloader,
            fastSyncDataDirectory,
            fastSyncState,
            SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);

    final CompletableFuture<PivotSyncState> result = resumedDownloader.start();

    verify(fastSyncActions).selectPivotBlock(fastSyncState);
    verify(fastSyncActions).downloadPivotBlockHeader(fastSyncState);
    verify(fastSyncActions)
        .createChainDownloader(fastSyncState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(chainDownloader).start();
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
    assertThat(result).isCompletedWithValue(fastSyncState);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldAbortIfSelectPivotBlockFails(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenThrow(new SyncException(SyncError.UNEXPECTED_ERROR));

    final CompletableFuture<PivotSyncState> result = downloader.start();

    assertCompletedExceptionally(result, SyncError.UNEXPECTED_ERROR);

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verifyNoMoreInteractions(fastSyncActions);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldAbortIfWorldStateDownloadFails(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    worldStateFuture.completeExceptionally(new SyncException(SyncError.NO_PEERS_AVAILABLE));
    verify(chainDownloader).cancel();
    chainFuture.completeExceptionally(new CancellationException());
    assertCompletedExceptionally(result, SyncError.NO_PEERS_AVAILABLE);
    assertThat(chainFuture).isCancelled();
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldAbortIfChainDownloadFails(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    chainFuture.completeExceptionally(new SyncException(SyncError.NO_PEERS_AVAILABLE));
    assertCompletedExceptionally(result, SyncError.NO_PEERS_AVAILABLE);
    assertThat(worldStateFuture).isCancelled();
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldAbortIfStopped(final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    doAnswer(
            invocation -> {
              CompletableFuture<PivotSyncState> future = new CompletableFuture<>();
              Executors.newSingleThreadScheduledExecutor()
                  .schedule(
                      () -> future.complete(downloadPivotBlockHeaderState),
                      500,
                      TimeUnit.MILLISECONDS);
              return future;
            })
        .when(fastSyncActions)
        .downloadPivotBlockHeader(selectPivotBlockState);

    final CompletableFuture<PivotSyncState> result = downloader.start();
    downloader.stop();

    Throwable thrown = catchThrowable(() -> result.get());
    assertThat(thrown).hasCauseExactlyInstanceOf(CancellationException.class);

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(worldStateDownloader).cancel();
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldNotConsiderFastSyncCompleteIfOnlyWorldStateDownloadIsComplete(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    worldStateFuture.complete(null);
    assertThat(result).isNotDone();
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldNotConsiderFastSyncCompleteIfOnlyChainDownloadIsComplete(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    chainFuture.complete(null);
    assertThat(result).isNotDone();
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldResetPivotSyncStateAndRestartProcessIfWorldStateIsUnavailable(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> firstWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> secondWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final ChainDownloader secondChainDownloader = mock(ChainDownloader.class);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final PivotSyncState secondSelectPivotBlockState = new PivotSyncState(90, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final BlockHeader secondPivotBlockHeader =
        new BlockHeaderTestFixture().number(90).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);
    final PivotSyncState secondDownloadPivotBlockHeaderState =
        new PivotSyncState(secondPivotBlockHeader, false);

    // First attempt
    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(
            completedFuture(selectPivotBlockState), completedFuture(secondSelectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(firstWorldStateFuture);

    // Second attempt with new pivot block
    when(fastSyncActions.downloadPivotBlockHeader(secondSelectPivotBlockState))
        .thenReturn(completedFuture(secondDownloadPivotBlockHeaderState));

    when(fastSyncActions.createChainDownloader(
            secondDownloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(secondChainDownloader);
    when(secondChainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(secondPivotBlockHeader, false))))
        .thenReturn(secondWorldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    firstWorldStateFuture.completeExceptionally(new StalledDownloadException("test"));
    assertThat(result).isNotDone();
    verify(chainDownloader).cancel();
    // A real chain downloader would cause the chainFuture to complete when cancel is called.
    chainFuture.completeExceptionally(new CancellationException());

    verify(fastSyncActions, times(2)).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(secondSelectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            secondDownloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(secondPivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    secondWorldStateFuture.complete(null);

    assertThat(result).isCompletedWithValue(secondDownloadPivotBlockHeaderState);
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldResetPivotSyncStateAndRestartProcessIfANonFastSyncExceptionOccurs(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final CompletableFuture<Void> firstWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> secondWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final ChainDownloader secondChainDownloader = mock(ChainDownloader.class);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final PivotSyncState secondSelectPivotBlockState = new PivotSyncState(90, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final BlockHeader secondPivotBlockHeader =
        new BlockHeaderTestFixture().number(90).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);
    final PivotSyncState secondDownloadPivotBlockHeaderState =
        new PivotSyncState(secondPivotBlockHeader, false);

    // First attempt
    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(
            completedFuture(selectPivotBlockState), completedFuture(secondSelectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(firstWorldStateFuture);
    when(fastSyncActions.scheduleFutureTask(any(), any()))
        .thenAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get());

    // Second attempt
    when(fastSyncActions.downloadPivotBlockHeader(secondSelectPivotBlockState))
        .thenReturn(completedFuture(secondDownloadPivotBlockHeaderState));

    when(fastSyncActions.createChainDownloader(
            secondDownloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(secondChainDownloader);
    when(secondChainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(secondPivotBlockHeader, false))))
        .thenReturn(secondWorldStateFuture);

    final CompletableFuture<PivotSyncState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    firstWorldStateFuture.completeExceptionally(new RuntimeException("Test"));

    assertThat(result).isNotDone();
    verify(chainDownloader).cancel();
    // A real chain downloader would cause the chainFuture to complete when cancel is called.
    chainFuture.completeExceptionally(new CancellationException());

    verify(fastSyncActions).scheduleFutureTask(any(), any());
    verify(fastSyncActions, times(2)).selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE);
    verify(fastSyncActions).downloadPivotBlockHeader(secondSelectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            secondDownloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(new PivotSyncState(secondPivotBlockHeader, false)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    secondWorldStateFuture.complete(null);

    assertThat(result).isCompletedWithValue(secondDownloadPivotBlockHeaderState);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldNotHaveTrailingPeerRequirementsBeforePivotBlockSelected(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    downloader.start();
    Assertions.assertThat(downloader.calculateTrailingPeerRequirements()).isEmpty();
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldNotAllowPeersBeforePivotBlockOnceSelected(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(new CompletableFuture<>());
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(new CompletableFuture<>());

    downloader.start();
    Assertions.assertThat(downloader.calculateTrailingPeerRequirements())
        .contains(new TrailingPeerRequirements(50, 0));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotSyncDownloaderTestArguments.class)
  public void shouldNotHaveTrailingPeerRequirementsAfterDownloadCompletes(
      final DataStorageFormat dataStorageFormat) {
    setup(dataStorageFormat);
    final PivotSyncState selectPivotBlockState = new PivotSyncState(50, false);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final PivotSyncState downloadPivotBlockHeaderState =
        new PivotSyncState(pivotBlockHeader, false);

    when(fastSyncActions.selectPivotBlock(PivotSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            downloadPivotBlockHeaderState, SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(new PivotSyncState(pivotBlockHeader, false))))
        .thenReturn(completedFuture(null));

    final CompletableFuture<PivotSyncState> result = downloader.start();
    assertThat(result).isDone();

    Assertions.assertThat(downloader.calculateTrailingPeerRequirements()).isEmpty();
  }

  private <T> void assertCompletedExceptionally(
      final CompletableFuture<T> future, final SyncError expectedError) {
    assertThat(future).isCompletedExceptionally();
    future.exceptionally(
        actualError -> {
          assertThat(actualError)
              .isInstanceOf(SyncException.class)
              .extracting(ex -> ((SyncException) ex).getError())
              .isEqualTo(expectedError);
          return null;
        });
  }
}
