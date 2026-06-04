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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

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
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.SyncError;
import org.hyperledger.besu.ethereum.eth.sync.common.SyncException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.StalledDownloadException;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloader;
import org.hyperledger.besu.metrics.SyncDurationMetrics;

import java.nio.file.Path;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapSyncDownloaderTest {

  @SuppressWarnings("unchecked")
  private final PivotSyncActions fastSyncActions = mock(PivotSyncActions.class);

  private final WorldStateDownloader worldStateDownloader = mock(WorldStateDownloader.class);

  private final ChainDownloader chainDownloader = mock(ChainDownloader.class);

  private final Path fastSyncDataDirectory = null;
  private SnapSyncDownloader downloader;

  public void setup() {
    downloader =
        new SnapSyncDownloader(
            fastSyncActions,
            worldStateDownloader,
            fastSyncDataDirectory,
            new SnapSyncProcessState(),
            SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
  }

  @Test
  public void shouldCompleteFastSyncSuccessfully() {
    setup();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);
    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(completedFuture(null));

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(chainDownloader).start();
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
    assertThat(result).isCompletedWithValue(snapSyncState(pivotBlockHeader));
  }

  @Test
  public void shouldResumeFastSync() {
    setup();
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState fastSyncState = new SnapSyncProcessState(pivotBlockHeader);
    final CompletableFuture<SnapSyncProcessState> complete = completedFuture(fastSyncState);
    when(fastSyncActions.selectPivotBlock(fastSyncState)).thenReturn(complete);
    when(fastSyncActions.resolvePivotBlockHeader(fastSyncState)).thenReturn(complete);
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(completedFuture(null));

    final SnapSyncDownloader resumedDownloader =
        new SnapSyncDownloader(
            fastSyncActions,
            worldStateDownloader,
            fastSyncDataDirectory,
            fastSyncState,
            SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);

    final CompletableFuture<SnapSyncProcessState> result = resumedDownloader.start();

    verify(fastSyncActions).selectPivotBlock(fastSyncState);
    verify(fastSyncActions).resolvePivotBlockHeader(fastSyncState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(chainDownloader).start();
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
    assertThat(result).isCompletedWithValue(snapSyncState(pivotBlockHeader));
  }

  @Test
  public void shouldAbortIfSelectPivotBlockFails() {
    setup();
    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenThrow(new SyncException(SyncError.UNEXPECTED_ERROR));

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    assertCompletedExceptionally(result, SyncError.UNEXPECTED_ERROR);

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verifyNoMoreInteractions(fastSyncActions);
  }

  @Test
  public void shouldAbortIfWorldStateDownloadFails() {
    setup();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    worldStateFuture.completeExceptionally(new SyncException(SyncError.NO_PEERS_AVAILABLE));
    verify(chainDownloader).cancel();
    chainFuture.completeExceptionally(new CancellationException());
    assertCompletedExceptionally(result, SyncError.NO_PEERS_AVAILABLE);
    assertThat(chainFuture).isCancelled();
  }

  @Test
  public void shouldAbortIfChainDownloadFails() {
    setup();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    chainFuture.completeExceptionally(new SyncException(SyncError.NO_PEERS_AVAILABLE));
    assertCompletedExceptionally(result, SyncError.NO_PEERS_AVAILABLE);
    assertThat(worldStateFuture).isCancelled();
  }

  @Test
  public void shouldAbortIfStopped() {
    setup();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    doAnswer(
            invocation -> {
              CompletableFuture<SnapSyncProcessState> future = new CompletableFuture<>();
              Executors.newSingleThreadScheduledExecutor()
                  .schedule(
                      () -> future.complete(resolvePivotBlockHeaderState),
                      500,
                      TimeUnit.MILLISECONDS);
              return future;
            })
        .when(fastSyncActions)
        .resolvePivotBlockHeader(selectPivotBlockState);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();
    downloader.stop();

    Throwable thrown = catchThrowable(() -> result.get());
    assertThat(thrown).hasCauseExactlyInstanceOf(CancellationException.class);

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(worldStateDownloader).cancel();
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);
  }

  @Test
  public void shouldNotConsiderFastSyncCompleteIfOnlyWorldStateDownloadIsComplete() {
    setup();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    worldStateFuture.complete(null);
    assertThat(result).isNotDone();
  }

  @Test
  public void shouldNotConsiderFastSyncCompleteIfOnlyChainDownloadIsComplete() {
    setup();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final CompletableFuture<Void> worldStateFuture = new CompletableFuture<>();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(worldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions);
    verifyNoMoreInteractions(worldStateDownloader);

    assertThat(result).isNotDone();

    chainFuture.complete(null);
    assertThat(result).isNotDone();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldResetPivotSyncStateAndRestartProcessIfWorldStateIsUnavailable() {
    setup();
    final CompletableFuture<Void> firstWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> secondWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final ChainDownloader secondChainDownloader = mock(ChainDownloader.class);
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final SnapSyncProcessState secondSelectPivotBlockState = new SnapSyncProcessState(90);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final BlockHeader secondPivotBlockHeader =
        new BlockHeaderTestFixture().number(90).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);
    final SnapSyncProcessState secondDownloadPivotBlockHeaderState =
        new SnapSyncProcessState(secondPivotBlockHeader);

    // First attempt
    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(
            completedFuture(selectPivotBlockState), completedFuture(secondSelectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(firstWorldStateFuture);

    // Second attempt with new pivot block
    when(fastSyncActions.resolvePivotBlockHeader(secondSelectPivotBlockState))
        .thenReturn(completedFuture(secondDownloadPivotBlockHeaderState));

    when(fastSyncActions.createChainDownloader(
            snapSyncState(secondPivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(secondChainDownloader);
    when(secondChainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(snapSyncState(secondPivotBlockHeader))))
        .thenReturn(secondWorldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    firstWorldStateFuture.completeExceptionally(new StalledDownloadException("test"));
    assertThat(result).isNotDone();
    verify(chainDownloader).cancel();
    // A real chain downloader would cause the chainFuture to complete when cancel is called.
    chainFuture.completeExceptionally(new CancellationException());

    verify(fastSyncActions, times(2)).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(secondSelectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(secondPivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(secondChainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(secondPivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    secondWorldStateFuture.complete(null);

    assertThat(result).isCompletedWithValue(snapSyncState(secondPivotBlockHeader));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldResetPivotSyncStateAndRestartProcessIfANonFastSyncExceptionOccurs() {
    setup();
    final CompletableFuture<Void> firstWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> secondWorldStateFuture = new CompletableFuture<>();
    final CompletableFuture<Void> chainFuture = new CompletableFuture<>();
    final ChainDownloader secondChainDownloader = mock(ChainDownloader.class);
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final SnapSyncProcessState secondSelectPivotBlockState = new SnapSyncProcessState(90);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final BlockHeader secondPivotBlockHeader =
        new BlockHeaderTestFixture().number(90).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);
    final SnapSyncProcessState secondDownloadPivotBlockHeaderState =
        new SnapSyncProcessState(secondPivotBlockHeader);

    // First attempt
    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(
            completedFuture(selectPivotBlockState), completedFuture(secondSelectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(chainFuture);
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(firstWorldStateFuture);
    when(fastSyncActions.scheduleFutureTask(any(), any()))
        .thenAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get());

    // Second attempt
    when(fastSyncActions.resolvePivotBlockHeader(secondSelectPivotBlockState))
        .thenReturn(completedFuture(secondDownloadPivotBlockHeaderState));

    when(fastSyncActions.createChainDownloader(
            snapSyncState(secondPivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(secondChainDownloader);
    when(secondChainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(
            any(PivotSyncActions.class), eq(snapSyncState(secondPivotBlockHeader))))
        .thenReturn(secondWorldStateFuture);

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();

    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(selectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(chainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    assertThat(result).isNotDone();

    firstWorldStateFuture.completeExceptionally(new RuntimeException("Test"));

    assertThat(result).isNotDone();
    verify(chainDownloader).cancel();
    // A real chain downloader would cause the chainFuture to complete when cancel is called.
    chainFuture.completeExceptionally(new CancellationException());

    verify(fastSyncActions).scheduleFutureTask(any(), any());
    verify(fastSyncActions, times(2)).selectPivotBlock(new SnapSyncProcessState());
    verify(fastSyncActions).resolvePivotBlockHeader(secondSelectPivotBlockState);
    verify(fastSyncActions)
        .createChainDownloader(
            snapSyncState(secondPivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS);
    verify(worldStateDownloader).setChainDownloader(secondChainDownloader);
    verify(worldStateDownloader)
        .run(any(PivotSyncActions.class), eq(snapSyncState(secondPivotBlockHeader)));
    verifyNoMoreInteractions(fastSyncActions, worldStateDownloader);

    secondWorldStateFuture.complete(null);

    assertThat(result).isCompletedWithValue(snapSyncState(secondPivotBlockHeader));
  }

  @Test
  public void shouldNotHaveTrailingPeerRequirementsBeforePivotBlockSelected() {
    setup();
    downloader.start();
    Assertions.assertThat(downloader.calculateTrailingPeerRequirements()).isEmpty();
  }

  @Test
  public void shouldNotAllowPeersBeforePivotBlockOnceSelected() {
    setup();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(new CompletableFuture<>());
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(new CompletableFuture<>());

    downloader.start();
    Assertions.assertThat(downloader.calculateTrailingPeerRequirements())
        .contains(new TrailingPeerRequirements(50, 0));
  }

  @Test
  public void shouldNotHaveTrailingPeerRequirementsAfterDownloadCompletes() {
    setup();
    final SnapSyncProcessState selectPivotBlockState = new SnapSyncProcessState(50);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(50).buildHeader();
    final SnapSyncProcessState resolvePivotBlockHeaderState =
        new SnapSyncProcessState(pivotBlockHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.resolvePivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(resolvePivotBlockHeaderState));
    when(fastSyncActions.createChainDownloader(
            snapSyncState(pivotBlockHeader), SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS))
        .thenReturn(chainDownloader);
    when(chainDownloader.start()).thenReturn(completedFuture(null));
    when(worldStateDownloader.run(any(PivotSyncActions.class), eq(snapSyncState(pivotBlockHeader))))
        .thenReturn(completedFuture(null));

    final CompletableFuture<SnapSyncProcessState> result = downloader.start();
    assertThat(result).isDone();

    Assertions.assertThat(downloader.calculateTrailingPeerRequirements()).isEmpty();
  }

  private SnapSyncProcessState snapSyncState(final BlockHeader pivotBlockHeader) {
    return new SnapSyncProcessState(pivotBlockHeader);
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
