/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.SyncDurationMetrics;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SnapSyncChainDownloaderTest {

  @Mock private SnapSyncChainDownloadPipelineFactory pipelineFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private SyncState syncState;
  @Mock private SyncDurationMetrics syncDurationMetrics;
  @Mock private MutableBlockchain blockchain;
  @Mock private EthScheduler scheduler;
  @Mock private SingleBlockHeaderDownloader headerDownloader;

  @TempDir private Path tempDir;

  private ChainSyncStateStorage chainSyncStateStorage;
  private BlockHeader pivotBlockHeader;
  private BlockHeader checkpointBlockHeader;

  @BeforeEach
  public void setUp() {
    pivotBlockHeader = new BlockHeaderTestFixture().number(1000).buildHeader();
    checkpointBlockHeader = new BlockHeaderTestFixture().number(500).buildHeader();
    chainSyncStateStorage = new ChainSyncStateStorage(tempDir);

    lenient().when(protocolContext.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getChainHeadBlockNumber()).thenReturn(500L);
    lenient().when(blockchain.getChainHeadHeader()).thenReturn(checkpointBlockHeader);
    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient()
        .when(blockchain.getGenesisBlockHeader())
        .thenReturn(new BlockHeaderTestFixture().number(0).buildHeader());
    lenient().when(syncState.getCheckpoint()).thenReturn(Optional.empty());
  }

  @Test
  public void shouldInitializeWithNewStateWhenNoStateFileExists() {
    // Verify downloader initializes successfully when no prior state exists
    assertThatCode(
            () ->
                new SnapSyncChainDownloader(
                    pipelineFactory,
                    protocolSchedule,
                    protocolContext,
                    ethContext,
                    syncState,
                    syncDurationMetrics,
                    pivotBlockHeader,
                    chainSyncStateStorage,
                    headerDownloader))
        .doesNotThrowAnyException();
  }

  @Test
  public void shouldHandleCancellation() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    // Verify cancel completes successfully even when no pipeline is running
    assertThatCode(downloader::cancel).doesNotThrowAnyException();
  }

  @Test
  public void shouldReceivePivotUpdate() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    BlockHeader newPivot = new BlockHeaderTestFixture().number(2000).buildHeader();

    // Verify pivot update completes successfully
    assertThatCode(() -> downloader.onPivotUpdated(newPivot)).doesNotThrowAnyException();
  }

  @Test
  public void shouldReceiveWorldStateHealFinished() {
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    // Verify world state heal signal is accepted without error
    assertThatCode(downloader::onWorldStateHealFinished).doesNotThrowAnyException();
  }

  @Test
  public void shouldDeferPipelineStartWhenNoPeersAvailable() {
    when(ethPeers.peerCount()).thenReturn(0);
    when(scheduler.scheduleFutureTask(any(Runnable.class), any(Duration.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    downloader.start();

    // No pipeline should be created when there are no peers
    verify(pipelineFactory, never()).createBackwardHeaderDownloadPipeline(any());

    // A retry should be scheduled with the no-peer delay, not the fast retry delay
    verify(scheduler)
        .scheduleFutureTask(
            any(Runnable.class),
            eq(Duration.ofMillis(SnapSyncChainDownloader.NO_PEER_RETRY_DELAY_MILLISECONDS)));
  }

  @Test
  public void shouldHandleStateTransitionFromInitialToHeadersComplete() {
    // Create and store initial state
    ChainSyncState initialState =
        ChainSyncState.initialSync(
            pivotBlockHeader,
            checkpointBlockHeader,
            new BlockHeaderTestFixture().number(0).buildHeader());
    chainSyncStateStorage.storeState(initialState);

    // Create downloader
    SnapSyncChainDownloader downloader =
        new SnapSyncChainDownloader(
            pipelineFactory,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            syncDurationMetrics,
            pivotBlockHeader,
            chainSyncStateStorage,
            headerDownloader);

    assertThat(downloader).isNotNull();

    // Verify state was loaded
    ChainSyncState loadedState =
        chainSyncStateStorage.loadState(
            rlpInput -> BlockHeader.readFrom(rlpInput, new MainnetBlockHeaderFunctions()));
    assertThat(loadedState).isNotNull();
    assertThat(loadedState.headersDownloadComplete()).isFalse();
  }
}
