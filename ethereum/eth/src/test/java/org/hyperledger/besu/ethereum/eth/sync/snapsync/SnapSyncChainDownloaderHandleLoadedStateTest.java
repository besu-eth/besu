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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.BackwardHeaderDriver;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.ChainSyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.common.SingleBlockHeaderDownloader;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.pipeline.Pipeline;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Integration tests for {@link SnapSyncChainDownloader#handleLoadedState} — the branch that runs
 * when a persisted {@link ChainSyncState} is found on restart with a new pivot header.
 *
 * <p>Each test covers one branch of the decision tree:
 *
 * <ol>
 *   <li>New pivot is on the local canonical chain → skip Stage 1, go straight to Stage 2.
 *   <li>Not canonical + headers complete + newPivot {@literal >} oldPivot → restart Stage 1 from
 *       old pivot down to genesis.
 *   <li>Not canonical + headers complete + newPivot {@literal ≤} oldPivot → restart Stage 1 from
 *       new pivot with block(newPivot−1) as anchor.
 *   <li>Not canonical + headers in progress + downloaded blocks ≥ threshold → keep loaded state for
 *       this cycle and queue new pivot for next cycle.
 *   <li>Not canonical + headers in progress + downloaded blocks {@literal <} threshold → discard
 *       partial work and restart Stage 1 with new pivot.
 *   <li>Not canonical + no header progress at all → restart Stage 1 with new pivot.
 * </ol>
 *
 * <p>Uses a real in-memory blockchain so {@code headerIsOnCanonicalChain} behaves correctly, and
 * mocked pipeline infrastructure so tests complete without real network activity.
 */
@ExtendWith(MockitoExtension.class)
class SnapSyncChainDownloaderHandleLoadedStateTest {

  // ── Threshold: use a small value so above/below tests need small block ranges ───────────────
  private static final long THRESHOLD = 50L;

  // ── Block heights used across tests ──────────────────────────────────────────────────────────
  /** A block that will be stored in the blockchain (canonical). */
  private static final int CANONICAL_BLOCK = 200;

  /** Old pivot from a previous sync session. */
  private static final int OLD_PIVOT = 100;

  /** New pivot provided at restart — NOT in the local canonical chain. */
  private static final int NEW_PIVOT_HIGHER = 300;

  /** New pivot that IS in the canonical chain. */
  private static final int NEW_PIVOT_CANONICAL = CANONICAL_BLOCK;

  // ── Shared chain ─────────────────────────────────────────────────────────────────────────────
  /** Single chain of 400 blocks used across all tests. Indices 0..400 == block numbers 0..400. */
  private static List<Block> chain;

  @BeforeAll
  static void buildChain() {
    final BlockDataGenerator gen = new BlockDataGenerator(42);
    chain = gen.blockSequence(401); // genesis(0) + blocks 1..400
  }

  // ── Per-test mocks ───────────────────────────────────────────────────────────────────────────
  @Mock private SnapSyncChainDownloadPipelineFactory pipelineFactory;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private EthContext ethContext;
  @Mock private EthPeers ethPeers;
  @Mock private EthScheduler scheduler;
  @Mock private SyncState syncState;
  @Mock private SyncDurationMetrics syncDurationMetrics;

  @TempDir private Path tempDir;

  private MutableBlockchain blockchain;
  private SynchronizerConfiguration syncConfig;

  @BeforeEach
  void setUp() {
    // Blockchain pre-loaded with blocks 0..CANONICAL_BLOCK (headers only beyond genesis)
    blockchain = createInMemoryBlockchain(chain.get(0));
    blockchain.storeBlockHeaders(
        chain.subList(1, CANONICAL_BLOCK + 1).stream().map(Block::getHeader).toList());

    syncConfig =
        SynchronizerConfiguration.builder().chainSyncContinuationThresholdBlocks(THRESHOLD).build();

    // Protocol schedule wiring for ScheduleBasedBlockHeaderFunctions (state deserialization)
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    lenient()
        .when(protocolSpec.getBlockHeaderFunctions())
        .thenReturn(new MainnetBlockHeaderFunctions());

    lenient().when(ethContext.getScheduler()).thenReturn(scheduler);
    lenient().when(ethContext.getEthPeers()).thenReturn(ethPeers);
    lenient().when(ethPeers.peerCount()).thenReturn(1);
    // scheduleFutureTask is used for retry delays — run immediately in tests
    lenient()
        .when(scheduler.scheduleFutureTask(any(Runnable.class), any()))
        .thenAnswer(
            invocation -> {
              ((Runnable) invocation.getArgument(0)).run();
              return CompletableFuture.completedFuture(null);
            });
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  /** Builds the downloader with a given initial pivot header. */
  private SnapSyncChainDownloader downloader(final BlockHeader initialPivot) {
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    return new SnapSyncChainDownloader(
        pipelineFactory,
        syncConfig,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        syncDurationMetrics,
        initialPivot,
        new ChainSyncStateStorage(tempDir),
        mock(SingleBlockHeaderDownloader.class));
  }

  /**
   * Persists a {@link ChainSyncState} to disk so the downloader finds it on startup, simulating a
   * previous sync session that was interrupted before completion.
   */
  private void persistLoadedState(final ChainSyncState state) {
    new ChainSyncStateStorage(tempDir).storeState(state);
  }

  /**
   * Wires the pipeline factory to capture the {@link ChainSyncState} passed to {@code
   * createBackwardHeaderDownloadPipeline} via a latch, then complete immediately.
   *
   * @return a latch that fires when the first backward pipeline is created
   */
  @SuppressWarnings("unchecked")
  private ArgumentCaptor<ChainSyncState> wireBackwardPipeline(final CountDownLatch latch) {
    final ArgumentCaptor<ChainSyncState> captor = ArgumentCaptor.forClass(ChainSyncState.class);
    final BackwardHeaderDriver driver = mock(BackwardHeaderDriver.class);
    lenient().when(driver.getMatchedAncestor()).thenReturn(Optional.empty());
    lenient()
        .when(driver.getLowestImportedHeader())
        .thenReturn(chain.get(NEW_PIVOT_HIGHER).getHeader());

    final Pipeline<Long> backwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createBackwardHeaderDownloadPipeline(captor.capture()))
        .thenAnswer(
            invocation -> {
              latch.countDown();
              return new SnapSyncChainDownloadPipelineFactory.BackwardHeaderPipelineResult(
                  backwardPipeline, driver);
            });

    // Forward bodies pipeline — always returns immediately
    @SuppressWarnings("rawtypes")
    final Pipeline forwardPipeline = mock(Pipeline.class);
    lenient()
        .when(
            pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(anyLong(), any(), any()))
        .thenReturn(forwardPipeline);

    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));

    return captor;
  }

  /**
   * Starts the downloader and signals world-state-heal-finished once the latch fires, ensuring the
   * download loop terminates after at most one full cycle.
   */
  private void startAndTerminate(
      final SnapSyncChainDownloader downloader, final CountDownLatch firstPipelineLatch)
      throws Exception {
    final CompletableFuture<?> startFuture = downloader.start();
    // Signal world-state-heal in a daemon thread once the first pipeline call is observed
    final Thread terminator =
        new Thread(
            () -> {
              try {
                firstPipelineLatch.await(5, TimeUnit.SECONDS);
                // Give the pipeline one pass before signalling completion
                Thread.sleep(100);
                downloader.onWorldStateHealFinished();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    terminator.setDaemon(true);
    terminator.start();
    startFuture.get(10, TimeUnit.SECONDS);
  }

  // ── test cases ──────────────────────────────────────────────────────────────────────────────

  /**
   * Case 1 – new pivot IS on the local canonical chain. Expected: headers already present → {@code
   * withCanonicalPivot} → skip Stage 1, go to Stage 2.
   */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void whenNewPivotIsCanonical_skipsStage1AndRunsStage2() throws Exception {
    // Persist a previous state that had completed headers
    final ChainSyncState previous =
        ChainSyncState.initialSync(
            chain.get(OLD_PIVOT).getHeader(),
            chain.get(0).getHeader(), // bodyCheckpoint = genesis
            chain.get(OLD_PIVOT).getHeader());
    persistLoadedState(previous.withHeadersDownloadComplete());

    // Forward pipeline captures the Stage-2 anchor block
    final CountDownLatch forwardLatch = new CountDownLatch(1);
    final ArgumentCaptor<Long> anchorCaptor = ArgumentCaptor.forClass(Long.class);
    final Pipeline forwardPipeline = mock(Pipeline.class);
    when(pipelineFactory.createForwardBodiesAndReceiptsDownloadPipeline(
            anchorCaptor.capture(), any(), any()))
        .thenAnswer(
            invocation -> {
              forwardLatch.countDown();
              return forwardPipeline;
            });
    when(scheduler.startPipeline(any())).thenReturn(CompletableFuture.completedFuture(null));

    final SnapSyncChainDownloader dl = downloader(chain.get(NEW_PIVOT_CANONICAL).getHeader());
    final Thread terminator =
        new Thread(
            () -> {
              try {
                forwardLatch.await(5, TimeUnit.SECONDS);
                Thread.sleep(100);
                dl.onWorldStateHealFinished();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    terminator.setDaemon(true);
    terminator.start();
    dl.start().get(10, TimeUnit.SECONDS);

    // The key property: Stage 1 must never have been requested (headers already present
    // for a canonical pivot — withCanonicalPivot sets headersDownloadComplete = true).
    verify(pipelineFactory, never()).createBackwardHeaderDownloadPipeline(any());

    // Stage 2 must have been started. The anchor is 0 (genesis — only block with a body
    // since we only stored headers). This is the correct behaviour: Stage 2 will import
    // all bodies from genesis to the pivot.
    assertThat(anchorCaptor.getValue())
        .as("Stage 2 anchor must be 0 (genesis, the only block with a stored body)")
        .isEqualTo(0L);
  }

  /**
   * Case 2 – not canonical, headers complete, newPivot {@literal >} oldPivot. Expected: restart
   * Stage 1 from newPivot down to oldPivot (which becomes the headerAnchor).
   */
  @Test
  void whenNotCanonical_headersComplete_newPivotHigher_restartsFromOldPivotAsAnchor()
      throws Exception {
    final BlockHeader oldPivotHeader = chain.get(OLD_PIVOT).getHeader();
    final BlockHeader newPivotHeader =
        chain.get(NEW_PIVOT_HIGHER).getHeader(); // 300, not canonical

    final ChainSyncState previous =
        ChainSyncState.initialSync(
                oldPivotHeader,
                chain.get(0).getHeader(), // bodyCheckpoint = genesis
                oldPivotHeader)
            .withHeadersDownloadComplete();
    persistLoadedState(previous);

    final CountDownLatch latch = new CountDownLatch(1);
    final ArgumentCaptor<ChainSyncState> captor = wireBackwardPipeline(latch);

    final SnapSyncChainDownloader dl = downloader(newPivotHeader);
    startAndTerminate(dl, latch);

    final ChainSyncState computed = captor.getValue();
    assertThat(computed.pivotBlockHeader())
        .as("Stage 1 must download to the new pivot")
        .isEqualTo(newPivotHeader);
    assertThat(computed.headerDownloadAnchor())
        .as("Stage 1 must stop at the old pivot (which is the header anchor)")
        .isEqualTo(oldPivotHeader);
    assertThat(computed.headersDownloadComplete())
        .as("headers must be reset to incomplete so Stage 1 actually runs")
        .isFalse();
  }

  /**
   * Case 3 – not canonical, headers complete, newPivot {@literal ≤} oldPivot. Expected: restart
   * Stage 1 from newPivot with block(newPivot−1) as anchor.
   */
  @Test
  void whenNotCanonical_headersComplete_newPivotLower_restartsWithBlockBelowNewPivotAsAnchor()
      throws Exception {
    // NEW_PIVOT_LOWER = 80, oldPivot = 100. Block 80 is NOT canonical (only 0..200 stored,
    // but block 80 IS in the blockchain via storeBlockHeaders). Wait — block 80 is stored, so
    // headerIsOnCanonicalChain(block 80) = true. We need a pivot that is truly not canonical.
    // Use a header from a DIFFERENT generated chain so its hash is not in the blockchain.
    final BlockDataGenerator otherGen = new BlockDataGenerator(99);
    final List<Block> otherChain = otherGen.blockSequence(81); // genesis..block(80)
    final BlockHeader nonCanonicalPivotAt80 = otherChain.get(80).getHeader();
    // nonCanonicalPivotAt80 is at height 80 but its hash is not in our blockchain

    final BlockHeader oldPivotHeader = chain.get(OLD_PIVOT).getHeader(); // height 100
    final ChainSyncState previous =
        ChainSyncState.initialSync(oldPivotHeader, chain.get(0).getHeader(), oldPivotHeader)
            .withHeadersDownloadComplete();
    persistLoadedState(previous);

    final CountDownLatch latch = new CountDownLatch(1);
    final ArgumentCaptor<ChainSyncState> captor = wireBackwardPipeline(latch);

    final SnapSyncChainDownloader dl = downloader(nonCanonicalPivotAt80);
    startAndTerminate(dl, latch);

    final ChainSyncState computed = captor.getValue();
    assertThat(computed.pivotBlockHeader())
        .as("pivot must be the new (lower) pivot")
        .isEqualTo(nonCanonicalPivotAt80);
    assertThat(computed.headerDownloadAnchor().getNumber())
        .as("anchor must be block(newPivot−1) = 79")
        .isEqualTo(79L);
    assertThat(computed.headersDownloadComplete()).isFalse();
  }

  /**
   * Case 4 – not canonical, headers in progress, downloaded blocks ≥ threshold. Expected: keep
   * loaded state for this cycle; queue new pivot for the next cycle. Verified by checking Stage 1
   * uses the LOADED state's pivot, not the new pivot.
   */
  @Test
  void whenNotCanonical_headersInProgress_aboveThreshold_keepsLoadedStateForCurrentCycle()
      throws Exception {
    // Simulate progress: THRESHOLD+10 headers already downloaded
    final BlockHeader progressHeader = chain.get((int) (OLD_PIVOT - THRESHOLD - 10)).getHeader();
    // headersDownloaded = OLD_PIVOT - progress.getNumber() = THRESHOLD+10 >= THRESHOLD ✓

    final BlockHeader oldPivotHeader = chain.get(OLD_PIVOT).getHeader();
    final ChainSyncState previous =
        ChainSyncState.initialSync(oldPivotHeader, chain.get(0).getHeader(), oldPivotHeader);
    persistLoadedState(previous.withHeaderProgress(progressHeader));

    // New pivot is NOT canonical
    final BlockDataGenerator otherGen = new BlockDataGenerator(77);
    final BlockHeader newNonCanonicalPivot =
        otherGen.blockSequence(NEW_PIVOT_HIGHER + 1).get(NEW_PIVOT_HIGHER).getHeader();

    final CountDownLatch latch = new CountDownLatch(1);
    final ArgumentCaptor<ChainSyncState> captor = wireBackwardPipeline(latch);

    final SnapSyncChainDownloader dl = downloader(newNonCanonicalPivot);
    startAndTerminate(dl, latch);

    // The FIRST Stage-1 call must use the OLD pivot (loaded state preserved)
    final ChainSyncState firstComputed = captor.getAllValues().get(0);
    assertThat(firstComputed.pivotBlockHeader())
        .as("first Stage-1 cycle must use the loaded state's old pivot, not the new one")
        .isEqualTo(oldPivotHeader);
  }

  /**
   * Case 5 – not canonical, headers in progress, downloaded blocks {@literal <} threshold.
   * Expected: discard partial work and restart Stage 1 with new pivot.
   */
  @Test
  void whenNotCanonical_headersInProgress_belowThreshold_restartsWithNewPivot() throws Exception {
    // Simulate tiny progress: only 5 headers downloaded, well below threshold
    final BlockHeader progressHeader = chain.get(OLD_PIVOT - 5).getHeader();
    // headersDownloaded = OLD_PIVOT - (OLD_PIVOT-5) = 5 < THRESHOLD ✓

    final BlockHeader oldPivotHeader = chain.get(OLD_PIVOT).getHeader();
    final ChainSyncState previous =
        ChainSyncState.initialSync(oldPivotHeader, chain.get(0).getHeader(), oldPivotHeader);
    persistLoadedState(previous.withHeaderProgress(progressHeader));

    final BlockDataGenerator otherGen = new BlockDataGenerator(55);
    final BlockHeader newNonCanonicalPivot =
        otherGen.blockSequence(NEW_PIVOT_HIGHER + 1).get(NEW_PIVOT_HIGHER).getHeader();

    final CountDownLatch latch = new CountDownLatch(1);
    final ArgumentCaptor<ChainSyncState> captor = wireBackwardPipeline(latch);

    final SnapSyncChainDownloader dl = downloader(newNonCanonicalPivot);
    startAndTerminate(dl, latch);

    final ChainSyncState computed = captor.getValue();
    assertThat(computed.pivotBlockHeader())
        .as("Stage 1 must use the new pivot after discarding partial work")
        .isEqualTo(newNonCanonicalPivot);
    assertThat(computed.headerDownloadAnchor())
        .as("anchor must be preserved from the loaded state")
        .isEqualTo(oldPivotHeader);
    assertThat(computed.headerDownloadProgress())
        .as("progress must be cleared on restart")
        .isNull();
  }

  /**
   * Case 6 – not canonical, no header progress at all. Expected: restart Stage 1 with the new pivot
   * from scratch.
   */
  @Test
  void whenNotCanonical_noHeaderProgress_restartsWithNewPivot() throws Exception {
    final BlockHeader oldPivotHeader = chain.get(OLD_PIVOT).getHeader();
    final ChainSyncState previous =
        ChainSyncState.initialSync(oldPivotHeader, chain.get(0).getHeader(), oldPivotHeader);
    // No withHeaderProgress call — progress is null
    persistLoadedState(previous);

    final BlockDataGenerator otherGen = new BlockDataGenerator(33);
    final BlockHeader newNonCanonicalPivot =
        otherGen.blockSequence(NEW_PIVOT_HIGHER + 1).get(NEW_PIVOT_HIGHER).getHeader();

    final CountDownLatch latch = new CountDownLatch(1);
    final ArgumentCaptor<ChainSyncState> captor = wireBackwardPipeline(latch);

    final SnapSyncChainDownloader dl = downloader(newNonCanonicalPivot);
    startAndTerminate(dl, latch);

    final ChainSyncState computed = captor.getValue();
    assertThat(computed.pivotBlockHeader())
        .as("Stage 1 must use the new pivot")
        .isEqualTo(newNonCanonicalPivot);
    assertThat(computed.headerDownloadAnchor())
        .as("anchor must be preserved from the loaded state")
        .isEqualTo(oldPivotHeader);
    assertThat(computed.headerDownloadProgress())
        .as("no progress to preserve — must be null")
        .isNull();
  }
}
