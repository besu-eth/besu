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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DownloadSyncBodiesStepTest {

  private PeerTaskExecutor peerTaskExecutor;
  private EthContext ethContext;
  private ProtocolSchedule protocolSchedule;

  @BeforeEach
  public void setUp() {
    peerTaskExecutor = mock(PeerTaskExecutor.class);
    protocolSchedule = mock(ProtocolSchedule.class);
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    when(protocolSpec.isPoS()).thenReturn(false);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    ethContext = mock(EthContext.class);
    when(ethContext.getScheduler()).thenReturn(new DeterministicEthScheduler(() -> false));
    when(ethContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);
  }

  @Test
  public void shouldDownloadBodiesSuccessfully() throws ExecutionException, InterruptedException {
    final List<BlockHeader> headers = createHeaders(5);
    final List<SyncBlock> syncBlocks = createSyncBlocks(5);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(success(syncBlocks));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(5).isEqualTo(syncBlocks);
    verify(peerTaskExecutor, times(1)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRequestOnlyRemainingBodiesAfterPartialSuccess()
      throws ExecutionException, InterruptedException {
    // 5 headers requested; first peer returns 3 bodies, second returns the last 2
    final List<BlockHeader> headers = createHeaders(5);
    final List<SyncBlock> firstBatch = createSyncBlocks(3);
    final List<SyncBlock> secondBatch = createSyncBlocks(2);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(success(firstBatch), success(secondBatch));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(5);
    assertThat(result.subList(0, 3)).isEqualTo(firstBatch);
    assertThat(result.subList(3, 5)).isEqualTo(secondBatch);
    verify(peerTaskExecutor, times(2)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryOnNoPeerAvailable() throws ExecutionException, InterruptedException {
    final List<BlockHeader> headers = createHeaders(3);
    final List<SyncBlock> syncBlocks = createSyncBlocks(3);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE), success(syncBlocks));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(3).isEqualTo(syncBlocks);
    verify(peerTaskExecutor, times(2)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryMultipleTimesBeforeSuccess()
      throws ExecutionException, InterruptedException {
    final List<BlockHeader> headers = createHeaders(4);
    final List<SyncBlock> syncBlocks = createSyncBlocks(4);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(
            failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE),
            failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE),
            failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE),
            success(syncBlocks));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(4).isEqualTo(syncBlocks);
    verify(peerTaskExecutor, times(4)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryOnDifferentErrorCodes() throws ExecutionException, InterruptedException {
    final List<BlockHeader> headers = createHeaders(3);
    final List<SyncBlock> syncBlocks = createSyncBlocks(3);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(
            failure(PeerTaskExecutorResponseCode.PEER_DISCONNECTED),
            failure(PeerTaskExecutorResponseCode.TIMEOUT),
            success(syncBlocks));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(3).isEqualTo(syncBlocks);
    verify(peerTaskExecutor, times(3)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldPreservePartialDownloadOnRetry()
      throws ExecutionException, InterruptedException {
    // 5 headers; first request returns 3 bodies, second fails, third returns the remaining 2
    final List<BlockHeader> headers = createHeaders(5);
    final List<SyncBlock> firstBatch = createSyncBlocks(3);
    final List<SyncBlock> secondBatch = createSyncBlocks(2);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(
            success(firstBatch),
            failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE),
            success(secondBatch));

    final List<SyncBlock> result = createStep().apply(new ArrayList<>(headers)).get();

    assertThat(result).hasSize(5);
    assertThat(result.subList(0, 3)).isEqualTo(firstBatch);
    assertThat(result.subList(3, 5)).isEqualTo(secondBatch);
    verify(peerTaskExecutor, times(3)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  public void shouldNotRetryOnInternalServerError() {
    final List<BlockHeader> headers = createHeaders(3);

    when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
        .thenReturn(failure(PeerTaskExecutorResponseCode.INTERNAL_SERVER_ERROR));

    final CompletableFuture<List<SyncBlock>> result = createStep().apply(new ArrayList<>(headers));

    assertThatThrownBy(result::get)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to download bodies for 3 blocks");

    verify(peerTaskExecutor, times(1)).execute(any(GetSyncBlockBodiesFromPeerTask.class));
  }

  @Test
  public void shouldTimeoutAfterConfiguredDuration() throws Exception {
    final EthScheduler realScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final EthContext realEthContext = mock(EthContext.class);
    when(realEthContext.getScheduler()).thenReturn(realScheduler);
    when(realEthContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);

    try {
      final DownloadSyncBodiesStep step =
          new DownloadSyncBodiesStep(protocolSchedule, realEthContext, Duration.ofMillis(100));

      when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
          .thenReturn(failure(PeerTaskExecutorResponseCode.TIMEOUT));

      final CompletableFuture<List<SyncBlock>> result =
          step.apply(new ArrayList<>(createHeaders(3)));

      assertThatThrownBy(result::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
    } finally {
      realScheduler.stop();
      realScheduler.awaitStop();
    }
  }

  @Test
  public void shouldNotRetryAfterTimeout() throws Exception {
    // The first request fails transiently, scheduling a retry after RETRY_DELAY (1 s).
    // The overall timeout fires before that retry, setting the cancelled flag.
    // When the retry eventually runs it must observe cancelled=true and exit without
    // making a second peer request.
    final EthScheduler realScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final EthContext realEthContext = mock(EthContext.class);
    when(realEthContext.getScheduler()).thenReturn(realScheduler);
    when(realEthContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);

    try {
      final DownloadSyncBodiesStep step =
          new DownloadSyncBodiesStep(protocolSchedule, realEthContext, Duration.ofMillis(500));

      when(peerTaskExecutor.execute(any(GetSyncBlockBodiesFromPeerTask.class)))
          .thenReturn(failure(PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE));

      final CompletableFuture<List<SyncBlock>> result =
          step.apply(new ArrayList<>(createHeaders(3)));

      // Timeout fires at ~500 ms; the retry is scheduled ~1 000 ms after the first failure.
      assertThatThrownBy(result::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);

      verify(peerTaskExecutor, after(1200).times(1))
          .execute(any(GetSyncBlockBodiesFromPeerTask.class));
    } finally {
      realScheduler.stop();
      realScheduler.awaitStop();
    }
  }

  // ---- helpers ----

  private DownloadSyncBodiesStep createStep() {
    return new DownloadSyncBodiesStep(protocolSchedule, ethContext, Duration.ofMinutes(1));
  }

  private List<BlockHeader> createHeaders(final int count) {
    return IntStream.range(0, count).mapToObj(i -> mock(BlockHeader.class)).toList();
  }

  private List<SyncBlock> createSyncBlocks(final int count) {
    return IntStream.range(0, count).mapToObj(i -> mock(SyncBlock.class)).toList();
  }

  private static PeerTaskExecutorResult<List<SyncBlock>> success(final List<SyncBlock> blocks) {
    return new PeerTaskExecutorResult<>(
        Optional.of(blocks), PeerTaskExecutorResponseCode.SUCCESS, emptyList());
  }

  private static PeerTaskExecutorResult<List<SyncBlock>> failure(
      final PeerTaskExecutorResponseCode code) {
    return new PeerTaskExecutorResult<>(Optional.empty(), code, emptyList());
  }
}
