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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.ParsedExtraData;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DownloadBackwardHeadersStepTest {

  private static ProtocolContext protocolContext;
  private static ProtocolSchedule protocolSchedule;
  private static MutableBlockchain blockchain;

  private PeerTaskExecutor peerTaskExecutor;
  private EthProtocolManager ethProtocolManager;
  private EthContext ethContext;

  @BeforeAll
  public static void setUpClass() {
    final BlockchainSetupUtil setupUtil = BlockchainSetupUtil.forTesting(DataStorageFormat.FOREST);
    setupUtil.importFirstBlocks(20);
    protocolContext = setupUtil.getProtocolContext();
    protocolSchedule = setupUtil.getProtocolSchedule();
    blockchain = setupUtil.getBlockchain();
  }

  @BeforeEach
  public void setUp() {
    peerTaskExecutor = mock(PeerTaskExecutor.class);
    TransactionPool transactionPool = mock(TransactionPool.class);
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(ProtocolScheduleFixture.TESTING_NETWORK)
            .setBlockchain(blockchain)
            .setEthScheduler(new DeterministicEthScheduler(() -> false))
            .setWorldStateArchive(protocolContext.getWorldStateArchive())
            .setTransactionPool(transactionPool)
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
    ethContext = ethProtocolManager.ethContext();
  }

  @Test
  public void shouldDownloadHeadersSuccessfully() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    assertThat(result.get()).isEqualTo(mockHeaders);
    verify(peerTaskExecutor, times(1)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCalculateCorrectHeadersToRequest()
      throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 100, 50, Duration.ofMinutes(1));

    // startBlock = 100, trustAnchor = 50, so remainingHeaders = 50
    // Math.min(100, 50) = 50 headers should be requested
    // First call returns 30, second call returns remaining 20
    final List<BlockHeader> firstBatch = createMockHeaders(30, 100);
    final List<BlockHeader> secondBatch = createMockHeaders(20, 70);

    final PeerTaskExecutorResult<List<BlockHeader>> firstResult =
        new PeerTaskExecutorResult<>(
            Optional.of(firstBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> secondResult =
        new PeerTaskExecutorResult<>(
            Optional.of(secondBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(firstResult, secondResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    // Should collect all 50 headers
    assertThat(result.get()).hasSize(50);
    verify(peerTaskExecutor, times(2)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryOnNoPeerAvailable() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 5, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(5, 100);

    // First call: NO_PEER_AVAILABLE, second call: SUCCESS
    final PeerTaskExecutorResult<List<BlockHeader>> noPeerResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(noPeerResult, successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(5);
    verify(peerTaskExecutor, times(2)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  public void shouldFailOnInternalServerError() {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final PeerTaskExecutorResult<List<BlockHeader>> errorResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.INTERNAL_SERVER_ERROR, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(errorResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThatThrownBy(result::get)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to download 10 headers starting from block 100");
  }

  @Test
  public void shouldHandleSmallRemainingHeaders() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 100, 95, Duration.ofMinutes(1));

    // startBlock = 100, trustAnchor = 95, so only 5 headers should be requested
    final List<BlockHeader> mockHeaders = createMockHeaders(5, 100);
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(5);
    verify(peerTaskExecutor, times(1)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldHandleEmptySuccessResponse() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 10, 90, Duration.ofMinutes(1));

    // First response is empty (peer misbehaving), second has the data
    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);

    final PeerTaskExecutorResult<List<BlockHeader>> emptyResult =
        new PeerTaskExecutorResult<>(
            Optional.of(emptyList()), PeerTaskExecutorResponseCode.SUCCESS, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(emptyResult, successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    verify(peerTaskExecutor, times(2)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  public void shouldHandleVeryLargeBlockNumbers() throws ExecutionException, InterruptedException {
    final long startBlock = Long.MAX_VALUE - 1000;
    final long trustAnchor = Long.MAX_VALUE - 2000;

    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 500, trustAnchor, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(500, startBlock);
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(startBlock);

    assertThat(result.get()).hasSize(500);
    verify(peerTaskExecutor, times(1)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryMultipleTimesBeforeSuccess()
      throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);

    // Fail 3 times with NO_PEER_AVAILABLE, then succeed on 4th attempt
    final PeerTaskExecutorResult<List<BlockHeader>> noPeerResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(noPeerResult, noPeerResult, noPeerResult, successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    assertThat(result.get()).isEqualTo(mockHeaders);
    verify(peerTaskExecutor, times(4)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRetryOnDifferentErrorCodes() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);

    // Test different error codes that should trigger retry
    final PeerTaskExecutorResult<List<BlockHeader>> peerDisconnectedResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.PEER_DISCONNECTED, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> timeoutResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(peerDisconnectedResult, timeoutResult, successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    verify(peerTaskExecutor, times(3)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldPreservePartialDownloadOnRetry()
      throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 50, 0, Duration.ofMinutes(1));

    // First batch: successfully download 30 headers
    final List<BlockHeader> firstBatch = createMockHeaders(30, 100);
    // Second batch: fail with NO_PEER_AVAILABLE
    // Third batch: successfully download remaining 20 headers
    final List<BlockHeader> secondBatch = createMockHeaders(20, 70);

    final PeerTaskExecutorResult<List<BlockHeader>> firstSuccessResult =
        new PeerTaskExecutorResult<>(
            Optional.of(firstBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> noPeerResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> secondSuccessResult =
        new PeerTaskExecutorResult<>(
            Optional.of(secondBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(firstSuccessResult, noPeerResult, secondSuccessResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    // Should have all 50 headers despite the retry
    assertThat(result.get()).hasSize(50);
    verify(peerTaskExecutor, times(3)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSucceedOnExactlyFifthRetry() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);

    // Fail 5 times, then succeed on the 6th attempt (5th retry)
    final PeerTaskExecutorResult<List<BlockHeader>> noPeerResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(
            noPeerResult, noPeerResult, noPeerResult, noPeerResult, noPeerResult, successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    assertThat(result.get()).isEqualTo(mockHeaders);
    // Should attempt 6 times total (initial + 5 retries)
    verify(peerTaskExecutor, times(6)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldHandleMixedFailureTypesDuringRetries()
      throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(10, 100);

    // Mix of different failure types before success
    final PeerTaskExecutorResult<List<BlockHeader>> noPeerResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> timeoutResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> peerDisconnectedResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.PEER_DISCONNECTED, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> invalidResponseResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.INVALID_RESPONSE, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(
            noPeerResult,
            timeoutResult,
            peerDisconnectedResult,
            invalidResponseResult,
            successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(10);
    assertThat(result.get()).isEqualTo(mockHeaders);
    verify(peerTaskExecutor, times(5)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  public void shouldNotRetryOnInternalServerError() {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 10, 0, Duration.ofMinutes(1));

    final PeerTaskExecutorResult<List<BlockHeader>> errorResult =
        new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.INTERNAL_SERVER_ERROR, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(errorResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThatThrownBy(result::get)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to download 10 headers starting from block 100");

    // Should only attempt once, no retries on INTERNAL_SERVER_ERROR
    verify(peerTaskExecutor, times(1)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  public void shouldThrowExceptionWhenHeaderRequestSizeIsZero() {
    assertThatThrownBy(
            () ->
                new DownloadBackwardHeadersStep(
                    protocolSchedule, ethContext, 0, 0, Duration.ofMinutes(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("headerRequestSize must be >= 1");
  }

  @Test
  public void shouldThrowExceptionWhenHeaderRequestSizeIsNegative() {
    assertThatThrownBy(
            () ->
                new DownloadBackwardHeadersStep(
                    protocolSchedule, ethContext, -1, 0, Duration.ofMinutes(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("headerRequestSize must be >= 1");
  }

  @Test
  public void shouldAcceptHeaderRequestSizeOfOne() throws ExecutionException, InterruptedException {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 1, 0, Duration.ofMinutes(1));

    final List<BlockHeader> mockHeaders = createMockHeaders(1, 100);
    final PeerTaskExecutorResult<List<BlockHeader>> successResult =
        new PeerTaskExecutorResult<>(
            Optional.of(mockHeaders), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(successResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThat(result.get()).hasSize(1);
    verify(peerTaskExecutor, times(1)).execute(any(GetHeadersFromPeerTask.class));
  }

  @Test
  public void shouldThrowExceptionWhenStartBlockEqualsAnchor() {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 10, 100, Duration.ofMinutes(1));

    assertThatThrownBy(() -> step.apply(100L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Number of headers to request is less than 1:0");
  }

  @Test
  public void shouldThrowExceptionWhenStartBlockLessThanAnchor() {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(
            protocolSchedule, ethContext, 10, 100, Duration.ofMinutes(1));

    assertThatThrownBy(() -> step.apply(50L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Number of headers to request is less than 1:");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowExceptionWhenParentHashDoesNotMatch() {
    final DownloadBackwardHeadersStep step =
        new DownloadBackwardHeadersStep(protocolSchedule, ethContext, 50, 0, Duration.ofMinutes(1));

    // Create first batch with specific hash
    final BlockHeaderFunctions bhf = new LocalBlockHeaderFunctions();
    final List<BlockHeader> firstBatch = new ArrayList<>();
    firstBatch.add(
        new BlockHeaderTestFixture()
            .number(100)
            .blockHeaderFunctions(bhf)
            .parentHash(Hash.EMPTY)
            .buildHeader());

    // Create second batch with DIFFERENT parent hash that doesn't match
    final Hash wrongParentHash = Hash.fromHexStringLenient("0x5678"); // Wrong!
    final List<BlockHeader> secondBatch = new ArrayList<>();
    secondBatch.add(
        new BlockHeaderTestFixture()
            .number(99)
            .blockHeaderFunctions(bhf)
            .parentHash(wrongParentHash) // This should match firstBatch.getLast().getHash()
            .buildHeader());

    final PeerTaskExecutorResult<List<BlockHeader>> firstResult =
        new PeerTaskExecutorResult<>(
            Optional.of(firstBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());
    final PeerTaskExecutorResult<List<BlockHeader>> secondResult =
        new PeerTaskExecutorResult<>(
            Optional.of(secondBatch), PeerTaskExecutorResponseCode.SUCCESS, emptyList());

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(firstResult, secondResult);

    final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

    assertThatThrownBy(result::get)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Parent hash of last header does not match first header");
  }

  @Test
  public void shouldTimeoutAfterConfiguredDuration() throws Exception {
    // Create a real EthScheduler (not deterministic) to test timeout behavior
    final EthScheduler realScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final EthContext realEthContext = mock(EthContext.class);
    when(realEthContext.getScheduler()).thenReturn(realScheduler);
    when(realEthContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);

    try {
      // Create a new instance with a very short timeout for testing (100ms)
      final DownloadBackwardHeadersStep shortTimeoutStep =
          new DownloadBackwardHeadersStep(
              protocolSchedule, realEthContext, 10, 0, Duration.ofMillis(100));

      // Mock continuous failures that would retry indefinitely without timeout
      final PeerTaskExecutorResult<List<BlockHeader>> failureResult =
          new PeerTaskExecutorResult<>(
              Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, emptyList());

      when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenReturn(failureResult);

      // When: downloading headers with short timeout
      final CompletableFuture<List<BlockHeader>> result = shortTimeoutStep.apply(100L);

      // Then: should timeout quickly with TimeoutException (wrapped in ExecutionException)
      assertThatThrownBy(result::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
    } finally {
      // Clean up the real scheduler
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
      final DownloadBackwardHeadersStep step =
          new DownloadBackwardHeadersStep(
              protocolSchedule, realEthContext, 10, 0, Duration.ofMillis(500));

      when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
          .thenReturn(
              new PeerTaskExecutorResult<>(
                  Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList()));

      final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

      // Timeout fires at ~500 ms; the retry is scheduled ~1 000 ms after the first failure.
      assertThatThrownBy(result::get)
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);

      verify(peerTaskExecutor, after(1200).times(1)).execute(any(GetHeadersFromPeerTask.class));
    } finally {
      realScheduler.stop();
      realScheduler.awaitStop();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotRetryAfterCancellation() throws Exception {
    // The first request fails transiently, scheduling a retry after RETRY_DELAY (1 s).
    // The pipeline cancels the future (as AsyncOperationProcessor.abort() does) before
    // the retry fires.  The cancelled flag must be set so the retry exits immediately
    // without making a second peer request.
    final EthScheduler realScheduler = new EthScheduler(1, 1, 1, new NoOpMetricsSystem());
    final EthContext realEthContext = mock(EthContext.class);
    when(realEthContext.getScheduler()).thenReturn(realScheduler);
    when(realEthContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);

    try {
      final DownloadBackwardHeadersStep step =
          new DownloadBackwardHeadersStep(
              protocolSchedule, realEthContext, 10, 0, Duration.ofMinutes(1));

      // Use a latch so we can wait until the first execute() has actually run before
      // cancelling. Without this, cancel() can race with the scheduler starting the
      // service task, causing the initial execute() call to never happen and the
      // verify() below to report WantedButNotInvoked.
      final CountDownLatch firstCallLatch = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                firstCallLatch.countDown();
                return new PeerTaskExecutorResult<>(
                    Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, emptyList());
              })
          .when(peerTaskExecutor)
          .execute(any(GetHeadersFromPeerTask.class));

      final CompletableFuture<List<BlockHeader>> result = step.apply(100L);

      // Wait until the first execute() has run before cancelling, so we are guaranteed
      // to be racing only the retry (scheduled 1 s later) and not the initial attempt.
      assertThat(firstCallLatch.await(5, TimeUnit.SECONDS))
          .as("peerTaskExecutor.execute should be called within 5 s")
          .isTrue();

      // Simulate pipeline abort cancelling the future before the 1-second retry fires.
      result.cancel(true);
      assertThat(result.isCancelled()).isTrue();

      // Wait 2 s — longer than the 1 s RETRY_DELAY — and confirm no second call was made.
      verify(peerTaskExecutor, after(2000).times(1)).execute(any(GetHeadersFromPeerTask.class));
    } finally {
      realScheduler.stop();
      realScheduler.awaitStop();
    }
  }

  private List<BlockHeader> createMockHeaders(final int count, final long startBlock) {
    final BlockHeaderFunctions bhf = new LocalBlockHeaderFunctions();
    final List<BlockHeader> headers = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      headers.add(
          new BlockHeaderTestFixture()
              .number(startBlock - i)
              .blockHeaderFunctions(bhf)
              .parentHash(Hash.EMPTY)
              .buildHeader());
    }
    return headers;
  }

  static class LocalBlockHeaderFunctions implements BlockHeaderFunctions {

    @Override
    public Hash hash(final BlockHeader header) {
      return Hash.EMPTY;
    }

    @Override
    public ParsedExtraData parseExtraData(final BlockHeader header) {
      return null;
    }
  }
}
