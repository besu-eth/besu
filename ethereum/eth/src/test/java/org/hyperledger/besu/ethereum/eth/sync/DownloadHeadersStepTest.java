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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer.blockchainResponder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.range.RangeHeaders;
import org.hyperledger.besu.ethereum.eth.sync.range.SyncTargetRange;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DownloadHeadersStepTest {

  private static final int HEADER_REQUEST_SIZE = 200;
  private static ProtocolSchedule protocolSchedule;
  private static ProtocolContext protocolContext;
  private static MutableBlockchain blockchain;

  private final EthPeer syncTarget = mock(EthPeer.class);
  private EthProtocolManager ethProtocolManager;
  private PeerTaskExecutor peerTaskExecutor;
  private DownloadHeadersStep downloader;
  private SyncTargetRange checkpointRange;

  @BeforeAll
  public static void setUpClass() {
    final BlockchainSetupUtil setupUtil = BlockchainSetupUtil.forTesting(DataStorageFormat.FOREST);
    setupUtil.importFirstBlocks(20);
    protocolSchedule = setupUtil.getProtocolSchedule();
    protocolContext = setupUtil.getProtocolContext();
    blockchain = protocolContext.getBlockchain();
  }

  @BeforeEach
  public void setUp() {
    peerTaskExecutor = Mockito.mock(PeerTaskExecutor.class);
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setBlockchain(blockchain)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();

    checkpointRange =
        new SyncTargetRange(
            syncTarget, blockchain.getBlockHeader(1).get(), blockchain.getBlockHeader(10).get());

    GetHeadersFromPeerTaskExecutorAnswer getHeadersAnswer =
        new GetHeadersFromPeerTaskExecutorAnswer(
            blockchain, ethProtocolManager.ethContext().getEthPeers());
    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenAnswer(getHeadersAnswer);
    when(peerTaskExecutor.executeAgainstPeer(any(GetHeadersFromPeerTask.class), any(EthPeer.class)))
        .thenAnswer(getHeadersAnswer);
  }

  @Test
  public void shouldRetrieveHeadersForCheckpointRange()
      throws ExecutionException, InterruptedException, TimeoutException {
    downloader =
        new DownloadHeadersStep(
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            () -> HeaderValidationMode.DETACHED_ONLY,
            HEADER_REQUEST_SIZE,
            new NoOpMetricsSystem());
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final CompletableFuture<RangeHeaders> result = downloader.apply(checkpointRange);

    peer.respond(blockchainResponder(blockchain));

    // The start of the range should have been imported as part of the previous batch hence 2-10.
    assertThat(result.get(1, TimeUnit.SECONDS))
        .isEqualTo(new RangeHeaders(checkpointRange, headersFromChain(2, 10)));
  }

  @Test
  public void shouldCancelRequestToPeerWhenReturnedFutureIsCancelled() {
    downloader =
        new DownloadHeadersStep(
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            () -> HeaderValidationMode.DETACHED_ONLY,
            HEADER_REQUEST_SIZE,
            new NoOpMetricsSystem());
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);

    final CompletableFuture<RangeHeaders> result = this.downloader.apply(checkpointRange);

    result.cancel(true);

    EthProtocolManagerTestUtil.disableEthSchedulerAutoRun(ethProtocolManager);

    peer.respond(blockchainResponder(blockchain));

    assertThat(EthProtocolManagerTestUtil.getPendingFuturesCount(ethProtocolManager)).isZero();
  }

  @Test
  public void shouldReturnOnlyEndHeaderWhenCheckpointRangeHasLengthOfOne() {
    downloader =
        new DownloadHeadersStep(
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            () -> HeaderValidationMode.DETACHED_ONLY,
            HEADER_REQUEST_SIZE,
            new NoOpMetricsSystem());
    final SyncTargetRange checkpointRange =
        new SyncTargetRange(
            syncTarget, blockchain.getBlockHeader(3).get(), blockchain.getBlockHeader(4).get());

    final CompletableFuture<RangeHeaders> result = this.downloader.apply(checkpointRange);

    assertThat(result)
        .isCompletedWithValue(new RangeHeaders(checkpointRange, headersFromChain(4, 4)));
  }

  @Test
  public void shouldGetRemainingHeadersWhenRangeHasNoEnd() {
    downloader =
        new DownloadHeadersStep(
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            () -> HeaderValidationMode.DETACHED_ONLY,
            HEADER_REQUEST_SIZE,
            new NoOpMetricsSystem());
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000);
    final SyncTargetRange checkpointRange =
        new SyncTargetRange(peer.getEthPeer(), blockchain.getBlockHeader(3).get());

    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetHeadersFromPeerTask.class)))
        .thenAnswer(
            (invocationOnMock) -> {
              GetHeadersFromPeerTask task =
                  invocationOnMock.getArgument(0, GetHeadersFromPeerTask.class);
              List<BlockHeader> result = new ArrayList<>();
              for (long i = task.getBlockNumber() + 1; i < task.getMaxHeaders(); i++) {
                Optional<BlockHeader> blockHeader = blockchain.getBlockHeader(i);
                if (blockHeader.isPresent()) {
                  result.add(blockHeader.get());
                } else {
                  // we have reached the end of the blockchain, do nothing and break out of the loop
                  break;
                }
              }
              return new PeerTaskExecutorResult<List<BlockHeader>>(
                  Optional.of(result),
                  PeerTaskExecutorResponseCode.SUCCESS,
                  Collections.emptyList());
            });

    final CompletableFuture<RangeHeaders> result = this.downloader.apply(checkpointRange);

    peer.respond(blockchainResponder(blockchain));

    assertThat(result)
        .isCompletedWithValue(new RangeHeaders(checkpointRange, headersFromChain(4, 19)));
  }

  private List<BlockHeader> headersFromChain(final long startNumber, final long endNumber) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (long i = startNumber; i <= endNumber; i++) {
      headers.add(blockchain.getBlockHeader(i).get());
    }
    return Collections.unmodifiableList(headers);
  }
}
