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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

public class PivotBlockRetrieverTest {

  private static final long PIVOT_BLOCK_NUMBER = 10;

  private EthProtocolManager ethProtocolManager;
  private MutableBlockchain blockchain;
  private TransactionPool transactionPool;
  private PivotBlockRetriever pivotBlockRetriever;
  private ProtocolSchedule protocolSchedule;
  private PeerTaskExecutor peerTaskExecutor;

  static class PivotBlockRetrieverTestArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageFormat.BONSAI), Arguments.of(DataStorageFormat.FOREST));
    }
  }

  public void setUp(final DataStorageFormat storageFormat) {
    final BlockchainSetupUtil blockchainSetupUtil = BlockchainSetupUtil.forTesting(storageFormat);
    blockchainSetupUtil.importAllBlocks();
    blockchain = blockchainSetupUtil.getBlockchain();
    protocolSchedule = blockchainSetupUtil.getProtocolSchedule();
    transactionPool = blockchainSetupUtil.getTransactionPool();
    peerTaskExecutor = Mockito.mock(PeerTaskExecutor.class);
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(blockchain)
            .setEthScheduler(new EthScheduler(1, 1, 1, new NoOpMetricsSystem()))
            .setWorldStateArchive(blockchainSetupUtil.getWorldArchive())
            .setTransactionPool(transactionPool)
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();

    pivotBlockRetriever = createPivotBlockRetriever(3, 1, 1);
  }

  private PivotBlockRetriever createPivotBlockRetriever(
      final int peersToQuery, final long pivotBlockDelta, final int maxRetries) {
    return pivotBlockRetriever =
        Mockito.spy(
            new PivotBlockRetriever(
                protocolSchedule,
                ethProtocolManager.ethContext(),
                PIVOT_BLOCK_NUMBER,
                peersToQuery,
                pivotBlockDelta,
                maxRetries));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldSucceedWhenAllPeersAgree(final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerC)));

    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC));

    assertThat(future)
        .isCompletedWithValue(
            new PivotSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get(), false));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldQueryBestPeersFirst(final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    pivotBlockRetriever = createPivotBlockRetriever(2, 1, 1);

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(1000), 1000)
            .getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(500), 500)
            .getEthPeer();
    final EthPeer peerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.of(1000), 1000)
            .getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerC)));

    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor, Mockito.never())
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC));

    assertThat(future)
        .isCompletedWithValue(
            new PivotSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get(), false));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldRecoverFromUnresponsivePeer(final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    pivotBlockRetriever = createPivotBlockRetriever(2, 1, 1);

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerC =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 500).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenAnswer(
            (invocationOnMock) -> {
              Thread.sleep(200);
              return new PeerTaskExecutorResult<>(
                  Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                  PeerTaskExecutorResponseCode.SUCCESS,
                  List.of(peerA));
            });
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB)))
        .thenAnswer(
            (invocationOnMock) -> {
              Thread.sleep(200);
              return new PeerTaskExecutorResult<>(
                  Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, List.of(peerB));
            });

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerC)));

    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerC));

    assertThat(future)
        .isCompletedWithValue(
            new PivotSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get(), false));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldRetryWhenPeersDisagreeOnPivot_successfulRetry(
      final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    final long pivotBlockDelta = 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER - 1).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(
                    List.of(
                        new BlockHeaderTestFixture()
                            .number(PIVOT_BLOCK_NUMBER)
                            .extraData(Bytes.of(1))
                            .buildHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER - 1).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)));
    // Execute task and wait for response
    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor, Mockito.times(2))
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor, Mockito.times(2))
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));

    assertThat(future)
        .isCompletedWithValue(
            new PivotSyncState(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER - 1).get(), false));
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldRetryWhenPeersDisagreeOnPivot_exceedMaxRetries(
      final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    final long pivotBlockDelta = 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER - 1).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(
                    List.of(
                        new BlockHeaderTestFixture()
                            .number(PIVOT_BLOCK_NUMBER)
                            .extraData(Bytes.of(1))
                            .buildHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(
                    List.of(
                        new BlockHeaderTestFixture()
                            .number(PIVOT_BLOCK_NUMBER - 1)
                            .extraData(Bytes.of(1))
                            .buildHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)));

    // Execute task and wait for response
    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor, Mockito.times(2))
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor, Mockito.times(2))
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));

    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .hasRootCauseInstanceOf(SyncException.class)
        .extracting(e -> ((SyncException) ExceptionUtils.rootCause(e)).getError())
        .isEqualTo(SyncError.PIVOT_BLOCK_HEADER_MISMATCH);
  }

  @ParameterizedTest
  @ArgumentsSource(PivotBlockRetrieverTestArguments.class)
  public void shouldRetryWhenPeersDisagreeOnPivot_pivotInvalidOnRetry(
      final DataStorageFormat storageFormat) {
    setUp(storageFormat);
    final long pivotBlockDelta = PIVOT_BLOCK_NUMBER + 1;
    pivotBlockRetriever = createPivotBlockRetriever(2, pivotBlockDelta, 1);

    final EthPeer peerA =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();
    final EthPeer peerB =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 1000).getEthPeer();

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(blockchain.getBlockHeader(PIVOT_BLOCK_NUMBER).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerA)));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(
                    List.of(
                        new BlockHeaderTestFixture()
                            .number(PIVOT_BLOCK_NUMBER)
                            .extraData(Bytes.of(1))
                            .buildHeader())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(peerB)));

    // Execute task and wait for response
    final CompletableFuture<PivotSyncState> future = pivotBlockRetriever.downloadPivotBlockHeader();

    waitUntilComplete(future);

    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerA));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(peerB));

    assertThat(future).isCompletedExceptionally();
    assertThatThrownBy(future::get)
        .hasRootCauseInstanceOf(SyncException.class)
        .extracting(e -> ((SyncException) ExceptionUtils.rootCause(e)).getError())
        .isEqualTo(SyncError.PIVOT_BLOCK_HEADER_MISMATCH);
  }

  private void waitUntilComplete(final CompletableFuture<?> future) {
    try {
      future.join();
    } catch (RuntimeException e) {
      // do nothing
    }
  }

  @Test
  void dryRunDetector() {
    assertThat(true)
        .withFailMessage("This test is here so gradle --dry-run executes this class")
        .isTrue();
  }
}
