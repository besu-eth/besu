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
package org.hyperledger.besu.ethereum.eth.sync.fullsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncTarget;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

public class FullSyncTargetManagerTest {

  private EthProtocolManager ethProtocolManager;

  private MutableBlockchain localBlockchain;
  private final WorldStateArchive localWorldState = mock(WorldStateArchive.class);
  private RespondingEthPeer.Responder responder;
  private FullSyncTargetManager syncTargetManager;
  private PeerTaskExecutor peerTaskExecutor;

  static class FullSyncTargetManagerTestArguments implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(DataStorageFormat.BONSAI), Arguments.of(DataStorageFormat.FOREST));
    }
  }

  public void setup(final DataStorageFormat storageFormat) {
    final BlockchainSetupUtil otherBlockchainSetup = BlockchainSetupUtil.forTesting(storageFormat);
    final Blockchain otherBlockchain = otherBlockchainSetup.getBlockchain();
    responder = RespondingEthPeer.blockchainResponder(otherBlockchain);

    final BlockchainSetupUtil localBlockchainSetup = BlockchainSetupUtil.forTesting(storageFormat);
    localBlockchain = localBlockchainSetup.getBlockchain();

    final ProtocolSchedule protocolSchedule = ProtocolScheduleFixture.TESTING_NETWORK;
    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(localBlockchain)
            .withWorldStateArchive(localWorldState)
            .build();
    peerTaskExecutor = mock(PeerTaskExecutor.class);
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(localBlockchain)
            .setEthScheduler(new EthScheduler(1, 1, 1, 1, new NoOpMetricsSystem()))
            .setWorldStateArchive(localBlockchainSetup.getWorldArchive())
            .setTransactionPool(localBlockchainSetup.getTransactionPool())
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
    final EthContext ethContext = ethProtocolManager.ethContext();
    localBlockchainSetup.importFirstBlocks(5);
    otherBlockchainSetup.importFirstBlocks(20);
    syncTargetManager =
        new FullSyncTargetManager(
            SynchronizerConfiguration.builder().build(),
            protocolSchedule,
            protocolContext,
            ethContext,
            new NoOpMetricsSystem(),
            SyncTerminationCondition.never());

    when(peerTaskExecutor.executeAgainstPeer(
            Mockito.any(GetHeadersFromPeerTask.class), Mockito.any(EthPeer.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(otherBlockchain, ethContext.getEthPeers()));
  }

  @AfterEach
  public void tearDown() {
    if (ethProtocolManager != null) {
      ethProtocolManager.stop();
    }
  }

  @ParameterizedTest
  @ArgumentsSource(FullSyncTargetManagerTest.FullSyncTargetManagerTestArguments.class)
  public void findSyncTarget_withHeightEstimates(final DataStorageFormat storageFormat)
      throws ExecutionException, InterruptedException, TimeoutException {
    setup(storageFormat);
    final BlockHeader chainHeadHeader = localBlockchain.getChainHeadHeader();
    when(localWorldState.isWorldStateAvailable(
            chainHeadHeader.getStateRoot(), chainHeadHeader.getHash()))
        .thenReturn(true);
    final RespondingEthPeer bestPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, Difficulty.MAX_VALUE, 4);
    Mockito.reset(peerTaskExecutor);
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.any(EthPeer.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.of(List.of(localBlockchain.getBlockHeader(4L).get())),
                PeerTaskExecutorResponseCode.SUCCESS,
                List.of(bestPeer.getEthPeer())));

    final CompletableFuture<SyncTarget> result = syncTargetManager.findSyncTarget();

    SyncTarget resultSyncTarget = result.get(5, TimeUnit.SECONDS);
    assertThat(resultSyncTarget)
        .isEqualTo(new SyncTarget(bestPeer.getEthPeer(), localBlockchain.getBlockHeader(4L).get()));
  }

  @ParameterizedTest
  @ArgumentsSource(FullSyncTargetManagerTest.FullSyncTargetManagerTestArguments.class)
  public void findSyncTarget_noHeightEstimates(final DataStorageFormat storageFormat) {
    setup(storageFormat);
    final BlockHeader chainHeadHeader = localBlockchain.getChainHeadHeader();
    when(localWorldState.isWorldStateAvailable(
            chainHeadHeader.getStateRoot(), chainHeadHeader.getHash()))
        .thenReturn(true);
    final RespondingEthPeer bestPeer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);

    final CompletableFuture<SyncTarget> result = syncTargetManager.findSyncTarget();
    bestPeer.respond(responder);

    assertThat(result).isNotCompleted();
  }

  @ParameterizedTest
  @ArgumentsSource(FullSyncTargetManagerTest.FullSyncTargetManagerTestArguments.class)
  public void shouldDisconnectPeerIfWorldStateIsUnavailableForCommonAncestor(
      final DataStorageFormat storageFormat) {
    setup(storageFormat);
    final BlockHeader chainHeadHeader = localBlockchain.getChainHeadHeader();
    when(localWorldState.isWorldStateAvailable(
            chainHeadHeader.getStateRoot(), chainHeadHeader.getHash()))
        .thenReturn(false);
    final RespondingEthPeer bestPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 20);

    final CompletableFuture<SyncTarget> result = syncTargetManager.findSyncTarget();

    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> bestPeer.getPeerConnection().isDisconnected());
    assertThat(result).isNotCompleted();
    assertThat(bestPeer.getPeerConnection().isDisconnected()).isTrue();
  }

  @ParameterizedTest
  @ArgumentsSource(FullSyncTargetManagerTest.FullSyncTargetManagerTestArguments.class)
  public void shouldAllowSyncTargetWhenIfWorldStateIsAvailableForCommonAncestor(
      final DataStorageFormat storageFormat)
      throws ExecutionException, InterruptedException, TimeoutException {
    setup(storageFormat);
    final BlockHeader chainHeadHeader = localBlockchain.getChainHeadHeader();
    when(localWorldState.isWorldStateAvailable(
            chainHeadHeader.getStateRoot(), chainHeadHeader.getHash()))
        .thenReturn(true);
    final RespondingEthPeer bestPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 20);

    final CompletableFuture<SyncTarget> result = syncTargetManager.findSyncTarget();

    SyncTarget resultSyncTarget = result.get(1, TimeUnit.SECONDS);

    assertThat(resultSyncTarget)
        .isEqualTo(new SyncTarget(bestPeer.getEthPeer(), localBlockchain.getChainHeadHeader()));
    assertThat(bestPeer.getPeerConnection().isDisconnected()).isFalse();
  }
}
