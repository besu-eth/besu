/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BackwardSyncAlgSpecTest {

  public static final int REMOTE_HEIGHT = 50;
  public static final int LOCAL_HEIGHT = 25;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  BackwardSyncContext context;

  private MutableBlockchain remoteBlockchain;
  private MutableBlockchain localBlockchain;
  @Captor ArgumentCaptor<BesuEvents.TTDReachedListener> ttdCaptor;
  @Captor ArgumentCaptor<BesuEvents.InitialSyncCompletionListener> completionCaptor;

  @InjectMocks BackwardSyncAlgorithm algorithm;
  @Mock private Hash hash;

  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private Block genesisBlock;

  @BeforeEach
  public void setUp() throws Exception {
    BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
    genesisBlock = blockDataGenerator.genesisBlock();
    remoteBlockchain = createInMemoryBlockchain(genesisBlock);
    localBlockchain = createInMemoryBlockchain(genesisBlock);

    for (int i = 1; i <= REMOTE_HEIGHT; i++) {
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(remoteBlockchain.getBlockHashByNumber(i - 1).orElseThrow());
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

      remoteBlockchain.appendBlock(block, receipts);
      if (i <= LOCAL_HEIGHT) {
        localBlockchain.appendBlock(block, receipts);
      }
    }

    algorithm =
        Mockito.spy(
            new BackwardSyncAlgorithm(
                context,
                FinalBlockConfirmation.confirmationChain(
                    FinalBlockConfirmation.genesisConfirmation(localBlockchain),
                    FinalBlockConfirmation.ancestorConfirmation(localBlockchain))));
    when(context.getProtocolContext().getBlockchain()).thenReturn(localBlockchain);
  }

  @Test
  public void shouldWaitWhenNotReady() throws Exception {
    doReturn(false).when(context).isReady();
    when(context.getSyncState().isInitialSyncPhaseDone()).thenReturn(Boolean.TRUE);
    when(context.getSyncState().subscribeTTDReached(any())).thenReturn(88L);
    when(context.getSyncState().subscribeCompletionReached(any())).thenReturn(99L);

    final CompletableFuture<Void> voidCompletableFuture = algorithm.waitForReady();

    verify(context.getSyncState()).subscribeTTDReached(ttdCaptor.capture());
    verify(context.getSyncState()).subscribeCompletionReached(completionCaptor.capture());
    verify(context.getSyncState(), never()).unsubscribeTTDReached(anyLong());
    assertThat(voidCompletableFuture).isNotCompleted();

    ttdCaptor.getValue().onTTDReached(true);
    completionCaptor.getValue().onInitialSyncCompleted();

    Thread.sleep(100);

    assertThat(voidCompletableFuture).isNotCompleted();
  }

  @Test
  public void shouldNotWaitWhenReady() throws Exception {
    doReturn(true).when(context).isReady();

    when(context.getSyncState().subscribeTTDReached(any())).thenReturn(88L);
    when(context.getSyncState().subscribeCompletionReached(any())).thenReturn(99L);

    final CompletableFuture<Void> voidCompletableFuture = algorithm.waitForReady();
    voidCompletableFuture.get(1, TimeUnit.SECONDS);
    assertThat(voidCompletableFuture).isCompleted();

    verify(context.getSyncState()).subscribeTTDReached(ttdCaptor.capture());

    verify(context.getSyncState()).unsubscribeTTDReached(88L);
    verify(context.getSyncState()).unsubscribeInitialConditionReached(99L);
  }

  @Test
  public void shouldAwokeWhenTTDReachedAndReady() throws Exception {
    doReturn(false).when(context).isReady();

    when(context.getSyncState().subscribeTTDReached(any())).thenReturn(88L);
    when(context.getSyncState().subscribeCompletionReached(any())).thenReturn(99L);
    when(context.getEthContext().getEthPeers().peerCount()).thenReturn(1);

    final CompletableFuture<Void> voidCompletableFuture = algorithm.waitForReady();

    Thread.sleep(50);
    assertThat(voidCompletableFuture).isNotCompleted();
    verify(context.getSyncState()).subscribeTTDReached(ttdCaptor.capture());

    doReturn(true).when(context).isReady();
    Thread.sleep(50);
    assertThat(voidCompletableFuture).isNotCompleted();

    ttdCaptor.getValue().onTTDReached(true);

    voidCompletableFuture.get(200, TimeUnit.MILLISECONDS);
    assertThat(voidCompletableFuture).isCompleted();

    verify(context.getSyncState()).unsubscribeTTDReached(88L);
    verify(context.getSyncState()).unsubscribeInitialConditionReached(99L);
  }

  @Test
  public void shouldAwokeWhenPeerConnectsAfterTTDReached() throws Exception {
    doReturn(false).when(context).isReady();
    when(context.getSyncState().subscribeTTDReached(any())).thenReturn(88L);
    when(context.getSyncState().subscribeCompletionReached(any())).thenReturn(99L);
    final CompletableFuture<EthPeer> peerConnection = new CompletableFuture<>();
    when(context.getEthContext().getEthPeers().waitForPeer(any())).thenReturn(peerConnection);

    final CompletableFuture<Void> voidCompletableFuture = algorithm.waitForReady();

    verify(context.getSyncState()).subscribeTTDReached(ttdCaptor.capture());
    ttdCaptor.getValue().onTTDReached(true);
    assertThat(voidCompletableFuture).isNotCompleted();

    doReturn(true).when(context).isReady();
    peerConnection.complete(Mockito.mock(EthPeer.class));

    voidCompletableFuture.get(1, TimeUnit.SECONDS);

    assertThat(voidCompletableFuture).isCompleted();
    verify(context.getSyncState()).unsubscribeTTDReached(88L);
    verify(context.getSyncState()).unsubscribeInitialConditionReached(99L);
  }

  @Test
  public void shouldAwokeWhenConditionReachedAndReady() throws Exception {
    doReturn(false).when(context).isReady();

    when(context.getSyncState().subscribeTTDReached(any())).thenReturn(88L);
    when(context.getSyncState().subscribeCompletionReached(any())).thenReturn(99L);
    when(context.getEthContext().getEthPeers().peerCount()).thenReturn(1);

    final CompletableFuture<Void> voidCompletableFuture = algorithm.waitForReady();
    Thread.sleep(50);

    verify(context.getSyncState()).subscribeCompletionReached(completionCaptor.capture());
    assertThat(voidCompletableFuture).isNotCompleted();

    doReturn(true).when(context).isReady();
    Thread.sleep(50);
    assertThat(voidCompletableFuture).isNotCompleted();

    completionCaptor.getValue().onInitialSyncCompleted();

    voidCompletableFuture.get(5, TimeUnit.SECONDS);
    assertThat(voidCompletableFuture).isCompleted();

    verify(context.getSyncState()).unsubscribeTTDReached(88L);
    verify(context.getSyncState()).unsubscribeInitialConditionReached(99L);
  }

  @Test
  public void shouldFinishWhenWorkIsDone() {
    doReturn(true).when(context).isReady();
    final CompletableFuture<Void> completableFuture = algorithm.pickNextStep();
    assertThat(completableFuture.isDone()).isTrue();
  }

  @Test
  public void shouldSyncWhenThereIsUnprocessedHash() {
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 10);
    doReturn(backwardChain).when(context).getBackwardChain();
    backwardChain.addNewHash(hash);
    doReturn(CompletableFuture.completedFuture(null)).when(algorithm).executeSyncStep(hash);

    doReturn(true).when(context).isReady();
    algorithm.pickNextStep();
    verify(algorithm).executeSyncStep(hash);
  }

  @Test
  public void shouldWaitForReadyWhenNotReady() {
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 10);
    doReturn(backwardChain).when(context).getBackwardChain();
    doReturn(false).when(context).isReady();
    doReturn(CompletableFuture.completedFuture(null)).when(algorithm).waitForReady();

    final CompletableFuture<Void> nextStep = algorithm.pickNextStep();
    verify(algorithm).waitForReady();
    assertThat(nextStep).isCompleted();
  }

  @Test
  public void shouldRunProcessKnownAncestorsWhenOnLocalHeight() {
    doReturn(true).when(context).isReady();
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT, LOCAL_HEIGHT + 10);
    doReturn(backwardChain).when(context).getBackwardChain();
    doReturn(CompletableFuture.completedFuture(null))
        .when(algorithm)
        .executeProcessKnownAncestors();

    algorithm.pickNextStep();
    verify(algorithm).executeProcessKnownAncestors();
  }

  @Test
  public void shouldRunForwardStepWhenOnLocalHeightPlusOne() {
    doReturn(true).when(context).isReady();
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT + 1, LOCAL_HEIGHT + 10);
    doReturn(backwardChain).when(context).getBackwardChain();
    doReturn(CompletableFuture.completedFuture(null)).when(algorithm).executeForwardAsync();

    algorithm.pickNextStep();
    verify(algorithm).executeForwardAsync();
  }

  @Test
  public void shouldRunBackwardStepWhenNotOnLocalHeight() {
    doReturn(true).when(context).isReady();
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT + 3, LOCAL_HEIGHT + 10);
    doReturn(backwardChain).when(context).getBackwardChain();
    doReturn(CompletableFuture.completedFuture(null)).when(algorithm).executeBackwardAsync(any());

    algorithm.pickNextStep();
    verify(algorithm).executeBackwardAsync(any());
  }

  @Test
  public void shouldStartForwardSyncIfGenesisIsReached() {
    doReturn(true).when(context).isReady();
    final MutableBlockchain otherLocalBlockchain = createForkedBlockchain(genesisBlock);
    when(context.getProtocolContext().getBlockchain()).thenReturn(otherLocalBlockchain);

    final BackwardChain backwardChain = createBackwardChain(0, 1);
    doReturn(backwardChain).when(context).getBackwardChain();
    doReturn(CompletableFuture.completedFuture(null))
        .when(algorithm)
        .executeProcessKnownAncestors();

    algorithm.pickNextStep();
    verify(algorithm).executeProcessKnownAncestors();
  }

  @Test
  public void shouldFailIfADifferentGenesisIsReached() {
    doReturn(true).when(context).isReady();
    final BlockDataGenerator.BlockOptions otherGenesisOptions =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(0)
            .setExtraData(Bytes.of("Other genesis".getBytes(StandardCharsets.UTF_8)));
    final Block otherGenesis = blockDataGenerator.genesisBlock(otherGenesisOptions);
    final MutableBlockchain otherLocalBlockchain = createForkedBlockchain(otherGenesis);
    when(context.getProtocolContext().getBlockchain()).thenReturn(otherLocalBlockchain);

    final BackwardChain backwardChain = createBackwardChain(0, 1);
    doReturn(backwardChain).when(context).getBackwardChain();
    algorithm =
        Mockito.spy(
            new BackwardSyncAlgorithm(
                context, FinalBlockConfirmation.genesisConfirmation(otherLocalBlockchain)));
    assertThatThrownBy(() -> algorithm.pickNextStep())
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("Should have reached header");
  }

  private MutableBlockchain createForkedBlockchain(final Block genesisBlock) {
    final MutableBlockchain otherLocalBlockchain = createInMemoryBlockchain(genesisBlock);
    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1)
            .setParentHash(genesisBlock.getHash())
            .setExtraData(Bytes.of("Fork of block 1".getBytes(StandardCharsets.UTF_8)));
    final Block block = blockDataGenerator.block(options);
    final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

    otherLocalBlockchain.appendBlock(block, receipts);
    return otherLocalBlockchain;
  }

  private BackwardChain createBackwardChain(final int from, final int until) {

    BackwardChain backwardChain = BackwardSyncContextTest.inMemoryBackwardChain();
    for (int i = until; i > from; --i) {
      backwardChain.prependAncestorsHeader(getBlockByNumber(i - 1).getHeader());
    }
    return backwardChain;
  }

  @NotNull
  private Block getBlockByNumber(final int number) {
    return remoteBlockchain.getBlockByNumber(number).orElseThrow();
  }
}
