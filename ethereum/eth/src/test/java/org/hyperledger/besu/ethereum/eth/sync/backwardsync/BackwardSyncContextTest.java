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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.referencetests.ForestReferenceTestWorldState;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BackwardSyncContextTest {

  public static final int LOCAL_HEIGHT = 25;
  public static final int REMOTE_HEIGHT = 50;
  public static final int UNCLE_HEIGHT = 25 - 3;

  public static final int NUM_OF_RETRIES = 1;
  public static final int TEST_MAX_BAD_CHAIN_EVENT_ENTRIES = 25;

  private BackwardSyncContext context;

  private MutableBlockchain remoteBlockchain;
  private RespondingEthPeer peer;
  private MutableBlockchain localBlockchain;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Spy
  private ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          new StubGenesisConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());

  @Spy
  private ProtocolSpec protocolSpec =
      protocolSchedule.getByBlockHeader(blockDataGenerator.header(0L));

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ProtocolContext protocolContext;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MetricsSystem metricsSystem;

  @Mock private BlockValidator blockValidator;
  @Mock private SyncState syncState;
  @Mock private PeerTaskExecutor peerTaskExecutor;
  private BackwardChain backwardChain;
  private Block uncle;
  private Block genesisBlock;

  @BeforeEach
  public void setup() {
    when(protocolSpec.getBlockValidator()).thenReturn(blockValidator);
    doReturn(protocolSpec).when(protocolSchedule).getByBlockHeader(any());
    genesisBlock = blockDataGenerator.genesisBlock();
    remoteBlockchain = createInMemoryBlockchain(genesisBlock);
    localBlockchain = createInMemoryBlockchain(genesisBlock);

    for (int i = 1; i <= REMOTE_HEIGHT; i++) {
      final Hash parentHash = remoteBlockchain.getBlockHashByNumber(i - 1).orElseThrow();
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions().setBlockNumber(i).setParentHash(parentHash);
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

      remoteBlockchain.appendBlock(block, receipts);
      if (i <= LOCAL_HEIGHT) {
        if (i == UNCLE_HEIGHT) {
          uncle =
              createUncle(
                  i, localBlockchain.getBlockByNumber(LOCAL_HEIGHT - 4).orElseThrow().getHash());
          localBlockchain.appendBlock(uncle, blockDataGenerator.receipts(uncle));
          localBlockchain.rewindToBlock(i - 1);
        }
        localBlockchain.appendBlock(block, receipts);
      }
    }
    when(protocolContext.getBlockchain()).thenReturn(localBlockchain);
    EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(localBlockchain)
            .setPeerTaskExecutor(peerTaskExecutor)
            .setEthScheduler(new EthScheduler(1, 1, 1, metricsSystem))
            .build();

    peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
    EthContext ethContext = ethProtocolManager.ethContext();

    when(blockValidator.validateAndProcessBlock(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final Object[] arguments = invocation.getArguments();
              Block block = (Block) arguments[1];
              return new BlockProcessingResult(
                  Optional.of(
                      new BlockProcessingOutputs(
                          // use forest-based worldstate since it does not require
                          // blockheader stateroot to match actual worldstate root
                          ForestReferenceTestWorldState.create(Collections.emptyMap()),
                          blockDataGenerator.receipts(block),
                          Optional.empty())));
            });

    backwardChain = inMemoryBackwardChain();
    backwardChain.appendTrustedBlock(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).orElseThrow());
    backwardChain.appendTrustedBlock(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 2).orElseThrow());
    backwardChain.appendTrustedBlock(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 3).orElseThrow());
    backwardChain.appendTrustedBlock(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 4).orElseThrow());
    context =
        spy(
            new BackwardSyncContext(
                protocolContext,
                protocolSchedule,
                SynchronizerConfiguration.builder().build(),
                metricsSystem,
                ethContext,
                syncState,
                backwardChain,
                NUM_OF_RETRIES,
                TEST_MAX_BAD_CHAIN_EVENT_ENTRIES));
    doReturn(true).when(context).isReady();
    doReturn(2).when(context).getBatchSize();

    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetHeadersFromPeerTask.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(remoteBlockchain, ethContext.getEthPeers()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetBodiesFromPeerTask.class)))
        .thenAnswer(
            new GetBodiesFromPeerTaskExecutorAnswer(remoteBlockchain, ethContext.getEthPeers()));
  }

  private Block createUncle(final int i, final Hash parentHash) {
    return createBlock(i, parentHash);
  }

  private Block createBlock(final int i, final Hash parentHash) {
    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(i)
            .setParentHash(parentHash)
            .transactionTypes(TransactionType.ACCESS_LIST);
    return blockDataGenerator.block(options);
  }

  public static BackwardChain inMemoryBackwardChain() {
    final GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    final GenericKeyValueStorageFacade<Hash, Block> blocksStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    final GenericKeyValueStorageFacade<Hash, Hash> chainStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            new InMemoryKeyValueStorage());
    final GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage =
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            BlocksHeadersConvertor.of(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    return new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
  }

  @Test
  public void shouldNotBeReadyUntilPeers() {
    // other conditions for isReady are true
    when(syncState.hasReachedTerminalDifficulty()).thenReturn(Optional.of(Boolean.TRUE));
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    // set up context with no peers
    EthProtocolManager ethProtocolManagerWithNoPeers =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(localBlockchain)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();

    EthContext ethContextWithNoPeers = ethProtocolManagerWithNoPeers.ethContext();
    BackwardSyncContext contextWithNoPeers =
        new BackwardSyncContext(
            protocolContext,
            protocolSchedule,
            SynchronizerConfiguration.builder().build(),
            metricsSystem,
            ethContextWithNoPeers,
            syncState,
            backwardChain,
            NUM_OF_RETRIES,
            TEST_MAX_BAD_CHAIN_EVENT_ENTRIES);

    // with no peers, we are not ready
    assertThat(ethContextWithNoPeers.getEthPeers().peerCount()).isEqualTo(0);
    assertThat(contextWithNoPeers.isReady()).isFalse();

    // add a peer
    EthProtocolManagerTestUtil.createPeer(ethProtocolManagerWithNoPeers);
    assertThat(ethContextWithNoPeers.getEthPeers().peerCount()).isEqualTo(1);

    // now we are ready
    assertThat(contextWithNoPeers.isReady()).isTrue();
  }

  @Test
  public void shouldSyncUntilHash() throws Exception {
    final Hash hash = getRemoteBlockByNumber(REMOTE_HEIGHT).getHash();
    final CompletableFuture<Void> future = context.syncBackwardsUntil(hash);

    // Use Awaitility with timeout like stable tests do
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              respondUntilFutureIsDone(future);
              assertThat(future).isDone();
            });

    future.get();
    assertThat(localBlockchain.getChainHeadBlock()).isEqualTo(remoteBlockchain.getChainHeadBlock());
  }

  @Test
  public void shouldNotSyncUntilHashWhenNotInSync() {
    doReturn(false).when(context).isReady();
    final Hash hash = getRemoteBlockByNumber(REMOTE_HEIGHT).getHash();
    final CompletableFuture<Void> future = context.syncBackwardsUntil(hash);

    respondUntilFutureIsDone(future);

    assertThatThrownBy(future::get)
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Backward sync is not ready");
    assertThat(backwardChain.getFirstHashToAppend()).isEmpty();
  }

  @Test
  public void shouldSyncUntilRemoteBranch() throws Exception {

    final CompletableFuture<Void> future =
        context.syncBackwardsUntil(getRemoteBlockByNumber(REMOTE_HEIGHT));

    // Use Awaitility with timeout like stable tests do
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              respondUntilFutureIsDone(future);
              assertThat(future).isDone();
            });

    future.get();
    assertThat(localBlockchain.getChainHeadBlock()).isEqualTo(remoteBlockchain.getChainHeadBlock());
  }

  @Test
  public void shouldAddExpectedBlock() throws Exception {

    // Append the higher block to the backward chain before starting sync,
    // so both targets are available when the sync session begins.
    // This avoids a race where the first sync session completes before the
    // second syncBackwardsUntil call can update the target height.
    final Block lowerBlock = getRemoteBlockByNumber(REMOTE_HEIGHT - 1);
    final Block higherBlock = getRemoteBlockByNumber(REMOTE_HEIGHT);

    final CompletableFuture<Void> future = context.syncBackwardsUntil(lowerBlock);
    final CompletableFuture<Void> secondFuture = context.syncBackwardsUntil(higherBlock);

    assertThat(future).isSameAs(secondFuture);

    // Use Awaitility with timeout like stable tests do
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              respondUntilFutureIsDone(future);
              assertThat(future).isDone();
            });

    future.get();
    assertThat(localBlockchain.getChainHeadBlock()).isEqualTo(remoteBlockchain.getChainHeadBlock());
  }

  private void respondUntilFutureIsDone(final CompletableFuture<Void> future) {
    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain);

    peer.respondWhileOtherThreadsWork(responder, () -> !future.isDone());
  }

  @NotNull
  private Block getRemoteBlockByNumber(final int number) {
    return remoteBlockchain.getBlockByNumber(number).orElseThrow();
  }

  @Test
  public void shouldMoveHead() {
    final Block lastSavedBlock = localBlockchain.getBlockByNumber(4).orElseThrow();
    context.possiblyMoveHead(lastSavedBlock);

    assertThat(localBlockchain.getChainHeadBlock().getHeader().getNumber()).isEqualTo(4);
  }

  @Test
  public void shouldNotMoveHeadWhenAlreadyHead() {
    final Block lastSavedBlock = localBlockchain.getBlockByNumber(25).orElseThrow();
    context.possiblyMoveHead(lastSavedBlock);

    assertThat(localBlockchain.getChainHeadBlock().getHeader().getNumber()).isEqualTo(25);
  }

  @Test
  public void shouldUpdateTargetHeightWhenStatusPresent() {
    // Given
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(blockHeader.getParentHash()).thenReturn(Hash.fromHexStringLenient("0x41"));
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getNumber()).thenReturn(42L);
    Block unknownBlock = Mockito.mock(Block.class);
    when(unknownBlock.getHeader()).thenReturn(blockHeader);
    when(unknownBlock.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(unknownBlock.toRlp()).thenReturn(Bytes.EMPTY);
    context.syncBackwardsUntil(unknownBlock); // set the status
    assertThat(context.getStatus().getTargetChainHeight()).isEqualTo(42);
    final Hash backwardChainHash =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 4).get().getHash();
    final Block backwardChainBlock = backwardChain.getTrustedBlock(backwardChainHash);

    // When
    context.maybeUpdateTargetHeight(backwardChainBlock.getHash());

    // Then
    assertThat(context.getStatus().getTargetChainHeight()).isEqualTo(29);
  }

  @Test
  public void shouldProcessExceptionsCorrectly() {
    assertThatThrownBy(
            () ->
                context.processException(
                    new RuntimeException(new BackwardSyncException("shouldThrow"))))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("shouldThrow");
    context.processException(
        new RuntimeException(new BackwardSyncException("shouldNotThrow", true)));
    context.processException(new RuntimeException(new RuntimeException("shouldNotThrow")));
  }

  @Test
  public void shouldEmitBadChainEvent() {
    Block block = Mockito.mock(Block.class);
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    BlockHeader childBlockHeader =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 2).get().getHeader();
    BlockHeader grandChildBlockHeader =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader();

    backwardChain.clear();
    backwardChain.prependAncestorsHeader(grandChildBlockHeader);
    backwardChain.prependAncestorsHeader(childBlockHeader);
    backwardChain.prependAncestorsHeader(blockHeader);

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result = new BlockProcessingResult("custom error");
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    verify(badChainListener)
        .onBadChain(
            block, Collections.emptyList(), List.of(childBlockHeader, grandChildBlockHeader));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldEmitBadChainEventWithIncludedBlockHeadersLimitedToMaxBadChainEventsSize() {
    Block block = Mockito.mock(Block.class);
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();

    for (int i = REMOTE_HEIGHT; i >= 0; i--) {
      backwardChain.prependAncestorsHeader(remoteBlockchain.getBlockByNumber(i).get().getHeader());
    }
    backwardChain.prependAncestorsHeader(blockHeader);

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result = new BlockProcessingResult("custom error");
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    final ArgumentCaptor<List<BlockHeader>> badBlockHeaderDescendants =
        ArgumentCaptor.forClass(List.class);
    verify(badChainListener)
        .onBadChain(eq(block), eq(Collections.emptyList()), badBlockHeaderDescendants.capture());
    assertThat(badBlockHeaderDescendants.getValue()).hasSize(TEST_MAX_BAD_CHAIN_EVENT_ENTRIES);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldEmitBadChainEventWithIncludedBlocksLimitedToMaxBadChainEventsSize() {
    Block block = Mockito.mock(Block.class);
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    for (int i = REMOTE_HEIGHT; i >= 0; i--) {
      backwardChain.prependAncestorsHeader(remoteBlockchain.getBlockByNumber(i).get().getHeader());
    }
    backwardChain.prependAncestorsHeader(blockHeader);

    for (int i = REMOTE_HEIGHT; i >= 0; i--) {
      backwardChain.appendTrustedBlock(remoteBlockchain.getBlockByNumber(i).get());
    }

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result = new BlockProcessingResult("custom error");
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    final ArgumentCaptor<List<Block>> badBlockDescendants = ArgumentCaptor.forClass(List.class);
    verify(badChainListener)
        .onBadChain(eq(block), badBlockDescendants.capture(), eq(Collections.emptyList()));
    assertThat(badBlockDescendants.getValue()).hasSize(TEST_MAX_BAD_CHAIN_EVENT_ENTRIES);
  }

  @Test
  public void shouldFailAfterMaxNumberOfRetries() {
    doReturn(CompletableFuture.failedFuture(new Exception()))
        .when(context)
        .prepareBackwardSyncFuture();

    final var syncFuture = context.syncBackwardsUntil(Hash.ZERO);

    try {
      syncFuture.get();
    } catch (final Throwable throwable) {
      if (throwable instanceof ExecutionException) {
        BackwardSyncException backwardSyncException = (BackwardSyncException) throwable.getCause();
        assertThat(backwardSyncException.getMessage())
            .contains("Max number of retries " + NUM_OF_RETRIES + " reached");
      }
    }
  }

  @Test
  public void whenBlockNotFoundInPeers_shouldRemoveBlockFromQueueAndProgressInNextSession()
      throws Exception {
    // This scenario can happen due to a reorg
    // Expectation we progress beyond the reorg block upon receiving the next FCU

    // choose an intermediate remote block to create a reorg block from
    int reorgBlockHeight = REMOTE_HEIGHT - 1; // 49
    final Hash reorgBlockParentHash = getRemoteBlockByNumber(reorgBlockHeight - 1).getHash();
    final Block reorgBlock = createBlock(reorgBlockHeight, reorgBlockParentHash);

    // represents first FCU with a block that will become reorged away
    final CompletableFuture<Void> fcuBeforeReorg = context.syncBackwardsUntil(reorgBlock.getHash());
    respondUntilFutureIsDone(fcuBeforeReorg);
    fcuBeforeReorg.get();
    assertThat(localBlockchain.getChainHeadBlockNumber()).isLessThan(reorgBlockHeight);

    // represents subsequent FCU with successfully reorged version of the same block
    final CompletableFuture<Void> fcuAfterReorg =
        context.syncBackwardsUntil(getRemoteBlockByNumber(reorgBlockHeight).getHash());
    respondUntilFutureIsDone(fcuAfterReorg);
    fcuAfterReorg.get();
    assertThat(localBlockchain.getChainHeadBlock())
        .isEqualTo(remoteBlockchain.getBlockByNumber(reorgBlockHeight).orElseThrow());
  }
}
