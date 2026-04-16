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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BackwardSyncStepTest {

  public static final int REMOTE_HEIGHT = 50;
  public static final int LOCAL_HEIGHT = 25;
  private static final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BackwardSyncContext context;

  private final ProtocolSchedule protocolSchedule =
      MainnetProtocolSchedule.fromConfig(
          new StubGenesisConfigOptions(),
          MiningConfiguration.MINING_DISABLED,
          new BadBlockManager(),
          false,
          BalConfiguration.DEFAULT,
          new NoOpMetricsSystem());

  private final DeterministicEthScheduler ethScheduler = new DeterministicEthScheduler();

  private MutableBlockchain localBlockchain;
  private MutableBlockchain remoteBlockchain;
  private RespondingEthPeer peer;
  @Mock private PeerTaskExecutor peerTaskExecutor;
  GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage;
  GenericKeyValueStorageFacade<Hash, Block> blocksStorage;
  GenericKeyValueStorageFacade<Hash, Hash> chainStorage;
  GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage;

  @BeforeEach
  public void setup() {
    headersStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    blocksStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new BlocksConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());
    chainStorage =
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            new InMemoryKeyValueStorage());
    sessionDataStorage =
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            new BlocksHeadersConvertor(new MainnetBlockHeaderFunctions()),
            new InMemoryKeyValueStorage());

    Block genesisBlock = blockDataGenerator.genesisBlock();
    remoteBlockchain = createInMemoryBlockchain(genesisBlock);
    localBlockchain = spy(createInMemoryBlockchain(genesisBlock));

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

    when(context.getProtocolContext().getBlockchain()).thenReturn(localBlockchain);
    when(context.getProtocolSchedule()).thenReturn(protocolSchedule);
    when(context.getBatchSize()).thenReturn(5);

    EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setEthScheduler(ethScheduler)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();

    peer =
        RespondingEthPeer.builder()
            .ethProtocolManager(ethProtocolManager)
            .estimatedHeight(REMOTE_HEIGHT)
            .build();
    EthContext ethContext = ethProtocolManager.ethContext();
    when(context.getEthContext()).thenReturn(ethContext);

    Answer<PeerTaskExecutorResult<List<BlockHeader>>> getHeadersAnswer =
        new GetHeadersFromPeerTaskExecutorAnswer(remoteBlockchain, ethContext.getEthPeers());
    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class))).thenAnswer(getHeadersAnswer);
    when(peerTaskExecutor.executeAgainstPeer(any(GetHeadersFromPeerTask.class), any(EthPeer.class)))
        .thenAnswer(getHeadersAnswer);
  }

  @Test
  public void shouldFindHeaderWhenRequested() throws Exception {
    final BackwardChain backwardChain = createBackwardChain(LOCAL_HEIGHT + 3);
    when(context.getBatchSize()).thenReturn(5);
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    BackwardSyncStep step = spy(new BackwardSyncStep(context, backwardChain));

    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain);

    final CompletableFuture<Void> future =
        step.executeAsync(backwardChain.getFirstAncestorHeader().orElseThrow());
    peer.respondWhileOtherThreadsWork(responder, () -> !future.isDone());
    future.get();
  }

  @Test
  public void shouldFindHashToSync() {
    final BackwardChain backwardChain = createBackwardChain(REMOTE_HEIGHT - 4, REMOTE_HEIGHT);
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    BackwardSyncStep step = new BackwardSyncStep(context, backwardChain);
    final Hash hash =
        step.possibleRestoreOldNodes(backwardChain.getFirstAncestorHeader().orElseThrow());
    assertThat(hash).isEqualTo(getBlockByNumber(REMOTE_HEIGHT - 4).getHeader().getParentHash());
  }

  @Test
  public void shouldRequestHeaderWhenAsked() throws Exception {
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    BackwardSyncStep step = new BackwardSyncStep(context, createBackwardChain(REMOTE_HEIGHT - 1));
    final Block lookingForBlock = getBlockByNumber(REMOTE_HEIGHT - 2);

    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain);

    final CompletableFuture<List<BlockHeader>> future =
        step.requestHeaders(lookingForBlock.getHeader().getHash());
    peer.respondWhileOtherThreadsWork(responder, () -> !future.isDone());

    final BlockHeader blockHeader = future.get().get(0);
    assertThat(blockHeader).isEqualTo(lookingForBlock.getHeader());
  }

  @Test
  public void shouldNotRequestHeaderIfAlreadyPresent() throws Exception {
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    BackwardSyncStep step = new BackwardSyncStep(context, createBackwardChain(REMOTE_HEIGHT - 1));
    final Block lookingForBlock = getBlockByNumber(LOCAL_HEIGHT);

    final CompletableFuture<List<BlockHeader>> future =
        step.requestHeaders(lookingForBlock.getHeader().getHash());

    verify(localBlockchain).getBlockHeader(lookingForBlock.getHash());
    verify(context, never()).getEthContext();
    final BlockHeader blockHeader = future.get().get(0);
    assertThat(blockHeader).isEqualTo(lookingForBlock.getHeader());
  }

  @Test
  public void shouldRequestHeaderBeforeCurrentHeight() throws Exception {
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    extendBlockchain(REMOTE_HEIGHT + 1, context.getProtocolContext().getBlockchain());

    BackwardSyncStep step = new BackwardSyncStep(context, createBackwardChain(REMOTE_HEIGHT - 1));
    final Block lookingForBlock = getBlockByNumber(REMOTE_HEIGHT - 2);

    final RespondingEthPeer.Responder responder =
        RespondingEthPeer.blockchainResponder(remoteBlockchain);

    final CompletableFuture<List<BlockHeader>> future =
        step.requestHeaders(lookingForBlock.getHeader().getHash());
    peer.respondWhileOtherThreadsWork(responder, () -> !future.isDone());

    final BlockHeader blockHeader = future.get().get(0);
    assertThat(blockHeader).isEqualTo(lookingForBlock.getHeader());
  }

  @Test
  public void shouldThrowWhenResponseIsEmptyWhenRequestingHeader() {
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    Mockito.reset(peerTaskExecutor);
    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(), PeerTaskExecutorResponseCode.SUCCESS, Collections.emptyList()));
    when(peerTaskExecutor.executeAgainstPeer(any(GetHeadersFromPeerTask.class), any(EthPeer.class)))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(), PeerTaskExecutorResponseCode.SUCCESS, Collections.emptyList()));
    BackwardSyncStep step = new BackwardSyncStep(context, createBackwardChain(REMOTE_HEIGHT - 1));
    final Block lookingForBlock = getBlockByNumber(REMOTE_HEIGHT - 2);

    final RespondingEthPeer.Responder responder = RespondingEthPeer.emptyResponder();

    final CompletableFuture<List<BlockHeader>> future =
        step.requestHeaders(lookingForBlock.getHeader().getHash());
    peer.respondWhileOtherThreadsWork(responder, () -> !future.isDone());

    assertThatThrownBy(future::get).cause().isInstanceOf(RuntimeException.class);
  }

  @Test
  public void shouldSaveHeaderDelegatesProperly() {
    when(context.getSynchronizerConfiguration())
        .thenReturn(SynchronizerConfiguration.builder().isPeerTaskSystemEnabled(true).build());
    final BackwardChain chain = Mockito.mock(BackwardChain.class);
    final BlockHeader header = Mockito.mock(BlockHeader.class);

    BackwardSyncStep step = new BackwardSyncStep(context, chain);

    step.saveHeader(header);

    verify(chain).prependAncestorsHeader(header);
  }

  private BackwardChain createBackwardChain(final int from, final int until) {
    BackwardChain chain = createBackwardChain(until);
    for (int i = until; i > from; --i) {
      chain.prependAncestorsHeader(getBlockByNumber(i - 1).getHeader());
    }
    return chain;
  }

  @NotNull
  private BackwardChain createBackwardChain(final int number) {
    final BackwardChain backwardChain =
        new BackwardChain(headersStorage, blocksStorage, chainStorage, sessionDataStorage);
    backwardChain.appendTrustedBlock(remoteBlockchain.getBlockByNumber(number).orElseThrow());
    return backwardChain;
  }

  @NotNull
  private Block getBlockByNumber(final int number) {
    return remoteBlockchain.getBlockByNumber(number).orElseThrow();
  }

  private void extendBlockchain(final int newHeight, final MutableBlockchain blockchain) {
    final long currentHeight = blockchain.getChainHeadBlockNumber();

    final long blocksToBuild = newHeight - currentHeight;

    for (long i = currentHeight + 1; i <= currentHeight + blocksToBuild; i++) {
      final BlockDataGenerator.BlockOptions options =
          new BlockDataGenerator.BlockOptions()
              .setBlockNumber(i)
              .setParentHash(blockchain.getBlockHashByNumber(i - 1).orElseThrow());
      final Block block = blockDataGenerator.block(options);
      final List<TransactionReceipt> receipts = blockDataGenerator.receipts(block);

      blockchain.appendBlock(block, receipts);
    }
  }
}
