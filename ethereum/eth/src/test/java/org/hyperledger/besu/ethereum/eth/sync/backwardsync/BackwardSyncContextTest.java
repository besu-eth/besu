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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.MainnetBlockValidatorBuilder;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
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
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.referencetests.ForestReferenceTestWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.NotNull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
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

  private final BadBlockManager badBlockManager = new BadBlockManager();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MetricsSystem metricsSystem;

  @Mock private BlockValidator blockValidator;
  @Mock private SyncState syncState;
  @Mock private PeerTaskExecutor peerTaskExecutor;
  @Mock private BackwardSyncAlgorithmFactory backwardSyncAlgorithmFactory;
  @Mock private BackwardSyncAlgorithm backwardSyncAlgorithm;
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
    when(protocolContext.getBadBlockManager()).thenReturn(badBlockManager);
    EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(localBlockchain)
            .setPeerTaskExecutor(peerTaskExecutor)
            .setEthScheduler(new EthScheduler(1, 1, 1, metricsSystem))
            .build();

    EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
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
                backwardSyncAlgorithmFactory,
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

  @Test
  public void shouldWarnOncePerBlockWhenParentWorldStateIsUnavailable() {
    final Block block = remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).orElseThrow();
    final Block otherBlock = remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 2).orElseThrow();
    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    doReturn(BlockProcessingResult.worldStateUnavailable("parent world state is not available"))
        .when(blockValidator)
        .validateAndProcessBlock(any(), any(), any(), any());

    final List<LogEvent> events =
        withLogCapture(
            BackwardSyncContext.class,
            () -> {
              for (final Block attempted : List.of(block, block, block, otherBlock)) {
                assertThatThrownBy(() -> context.saveBlock(attempted))
                    .isInstanceOf(BackwardSyncException.class);
              }
            });

    final List<String> warnings =
        events.stream()
            .filter(event -> Level.WARN.equals(event.getLevel()))
            .map(event -> event.getMessage().getFormattedMessage())
            .toList();
    assertThat(warnings).hasSize(2);
    assertThat(warnings.get(0)).contains(block.toLogString());
    assertThat(warnings.get(1)).contains(otherBlock.toLogString());
  }

  @SuppressWarnings("BannedMethod")
  private static List<LogEvent> withLogCapture(final Class<?> loggerClass, final Runnable action) {
    final Logger logger = (Logger) LogManager.getLogger(loggerClass);
    final List<LogEvent> events = new CopyOnWriteArrayList<>();
    final AbstractAppender appender =
        new AbstractAppender("test-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    return events;
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
            backwardSyncAlgorithmFactory,
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
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Hash hash = getRemoteBlockByNumber(REMOTE_HEIGHT).getHash();
    final CompletableFuture<Void> future = context.syncBackwardsUntil(hash);
    future.orTimeout(30, TimeUnit.SECONDS);

    future.get();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void shouldQueueHashForSyncWhenNotReady() throws Exception {
    doReturn(false).when(context).isReady();
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Hash hash = getRemoteBlockByNumber(REMOTE_HEIGHT).getHash();
    final CompletableFuture<Void> future = context.syncBackwardsUntil(hash);

    future.orTimeout(30, TimeUnit.SECONDS);
    future.get();

    assertThat(backwardChain.getFirstHashToAppend()).contains(hash);
  }

  @Test
  public void shouldKeepOnlyLatestHashQueuedWhileNotReady() {
    doReturn(false).when(context).isReady();
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null)).thenReturn(new CompletableFuture<>());

    final Hash firstHash = getRemoteBlockByNumber(REMOTE_HEIGHT - 1).getHash();
    final Hash latestHash = getRemoteBlockByNumber(REMOTE_HEIGHT).getHash();
    context.syncBackwardsUntil(firstHash);
    context.syncBackwardsUntil(latestHash);

    assertThat(backwardChain.getHashesToAppend()).containsExactly(latestHash);
  }

  @Test
  public void shouldSyncUntilRemoteBranch() throws Exception {
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    final CompletableFuture<Void> future =
        context.syncBackwardsUntil(getRemoteBlockByNumber(REMOTE_HEIGHT));
    future.orTimeout(30, TimeUnit.SECONDS);
    future.get();
    assertThat(future.isDone()).isTrue();
  }

  @Test
  public void shouldAddExpectedBlock() throws Exception {
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Append the higher block to the backward chain before starting sync,
    // so both targets are available when the sync session begins.
    // This avoids a race where the first sync session completes before the
    // second syncBackwardsUntil call can update the target height.
    final Block lowerBlock = getRemoteBlockByNumber(REMOTE_HEIGHT - 1);
    final Block higherBlock = getRemoteBlockByNumber(REMOTE_HEIGHT);

    final CompletableFuture<Void> future = context.syncBackwardsUntil(lowerBlock);
    final CompletableFuture<Void> secondFuture = context.syncBackwardsUntil(higherBlock);

    assertThat(future).isSameAs(secondFuture);
    future.orTimeout(30, TimeUnit.SECONDS);

    future.get();
    assertThat(backwardChain.getTrustedBlock(higherBlock.getHash())).isEqualTo(higherBlock);
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
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    BlockHeader unknownBlockHeader = Mockito.mock(BlockHeader.class);
    when(unknownBlockHeader.getParentHash()).thenReturn(Hash.fromHexStringLenient("0x41"));
    when(unknownBlockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(unknownBlockHeader.getNumber()).thenReturn(42L);
    Block unknownBlock = Mockito.mock(Block.class);
    when(unknownBlock.getHeader()).thenReturn(unknownBlockHeader);
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
    Block block = mockBlockWithParentOnChain();
    BlockHeader blockHeader = block.getHeader();
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
    rejectAndRecord(block, new BlockProcessingResult("custom error"));

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    verify(badChainListener)
        .onBadChain(
            blockHeader, Collections.emptyList(), List.of(childBlockHeader, grandChildBlockHeader));
  }

  @Test
  public void shouldNotEmitBadChainEventWhenTheFailureIsLocal() {
    Block block = mockBlockWithParentOnChain();
    BlockHeader blockHeader = block.getHeader();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    backwardChain.prependAncestorsHeader(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader());
    backwardChain.prependAncestorsHeader(blockHeader);

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result =
        new BlockProcessingResult(Optional.empty(), new StorageException("database bedlam"));
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block)).isInstanceOf(BackwardSyncException.class);

    verify(badChainListener, never()).onBadChain(any(), any(), any());
  }

  @Test
  public void shouldNotEmitBadChainEventOnMerkleTrieException() {
    Block block = mockBlockWithParentOnChain();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    backwardChain.prependAncestorsHeader(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader());
    backwardChain.prependAncestorsHeader(block.getHeader());

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result =
        new BlockProcessingResult(Optional.empty(), new MerkleTrieException("missing node"));
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block)).isInstanceOf(BackwardSyncException.class);

    verify(badChainListener, never()).onBadChain(any(), any(), any());
  }

  @Test
  public void shouldEmitBadChainEventWhenAnInvalidBlockIsCarriedByAnException() {
    Block block = mockBlockWithParentOnChain();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    BlockHeader childBlockHeader =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader();
    backwardChain.clear();
    backwardChain.prependAncestorsHeader(childBlockHeader);
    backwardChain.prependAncestorsHeader(block.getHeader());

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    // the block processor wraps e.g. a failed withdrawals processing this way
    rejectAndRecord(
        block,
        new BlockProcessingResult(
            Optional.empty(), new IllegalStateException("failed processing withdrawals")));

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("failed processing withdrawals");

    verify(badChainListener)
        .onBadChain(block.getHeader(), Collections.emptyList(), List.of(childBlockHeader));
  }

  @Test
  public void shouldNotEmitBadChainEventWhenTheParentIsMissing() {
    Block block = Mockito.mock(Block.class);
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(block.getHeader()).thenReturn(blockHeader);
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getParentHash()).thenReturn(Hash.fromHexStringLenient("0xdead"));
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    backwardChain.prependAncestorsHeader(
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader());
    backwardChain.prependAncestorsHeader(blockHeader);

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    BlockProcessingResult result = new BlockProcessingResult("Parent block not present");
    doReturn(result).when(blockValidator).validateAndProcessBlock(any(), any(), any(), any());

    assertThatThrownBy(() -> context.saveBlock(block)).isInstanceOf(BackwardSyncException.class);

    verify(badChainListener, never()).onBadChain(any(), any(), any());
  }

  @Test
  public void shouldFailWithoutValidatingAKnownBadBlock() {
    Block block = mockBlockWithParentOnChain();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    BlockHeader childBlockHeader =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader();
    backwardChain.clear();
    backwardChain.prependAncestorsHeader(childBlockHeader);
    backwardChain.prependAncestorsHeader(block.getHeader());

    badBlockManager.addBadHeader(
        block.getHeader(), BadBlockCause.fromValidationFailure("failed last session"));

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("known bad block");

    verify(blockValidator, never()).validateAndProcessBlock(any(), any(), any(), any());
    verify(badChainListener)
        .onBadChain(block.getHeader(), Collections.emptyList(), List.of(childBlockHeader));
  }

  @Test
  public void shouldFailWhenLinkingAHeaderKnownAsBad() {
    BlockHeader badHeader = remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).get().getHeader();
    BlockHeader childBlockHeader =
        remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 2).get().getHeader();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    backwardChain.prependAncestorsHeader(childBlockHeader);
    backwardChain.prependAncestorsHeader(badHeader);
    badBlockManager.addBadHeader(badHeader, BadBlockCause.fromValidationFailure("failed"));

    assertThatThrownBy(() -> context.failIfBadBlock(badHeader))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("known bad block");
    verify(badChainListener)
        .onBadChain(badHeader, Collections.emptyList(), List.of(childBlockHeader));
  }

  @Test
  public void shouldRecordTheBadBlockBeforeTheNextSessionReChecksIt() {
    // the real validator records the block, the pre-execution check of the next session relies on
    // it
    final BlockHeaderValidator rejectingHeaderValidator = Mockito.mock(BlockHeaderValidator.class);
    final BlockProcessor blockProcessor = Mockito.mock(BlockProcessor.class);
    when(rejectingHeaderValidator.validateHeader(any(), any(), any(), any())).thenReturn(false);
    final BlockValidator realValidator =
        MainnetBlockValidatorBuilder.frontier(
            rejectingHeaderValidator,
            Mockito.mock(BlockBodyValidator.class),
            blockProcessor,
            Mockito.mock(BlockAccessListValidator.class));
    doReturn(realValidator).when(context).getBlockValidatorForBlock(any());
    final Block block = remoteBlockchain.getBlockByNumber(LOCAL_HEIGHT + 1).orElseThrow();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("Header validation failed");
    assertThat(badBlockManager.getBadBlock(block.getHash())).contains(block);

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("known bad block");

    verify(rejectingHeaderValidator, times(1)).validateHeader(any(), any(), any(), any());
    verifyNoInteractions(blockProcessor);
    verify(badChainListener, times(2)).onBadChain(eq(block.getHeader()), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSkipDescendantsAlreadyMarkedWhenEmittingBadChainEvent() {
    Block block = mockBlockWithParentOnChain();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();
    for (int i = REMOTE_HEIGHT; i >= 0; i--) {
      backwardChain.prependAncestorsHeader(remoteBlockchain.getBlockByNumber(i).get().getHeader());
    }
    backwardChain.prependAncestorsHeader(block.getHeader());

    // an earlier session got as far as marking the first descendants
    final int alreadyMarked = 10;
    for (int i = 0; i < alreadyMarked; i++) {
      badBlockManager.addBadHeader(
          remoteBlockchain.getBlockByNumber(i).get().getHeader(),
          BadBlockCause.fromValidationFailure("marked"));
    }

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    rejectAndRecord(block, new BlockProcessingResult("custom error"));

    assertThatThrownBy(() -> context.saveBlock(block)).isInstanceOf(BackwardSyncException.class);

    final ArgumentCaptor<List<BlockHeader>> badBlockHeaderDescendants =
        ArgumentCaptor.forClass(List.class);
    verify(badChainListener)
        .onBadChain(
            eq(block.getHeader()),
            eq(Collections.emptyList()),
            badBlockHeaderDescendants.capture());
    assertThat(badBlockHeaderDescendants.getValue())
        .hasSize(TEST_MAX_BAD_CHAIN_EVENT_ENTRIES)
        .first()
        .isEqualTo(remoteBlockchain.getBlockByNumber(alreadyMarked).get().getHeader());
  }

  @Test
  public void shouldForgetAStaleEntryForABlockAlreadyOnTheChain() {
    final BlockHeader onChain = localBlockchain.getChainHeadHeader();
    badBlockManager.addBadHeader(onChain, BadBlockCause.fromValidationFailure("stale"));
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    context.failIfBadBlock(onChain);

    assertThat(badBlockManager.isBadBlock(onChain.getHash())).isFalse();
    verify(badChainListener, never()).onBadChain(any(), any(), any());
  }

  /** The validator records a rejected block before returning, like the real one does. */
  private void rejectAndRecord(final Block block, final BlockProcessingResult result) {
    doAnswer(
            invocation -> {
              badBlockManager.addBadBlock(
                  block, BadBlockCause.fromValidationFailure(result.errorMessage.orElseThrow()));
              return result;
            })
        .when(blockValidator)
        .validateAndProcessBlock(any(), any(), any(), any());
  }

  private Block mockBlockWithParentOnChain() {
    Block block = Mockito.mock(Block.class);
    BlockHeader blockHeader = Mockito.mock(BlockHeader.class);
    when(block.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(block.getHeader()).thenReturn(blockHeader);
    when(blockHeader.getHash()).thenReturn(Hash.fromHexStringLenient("0x42"));
    when(blockHeader.getParentHash()).thenReturn(localBlockchain.getChainHeadHash());
    return block;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldEmitBadChainEventWithIncludedBlockHeadersLimitedToMaxBadChainEventsSize() {
    Block block = mockBlockWithParentOnChain();
    BlockHeader blockHeader = block.getHeader();
    BadChainListener badChainListener = Mockito.mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);

    backwardChain.clear();

    for (int i = REMOTE_HEIGHT; i >= 0; i--) {
      backwardChain.prependAncestorsHeader(remoteBlockchain.getBlockByNumber(i).get().getHeader());
    }
    backwardChain.prependAncestorsHeader(blockHeader);

    doReturn(blockValidator).when(context).getBlockValidatorForBlock(any());
    rejectAndRecord(block, new BlockProcessingResult("custom error"));

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    final ArgumentCaptor<List<BlockHeader>> badBlockHeaderDescendants =
        ArgumentCaptor.forClass(List.class);
    verify(badChainListener)
        .onBadChain(
            eq(blockHeader), eq(Collections.emptyList()), badBlockHeaderDescendants.capture());
    assertThat(badBlockHeaderDescendants.getValue()).hasSize(TEST_MAX_BAD_CHAIN_EVENT_ENTRIES);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldEmitBadChainEventWithIncludedBlocksLimitedToMaxBadChainEventsSize() {
    Block block = mockBlockWithParentOnChain();
    BlockHeader blockHeader = block.getHeader();
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
    rejectAndRecord(block, new BlockProcessingResult("custom error"));

    assertThatThrownBy(() -> context.saveBlock(block))
        .isInstanceOf(BackwardSyncException.class)
        .hasMessageContaining("custom error");

    final ArgumentCaptor<List<Block>> badBlockDescendants = ArgumentCaptor.forClass(List.class);
    final ArgumentCaptor<List<BlockHeader>> badBlockHeaderDescendants =
        ArgumentCaptor.forClass(List.class);
    verify(badChainListener)
        .onBadChain(
            eq(blockHeader), badBlockDescendants.capture(), badBlockHeaderDescendants.capture());
    // the bodies beyond the descendant body cap are handed over as headers
    assertThat(badBlockDescendants.getValue()).hasSize(BadBlockManager.MAX_BAD_DESCENDANT_BODIES);
    assertThat(badBlockHeaderDescendants.getValue())
        .hasSize(TEST_MAX_BAD_CHAIN_EVENT_ENTRIES - BadBlockManager.MAX_BAD_DESCENDANT_BODIES);
  }

  @Test
  public void shouldFailAfterMaxNumberOfRetries() {
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    final var syncFuture = context.syncBackwardsUntil(Hash.ZERO);

    assertThatThrownBy(syncFuture::get)
        .cause()
        .hasMessageContaining("Max number of retries " + NUM_OF_RETRIES + " reached");
  }

  @Test
  public void whenBlockNotFoundInPeers_shouldRemoveBlockFromQueueAndProgressInNextSession()
      throws Exception {
    when(backwardSyncAlgorithmFactory.createBackwardSyncAlgorithm(context))
        .thenReturn(backwardSyncAlgorithm);
    when(backwardSyncAlgorithm.executeBackwardsSync(null))
        .thenReturn(CompletableFuture.completedFuture(null));

    // This scenario can happen due to a reorg
    // Expectation we progress beyond the reorg block upon receiving the next FCU

    // choose an intermediate remote block to create a reorg block from
    int reorgBlockHeight = REMOTE_HEIGHT - 1; // 49
    final Hash reorgBlockParentHash = getRemoteBlockByNumber(reorgBlockHeight - 1).getHash();
    final Block reorgBlock = createBlock(reorgBlockHeight, reorgBlockParentHash);

    // represents first FCU with a block that will become reorged away
    final CompletableFuture<Void> fcuBeforeReorg = context.syncBackwardsUntil(reorgBlock.getHash());
    fcuBeforeReorg.get();
    assertThat(localBlockchain.getChainHeadBlockNumber()).isLessThan(reorgBlockHeight);

    // represents subsequent FCU with successfully reorged version of the same block
    final CompletableFuture<Void> fcuAfterReorg =
        context.syncBackwardsUntil(getRemoteBlockByNumber(reorgBlockHeight).getHash());
    fcuAfterReorg.get();
    assertThat(backwardChain.getHashesToAppend().getLast())
        .isEqualTo(getRemoteBlockByNumber(reorgBlockHeight).getHash());
  }
}
