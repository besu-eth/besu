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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer;
import org.hyperledger.besu.ethereum.eth.manager.RespondingEthPeer.Responder;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockHashesMessage;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockMessage;
import org.hyperledger.besu.ethereum.eth.sync.BlockPropagationManager.ProcessingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.PendingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.ByteUnits;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public abstract class AbstractBlockPropagationManagerTest {

  private static final Bytes NODE_ID_1 = Bytes.fromHexString("0x00");

  protected BlockchainSetupUtil blockchainUtil;
  protected ProtocolSchedule protocolSchedule;
  protected ProtocolContext protocolContext;
  protected MutableBlockchain blockchain;
  protected BlockBroadcaster blockBroadcaster;
  protected EthProtocolManager ethProtocolManager;
  protected BlockPropagationManager blockPropagationManager;
  protected SynchronizerConfiguration syncConfig;
  private PeerTaskExecutor peerTaskExecutor;
  protected final PendingBlocksManager pendingBlocksManager =
      spy(
          new PendingBlocksManager(
              SynchronizerConfiguration.builder().blockPropagationRange(-10, 30).build()));
  protected final ProcessingBlocksManager processingBlocksManager =
      spy(new ProcessingBlocksManager());
  protected SyncState syncState;
  protected final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  private final Hash finalizedHash = Hash.fromHexStringLenient("0x1337");
  private final int maxMessageSize = 10 * ByteUnits.MEGABYTE;

  protected void setup(final DataStorageFormat dataStorageFormat) {
    peerTaskExecutor = Mockito.mock(PeerTaskExecutor.class);
    blockchainUtil = BlockchainSetupUtil.forTesting(dataStorageFormat);
    blockchain = blockchainUtil.getBlockchain();
    protocolSchedule = blockchainUtil.getProtocolSchedule();
    final ProtocolContext tempProtocolContext = blockchainUtil.getProtocolContext();
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(tempProtocolContext.getWorldStateArchive())
            .withConsensusContext(tempProtocolContext.getConsensusContext(ConsensusContext.class))
            .build();
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(blockchain)
            .setWorldStateArchive(blockchainUtil.getWorldArchive())
            .setTransactionPool(blockchainUtil.getTransactionPool())
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .setPeerTaskExecutor(peerTaskExecutor)
            .build();
    syncConfig = SynchronizerConfiguration.builder().blockPropagationRange(-3, 5).build();
    syncState = new SyncState(blockchain, ethProtocolManager.ethContext().getEthPeers());
    blockBroadcaster = mock(BlockBroadcaster.class);
    blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster,
            processingBlocksManager);

    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetHeadersFromPeerTask.class), Mockito.any(EthPeer.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetHeadersFromPeerTask.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetBodiesFromPeerTask.class), Mockito.any(EthPeer.class)))
        .thenAnswer(
            new GetBodiesFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetBodiesFromPeerTask.class)))
        .thenAnswer(
            new GetBodiesFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));
  }

  @Test
  public void importsAnnouncedBlocks_aheadOfChainInOrder() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);
    final Block nextNextBlock = blockchainUtil.getBlock(3);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup additional peer for best peers list
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));
    final NewBlockHashesMessage nextNextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextNextBlock.getHash(), nextNextBlock.getHeader().getNumber())));
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast second message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextNextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isTrue();
  }

  @Test
  public void importsAnnouncedBlocks_aheadOfChainOutOfOrder() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);
    final Block nextNextBlock = blockchainUtil.getBlock(3);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));
    final NewBlockHashesMessage nextNextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextNextBlock.getHash(), nextNextBlock.getHeader().getNumber())));
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast second message first
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextNextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isTrue();
  }

  @Test
  public void importsAnnouncedNewBlocks_aheadOfChainInOrder() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);
    final Block nextNextBlock = blockchainUtil.getBlock(3);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage nextAnnouncement =
        NewBlockMessage.create(
            nextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get(),
            maxMessageSize);
    final NewBlockMessage nextNextAnnouncement =
        NewBlockMessage.create(
            nextNextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextNextBlock.getHash()).get(),
            maxMessageSize);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast second message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextNextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isTrue();
  }

  @Test
  public void importsAnnouncedNewBlocks_aheadOfChainOutOfOrder() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);
    final Block nextNextBlock = blockchainUtil.getBlock(3);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage nextAnnouncement =
        NewBlockMessage.create(
            nextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get(),
            maxMessageSize);
    final NewBlockMessage nextNextAnnouncement =
        NewBlockMessage.create(
            nextNextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextNextBlock.getHash()).get(),
            maxMessageSize);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast second message first
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextNextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    assertThat(blockchain.contains(nextNextBlock.getHash())).isTrue();
  }

  @Test
  public void importsMixedOutOfOrderMessages() {
    blockchainUtil.importFirstBlocks(2);
    final Block block1 = blockchainUtil.getBlock(2);
    final Block block2 = blockchainUtil.getBlock(3);
    final Block block3 = blockchainUtil.getBlock(4);
    final Block block4 = blockchainUtil.getBlock(5);

    // Sanity check
    assertThat(blockchain.contains(block1.getHash())).isFalse();
    assertThat(blockchain.contains(block2.getHash())).isFalse();
    assertThat(blockchain.contains(block3.getHash())).isFalse();
    assertThat(blockchain.contains(block4.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage block1Msg =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    block1.getHash(), block1.getHeader().getNumber())));
    final NewBlockMessage block2Msg =
        NewBlockMessage.create(
            block2,
            getFullBlockchain().getTotalDifficultyByHash(block2.getHash()).get(),
            maxMessageSize);
    final NewBlockHashesMessage block3Msg =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    block3.getHash(), block3.getHeader().getNumber())));
    final NewBlockMessage block4Msg =
        NewBlockMessage.create(
            block4,
            getFullBlockchain().getTotalDifficultyByHash(block4.getHash()).get(),
            maxMessageSize);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast older blocks
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, block3Msg);
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, block4Msg);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, block2Msg);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast first block
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, block1Msg);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(block1.getHash())).isTrue();
    assertThat(blockchain.contains(block2.getHash())).isTrue();
    assertThat(blockchain.contains(block3.getHash())).isTrue();
    assertThat(blockchain.contains(block4.getHash())).isTrue();
  }

  @Test
  public void handlesDuplicateAnnouncements() {

    final ProtocolSchedule stubProtocolSchedule = spy(protocolSchedule);
    final ProtocolSpec stubProtocolSpec = spy(protocolSchedule.getByBlockHeader(blockHeader(2)));
    final BlockImporter stubBlockImporter = spy(stubProtocolSpec.getBlockImporter());
    doReturn(stubProtocolSpec).when(stubProtocolSchedule).getByBlockHeader(any());
    doReturn(stubBlockImporter).when(stubProtocolSpec).getBlockImporter();
    final BlockPropagationManager blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            stubProtocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster);

    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage newBlockHash =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));
    final NewBlockMessage newBlock =
        NewBlockMessage.create(
            nextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get(),
            maxMessageSize);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlock);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast duplicate
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlockHash);
    peer.respondWhile(responder, peer::hasOutstandingRequests);
    // Broadcast duplicate
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlock);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    verify(stubBlockImporter, times(1)).importBlock(eq(protocolContext), eq(nextBlock), any());
  }

  @Test
  public void handlesPendingDuplicateAnnouncements() {
    final ProtocolSchedule stubProtocolSchedule = spy(protocolSchedule);
    final ProtocolSpec stubProtocolSpec = spy(protocolSchedule.getByBlockHeader(blockHeader(2)));
    final BlockImporter stubBlockImporter = spy(stubProtocolSpec.getBlockImporter());
    doReturn(stubProtocolSpec).when(stubProtocolSchedule).getByBlockHeader(any());
    doReturn(stubBlockImporter).when(stubProtocolSpec).getBlockImporter();
    final BlockPropagationManager blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            stubProtocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster);
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage newBlockHash =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));
    final NewBlockMessage newBlock =
        NewBlockMessage.create(
            nextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get(),
            maxMessageSize);

    // Broadcast messages
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlock);
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlockHash);
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlock);
    // Respond
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
    verify(stubBlockImporter, times(1)).importBlock(eq(protocolContext), eq(nextBlock), any());
  }

  @Test
  public void ignoresFutureNewBlockHashAnnouncement() {
    blockchainUtil.importFirstBlocks(2);
    final Block futureBlock = blockchainUtil.getBlock(11);

    // Sanity check
    assertThat(blockchain.contains(futureBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage futureAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    futureBlock.getHash(), futureBlock.getHeader().getNumber())));

    // Broadcast
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, futureAnnouncement);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(futureBlock.getHash())).isFalse();
  }

  @Test
  public void ignoresFutureNewBlockAnnouncement() {
    blockchainUtil.importFirstBlocks(2);
    final Block futureBlock = blockchainUtil.getBlock(11);

    // Sanity check
    assertThat(blockchain.contains(futureBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage futureAnnouncement =
        NewBlockMessage.create(
            futureBlock,
            getFullBlockchain().getTotalDifficultyByHash(futureBlock.getHash()).get(),
            maxMessageSize);

    // Broadcast
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, futureAnnouncement);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockchain.contains(futureBlock.getHash())).isFalse();
  }

  @Test
  public void ignoresOldNewBlockHashAnnouncement() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    blockchainUtil.importFirstBlocks(10);
    final Block blockOne = blockchainUtil.getBlock(1);
    final Block oldBlock = gen.nextBlock(blockOne);

    // Sanity check
    assertThat(blockchain.contains(oldBlock.getHash())).isFalse();

    final BlockPropagationManager propManager = spy(blockPropagationManager);
    propManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage oldAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    oldBlock.getHash(), oldBlock.getHeader().getNumber())));

    // Broadcast
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, oldAnnouncement);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    verify(propManager, times(0)).importOrSavePendingBlock(any(), any(Bytes.class));
    assertThat(blockchain.contains(oldBlock.getHash())).isFalse();
  }

  @Test
  public void ignoresOldNewBlockAnnouncement() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    blockchainUtil.importFirstBlocks(10);
    final Block blockOne = blockchainUtil.getBlock(1);
    final Block oldBlock = gen.nextBlock(blockOne);

    // Sanity check
    assertThat(blockchain.contains(oldBlock.getHash())).isFalse();

    final BlockPropagationManager propManager = spy(blockPropagationManager);
    propManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage oldAnnouncement =
        NewBlockMessage.create(oldBlock, Difficulty.ZERO, maxMessageSize);

    // Broadcast
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, oldAnnouncement);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    verify(propManager, times(0)).importOrSavePendingBlock(any(), any(Bytes.class));
    assertThat(blockchain.contains(oldBlock.getHash())).isFalse();
  }

  @Test
  public void purgesOldBlocks() {
    final int oldBlocksToImport = 3;
    syncConfig =
        SynchronizerConfiguration.builder().blockPropagationRange(-oldBlocksToImport, 5).build();
    final BlockPropagationManager blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethProtocolManager.ethContext(),
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster);

    final BlockDataGenerator gen = new BlockDataGenerator();
    // Import some blocks
    blockchainUtil.importFirstBlocks(5);
    // Set up test block next to head, that should eventually be purged
    final Block blockToPurge =
        gen.block(BlockOptions.create().setBlockNumber(blockchain.getChainHeadBlockNumber()));

    blockPropagationManager.start();
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage blockAnnouncementMsg =
        NewBlockMessage.create(blockToPurge, Difficulty.ZERO, maxMessageSize);

    // Broadcast
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, blockAnnouncementMsg);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    // Check that we pushed our block into the pending collection
    assertThat(blockchain.contains(blockToPurge.getHash())).isFalse();
    assertThat(pendingBlocksManager.contains(blockToPurge.getHash())).isTrue();

    // Import blocks until we bury the target block far enough to be cleaned up
    for (int i = 0; i < oldBlocksToImport; i++) {
      blockchainUtil.importBlockAtIndex((int) blockchain.getChainHeadBlockNumber() + 1);

      assertThat(blockchain.contains(blockToPurge.getHash())).isFalse();
      assertThat(pendingBlocksManager.contains(blockToPurge.getHash())).isTrue();
    }

    // Import again to trigger cleanup
    blockchainUtil.importBlockAtIndex((int) blockchain.getChainHeadBlockNumber() + 1);
    assertThat(blockchain.contains(blockToPurge.getHash())).isFalse();
    assertThat(pendingBlocksManager.contains(blockToPurge.getHash())).isFalse();
  }

  @Test
  public void updatesChainHeadWhenNewBlockMessageReceived() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final Difficulty parentTotalDifficulty =
        getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHeader().getParentHash()).get();
    final Difficulty totalDifficulty =
        getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get();
    final NewBlockMessage nextAnnouncement =
        NewBlockMessage.create(nextBlock, totalDifficulty, maxMessageSize);

    // Broadcast message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(peer.getEthPeer().chainState().getBestBlock().getHash())
        .isEqualTo(nextBlock.getHeader().getParentHash());
    assertThat(peer.getEthPeer().chainState().getEstimatedHeight())
        .isEqualTo(nextBlock.getHeader().getNumber() - 1);
    assertThat(peer.getEthPeer().chainState().getBestBlock().getTotalDifficulty())
        .isEqualTo(parentTotalDifficulty);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldNotImportBlocksThatAreAlreadyBeingImported() {
    final EthScheduler ethScheduler = mock(EthScheduler.class);
    when(ethScheduler.scheduleSyncWorkerTask(any(Supplier.class)))
        .thenReturn(new CompletableFuture<>());
    final EthContext ethContext =
        new EthContext(
            new EthPeers(
                () -> protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()),
                TestClock.fixed(),
                metricsSystem,
                EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
                Collections.emptyList(),
                Bytes.random(64),
                25,
                25,
                false,
                SyncMode.SNAP,
                new ForkIdManager(blockchain, Collections.emptyList(), Collections.emptyList())),
            new EthMessages(),
            ethScheduler,
            null);
    final BlockPropagationManager blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster);

    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    blockPropagationManager.importOrSavePendingBlock(nextBlock, NODE_ID_1);
    blockPropagationManager.importOrSavePendingBlock(nextBlock, NODE_ID_1);

    verify(ethScheduler, times(1)).scheduleSyncWorkerTask(any(Supplier.class));
  }

  @Test
  public void shouldRequestLowestAnnouncedPendingBlockParent() {
    // test if block propagation manager can recover if one block is missed

    blockchainUtil.importFirstBlocks(2);
    final List<Block> blocks = blockchainUtil.getBlocks().subList(2, 4);

    blockPropagationManager.start();

    // Create peer and responder
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // skip first block then create messages from blocklist
    blocks.stream()
        .skip(1)
        .map(this::createNewBlockHashMessage)
        .forEach(
            message -> { // Broadcast new block hash message
              EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, message);
            });

    peer.respondWhile(responder, peer::hasOutstandingRequests);

    // assert all blocks were imported
    blocks.forEach(
        block -> {
          assertThat(blockchain.contains(block.getHash())).isTrue();
        });
  }

  @Test
  public void shouldRequestLowestAnnouncedPendingBlockParent_twoMissingBlocks() {
    // test if block propagation manager can recover if one block is missed
    blockchainUtil.importFirstBlocks(2);
    final List<Block> blocks = blockchainUtil.getBlocks().subList(2, 6);

    blockPropagationManager.start();

    // Create peer and responder
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    // skip two block then create messages from blocklist
    blocks.stream()
        .skip(2)
        .map(this::createNewBlockHashMessage)
        .forEach(
            message -> { // Broadcast new block hash message
              EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, message);
            });

    peer.respondWhile(responder, peer::hasOutstandingRequests);

    // assert all blocks were imported
    blocks.forEach(
        block -> {
          assertThat(blockchain.contains(block.getHash())).isTrue();
        });
  }

  private NewBlockHashesMessage createNewBlockHashMessage(final Block block) {
    return NewBlockHashesMessage.create(
        Collections.singletonList(
            new NewBlockHashesMessage.NewBlockHash(
                block.getHash(), block.getHeader().getNumber())));
  }

  @Test
  public void verifyBroadcastBlockInvocation() {
    blockchainUtil.importFirstBlocks(2);
    final Block block = blockchainUtil.getBlock(2);
    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);

    final Difficulty totalDifficulty =
        getFullBlockchain().getTotalDifficultyByHash(block.getHash()).get();
    final NewBlockMessage newBlockMessage =
        NewBlockMessage.create(block, totalDifficulty, maxMessageSize);

    // Broadcast message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, newBlockMessage);

    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    verify(blockBroadcaster, times(1)).propagate(block, totalDifficulty);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDetectAndCacheInvalidBlocks() {
    final EthScheduler ethScheduler = mock(EthScheduler.class);
    when(ethScheduler.scheduleSyncWorkerTask(any(Supplier.class)))
        .thenAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(final InvocationOnMock invocation) throws Throwable {
                return invocation.getArgument(0, Supplier.class).get();
              }
            });

    final EthContext ethContext =
        new EthContext(
            new EthPeers(
                () -> protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()),
                TestClock.fixed(),
                metricsSystem,
                EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
                Collections.emptyList(),
                Bytes.random(64),
                25,
                25,
                false,
                SyncMode.SNAP,
                new ForkIdManager(blockchain, Collections.emptyList(), Collections.emptyList())),
            new EthMessages(),
            ethScheduler,
            null);
    final BlockPropagationManager blockPropagationManager =
        new BlockPropagationManager(
            syncConfig,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            pendingBlocksManager,
            metricsSystem,
            blockBroadcaster);

    blockchainUtil.importFirstBlocks(2);
    final Block firstBlock = blockchainUtil.getBlock(1);
    final BadBlockManager badBlocksManager = protocolContext.getBadBlockManager();
    final Block badBlock =
        new BlockDataGenerator()
            .block(
                BlockDataGenerator.BlockOptions.create()
                    .setBlockNumber(1)
                    .setParentHash(firstBlock.getHash())
                    .setBlockHeaderFunctions(new MainnetBlockHeaderFunctions()));

    assertThat(badBlocksManager.getBadBlocks()).isEmpty();
    blockPropagationManager.importOrSavePendingBlock(badBlock, NODE_ID_1);
    assertThat(badBlocksManager.getBadBlocks().size()).isEqualTo(1);

    verify(ethScheduler, times(1)).scheduleSyncWorkerTask(any(Supplier.class));
  }

  @Test
  public void shouldTryWithAnotherPeerWhenFailedDownloadingBlock() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final RespondingEthPeer secondPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 2);

    // Pretend the second peer is busier, so the first is selected a first
    when(spy(secondPeer.getEthPeer()).outstandingRequests()).thenReturn(1);

    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));

    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(RespondingEthPeer.emptyResponder(), peer::hasOutstandingRequests);
    secondPeer.respondWhile(
        RespondingEthPeer.blockchainResponder(getFullBlockchain()),
        secondPeer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
  }

  @Test
  public void shouldThrowErrorWhenNoValidPeerAvailable() {
    Mockito.reset(peerTaskExecutor);
    Mockito.when(peerTaskExecutor.executeAgainstPeer(Mockito.any(), Mockito.any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE,
                Collections.emptyList()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any()))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(),
                PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE,
                Collections.emptyList()));
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    // Setup peer and messages
    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final RespondingEthPeer secondPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);

    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));

    // Broadcast first message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(RespondingEthPeer.emptyResponder(), peer::hasOutstandingRequests);
    secondPeer.respondWhile(RespondingEthPeer.emptyResponder(), secondPeer::hasOutstandingRequests);

    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
  }

  @Test
  public void shouldStopWhenFinalized() {
    blockPropagationManager.start();
    // syncState.setReachedTerminalDifficulty(true);
    blockPropagationManager.onNewUnverifiedForkchoice(
        new ForkchoiceEvent(null, null, this.finalizedHash));
    assertThat(blockPropagationManager.isRunning()).isFalse();
    assertThat(ethProtocolManager.ethContext().getEthMessages().messageCodesHandled())
        .doesNotContain(EthProtocolMessages.NEW_BLOCK_HASHES, EthProtocolMessages.NEW_BLOCK);
  }

  @Test
  public void shouldRestartWhenTTDReachedReturnsFalseAfterFinalizing() {
    blockPropagationManager.start();
    syncState.setReachedTerminalDifficulty(true);
    blockPropagationManager.onNewUnverifiedForkchoice(
        new ForkchoiceEvent(null, null, this.finalizedHash));
    assertThat(blockPropagationManager.isRunning()).isFalse();
    syncState.setReachedTerminalDifficulty(false);
    assertThat(blockPropagationManager.isRunning()).isTrue();
  }

  @Test
  public void shouldNotListenToNewBlockHashesAnnouncementsWhenTTDReachedAndFinal() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    syncState.setReachedTerminalDifficulty(true);
    blockPropagationManager.onNewUnverifiedForkchoice(
        new ForkchoiceEvent(null, null, this.finalizedHash));
    // Broadcast message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockPropagationManager.isRunning()).isFalse();
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
  }

  @Test
  public void shouldNotListenToNewBlockAnnouncementsWhenTTDReachedAndFinal() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    final RespondingEthPeer peer = EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockMessage nextAnnouncement =
        NewBlockMessage.create(
            nextBlock,
            getFullBlockchain().getTotalDifficultyByHash(nextBlock.getHash()).get(),
            maxMessageSize);
    final Responder responder = RespondingEthPeer.blockchainResponder(getFullBlockchain());

    syncState.setReachedTerminalDifficulty(true);
    blockPropagationManager.onNewUnverifiedForkchoice(
        new ForkchoiceEvent(null, null, this.finalizedHash));
    // Broadcast message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, peer, nextAnnouncement);
    peer.respondWhile(responder, peer::hasOutstandingRequests);

    assertThat(blockPropagationManager.isRunning()).isFalse();
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();
  }

  @Test
  public void shouldNotListenToBlockAddedEventsWhenTTDReachedAndFinal() {
    blockchainUtil.importFirstBlocks(2);

    blockPropagationManager.start();

    syncState.setReachedTerminalDifficulty(true);
    blockPropagationManager.onNewUnverifiedForkchoice(
        new ForkchoiceEvent(null, null, this.finalizedHash));
    blockchainUtil.importBlockAtIndex(2);

    assertThat(blockPropagationManager.isRunning()).isFalse();
    verifyNoInteractions(pendingBlocksManager);
  }

  @Test
  public void shouldRequestBlockFromOtherPeersIfFirstPeerFails() {
    blockchainUtil.importFirstBlocks(2);
    final Block nextBlock = blockchainUtil.getBlock(2);

    // Sanity check
    assertThat(blockchain.contains(nextBlock.getHash())).isFalse();

    blockPropagationManager.start();

    final RespondingEthPeer firstPeer =
        EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager, 0);
    final NewBlockHashesMessage nextAnnouncement =
        NewBlockHashesMessage.create(
            Collections.singletonList(
                new NewBlockHashesMessage.NewBlockHash(
                    nextBlock.getHash(), nextBlock.getHeader().getNumber())));

    Mockito.reset(peerTaskExecutor);
    when(peerTaskExecutor.executeAgainstPeer(
            Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(firstPeer.getEthPeer())))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, Collections.emptyList()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetHeadersFromPeerTask.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));
    Mockito.when(
            peerTaskExecutor.executeAgainstPeer(
                Mockito.any(GetBodiesFromPeerTask.class), Mockito.eq(firstPeer.getEthPeer())))
        .thenReturn(
            new PeerTaskExecutorResult<>(
                Optional.empty(), PeerTaskExecutorResponseCode.TIMEOUT, Collections.emptyList()));
    Mockito.when(peerTaskExecutor.execute(Mockito.any(GetBodiesFromPeerTask.class)))
        .thenAnswer(
            new GetBodiesFromPeerTaskExecutorAnswer(
                getFullBlockchain(), ethProtocolManager.ethContext().getEthPeers()));

    // Broadcast message
    EthProtocolManagerTestUtil.broadcastMessage(ethProtocolManager, firstPeer, nextAnnouncement);

    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(
            Mockito.any(GetHeadersFromPeerTask.class), Mockito.eq(firstPeer.getEthPeer()));
    Mockito.verify(peerTaskExecutor).execute(Mockito.any(GetHeadersFromPeerTask.class));
    Mockito.verify(peerTaskExecutor)
        .executeAgainstPeer(
            Mockito.any(GetBodiesFromPeerTask.class), Mockito.eq(firstPeer.getEthPeer()));
    Mockito.verify(peerTaskExecutor).execute(Mockito.any(GetBodiesFromPeerTask.class));
    Mockito.verifyNoMoreInteractions(peerTaskExecutor);

    verify(processingBlocksManager).addRequestedBlock(nextBlock.getHash());
    verify(processingBlocksManager).addImportingBlock(nextBlock.getHash());
    verify(processingBlocksManager).registerReceivedBlock(nextBlock);
    verify(processingBlocksManager).registerBlockImportDone(nextBlock.getHash());

    assertThat(blockchain.contains(nextBlock.getHash())).isTrue();
  }

  public abstract Blockchain getFullBlockchain();

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
