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

import static org.hyperledger.besu.util.FutureUtils.exceptionallyCompose;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceListener;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockHashesMessage;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockHashesMessage.NewBlockHash;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockMessage;
import org.hyperledger.besu.ethereum.eth.sync.state.PendingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.sync.tasks.PersistBlockTask;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.plugin.data.AddedBlockContext.EventType;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockPropagationManager implements UnverifiedForkchoiceListener {
  private static final Logger LOG = LoggerFactory.getLogger(BlockPropagationManager.class);
  private static final long TIMEOUT = Duration.ofSeconds(60L).toMillis();
  private final SynchronizerConfiguration config;
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final SyncState syncState;
  private final MetricsSystem metricsSystem;
  private final BlockBroadcaster blockBroadcaster;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ProcessingBlocksManager processingBlocksManager;
  private final PendingBlocksManager pendingBlocksManager;
  private Optional<Long> onBlockAddedSId = Optional.empty();
  private Optional<Long> newBlockSId;
  private Optional<Long> newBlockHashesSId;

  BlockPropagationManager(
      final SynchronizerConfiguration config,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final PendingBlocksManager pendingBlocksManager,
      final MetricsSystem metricsSystem,
      final BlockBroadcaster blockBroadcaster) {
    this(
        config,
        protocolSchedule,
        protocolContext,
        ethContext,
        syncState,
        pendingBlocksManager,
        metricsSystem,
        blockBroadcaster,
        new ProcessingBlocksManager());
  }

  BlockPropagationManager(
      final SynchronizerConfiguration config,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final PendingBlocksManager pendingBlocksManager,
      final MetricsSystem metricsSystem,
      final BlockBroadcaster blockBroadcaster,
      final ProcessingBlocksManager processingBlocksManager) {
    this.config = config;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
    this.blockBroadcaster = blockBroadcaster;
    this.syncState = syncState;
    this.pendingBlocksManager = pendingBlocksManager;
    this.syncState.subscribeTTDReached(this::reactToTTDReachedEvent);
    this.processingBlocksManager = processingBlocksManager;
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      setupListeners();
    } else {
      throw new IllegalStateException(
          "Attempt to start an already started " + this.getClass().getSimpleName() + ".");
    }
  }

  public void stop() {
    if (started.get()) {
      clearListeners();
      started.set(false);
    } else {
      LOG.debug("Attempted to stop when we are not even running...");
    }
  }

  public boolean isRunning() {
    return started.get();
  }

  private void setupListeners() {
    onBlockAddedSId =
        Optional.of(protocolContext.getBlockchain().observeBlockAdded(this::onBlockAdded));
    newBlockSId =
        Optional.of(
            ethContext
                .getEthMessages()
                .subscribe(EthProtocolMessages.NEW_BLOCK, this::handleNewBlockFromNetwork));
    newBlockHashesSId =
        Optional.of(
            ethContext
                .getEthMessages()
                .subscribe(
                    EthProtocolMessages.NEW_BLOCK_HASHES, this::handleNewBlockHashesFromNetwork));
  }

  private void clearListeners() {
    onBlockAddedSId.ifPresent(id -> protocolContext.getBlockchain().removeObserver(id));
    newBlockSId.ifPresent(
        id -> ethContext.getEthMessages().unsubscribe(id, EthProtocolMessages.NEW_BLOCK));
    newBlockHashesSId.ifPresent(
        id -> ethContext.getEthMessages().unsubscribe(id, EthProtocolMessages.NEW_BLOCK_HASHES));
    onBlockAddedSId = Optional.empty();
    newBlockSId = Optional.empty();
    newBlockHashesSId = Optional.empty();
  }

  private void onBlockAdded(final BlockAddedEvent blockAddedEvent) {
    // Check to see if any of our pending blocks are now ready for import
    LOG.atTrace()
        .setMessage("Block added event type {} for block {}. Current status {}")
        .addArgument(blockAddedEvent::getEventType)
        .addArgument(blockAddedEvent.getHeader()::toLogString)
        .addArgument(this)
        .log();

    // If there is no children to process, maybe try non announced blocks
    if (!maybeProcessPendingChildrenBlocks(blockAddedEvent.getHeader())) {
      LOG.atTrace()
          .setMessage("There are no pending blocks ready to import for block {}")
          .addArgument(blockAddedEvent.getHeader()::toLogString)
          .log();
      maybeProcessNonAnnouncedBlocks(blockAddedEvent);
    }

    if (blockAddedEvent.getEventType().equals(EventType.HEAD_ADVANCED)) {
      final long head = protocolContext.getBlockchain().getChainHeadBlockNumber();
      final long cutoff = head + config.getBlockPropagationRange().lowerEndpoint();
      pendingBlocksManager.purgeBlocksOlderThan(cutoff);
    }
  }

  /**
   * Process pending Children if any
   *
   * @param header the header of the block to process the children of
   * @return true if block has any pending child
   */
  private boolean maybeProcessPendingChildrenBlocks(final BlockHeader header) {
    final List<Block> readyForImport;
    synchronized (pendingBlocksManager) {
      // Remove block from pendingBlocks list
      pendingBlocksManager.deregisterPendingBlock(header);

      // Import any pending blocks that are children of the newly added block
      readyForImport = pendingBlocksManager.childrenOf(header.getHash());
    }

    if (!readyForImport.isEmpty()) {

      LOG.atTrace()
          .setMessage("Ready to import pending blocks found [{}] for block {}")
          .addArgument(
              () ->
                  readyForImport.stream().map(Block::toLogString).collect(Collectors.joining(", ")))
          .addArgument(header::toLogString)
          .log();

      final Supplier<CompletableFuture<List<Block>>> importBlocksTask =
          PersistBlockTask.forUnorderedBlocks(
              protocolSchedule,
              protocolContext,
              ethContext,
              readyForImport,
              HeaderValidationMode.FULL,
              metricsSystem);
      ethContext
          .getScheduler()
          .scheduleSyncWorkerTask(importBlocksTask)
          .whenComplete(
              (r, t) -> {
                if (r != null) {
                  LOG.info(
                      "Imported {} pending blocks: {}",
                      r.size(),
                      r.stream().map(b -> b.getHeader().getNumber()).collect(Collectors.toList()));
                }
                if (t != null) {
                  LOG.error("Error importing pending blocks", t);
                }
              });
    }
    return !readyForImport.isEmpty();
  }

  private void maybeProcessNonAnnouncedBlocks(final BlockAddedEvent newBlock) {
    final long localHeadBlockNumber = protocolContext.getBlockchain().getChainHeadBlockNumber();

    if (newBlock.getHeader().getNumber() > localHeadBlockNumber) {
      pendingBlocksManager
          .lowestAnnouncedBlock()
          .map(ProcessableBlockHeader::getNumber)
          .ifPresent(
              minAnnouncedBlockNumber -> {
                final long distance = minAnnouncedBlockNumber - localHeadBlockNumber;
                LOG.trace(
                    "Found lowest announced block {} with distance {}",
                    minAnnouncedBlockNumber,
                    distance);

                final long firstNonAnnouncedBlockNumber = newBlock.getHeader().getNumber() + 1;

                if (distance < config.getBlockPropagationRange().upperEndpoint()
                    && minAnnouncedBlockNumber > firstNonAnnouncedBlockNumber) {

                  if (processingBlocksManager.addNonAnnouncedBlocks(firstNonAnnouncedBlockNumber)) {
                    retrieveNonAnnouncedBlock(firstNonAnnouncedBlockNumber);
                  }
                }
              });
    }
  }

  private void handleNewBlockFromNetwork(final EthMessage message) {
    final Blockchain blockchain = protocolContext.getBlockchain();
    final NewBlockMessage newBlockMessage = NewBlockMessage.readFrom(message.getData());
    try {
      final Block block = newBlockMessage.block(protocolSchedule);
      LOG.atTrace()
          .setMessage("New block from network {} from peer {}. Current status {}")
          .addArgument(block::toLogString)
          .addArgument(message::getPeer)
          .addArgument(this)
          .log();

      final Difficulty totalDifficulty = newBlockMessage.totalDifficulty(protocolSchedule);

      message.getPeer().chainState().updateForAnnouncedBlock(block.getHeader(), totalDifficulty);

      // Return early if we don't care about this block
      final long localChainHeight = protocolContext.getBlockchain().getChainHeadBlockNumber();
      final long bestChainHeight = syncState.bestChainHeight(localChainHeight);
      if (!shouldImportBlockAtHeight(
          block.getHeader().getNumber(), localChainHeight, bestChainHeight)) {
        LOG.atTrace()
            .setMessage(
                "Do not import new block from network {}, current chain heights are: local {}, best {}")
            .addArgument(block::toLogString)
            .addArgument(localChainHeight)
            .addArgument(bestChainHeight)
            .log();
        return;
      }
      if (pendingBlocksManager.contains(block.getHash())) {
        LOG.atTrace()
            .setMessage("New block from network {} is already pending")
            .addArgument(block::toLogString)
            .log();
        return;
      }
      if (blockchain.contains(block.getHash())) {
        LOG.atTrace()
            .setMessage("New block from network {} is already present")
            .addArgument(block::toLogString)
            .log();
        return;
      }
      if (protocolContext.getBadBlockManager().isBadBlock(block.getHash())) {
        LOG.atTrace()
            .setMessage("Ignoring known-bad block {} from peer {}")
            .addArgument(block::toLogString)
            .addArgument(message::getPeer)
            .log();
        return;
      }

      importOrSavePendingBlock(block, message.getPeer().nodeId());
    } catch (final RLPException e) {
      LOG.debug(
          "Malformed NEW_BLOCK message received from peer (BREACH_OF_PROTOCOL), disconnecting: {}",
          message.getPeer(),
          e);
      message.getPeer().disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    }
  }

  private void handleNewBlockHashesFromNetwork(final EthMessage message) {
    final Blockchain blockchain = protocolContext.getBlockchain();
    final NewBlockHashesMessage newBlockHashesMessage =
        NewBlockHashesMessage.readFrom(message.getData());
    try {
      // Register announced blocks
      final List<NewBlockHash> announcedBlocks =
          Lists.newArrayList(newBlockHashesMessage.getNewHashes());
      LOG.atTrace()
          .setMessage("New block hashes from network {} from peer {}. Current status {}")
          .addArgument(() -> toLogString(announcedBlocks))
          .addArgument(message::getPeer)
          .addArgument(this)
          .log();

      for (final NewBlockHash announcedBlock : announcedBlocks) {
        message.getPeer().registerKnownBlock(announcedBlock.hash());
        message.getPeer().registerHeight(announcedBlock.hash(), announcedBlock.number());
      }

      // Filter announced blocks for blocks we care to import
      final long localChainHeight = protocolContext.getBlockchain().getChainHeadBlockNumber();
      final long bestChainHeight = syncState.bestChainHeight(localChainHeight);
      final List<NewBlockHash> relevantAnnouncements =
          announcedBlocks.stream()
              .filter(a -> shouldImportBlockAtHeight(a.number(), localChainHeight, bestChainHeight))
              .collect(Collectors.toList());

      // Filter for blocks we don't yet know about
      final List<NewBlockHash> newBlocks = new ArrayList<>();
      for (final NewBlockHash announcedBlock : relevantAnnouncements) {
        if (pendingBlocksManager.contains(announcedBlock.hash())) {
          LOG.trace("New block hash from network {} is already pending", announcedBlock);
          continue;
        }
        if (processingBlocksManager.alreadyImporting(announcedBlock.hash())) {
          LOG.trace("New block hash from network {} is already importing", announcedBlock);
          continue;
        }
        if (blockchain.contains(announcedBlock.hash())) {
          LOG.trace("New block hash from network {} was already imported", announcedBlock);
          continue;
        }
        if (protocolContext.getBadBlockManager().isBadBlock(announcedBlock.hash())) {
          LOG.trace("New block hash from network {} is a known bad block", announcedBlock);
          continue;
        }
        if (processingBlocksManager.addRequestedBlock(announcedBlock.hash())) {
          newBlocks.add(announcedBlock);
        } else {
          LOG.trace("New block hash from network {} was already requested", announcedBlock);
        }
      }

      // Process known blocks we care about
      for (final NewBlockHash newBlock : newBlocks) {
        processAnnouncedBlock(message.getPeer(), newBlock);
      }
    } catch (final RLPException e) {
      LOG.debug(
          "Malformed NEW_BLOCK_HASHES message received from peer (BREACH_OF_PROTOCOL), disconnecting: {}",
          message.getPeer(),
          e);
      message.getPeer().disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    }
  }

  private CompletableFuture<Block> retrieveNonAnnouncedBlock(final long blockNumber) {
    LOG.trace("Retrieve non announced block {} from peers", blockNumber);
    return getBlockFromPeers(Optional.empty(), blockNumber, Optional.empty());
  }

  private CompletableFuture<Block> processAnnouncedBlock(
      final EthPeer peer, final NewBlockHash blockHash) {
    LOG.trace("Retrieve announced block by header {} from peers", blockHash);
    return getBlockFromPeers(Optional.of(peer), blockHash.number(), Optional.of(blockHash.hash()));
  }

  private void requestParentBlock(final Block block) {
    final BlockHeader blockHeader = block.getHeader();
    if (processingBlocksManager.addRequestedBlock(blockHeader.getParentHash())) {
      retrieveParentBlock(blockHeader);
    } else {
      LOG.debug("Parent block with hash {} is already requested", blockHeader.getParentHash());
    }
  }

  private CompletableFuture<Block> retrieveParentBlock(final BlockHeader blockHeader) {
    final long targetParentBlockNumber = blockHeader.getNumber() - 1L;
    final Hash targetParentBlockHash = blockHeader.getParentHash();
    LOG.info("Retrieving parent {} of block {}", targetParentBlockHash, blockHeader.toLogString());
    return getBlockFromPeers(
        Optional.empty(), targetParentBlockNumber, Optional.of(targetParentBlockHash));
  }

  private CompletableFuture<Block> getBlockFromPeers(
      final Optional<EthPeer> preferredPeer,
      final long blockNumber,
      final Optional<Hash> maybeBlockHash) {
    return repeatableGetBlockFromPeer(preferredPeer, blockNumber, maybeBlockHash)
        .whenComplete(
            (block, throwable) -> {
              if (block != null) {
                LOG.atDebug()
                    .setMessage("Successfully retrieved block {}")
                    .addArgument(block::toLogString)
                    .log();
                processingBlocksManager.registerReceivedBlock(block);
              } else {
                if (throwable != null) {
                  LOG.warn(
                      "Failed to retrieve block "
                          + logBlockNumberMaybeHash(blockNumber, maybeBlockHash),
                      throwable);
                } else {
                  // this could happen if we give up at some point since we find that it make no
                  // sense to retry
                  LOG.atDebug()
                      .setMessage("Block {} not retrieved")
                      .addArgument(() -> logBlockNumberMaybeHash(blockNumber, maybeBlockHash))
                      .log();
                }
                processingBlocksManager.registerFailedGetBlock(blockNumber, maybeBlockHash);
              }
            });
  }

  private CompletableFuture<Block> repeatableGetBlockFromPeer(
      final Optional<EthPeer> preferredPeer,
      final long blockNumber,
      final Optional<Hash> maybeBlockHash) {
    return exceptionallyCompose(
            scheduleGetBlockFromPeers(preferredPeer, blockNumber, maybeBlockHash),
            handleGetBlockErrors(blockNumber, maybeBlockHash))
        .thenCompose(r -> maybeRepeatGetBlock(blockNumber, maybeBlockHash));
  }

  private Function<Throwable, CompletionStage<Block>> handleGetBlockErrors(
      final long blockNumber, final Optional<Hash> maybeBlockHash) {
    return throwable -> {
      LOG.atDebug()
          .setMessage("Temporary failure retrieving block {} from peers with error {}")
          .addArgument(() -> logBlockNumberMaybeHash(blockNumber, maybeBlockHash))
          .addArgument(throwable)
          .log();
      return CompletableFuture.completedFuture(null);
    };
  }

  private CompletableFuture<Block> maybeRepeatGetBlock(
      final long blockNumber, final Optional<Hash> maybeBlockHash) {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    final Optional<Block> maybeBlock =
        maybeBlockHash
            .map(hash -> blockchain.getBlockByHash(hash))
            .orElseGet(() -> blockchain.getBlockByNumber(blockNumber));

    // check if we got this block by other means
    if (maybeBlock.isPresent()) {
      final Block block = maybeBlock.get();
      LOG.atDebug()
          .setMessage("No need to retry to get block {} since it is already present")
          .addArgument(block::toLogString)
          .log();
      return CompletableFuture.completedFuture(block);
    }

    final long localChainHeight = blockchain.getChainHeadBlockNumber();
    final long bestChainHeight = syncState.bestChainHeight(localChainHeight);
    if (!shouldImportBlockAtHeight(blockNumber, localChainHeight, bestChainHeight)) {
      LOG.atDebug()
          .setMessage("Not retrying to get block {} since we are too far from local chain head {}")
          .addArgument(() -> logBlockNumberMaybeHash(blockNumber, maybeBlockHash))
          .addArgument(blockchain.getChainHead()::toLogString)
          .log();
      return CompletableFuture.completedFuture(null);
    }

    LOG.atDebug()
        .setMessage("Retrying to get block {}")
        .addArgument(() -> logBlockNumberMaybeHash(blockNumber, maybeBlockHash))
        .log();

    return ethContext
        .getScheduler()
        .scheduleSyncWorkerTask(
            () -> repeatableGetBlockFromPeer(Optional.empty(), blockNumber, maybeBlockHash));
  }

  private CompletableFuture<Block> scheduleGetBlockFromPeers(
      final Optional<EthPeer> maybePreferredPeer,
      final long blockNumber,
      final Optional<Hash> maybeBlockHash) {

    return ethContext
        .getScheduler()
        .scheduleServiceTask(() -> getBlockHeader(maybePreferredPeer, blockNumber, maybeBlockHash))
        .thenApply((blockHeader) -> getBlock(maybePreferredPeer, blockHeader))
        .thenCompose(
            blockExecutorResult ->
                importOrSavePendingBlock(
                    blockExecutorResult.result().get().getFirst(),
                    blockExecutorResult.ethPeers().getLast().nodeId()))
        .orTimeout(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  private CompletableFuture<BlockHeader> getBlockHeader(
      final Optional<EthPeer> maybePreferredPeer,
      final long blockNumber,
      final Optional<Hash> maybeBlockHash) {
    GetHeadersFromPeerTask headersFromPeerTask =
        new GetHeadersFromPeerTask(
            maybeBlockHash.orElse(null),
            blockNumber,
            1,
            0,
            Direction.FORWARD,
            Math.max(1, ethContext.getEthPeers().peerCount()),
            protocolSchedule);
    if (maybePreferredPeer.isPresent()) {
      PeerTaskExecutorResult<List<BlockHeader>> executorResult =
          ethContext
              .getPeerTaskExecutor()
              .executeAgainstPeer(headersFromPeerTask, maybePreferredPeer.get());
      if (executorResult.result().isPresent()
          && executorResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS) {
        return CompletableFuture.completedFuture(executorResult.result().get().getFirst());
      }
    }
    PeerTaskExecutorResult<List<BlockHeader>> executorResult =
        ethContext.getPeerTaskExecutor().execute(headersFromPeerTask);
    if (executorResult.result().isEmpty()
        || executorResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
      return CompletableFuture.failedFuture(new InvalidPeerTaskResponseException());
    } else {
      return CompletableFuture.completedFuture(executorResult.result().get().getFirst());
    }
  }

  private PeerTaskExecutorResult<List<Block>> getBlock(
      final Optional<EthPeer> maybePreferredPeer, final BlockHeader blockHeader) {
    GetBodiesFromPeerTask bodiesTask =
        new GetBodiesFromPeerTask(List.of(blockHeader), protocolSchedule);
    if (maybePreferredPeer.isPresent()) {
      PeerTaskExecutorResult<List<Block>> executorResult =
          ethContext.getPeerTaskExecutor().executeAgainstPeer(bodiesTask, maybePreferredPeer.get());
      if (executorResult.result().isPresent()
          && executorResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS) {
        return executorResult;
      }
    }
    PeerTaskExecutorResult<List<Block>> executorResult =
        ethContext.getPeerTaskExecutor().execute(bodiesTask);
    if (executorResult.result().isEmpty()
        || executorResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
      throw new RuntimeException(new InvalidPeerTaskResponseException());
    } else {
      return executorResult;
    }
  }

  private void broadcastBlock(final Block block, final BlockHeader parent) {
    final Difficulty totalDifficulty =
        protocolContext
            .getBlockchain()
            .getTotalDifficultyByHash(parent.getHash())
            .get()
            .add(block.getHeader().getDifficulty());
    blockBroadcaster.propagate(block, totalDifficulty);
  }

  @VisibleForTesting
  CompletableFuture<Block> importOrSavePendingBlock(final Block block, final Bytes nodeId) {
    // Synchronize to avoid race condition where block import event fires after the
    // blockchain.contains() check and before the block is registered, causing onBlockAdded() to be
    // invoked for the parent of this block before we are able to register it.
    LOG.atTrace()
        .setMessage("Import or save pending block {}")
        .addArgument(block::toLogString)
        .log();

    if (protocolContext.getBadBlockManager().isBadBlock(block.getHash())) {
      LOG.atTrace()
          .setMessage("Skipping known-bad block {} in importOrSavePendingBlock")
          .addArgument(block::toLogString)
          .log();
      return CompletableFuture.completedFuture(block);
    }

    if (!protocolContext.getBlockchain().contains(block.getHeader().getParentHash())) {
      // Block isn't connected to local chain, save it to pending blocks collection
      if (savePendingBlock(block, nodeId)) {
        // if block is saved as pending, try to resolve it
        maybeProcessPendingBlocks(block);
      }
      return CompletableFuture.completedFuture(block);
    }

    if (!processingBlocksManager.addImportingBlock(block.getHash())) {
      LOG.atTrace()
          .setMessage("We're already importing this block {}")
          .addArgument(block::toLogString)
          .log();
      return CompletableFuture.completedFuture(block);
    }

    if (protocolContext.getBlockchain().contains(block.getHash())) {
      LOG.atTrace()
          .setMessage("We've already imported this block {}")
          .addArgument(block::toLogString)
          .log();
      processingBlocksManager.registerBlockImportDone(block.getHash());
      return CompletableFuture.completedFuture(block);
    }

    final BlockHeader parent =
        protocolContext
            .getBlockchain()
            .getBlockHeader(block.getHeader().getParentHash())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Incapable of retrieving header from non-existent parent of "
                            + block.toLogString()));
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());
    final BlockHeaderValidator blockHeaderValidator = protocolSpec.getBlockHeaderValidator();
    final BadBlockManager badBlockManager = protocolContext.getBadBlockManager();
    return ethContext
        .getScheduler()
        .scheduleSyncWorkerTask(
            () ->
                validateAndProcessPendingBlock(
                    blockHeaderValidator, block, parent, badBlockManager));
  }

  /**
   * Save the given block.
   *
   * @param block the block to track
   * @param nodeId node that sent the block
   * @return true if the block was added (was not previously present)
   */
  private boolean savePendingBlock(final Block block, final Bytes nodeId) {
    synchronized (pendingBlocksManager) {
      if (pendingBlocksManager.registerPendingBlock(block, nodeId)) {
        LOG.info(
            "Saved announced block for future import {} - {} saved block(s)",
            block.toLogString(),
            pendingBlocksManager.size());
        return true;
      }
      return false;
    }
  }

  /**
   * Try to request the lowest ancestor for the given pending block or process the descendants if
   * the ancestor is already in the chain
   */
  private void maybeProcessPendingBlocks(final Block block) {
    // Try to get the lowest ancestor pending for this block, so we can import it
    final Optional<Block> lowestPending = pendingBlocksManager.pendingAncestorBlockOf(block);
    if (lowestPending.isPresent()) {
      final Block lowestPendingBlock = lowestPending.get();
      // If the parent of the lowest ancestor is not in the chain, request it.
      if (!protocolContext
          .getBlockchain()
          .contains(lowestPendingBlock.getHeader().getParentHash())) {
        requestParentBlock(lowestPendingBlock);
      } else {
        LOG.trace("Parent block is already in the chain");
        // if the parent is already imported, process its children
        maybeProcessPendingChildrenBlocks(lowestPendingBlock.getHeader());
      }
    }
  }

  private CompletableFuture<Block> validateAndProcessPendingBlock(
      final BlockHeaderValidator blockHeaderValidator,
      final Block block,
      final BlockHeader parent,
      final BadBlockManager badBlockManager) {
    final HeaderValidationMode validationMode = HeaderValidationMode.FULL;
    if (blockHeaderValidator.validateHeader(
        block.getHeader(), parent, protocolContext, validationMode)) {
      ethContext.getScheduler().scheduleSyncWorkerTask(() -> broadcastBlock(block, parent));
      return runImportTask(block);
    } else {
      processingBlocksManager.registerBlockImportDone(block.getHash());
      final String description = String.format("Failed header validation (%s)", validationMode);
      badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure(description));
      LOG.warn(
          "Added to bad block manager for invalid header, failed to import announced block {}",
          block.toLogString());
      return CompletableFuture.completedFuture(block);
    }
  }

  private CompletableFuture<Block> runImportTask(final Block block) {
    final PersistBlockTask importTask =
        PersistBlockTask.create(
            protocolSchedule,
            protocolContext,
            ethContext,
            block,
            HeaderValidationMode.NONE,
            metricsSystem);
    return importTask
        .run()
        .whenComplete(
            (result, throwable) -> {
              processingBlocksManager.registerBlockImportDone(block.getHash());
              if (throwable != null) {
                LOG.warn("Failed to import announced block {}", block.toLogString());
              }
            });
  }

  // Only import blocks within a certain range of our head and sync target
  private boolean shouldImportBlockAtHeight(
      final long blockNumber, final long localHeight, final long bestChainHeight) {
    final long distanceFromLocalHead = blockNumber - localHeight;
    final long distanceFromBestPeer = blockNumber - bestChainHeight;
    final Range<Long> importRange = config.getBlockPropagationRange();
    return importRange.contains(distanceFromLocalHead)
        && importRange.contains(distanceFromBestPeer);
  }

  private String toLogString(final Collection<NewBlockHash> newBlockHashs) {
    return newBlockHashs.stream()
        .map(NewBlockHash::toString)
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private void reactToTTDReachedEvent(final boolean ttdReached) {
    if (started.get() && ttdReached) {
      LOG.info("Block propagation was running, then ttd reached");
    } else if (!started.get()) {
      start();
    }
  }

  @Override
  public String toString() {
    return "BlockPropagationManager{"
        + processingBlocksManager
        + ", pendingBlocksManager="
        + pendingBlocksManager
        + '}';
  }

  private String logBlockNumberMaybeHash(
      final long blockNumber, final Optional<Hash> maybeBlockHash) {
    return blockNumber + maybeBlockHash.map(h -> " (" + h + ")").orElse("");
  }

  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    if (event.hasValidFinalizedBlockHash()) {
      stop();
    }
  }

  static class ProcessingBlocksManager {
    private final Set<Hash> importingBlocks = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Hash> requestedBlocks = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Long> requestedNonAnnouncedBlocks =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    boolean addRequestedBlock(final Hash hash) {
      return requestedBlocks.add(hash);
    }

    public boolean addNonAnnouncedBlocks(final long blockNumber) {
      return requestedNonAnnouncedBlocks.add(blockNumber);
    }

    public boolean alreadyImporting(final Hash hash) {
      return importingBlocks.contains(hash);
    }

    public synchronized void registerReceivedBlock(final Block block) {
      requestedBlocks.remove(block.getHash());
      requestedNonAnnouncedBlocks.remove(block.getHeader().getNumber());
    }

    public synchronized void registerFailedGetBlock(
        final long blockNumber, final Optional<Hash> maybeBlockHash) {
      requestedNonAnnouncedBlocks.remove(blockNumber);
      maybeBlockHash.ifPresent(requestedBlocks::remove);
    }

    public boolean addImportingBlock(final Hash hash) {
      return importingBlocks.add(hash);
    }

    public void registerBlockImportDone(final Hash hash) {
      importingBlocks.remove(hash);
    }

    @Override
    public synchronized String toString() {
      return "ProcessingBlocksManager{"
          + "importingBlocks="
          + importingBlocks
          + ", requestedBlocks="
          + requestedBlocks
          + ", requestedNonAnnouncedBlocks="
          + requestedNonAnnouncedBlocks
          + '}';
    }
  }
}
