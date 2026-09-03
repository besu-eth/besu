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
package org.hyperledger.besu.services;

import static java.util.stream.Collectors.toUnmodifiableList;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.ethereum.api.query.LogsQuery;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.eth.sync.BlockBroadcaster;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.data.AddedBlockContext.EventType;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.List;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** A concrete implementation of BesuEvents used in Besu plugin framework. */
public class BesuEventsImpl implements BesuEvents {
  private final Blockchain blockchain;
  private final BlockBroadcaster blockBroadcaster;
  private final TransactionPool transactionPool;
  private final SyncState syncState;
  private final BadBlockManager badBlockManager;

  /**
   * Constructor for BesuEventsImpl
   *
   * @param blockchain An instance of Blockchain
   * @param blockBroadcaster An instance of BlockBroadcaster
   * @param transactionPool An instance of TransactionPool
   * @param syncState An instance of SyncState
   * @param badBlockManager A cache of bad blocks encountered on the network
   */
  public BesuEventsImpl(
      final Blockchain blockchain,
      final BlockBroadcaster blockBroadcaster,
      final TransactionPool transactionPool,
      final SyncState syncState,
      final BadBlockManager badBlockManager) {
    this.blockchain = blockchain;
    this.blockBroadcaster = blockBroadcaster;
    this.transactionPool = transactionPool;
    this.syncState = syncState;
    this.badBlockManager = badBlockManager;
  }

  @Override
  public long addBlockPropagatedListener(final BlockPropagatedListener listener) {
    return blockBroadcaster.subscribePropagateNewBlocks(
        (block, totalDifficulty) ->
            listener.onBlockPropagated(
                blockPropagatedContext(block::getHeader, block::getBody, () -> totalDifficulty)));
  }

  @Override
  public void removeBlockPropagatedListener(final long listenerIdentifier) {
    blockBroadcaster.unsubscribePropagateNewBlocks(listenerIdentifier);
  }

  @Override
  public long addBlockAddedListener(final BlockAddedListener listener) {
    return blockchain.observeBlockAdded(
        event ->
            listener.onBlockAdded(
                blockAddedContext(
                    event.getEventType(),
                    event::getHeader,
                    () -> event.getBlock().getBody(),
                    event::getTransactionReceipts)));
  }

  @Override
  public void removeBlockAddedListener(final long listenerIdentifier) {
    blockchain.removeObserver(listenerIdentifier);
  }

  @Override
  public long addBlockReorgListener(final BlockReorgListener listener) {
    return blockchain.observeChainReorg(
        (blockWithReceipts, chain) ->
            listener.onBlockReorg(
                blockAddedContext(
                    EventType.CHAIN_REORG,
                    blockWithReceipts::getHeader,
                    blockWithReceipts.getBlock()::getBody,
                    blockWithReceipts::getReceipts)));
  }

  @Override
  public void removeBlockReorgListener(final long listenerIdentifier) {
    blockchain.removeChainReorgObserver(listenerIdentifier);
  }

  @Override
  public long addInitialSyncCompletionListener(final InitialSyncCompletionListener listener) {
    return syncState.subscribeCompletionReached(listener);
  }

  @Override
  public long addTransactionAddedListener(final TransactionAddedListener listener) {
    return transactionPool.subscribePendingTransactions(listener::onTransactionAdded);
  }

  @Override
  public void removeTransactionAddedListener(final long listenerIdentifier) {
    transactionPool.unsubscribePendingTransactions(listenerIdentifier);
  }

  @Override
  public long addTransactionDroppedListener(
      final TransactionDroppedListener transactionDroppedListener) {
    return transactionPool.subscribeDroppedTransactions(
        (transaction, reason) ->
            transactionDroppedListener.onTransactionDropped(transaction, reason.label()));
  }

  @Override
  public void removeTransactionDroppedListener(final long listenerIdentifier) {
    transactionPool.unsubscribeDroppedTransactions(listenerIdentifier);
  }

  @Override
  public long addSyncStatusListener(final SyncStatusListener syncStatusListener) {
    return syncState.subscribeSyncStatus(syncStatusListener);
  }

  @Override
  public void removeSyncStatusListener(final long listenerIdentifier) {
    syncState.unsubscribeSyncStatus(listenerIdentifier);
  }

  @Override
  public long addLogListener(
      final List<Address> addresses,
      final List<List<Bytes32>> topics,
      final LogListener logListener) {
    final List<List<LogTopic>> besuTopics =
        topics.stream()
            .map(subList -> subList.stream().map(LogTopic::wrap).collect(toUnmodifiableList()))
            .collect(toUnmodifiableList());

    final LogsQuery logsQuery = new LogsQuery(addresses, besuTopics);

    return blockchain.observeLogs(
        logWithMetadata -> {
          if (logsQuery.matches(LogWithMetadata.fromPlugin(logWithMetadata))) {
            logListener.onLogEmitted(logWithMetadata);
          }
        });
  }

  @Override
  public void removeLogListener(final long listenerIdentifier) {
    blockchain.removeObserver(listenerIdentifier);
  }

  @Override
  public long addBadBlockListener(final BadBlockListener listener) {
    return badBlockManager.subscribeToBadBlocks(listener);
  }

  @Override
  public void removeBadBlockListener(final long listenerIdentifier) {
    badBlockManager.unsubscribeFromBadBlocks(listenerIdentifier);
  }

  private static PropagatedBlockContext blockPropagatedContext(
      final Supplier<BlockHeader> blockHeaderSupplier,
      final Supplier<BlockBody> blockBodySupplier,
      final Supplier<Difficulty> totalDifficultySupplier) {
    return new PropagatedBlockContext() {
      @Override
      public BlockHeader getBlockHeader() {
        return blockHeaderSupplier.get();
      }

      @Override
      public BlockBody getBlockBody() {
        return blockBodySupplier.get();
      }

      @Override
      public UInt256 getTotalDifficulty() {
        return totalDifficultySupplier.get().toUInt256();
      }
    };
  }

  private static AddedBlockContext blockAddedContext(
      final EventType eventType,
      final Supplier<BlockHeader> blockHeaderSupplier,
      final Supplier<BlockBody> blockBodySupplier,
      final Supplier<List<TransactionReceipt>> transactionReceiptsSupplier) {
    return new AddedBlockContext() {
      @Override
      public BlockHeader getBlockHeader() {
        return blockHeaderSupplier.get();
      }

      @Override
      public BlockBody getBlockBody() {
        return blockBodySupplier.get();
      }

      @Override
      public List<TransactionReceipt> getTransactionReceipts() {
        return transactionReceiptsSupplier.get();
      }

      @Override
      public EventType getEventType() {
        return eventType;
      }
    };
  }
}
