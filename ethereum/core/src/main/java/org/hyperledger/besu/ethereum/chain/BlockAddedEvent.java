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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.plugin.data.AddedBlockContext.EventType;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class BlockAddedEvent {

  private final Supplier<Block> blockSupplier;
  private final List<Transaction> addedTransactions;
  private final List<Transaction> removedTransactions;
  private final List<TransactionReceipt> transactionReceipts;
  private final EventType eventType;
  private final Supplier<Stream<LogWithMetadata>> logsWithMetadata;
  private final Hash commonAncestorHash;
  private final BlockHeader header;

  private BlockAddedEvent(
      final EventType eventType,
      final Supplier<Block> blockSupplier,
      final BlockHeader header,
      final List<Transaction> addedTransactions,
      final List<Transaction> removedTransactions,
      final List<TransactionReceipt> transactionReceipts,
      final Supplier<Stream<LogWithMetadata>> logsWithMetadata,
      final Hash commonAncestorHash) {
    this.eventType = eventType;
    this.blockSupplier = blockSupplier;
    this.header = header;
    this.addedTransactions = addedTransactions;
    this.removedTransactions = removedTransactions;
    this.transactionReceipts = transactionReceipts;
    this.logsWithMetadata = logsWithMetadata;
    this.commonAncestorHash = commonAncestorHash;
  }

  public static BlockAddedEvent createForSyncHeadAdvancement(
      final BlockHeader blockHeader,
      final Supplier<Block> blockSupplier,
      final List<LogWithMetadata> logsWithMetadata,
      final List<TransactionReceipt> transactionReceipts) {
    return new BlockAddedEvent(
        EventType.HEAD_ADVANCED,
        blockSupplier,
        blockHeader,
        Collections.emptyList(),
        Collections.emptyList(),
        transactionReceipts,
        logsWithMetadata::stream,
        blockHeader.getParentHash());
  }

  public static BlockAddedEvent createForHeadAdvancement(
      final Block block,
      final List<LogWithMetadata> logsWithMetadata,
      final List<TransactionReceipt> transactionReceipts) {
    return new BlockAddedEvent(
        EventType.HEAD_ADVANCED,
        () -> block,
        block.getHeader(),
        block.getBody().getTransactions(),
        Collections.emptyList(),
        transactionReceipts,
        logsWithMetadata::stream,
        block.getHeader().getParentHash());
  }

  public static BlockAddedEvent createForChainReorg(
      final Block block,
      final List<Transaction> addedTransactions,
      final List<Transaction> removedTransactions,
      final List<TransactionReceipt> transactionReceipts,
      final List<LogWithMetadata> logsWithMetadata,
      final Hash commonAncestorHash) {
    return new BlockAddedEvent(
        EventType.CHAIN_REORG,
        () -> block,
        block.getHeader(),
        addedTransactions,
        removedTransactions,
        transactionReceipts,
        logsWithMetadata::stream,
        commonAncestorHash);
  }

  /**
   * Creates the event of a reorg too deep to hold its transactions and logs in memory. Like a head
   * advancement, it gives the transaction pool only the new head's transactions, so the removed
   * ones are not put back into it, and its logs are read back from storage whenever they are
   * streamed.
   *
   * @param block the new head
   * @param transactionReceipts the receipts of the new head
   * @param logsWithMetadata the removed and then the added logs, read from storage on each call
   * @param commonAncestorHash the last block both branches share
   * @return the event
   */
  public static BlockAddedEvent createForDeepChainReorg(
      final Block block,
      final List<TransactionReceipt> transactionReceipts,
      final Supplier<Stream<LogWithMetadata>> logsWithMetadata,
      final Hash commonAncestorHash) {
    return new BlockAddedEvent(
        EventType.CHAIN_REORG,
        () -> block,
        block.getHeader(),
        block.getBody().getTransactions(),
        Collections.emptyList(),
        transactionReceipts,
        logsWithMetadata,
        commonAncestorHash);
  }

  public static BlockAddedEvent createForFork(final Block block) {
    return new BlockAddedEvent(
        EventType.FORK,
        () -> block,
        block.getHeader(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Stream::empty,
        block.getHeader().getParentHash());
  }

  public static BlockAddedEvent createForStoredOnly(final Block block) {
    return new BlockAddedEvent(
        EventType.STORED_ONLY,
        () -> block,
        block.getHeader(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Stream::empty,
        block.getHeader().getParentHash());
  }

  public Block getBlock() {
    return blockSupplier.get();
  }

  public BlockHeader getHeader() {
    return header;
  }

  public boolean isNewCanonicalHead() {
    return eventType == EventType.HEAD_ADVANCED || eventType == EventType.CHAIN_REORG;
  }

  public EventType getEventType() {
    return eventType;
  }

  public List<Transaction> getAddedTransactions() {
    return addedTransactions;
  }

  public List<Transaction> getRemovedTransactions() {
    return removedTransactions;
  }

  public List<TransactionReceipt> getTransactionReceipts() {
    return transactionReceipts;
  }

  /**
   * Gets the added and removed logs. A deep reorg reads them from storage on every call, so
   * consumers stream them with {@link #streamLogsWithMetadata()} instead.
   *
   * @return the logs
   */
  public List<LogWithMetadata> getLogsWithMetadata() {
    return logsWithMetadata.get().toList();
  }

  /**
   * Streams the added and removed logs, in the same order as {@link #getLogsWithMetadata()}.
   *
   * @return the logs
   */
  public Stream<LogWithMetadata> streamLogsWithMetadata() {
    return logsWithMetadata.get();
  }

  public Hash getCommonAncestorHash() {
    return commonAncestorHash;
  }

  @Override
  public String toString() {
    return "BlockAddedEvent{"
        + "eventType="
        + eventType
        + ", block="
        + header.toLogString()
        + ", commonAncestorHash="
        + commonAncestorHash
        + ", addedTransactions count="
        + addedTransactions.size()
        + ", removedTransactions count="
        + removedTransactions.size()
        + ", transactionReceipts count ="
        + transactionReceipts.size()
        + '}';
  }
}
