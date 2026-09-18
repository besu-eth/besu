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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.BesuEvents.BadBlockListener;
import org.hyperledger.besu.util.Subscribers;

import java.util.Collection;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BadBlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BadBlockManager.class);

  public static final int MAX_BAD_BLOCKS_SIZE = 100;

  /**
   * A bad chain can grow by one block per slot for as long as the consensus client stays on it, so
   * the caches that only hold a hash or a header track far more entries than the ones holding
   * bodies.
   */
  public static final int MAX_BAD_CHAIN_SIZE = 1024;

  private final Cache<Hash, BlockHeader> badHeaders =
      CacheBuilder.newBuilder().maximumSize(MAX_BAD_CHAIN_SIZE).concurrencyLevel(1).build();
  private final Cache<Hash, Block> badBlocks =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_BAD_BLOCKS_SIZE)
          .concurrencyLevel(1)
          .removalListener(
              (RemovalNotification<Hash, Block> notification) -> {
                // an executed bad block must stay detectable after its body is evicted: its
                // descendants outlive it in the larger header cache and their detection walks
                // through its hash
                if (notification.wasEvicted()) {
                  badHeaders.put(notification.getKey(), notification.getValue().getHeader());
                }
              })
          .build();
  private final Cache<Hash, Hash> latestValidHashes =
      CacheBuilder.newBuilder().maximumSize(MAX_BAD_CHAIN_SIZE).concurrencyLevel(1).build();
  private final Cache<Hash, BlockAccessList> blockAccessLists =
      CacheBuilder.newBuilder().maximumSize(MAX_BAD_BLOCKS_SIZE).concurrencyLevel(1).build();
  private final Cache<Hash, BlockAccessList> generatedBlockAccessLists =
      CacheBuilder.newBuilder().maximumSize(MAX_BAD_BLOCKS_SIZE).concurrencyLevel(1).build();
  private final Subscribers<BadBlockListener> badBlockSubscribers = Subscribers.create(true);

  /**
   * Add a new invalid block.
   *
   * @param badBlock the invalid block
   * @param cause the cause detailing why the block is considered invalid
   */
  public void addBadBlock(final Block badBlock, final BadBlockCause cause) {
    addBadBlock(badBlock, cause, Optional.empty(), Optional.empty());
  }

  public void addBadBlock(
      final Block badBlock,
      final BadBlockCause cause,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<BlockAccessList> generatedBlockAccessList) {
    LOG.debug("Register bad block {} with cause: {}", badBlock.toLogString(), cause);
    this.badBlocks.put(badBlock.getHash(), badBlock);
    blockAccessList.ifPresent(bal -> this.blockAccessLists.put(badBlock.getHash(), bal));
    generatedBlockAccessList.ifPresent(
        bal -> this.generatedBlockAccessLists.put(badBlock.getHash(), bal));
    badBlockSubscribers.forEach(s -> s.onBadBlockAdded(badBlock.getHeader(), cause));
  }

  public void reset() {
    this.badBlocks.invalidateAll();
    this.badHeaders.invalidateAll();
    this.latestValidHashes.invalidateAll();
    this.blockAccessLists.invalidateAll();
    this.generatedBlockAccessLists.invalidateAll();
  }

  /**
   * Return all invalid blocks
   *
   * @return a collection of invalid blocks
   */
  public Collection<Block> getBadBlocks() {
    return badBlocks.asMap().values();
  }

  @VisibleForTesting
  public Collection<BlockHeader> getBadHeaders() {
    return badHeaders.asMap().values();
  }

  /**
   * Return an invalid block based on the hash
   *
   * @param hash of the block
   * @return an invalid block
   */
  public Optional<Block> getBadBlock(final Hash hash) {
    return Optional.ofNullable(badBlocks.getIfPresent(hash));
  }

  /**
   * Return the header of an invalid block, whether the full block or only its header is known
   *
   * @param hash of the block
   * @return the header of an invalid block
   */
  public Optional<BlockHeader> getBadHeader(final Hash hash) {
    return getBadBlock(hash)
        .map(Block::getHeader)
        .or(() -> Optional.ofNullable(badHeaders.getIfPresent(hash)));
  }

  public void addBadHeader(final BlockHeader header, final BadBlockCause cause) {
    LOG.debug("Register bad block header {} with cause: {}", header.toLogString(), cause);
    badHeaders.put(header.getHash(), header);
    badBlockSubscribers.forEach(s -> s.onBadBlockAdded(header, cause));
  }

  public boolean isBadBlock(final Hash blockHash) {
    return badBlocks.asMap().containsKey(blockHash) || badHeaders.asMap().containsKey(blockHash);
  }

  /**
   * Indicate whether any bad block or bad header is currently tracked, as a cheap in-memory
   * pre-check before more expensive descendant lookups.
   *
   * @return true when no bad block or header is tracked
   */
  public boolean isEmpty() {
    return badBlocks.size() == 0 && badHeaders.size() == 0;
  }

  /**
   * Record a block as bad because it descends from a bad block. Only the header is kept, the body
   * of a block that was never executed is not needed to reject its own descendants.
   *
   * @param descendant the header of the descendant
   * @param badAncestor the header of the bad ancestor
   * @param maybeLatestValidHash the latest valid hash of the chain, if known
   */
  public void addBadDescendant(
      final BlockHeader descendant,
      final BlockHeader badAncestor,
      final Optional<Hash> maybeLatestValidHash) {
    addBadHeader(descendant, BadBlockCause.fromBadAncestorHeader(badAncestor));
    maybeLatestValidHash.ifPresent(
        latestValidHash -> addLatestValidHash(descendant.getHash(), latestValidHash));
  }

  /**
   * Check whether a block descends from a bad block, recording it as a bad descendant that inherits
   * the parent's latest valid hash if so. Only the direct parent is checked, deeper ancestors are
   * covered as long as every block in between has been checked.
   *
   * @param header the header of the block to check
   * @return the header of the bad parent, empty if the parent is not known as bad
   */
  public Optional<BlockHeader> checkAndMarkBadDescendant(final BlockHeader header) {
    final Hash parentHash = header.getParentHash();
    final Optional<BlockHeader> maybeBadParentHeader = getBadHeader(parentHash);
    if (maybeBadParentHeader.isPresent() && !isBadBlock(header.getHash())) {
      addBadDescendant(header, maybeBadParentHeader.get(), getLatestValidHash(parentHash));
    }
    return maybeBadParentHeader;
  }

  public void addLatestValidHash(final Hash blockHash, final Hash latestValidHash) {
    this.latestValidHashes.put(blockHash, latestValidHash);
  }

  public Optional<Hash> getLatestValidHash(final Hash blockHash) {
    return Optional.ofNullable(latestValidHashes.getIfPresent(blockHash));
  }

  public Optional<BlockAccessList> getGeneratedBlockAccessList(final Hash blockHash) {
    return Optional.ofNullable(generatedBlockAccessLists.getIfPresent(blockHash));
  }

  public Optional<BlockAccessList> getBlockAccessList(final Hash blockHash) {
    return Optional.ofNullable(blockAccessLists.getIfPresent(blockHash));
  }

  public long subscribeToBadBlocks(final BadBlockListener listener) {
    return badBlockSubscribers.subscribe(listener);
  }

  public void unsubscribeFromBadBlocks(final long id) {
    badBlockSubscribers.unsubscribe(id);
  }
}
