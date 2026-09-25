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
import org.hyperledger.besu.plugin.data.BadBlockCause.BadBlockReason;
import org.hyperledger.besu.plugin.services.BesuEvents.BadBlockListener;
import org.hyperledger.besu.util.Subscribers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

  /** A bad block and its cause are evicted together, so a tracked block never loses its cause. */
  private record BadBlock(Block block, BadBlockCause cause) {}

  private record BadHeader(BlockHeader header, BadBlockCause cause) {}

  /** A subscriber notification, delivered once the recording monitor is released. */
  private record Notification(BlockHeader header, BadBlockCause cause) {}

  private final Cache<Hash, BadHeader> badHeaders =
      CacheBuilder.newBuilder().maximumSize(MAX_BAD_CHAIN_SIZE).concurrencyLevel(1).build();
  private final Cache<Hash, BadBlock> badBlocks =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_BAD_BLOCKS_SIZE)
          .concurrencyLevel(1)
          .removalListener(
              (RemovalNotification<Hash, BadBlock> notification) -> {
                // an executed bad block must stay detectable after its body is evicted: its
                // descendants outlive it in the larger header cache and their detection walks
                // through its hash
                if (notification.wasEvicted()) {
                  final BadBlock evicted = notification.getValue();
                  badHeaders.put(
                      notification.getKey(),
                      new BadHeader(evicted.block().getHeader(), evicted.cause()));
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
    recordBadBlock(badBlock, cause, blockAccessList, generatedBlockAccessList);
    notify(new Notification(badBlock.getHeader(), cause));
  }

  private void recordBadBlock(
      final Block badBlock,
      final BadBlockCause cause,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<BlockAccessList> generatedBlockAccessList) {
    LOG.debug("Register bad block {} with cause: {}", badBlock.toLogString(), cause);
    this.badBlocks.put(badBlock.getHash(), new BadBlock(badBlock, cause));
    blockAccessList.ifPresent(bal -> this.blockAccessLists.put(badBlock.getHash(), bal));
    generatedBlockAccessList.ifPresent(
        bal -> this.generatedBlockAccessLists.put(badBlock.getHash(), bal));
  }

  // subscribers are plugins, they must not run while the monitor blocks import and engine threads
  private void notify(final Notification notification) {
    badBlockSubscribers.forEach(
        s -> s.onBadBlockAdded(notification.header(), notification.cause()));
  }

  /**
   * Forget every bad block. Synchronized with the descendant marking so a mark that read its parent
   * before the reset cannot re-insert a stale entry after it.
   */
  public synchronized void reset() {
    this.badBlocks.invalidateAll();
    this.badHeaders.invalidateAll();
    this.latestValidHashes.invalidateAll();
    this.blockAccessLists.invalidateAll();
    this.generatedBlockAccessLists.invalidateAll();
  }

  /**
   * Forget a block that turned out to be valid after all, together with the descendants that were
   * only marked on its account. A block that is imported successfully cannot be bad, whatever an
   * earlier attempt recorded, and neither can a block be bad for descending from it. Synchronized
   * with the descendant marking for the same reason as {@link #reset()}.
   *
   * @param blockHash the hash of the block to forget
   */
  public void removeBadBlock(final Hash blockHash) {
    if (!isBadBlock(blockHash) && latestValidHashes.getIfPresent(blockHash) == null) {
      return;
    }
    synchronized (this) {
      LOG.debug("Forget bad block {} after it was imported successfully", blockHash);
      final Map<Hash, List<Hash>> markedChildren = new HashMap<>();
      Stream.concat(
              badBlocks.asMap().values().stream()
                  .map(bad -> new BadHeader(bad.block().getHeader(), bad.cause())),
              badHeaders.asMap().values().stream())
          .filter(bad -> bad.cause().getReason() == BadBlockReason.DESCENDS_FROM_BAD_BLOCK)
          .forEach(
              bad ->
                  markedChildren
                      .computeIfAbsent(bad.header().getParentHash(), unused -> new ArrayList<>())
                      .add(bad.header().getHash()));

      final Deque<Hash> toForget = new ArrayDeque<>();
      toForget.add(blockHash);
      while (!toForget.isEmpty()) {
        final Hash hash = toForget.poll();
        toForget.addAll(markedChildren.getOrDefault(hash, List.of()));
        this.badBlocks.invalidate(hash);
        this.badHeaders.invalidate(hash);
        this.latestValidHashes.invalidate(hash);
        this.blockAccessLists.invalidate(hash);
        this.generatedBlockAccessLists.invalidate(hash);
      }
    }
  }

  /**
   * Return all invalid blocks
   *
   * @return a collection of invalid blocks
   */
  public Collection<Block> getBadBlocks() {
    return badBlocks.asMap().values().stream().map(BadBlock::block).toList();
  }

  @VisibleForTesting
  public Collection<BlockHeader> getBadHeaders() {
    return badHeaders.asMap().values().stream().map(BadHeader::header).toList();
  }

  /**
   * Return an invalid block based on the hash
   *
   * @param hash of the block
   * @return an invalid block
   */
  public Optional<Block> getBadBlock(final Hash hash) {
    return Optional.ofNullable(badBlocks.getIfPresent(hash)).map(BadBlock::block);
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
        .or(() -> Optional.ofNullable(badHeaders.getIfPresent(hash)).map(BadHeader::header));
  }

  public void addBadHeader(final BlockHeader header, final BadBlockCause cause) {
    recordBadHeader(header, cause);
    notify(new Notification(header, cause));
  }

  private void recordBadHeader(final BlockHeader header, final BadBlockCause cause) {
    LOG.debug("Register bad block header {} with cause: {}", header.toLogString(), cause);
    badHeaders.put(header.getHash(), new BadHeader(header, cause));
  }

  /**
   * Return why a block was recorded as bad.
   *
   * @param blockHash the hash of the bad block
   * @return the cause, empty if the block is not known as bad
   */
  public Optional<BadBlockCause> getBadBlockCause(final Hash blockHash) {
    return Optional.ofNullable(badBlocks.getIfPresent(blockHash))
        .map(BadBlock::cause)
        .or(() -> Optional.ofNullable(badHeaders.getIfPresent(blockHash)).map(BadHeader::cause));
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
   * @return the cause recorded for the descendant
   */
  public BadBlockCause addBadDescendant(
      final BlockHeader descendant,
      final BlockHeader badAncestor,
      final Optional<Hash> maybeLatestValidHash) {
    final Notification notification =
        recordBadDescendant(descendant, badAncestor, maybeLatestValidHash);
    notify(notification);
    return notification.cause();
  }

  private Notification recordBadDescendant(
      final BlockHeader descendant,
      final BlockHeader badAncestor,
      final Optional<Hash> maybeLatestValidHash) {
    final BadBlockCause cause = causeForDescendantOf(badAncestor);
    recordBadHeader(descendant, cause);
    maybeLatestValidHash.ifPresent(
        latestValidHash -> addLatestValidHash(descendant.getHash(), latestValidHash));
    return new Notification(descendant, cause);
  }

  /**
   * Record a block whose body is known as bad because it descends from a bad block. The body is
   * kept so the debug RPCs can still inspect it, it is evicted to a header like any other bad block
   * body.
   *
   * @param descendant the descendant
   * @param badAncestor the header of the bad ancestor
   * @param maybeLatestValidHash the latest valid hash of the chain, if known
   * @return the cause recorded for the descendant
   */
  public BadBlockCause addBadDescendant(
      final Block descendant,
      final BlockHeader badAncestor,
      final Optional<Hash> maybeLatestValidHash) {
    final Notification notification =
        recordBadDescendant(descendant, badAncestor, maybeLatestValidHash);
    notify(notification);
    return notification.cause();
  }

  private Notification recordBadDescendant(
      final Block descendant,
      final BlockHeader badAncestor,
      final Optional<Hash> maybeLatestValidHash) {
    final BadBlockCause cause = causeForDescendantOf(badAncestor);
    recordBadBlock(descendant, cause, Optional.empty(), Optional.empty());
    maybeLatestValidHash.ifPresent(
        latestValidHash -> addLatestValidHash(descendant.getHash(), latestValidHash));
    return new Notification(descendant.getHeader(), cause);
  }

  /**
   * Record the descendants of a bad block as bad. A root that is no longer tracked, e.g. after a
   * {@link #reset()} between its detection and this call, marks nothing, so the stale descendants
   * cannot outlive the reset that forgot the root.
   *
   * @param badBlock the header of the bad block
   * @param badBlockDescendants descendants whose body is known
   * @param badBlockHeaderDescendants descendants only known by header
   * @param maybeLatestValidHash the latest valid hash of the chain, if known
   * @return true if the root was still tracked and the descendants were marked
   */
  public boolean markBadChain(
      final BlockHeader badBlock,
      final List<Block> badBlockDescendants,
      final List<BlockHeader> badBlockHeaderDescendants,
      final Optional<Hash> maybeLatestValidHash) {
    final List<Notification> notifications = new ArrayList<>();
    synchronized (this) {
      if (!isBadBlock(badBlock.getHash())) {
        LOG.debug(
            "Bad block {} is no longer tracked, not marking its descendants", badBlock.getHash());
        return false;
      }
      maybeLatestValidHash.ifPresent(
          latestValidHash -> addLatestValidHash(badBlock.getHash(), latestValidHash));
      badBlockDescendants.forEach(
          block -> notifications.add(recordBadDescendant(block, badBlock, maybeLatestValidHash)));
      badBlockHeaderDescendants.forEach(
          header -> notifications.add(recordBadDescendant(header, badBlock, maybeLatestValidHash)));
    }
    notifications.forEach(this::notify);
    return true;
  }

  /**
   * A descendant of a descendant is still invalid because of the root that failed validation, so
   * the cause keeps naming that root rather than the intermediate block it was checked against.
   */
  private BadBlockCause causeForDescendantOf(final BlockHeader badAncestor) {
    return getBadBlockCause(badAncestor.getHash())
        .filter(cause -> cause.getReason() == BadBlockReason.DESCENDS_FROM_BAD_BLOCK)
        .orElseGet(() -> BadBlockCause.fromBadAncestorHeader(badAncestor));
  }

  /**
   * Check whether a block descends from a bad block, recording it as a bad descendant that inherits
   * the parent's latest valid hash if so. Only the direct parent is checked, deeper ancestors are
   * covered as long as every block in between has been checked. Synchronized with {@link #reset()}
   * so the parent read and the descendant write cannot straddle a reset.
   *
   * @param header the header of the block to check
   * @return the cause the block is bad for, empty if the parent is not known as bad
   */
  public Optional<BadBlockCause> checkAndMarkBadDescendant(final BlockHeader header) {
    final Notification notification;
    synchronized (this) {
      final Hash parentHash = header.getParentHash();
      final Optional<BlockHeader> maybeBadParentHeader = getBadHeader(parentHash);
      if (maybeBadParentHeader.isEmpty()) {
        return Optional.empty();
      }
      final Optional<BadBlockCause> alreadyRecorded = getBadBlockCause(header.getHash());
      if (alreadyRecorded.isPresent()) {
        return alreadyRecorded;
      }
      notification =
          recordBadDescendant(header, maybeBadParentHeader.get(), getLatestValidHash(parentHash));
    }
    notify(notification);
    return Optional.of(notification.cause());
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
