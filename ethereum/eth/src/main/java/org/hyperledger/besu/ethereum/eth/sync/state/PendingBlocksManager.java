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
package org.hyperledger.besu.ethereum.eth.sync.state;

import static java.util.Collections.newSetFromMap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.cache.ImmutablePendingBlock;
import org.hyperledger.besu.ethereum.eth.sync.state.cache.PendingBlockCache;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PendingBlocksManager {

  private final PendingBlockCache pendingBlocks;

  private final Map<Hash, Set<Hash>> pendingBlocksByParentHash = new ConcurrentHashMap<>();

  public PendingBlocksManager(final SynchronizerConfiguration synchronizerConfiguration) {

    pendingBlocks =
        new PendingBlockCache(
            (Math.abs(synchronizerConfiguration.getBlockPropagationRange().lowerEndpoint())
                + Math.abs(synchronizerConfiguration.getBlockPropagationRange().upperEndpoint())));
  }

  /**
   * Track the given block.
   *
   * @param block the block to track
   * @param peer node that sent the block
   * @return true if the block was added (was not previously present)
   */
  public boolean registerPendingBlock(final Block block, final EthPeer peer) {

    final ImmutablePendingBlock previousValue =
        this.pendingBlocks.putIfAbsent(
            block.getHash(),
            ImmutablePendingBlock.builder().block(block).nodeId(peer.nodeId()).build());
    if (previousValue != null) {
      return false;
    }

    pendingBlocksByParentHash
        .computeIfAbsent(
            block.getHeader().getParentHash(),
            h -> {
              final Set<Hash> set = newSetFromMap(new ConcurrentHashMap<>());
              // Go ahead and add our value at construction, so that we don't set an empty set which
              // could be removed in deregisterPendingBlock
              set.add(block.getHash());
              return set;
            })
        .add(block.getHash());

    return true;
  }

  /**
   * Stop tracking the given block.
   *
   * @param header the header of the block that is no longer pending
   * @return true if this block was removed
   */
  public boolean deregisterPendingBlock(final BlockHeader header) {
    final Hash parentHash = header.getParentHash();
    final ImmutablePendingBlock removed = pendingBlocks.remove(header.getHash());
    final Set<Hash> blocksForParent = pendingBlocksByParentHash.get(parentHash);
    if (blocksForParent != null) {
      blocksForParent.remove(header.getHash());
      pendingBlocksByParentHash.remove(parentHash, Collections.emptySet());
    }
    return removed != null;
  }

  public void purgeBlocksOlderThan(final long blockNumber) {
    pendingBlocks.values().stream()
        .filter(b -> b.block().getHeader().getNumber() < blockNumber)
        .map(ImmutablePendingBlock::block)
        .forEach(block -> deregisterPendingBlock(block.getHeader()));
  }

  public boolean contains(final Hash blockHash) {
    return pendingBlocks.containsKey(blockHash);
  }

  public int size() {
    return pendingBlocks.size();
  }

  public List<Block> childrenOf(final Hash parentBlock) {
    final Set<Hash> blocksByParent = pendingBlocksByParentHash.get(parentBlock);
    if (blocksByParent == null || blocksByParent.size() == 0) {
      return Collections.emptyList();
    }
    return blocksByParent.stream()
        .map(pendingBlocks::get)
        .filter(Objects::nonNull)
        .map(ImmutablePendingBlock::block)
        .collect(Collectors.toList());
  }

  public Optional<BlockHeader> lowestAnnouncedBlock() {
    return pendingBlocks.values().stream()
        .map(ImmutablePendingBlock::block)
        .map(Block::getHeader)
        .min(Comparator.comparing(BlockHeader::getNumber));
  }

  /**
   * Get the lowest pending ancestor block saved for a block
   *
   * @param block target block
   * @return An optional with the lowest ancestor pending block
   */
  public Optional<Block> pendingAncestorBlockOf(final Block block) {
    Block ancestor = block;
    int ancestorLevel = 0;
    while (pendingBlocks.containsKey(ancestor.getHeader().getParentHash())
        && ancestorLevel++ < pendingBlocks.size()) {
      ancestor = pendingBlocks.get(ancestor.getHeader().getParentHash()).block();
    }
    return Optional.of(ancestor);
  }

  @Override
  public String toString() {
    return "PendingBlocksManager{"
        + "pendingBlocks ["
        + pendingBlocks.values().stream()
            .map(ImmutablePendingBlock::block)
            .map(b -> b.getHeader().getNumber() + " (" + b.getHash() + ")")
            .collect(Collectors.joining(", "))
        + "], pendingBlocksByParentHash="
        + pendingBlocksByParentHash
        + '}';
  }
}
