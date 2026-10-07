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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PendingBlocksManagerTest {

  private static final EthPeer PEER_1 = peerWithNodeId(Bytes.fromHexString("0x00"));
  private static final EthPeer PEER_2 = peerWithNodeId(Bytes.fromHexString("0x01"));

  private static EthPeer peerWithNodeId(final Bytes nodeId) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.nodeId()).thenReturn(nodeId);
    return peer;
  }

  private PendingBlocksManager pendingBlocksManager;
  private BlockDataGenerator gen;

  @BeforeEach
  public void setup() {
    pendingBlocksManager =
        new PendingBlocksManager(
            SynchronizerConfiguration.builder().blockPropagationRange(-10, 30).build());
    gen = new BlockDataGenerator();
  }

  @Test
  public void registerPendingBlock() {
    final Block block = gen.block();

    // Sanity check
    assertThat(pendingBlocksManager.contains(block.getHash())).isFalse();

    pendingBlocksManager.registerPendingBlock(block, PEER_1);

    assertThat(pendingBlocksManager.contains(block.getHash())).isTrue();
    final List<Block> pendingBlocksForParent =
        pendingBlocksManager.childrenOf(block.getHeader().getParentHash());
    assertThat(pendingBlocksForParent).isEqualTo(Collections.singletonList(block));
  }

  @Test
  public void deregisterPendingBlock() {
    final Block block = gen.block();
    pendingBlocksManager.registerPendingBlock(block, PEER_1);
    pendingBlocksManager.deregisterPendingBlock(block.getHeader());

    assertThat(pendingBlocksManager.contains(block.getHash())).isFalse();
    final List<Block> pendingBlocksForParent =
        pendingBlocksManager.childrenOf(block.getHeader().getParentHash());
    assertThat(pendingBlocksForParent).isEqualTo(Collections.emptyList());
  }

  @Test
  public void registerSiblingBlocks() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block parentBlock = gen.block();
    final Block childBlock = gen.nextBlock(parentBlock);
    final Block childBlock2 = gen.nextBlock(parentBlock);
    final List<Block> children = Arrays.asList(childBlock, childBlock2);

    pendingBlocksManager.registerPendingBlock(childBlock, PEER_1);
    pendingBlocksManager.registerPendingBlock(childBlock2, PEER_1);

    assertThat(pendingBlocksManager.contains(childBlock.getHash())).isTrue();
    assertThat(pendingBlocksManager.contains(childBlock2.getHash())).isTrue();

    final List<Block> pendingBlocksForParent =
        pendingBlocksManager.childrenOf(parentBlock.getHash());
    assertThat(pendingBlocksForParent.size()).isEqualTo(2);
    assertThat(new HashSet<>(pendingBlocksForParent)).isEqualTo(new HashSet<>(children));
  }

  @Test
  public void deregisterSubsetOfSiblingBlocks() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block parentBlock = gen.block();
    final Block childBlock = gen.nextBlock(parentBlock);
    final Block childBlock2 = gen.nextBlock(parentBlock);

    pendingBlocksManager.registerPendingBlock(childBlock, PEER_1);
    pendingBlocksManager.registerPendingBlock(childBlock2, PEER_1);
    pendingBlocksManager.deregisterPendingBlock(childBlock.getHeader());

    assertThat(pendingBlocksManager.contains(childBlock.getHash())).isFalse();
    assertThat(pendingBlocksManager.contains(childBlock2.getHash())).isTrue();

    final List<Block> pendingBlocksForParent =
        pendingBlocksManager.childrenOf(parentBlock.getHash());
    assertThat(pendingBlocksForParent).isEqualTo(Collections.singletonList(childBlock2));
  }

  @Test
  public void purgeBlocks() {
    pendingBlocksManager =
        new PendingBlocksManager(
            SynchronizerConfiguration.builder().blockPropagationRange(0, 15).build());
    final List<Block> blocks = gen.blockSequence(10);

    for (final Block block : blocks) {
      pendingBlocksManager.registerPendingBlock(block, PEER_1);
      assertThat(pendingBlocksManager.contains(block.getHash())).isTrue();
    }

    final List<Block> blocksToPurge = blocks.subList(0, 5);
    final List<Block> blocksToKeep = blocks.subList(5, blocks.size());
    pendingBlocksManager.purgeBlocksOlderThan(blocksToKeep.get(0).getHeader().getNumber());

    for (final Block block : blocksToPurge) {
      assertThat(pendingBlocksManager.contains(block.getHash())).isFalse();
      assertThat(pendingBlocksManager.childrenOf(block.getHeader().getParentHash()).size())
          .isEqualTo(0);
    }
    for (final Block block : blocksToKeep) {
      assertThat(pendingBlocksManager.contains(block.getHash())).isTrue();
      assertThat(pendingBlocksManager.childrenOf(block.getHeader().getParentHash()).size())
          .isEqualTo(1);
    }
  }

  @Test
  public void shouldPreventNodeFromFillingCache() {
    final int nbBlocks = 4;
    pendingBlocksManager =
        new PendingBlocksManager(
            SynchronizerConfiguration.builder().blockPropagationRange(-1, 2).build());
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block parentBlock = gen.block();

    // add new blocks from node 1
    final ArrayDeque<Block> childBlockFromNodeOne = new ArrayDeque<>();
    for (int i = 0; i < nbBlocks; i++) {
      final Block generatedBlock =
          gen.block(gen.nextBlockOptions(parentBlock).setTimestamp((long) i));
      childBlockFromNodeOne.add(generatedBlock);
      pendingBlocksManager.registerPendingBlock(generatedBlock, PEER_1);
    }

    // add new block from node 2
    final Block childBlockFromNodeTwo = gen.nextBlock(parentBlock);
    pendingBlocksManager.registerPendingBlock(childBlockFromNodeTwo, PEER_2);

    // check blocks from node 1 in the cache (node 1 should replace the lowest priority block)
    final List<Block> pendingBlocksForParent =
        pendingBlocksManager.childrenOf(parentBlock.getHash());
    for (int i = 0; i < nbBlocks; i++) {
      final Block foundBlock = childBlockFromNodeOne.poll();
      if (i != 0) {
        assertThat(pendingBlocksManager.contains(foundBlock.getHash())).isTrue();
        assertThat(pendingBlocksForParent).contains(foundBlock);
      } else {
        assertThat(pendingBlocksManager.contains(foundBlock.getHash())).isFalse();
        assertThat(pendingBlocksForParent).doesNotContain(foundBlock);
      }
    }
    // check blocks from node 2 in the cache (node 1 could not prevent node 2 from adding its
    // blocks)
    assertThat(pendingBlocksManager.contains(childBlockFromNodeTwo.getHash())).isTrue();
    assertThat(pendingBlocksForParent).contains(childBlockFromNodeTwo);
  }

  @Test
  public void shouldReplaceLowestPriorityBlockWhenCacheIsFull() {
    pendingBlocksManager =
        new PendingBlocksManager(
            SynchronizerConfiguration.builder().blockPropagationRange(-1, 3).build());
    final BlockDataGenerator gen = new BlockDataGenerator();

    final ArrayDeque<Block> childBlockFromNodeOne = new ArrayDeque<>();

    // block 0
    childBlockFromNodeOne.add(
        gen.block(new BlockDataGenerator.BlockOptions().setBlockNumber(0).setTimestamp(0L)));
    pendingBlocksManager.registerPendingBlock(childBlockFromNodeOne.getLast(), PEER_1);

    // block 1
    childBlockFromNodeOne.add(
        gen.block(
            gen.nextBlockOptions(childBlockFromNodeOne.element())
                .setBlockNumber(1)
                .setTimestamp(1L)));
    pendingBlocksManager.registerPendingBlock(childBlockFromNodeOne.getLast(), PEER_1);

    // block 1 reorg
    final Block reorgBlock =
        gen.block(
            gen.nextBlockOptions(childBlockFromNodeOne.element())
                .setBlockNumber(1)
                .setTimestamp(3L));
    childBlockFromNodeOne.add(reorgBlock);
    pendingBlocksManager.registerPendingBlock(reorgBlock, PEER_1);

    // block 2
    childBlockFromNodeOne.add(
        gen.block(
            gen.nextBlockOptions(childBlockFromNodeOne.element())
                .setBlockNumber(2)
                .setTimestamp(2L)));
    pendingBlocksManager.registerPendingBlock(childBlockFromNodeOne.getLast(), PEER_1);

    assertThat(pendingBlocksManager.contains(reorgBlock.getHash())).isTrue();

    // try to add a new block (not added because low priority : block number too high)
    final Block lowPriorityBlock =
        gen.block(BlockDataGenerator.BlockOptions.create().setBlockNumber(10));
    pendingBlocksManager.registerPendingBlock(lowPriorityBlock, PEER_1);
    assertThat(pendingBlocksManager.contains(lowPriorityBlock.getHash())).isFalse();

    // try to add a new block (added because high priority : low block number and high timestamp)
    final Block highPriorityBlock =
        gen.block(
            gen.nextBlockOptions(childBlockFromNodeOne.getFirst()).setTimestamp(Long.MAX_VALUE));
    pendingBlocksManager.registerPendingBlock(highPriorityBlock, PEER_1);
    assertThat(pendingBlocksManager.contains(highPriorityBlock.getHash())).isTrue();

    // check blocks in the cache
    // and verify remove the block with the lowest priority (BLOCK-2)
    for (final Block block : childBlockFromNodeOne) {
      if (block.getHeader().getNumber() == 2) {
        assertThat(pendingBlocksManager.contains(block.getHash())).isFalse();
      } else {
        assertThat(pendingBlocksManager.contains(block.getHash())).isTrue();
      }
    }
    assertThat(pendingBlocksManager.contains(reorgBlock.getHash())).isTrue();
  }

  @Test
  public void shouldReturnLowestBlockByNumber() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block parentBlock = gen.block();
    final Block childBlock = gen.nextBlock(parentBlock);
    final Block childBlock2 = gen.nextBlock(parentBlock);

    pendingBlocksManager.registerPendingBlock(parentBlock, PEER_1);
    pendingBlocksManager.registerPendingBlock(childBlock, PEER_1);
    pendingBlocksManager.registerPendingBlock(childBlock2, PEER_1);

    assertThat(pendingBlocksManager.lowestAnnouncedBlock()).contains(parentBlock.getHeader());
  }

  @Test
  public void shouldReturnLowestAncestorPendingBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block parentBlock = gen.block();

    final Block block = gen.nextBlock(parentBlock);
    final Block child = gen.nextBlock(block);

    final Block forkBlock = gen.nextBlock(parentBlock);
    final Block forkChild = gen.nextBlock(forkBlock);

    // register chain with one missing block
    pendingBlocksManager.registerPendingBlock(block, PEER_1);
    pendingBlocksManager.registerPendingBlock(child, PEER_1);

    // Register fork with one missing parent
    pendingBlocksManager.registerPendingBlock(forkBlock, PEER_1);
    pendingBlocksManager.registerPendingBlock(forkChild, PEER_1);

    // assert it is able to follow the chain
    final Optional<Block> blockAncestor = pendingBlocksManager.pendingAncestorBlockOf(child);
    assertThat(blockAncestor.get().getHeader().getHash()).isEqualTo(block.getHeader().getHash());

    // assert it is able to follow the fork
    final Optional<Block> forkAncestor = pendingBlocksManager.pendingAncestorBlockOf(forkChild);
    assertThat(forkAncestor.get().getHeader().getHash()).isEqualTo(forkBlock.getHeader().getHash());

    // Both forks result in the same parent
    assertThat(forkAncestor.get().getHeader().getParentHash())
        .isEqualTo(blockAncestor.get().getHeader().getParentHash());
  }

  @Test
  public void shouldReturnLowestAncestorPendingBlock_sameBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block block = gen.block();
    pendingBlocksManager.registerPendingBlock(block, PEER_1);
    final Optional<Block> b = pendingBlocksManager.pendingAncestorBlockOf(block);
    assertThat(b).contains(block);
  }
}
