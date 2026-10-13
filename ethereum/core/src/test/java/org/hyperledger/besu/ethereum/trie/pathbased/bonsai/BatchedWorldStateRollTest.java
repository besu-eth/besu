/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Rolls of the head world state that span several batches of trie logs. */
public class BatchedWorldStateRollTest extends AbstractIsolationTests {

  private static final int TRIE_LOGS_PER_BATCH = 3;

  private long nextNonce = 0;
  private int nextRecipient = 1;

  @BeforeEach
  void useSmallBatches() {
    archive.setTrieLogsPerRollBatch(TRIE_LOGS_PER_BATCH);
  }

  @Test
  void rollsBackAndForwardAcrossBatches() {
    final List<Block> chain = extendCanonicalChain(10);

    assertHeadWorldStateAt(moveHeadTo(chain.get(1)), chain.get(1));
    assertHeadWorldStateAt(moveHeadTo(chain.get(9)), chain.get(9));
  }

  @Test
  void rollsAcrossForksInBatches() {
    final List<Block> canonical = extendCanonicalChain(8);
    moveHeadTo(canonical.get(2));
    final List<Block> fork = extendForkOnHead(canonical.get(2), 4);
    assertHeadWorldStateAt(Optional.of(archive.getWorldState()), fork.getLast());

    // 4 roll backs down to the fork point and 5 roll forwards, in 3 batches
    assertHeadWorldStateAt(moveHeadTo(canonical.getLast()), canonical.getLast());
    assertHeadWorldStateAt(moveHeadTo(fork.getLast()), fork.getLast());
  }

  @Test
  void interruptedRollLeavesHeadWorldStateAtTheLastPersistedBatch() {
    final List<Block> chain = extendCanonicalChain(10);
    // rolling back from block 10 to block 1 undoes blocks 10 to 2: [10, 9, 8], [7, 6, 5], [4, 3, 2]
    archive.getWorldStateKeyValueStorage().pruneTrieLog(chain.get(2).getHash());

    assertThatThrownBy(() -> moveHeadTo(chain.getFirst())).isInstanceOf(MerkleTrieException.class);

    final Block lastPersisted = chain.get(3);
    assertThat(archive.getWorldStateKeyValueStorage().getWorldStateBlockHash())
        .contains(lastPersisted.getHash());
    assertHeadWorldStateAt(Optional.of(archive.getWorldState()), lastPersisted);
  }

  @Test
  void headRollIsBatchedOnlyBeyondOneBatch() {
    final List<Block> chain = extendCanonicalChain(10);

    assertThat(archive.requiresBatchedHeadRoll(chain.get(9).getHeader())).isFalse();
    assertThat(archive.requiresBatchedHeadRoll(chain.get(6).getHeader())).isFalse();
    assertThat(archive.requiresBatchedHeadRoll(chain.get(5).getHeader())).isTrue();
  }

  private Optional<MutableWorldState> moveHeadTo(final Block block) {
    return archive.getWorldState(withBlockHeaderAndUpdateNodeHead(block.getHeader()));
  }

  private void assertHeadWorldStateAt(
      final Optional<MutableWorldState> worldState, final Block block) {
    assertThat(worldState).contains(archive.getWorldState());
    assertThat(archive.getWorldState().rootHash()).isEqualTo(block.getHeader().getStateRoot());
    assertThat(archive.getWorldStateKeyValueStorage().getWorldStateBlockHash())
        .contains(block.getHash());
  }

  /** Appends blocks to the chain head, each sending to a new account. */
  private List<Block> extendCanonicalChain(final int count) {
    final List<Block> blocks = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final Block block = nextBlock(blockchain.getChainHeadHeader());
      assertThat(executeBlock(archive.getWorldState(), block).isSuccessful()).isTrue();
      blocks.add(block);
    }
    return blocks;
  }

  /** Stores blocks off the chain on top of the head world state, which must be at the parent. */
  private List<Block> extendForkOnHead(final Block parent, final int count) {
    nextNonce = parent.getHeader().getNumber();
    final List<Block> blocks = new ArrayList<>();
    BlockHeader parentHeader = parent.getHeader();
    for (int i = 0; i < count; i++) {
      final Block block = nextBlock(parentHeader);
      final BlockProcessingResult result =
          protocolSchedule
              .getByBlockHeader(block.getHeader())
              .getBlockProcessor()
              .processBlock(
                  BlockExecutionContext.builder()
                      .protocolContext(protocolContext)
                      .worldState(archive.getWorldState())
                      .block(block)
                      .blockAccessList(Optional.empty())
                      .build());
      assertThat(result.isSuccessful()).isTrue();
      blockchain.storeBlock(block, result.getReceipts(), Optional.empty());
      blocks.add(block);
      parentHeader = block.getHeader();
    }
    return blocks;
  }

  private Block nextBlock(final BlockHeader parent) {
    final Address recipient = Address.fromHexString(String.format("0x%040x", nextRecipient++));
    return forTransactions(List.of(burnTransaction(sender1, nextNonce++, recipient)), parent);
  }
}
