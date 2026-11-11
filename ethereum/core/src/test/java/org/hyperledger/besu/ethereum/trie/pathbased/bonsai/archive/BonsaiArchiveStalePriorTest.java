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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration.Unstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveReadTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** A stale node left on live storage by an uncaptured write becomes the diff base. */
class BonsaiArchiveStalePriorTest {

  private final SegmentedKeyValueStorage storage =
      new SegmentedInMemoryKeyValueStorage(
          List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
  private final ExecutorService pool = Executors.newFixedThreadPool(2);
  private final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(storage);
  private final ArchiveHistoryReader historyReader = new ArchiveHistoryReader(historyStore);
  private final ArchiveTrieNodeStrategy strategy =
      new ArchiveTrieNodeStrategy(
          new BonsaiTrieNodeStrategy(),
          new ArchiveTrieNodeWriter(
              historyStore,
              pool,
              DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
              DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL),
          historyStore,
          s -> s == storage,
          () -> false);
  private final List<BlockHeader> chain = new ArrayList<>();

  @AfterEach
  void tearDown() {
    pool.shutdownNow();
  }

  private BlockHeader header(final long block) {
    while (chain.size() <= block) {
      final Hash parentHash = chain.isEmpty() ? Hash.EMPTY : chain.getLast().getHash();
      chain.add(
          new BlockHeaderTestFixture().number(chain.size()).parentHash(parentHash).buildHeader());
    }
    return chain.get((int) block);
  }

  // Branch node with slots 0 and 1 set from the seeds, so a diff touches only one slot.
  private static Bytes branch(final int slot0, final int slot1) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(childRef(slot0));
    out.writeRaw(childRef(slot1));
    for (int i = 2; i < 16; i++) {
      out.writeRaw(childRef(100 + i));
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  private static Bytes childRef(final int seed) {
    final byte[] ref = new byte[33];
    ref[0] = (byte) 0xa0;
    for (int i = 1; i < 33; i++) {
      ref[i] = (byte) (i + seed);
    }
    return Bytes.wrap(ref);
  }

  private static Bytes32 hash(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private void capturedBlock(final long block, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.onBlockPersist(storage, tx, header(block));
    strategy.putTrieNode(storage, tx, location, hash(node), node);
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        header(block).getHash().getBytes().toArrayUnsafe());
    strategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  @Test
  void diffAfterUncapturedWriteReadsBackTheWrittenNode() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes other = Bytes.of(0x0f);
    final Bytes archived = branch(1, 1);
    final Bytes stale = branch(1, 2);
    final Bytes written = branch(3, 2);

    capturedBlock(0, location, archived);
    capturedBlock(1, other, branch(9, 9));

    // A sibling block 2′ writes `stale` at the location, then the state rolls back to block 1.
    // The location is unreachable at block 1, so the roll leaves `stale` in place.
    final SegmentedKeyValueStorageTransaction sibling = storage.startTransaction();
    strategy.putTrieNode(storage, sibling, location, hash(stale), stale);
    sibling.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, header(1).getHash().getBytes().toArrayUnsafe());
    strategy.onBeforeCommit(storage, sibling);
    sibling.commit();

    // Canonical block 2 writes `written`. The trie never loads the unreachable `stale` node.
    capturedBlock(2, location, written);
    assertThat(historyStore.getArchivedBlock())
        .map(ArchiveNodeHistoryStore.ArchivedBlock::number)
        .contains(2L);

    assertThat(
            new ArchiveReadTrieNodeStrategy(2L, historyReader)
                .getTrieNode(storage, location, hash(written)))
        .contains(written);
    assertThat(
            historyStore
                .getLatestBefore(ArchiveNodeKey.account(location), 2L)
                .orElseThrow()
                .codecEntry()
                .isFull())
        .isTrue();
  }

  @Test
  void loadedPriorIsStillDiffed() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes archived = branch(1, 1);
    final Bytes written = branch(3, 1);

    capturedBlock(0, location, archived);
    // The state-root trie loads the node it replaces.
    strategy
        .overrideNodeLoader(storage, (l, h) -> Optional.of(archived), Function.identity())
        .getNode(location, hash(archived));
    capturedBlock(1, location, written);

    final ArchiveNodeHistoryStore.HistoryEntry entry =
        historyStore.getLatestBefore(ArchiveNodeKey.account(location), 1L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isFalse();
    assertThat(
            new ArchiveReadTrieNodeStrategy(1L, historyReader)
                .getTrieNode(storage, location, hash(written)))
        .contains(written);
  }
}
