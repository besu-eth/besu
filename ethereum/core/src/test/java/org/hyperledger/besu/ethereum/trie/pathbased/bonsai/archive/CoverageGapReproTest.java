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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveProofNodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduces the archive coverage gap.
 *
 * <p>The capture gate is {@code !syncState.isInSync(maxLayersToLoad)}. With no peers, {@code
 * getBestPeerChainHead()} and {@code getSyncTargetChainHead()} are both empty and {@code SyncState}
 * falls back to {@code .orElse(true)}, so the node reports "in sync" regardless of how far behind
 * it is and the gate closes. Blocks imported during that window (e.g. driven by the CL over
 * engine_newPayload) are never archived.
 *
 * <p>{@link ArchiveCoverageTracker} stores coverage as a single {@code [start, last]} range, so
 * when the gate reopens at a higher block the window silently spans the un-archived hole.
 */
class CoverageGapReproTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveHistoryReader historyReader;
  private ArchiveTrieNodeStrategy archiveStrategy;

  /**
   * Stands in for peer availability: true = peers present and we are behind, so the gate is open.
   */
  private final AtomicBoolean gateOpen = new AtomicBoolean(true);

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    historyReader = new ArchiveHistoryReader(historyStore);
    archiveStrategy =
        new ArchiveTrieNodeStrategy(
            new BonsaiTrieNodeStrategy(), historyStore, coverageTracker, gateOpen::get);
  }

  @Test
  void coverageClaimsBlocksImportedWhileTheGateWasClosed() {
    final Bytes location = Bytes.of(0x0a);

    // --- Blocks 1..2: peers present, gate open, nodes archived ---
    importBlock(1L, location, Bytes.fromHexString("0x1111"));
    importBlock(2L, location, Bytes.fromHexString("0x2222"));

    // --- All peers drop. isInSync() returns true vacuously, so the gate closes. ---
    gateOpen.set(false);

    // Head keeps moving (CL-driven). Blocks 3..5 are imported but NOT archived.
    importBlock(3L, location, Bytes.fromHexString("0x3333"));
    importBlock(4L, location, Bytes.fromHexString("0x4444"));
    importBlock(5L, location, Bytes.fromHexString("0x5555"));

    // Coverage is still honest at this point: nothing was recorded for 3..5.
    assertThat(coverageTracker.hasArchiveBlock(4L))
        .as("gap block while gate still closed")
        .isFalse();

    // --- Peers reconnect, gate reopens, archiving resumes at block 6 ---
    gateOpen.set(true);
    importBlock(6L, location, Bytes.fromHexString("0x6666"));

    // The single [start, last] range must not span the hole.
    assertThat(coverageTracker.hasArchiveBlock(6L)).isTrue();
    assertThat(coverageTracker.hasArchiveBlock(4L))
        .as("block 4 was never archived, so coverage must not claim it")
        .isFalse();

    // And this is why it matters: the archive cannot serve the correct node for a gap block.
    final NodeLoader loader = ArchiveProofNodeLoader.forAccount(historyReader, 4L);
    assertThat(loader.getNode(location, hash(Bytes.fromHexString("0x4444"))))
        .as("archive cannot serve the node for a block inside the gap")
        .isEmpty();
  }

  /** Writes one trie node as block {@code blockNumber}, then advances WORLD_BLOCK_NUMBER_KEY. */
  private void importBlock(final long blockNumber, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.commit();

    // ArchiveTrieNodeStrategy derives the block as WORLD_BLOCK_NUMBER_KEY + 1, so the key must
    // trail by one for the next import to be stamped blockNumber + 1.
    final SegmentedKeyValueStorageTransaction advance = storage.startTransaction();
    advance.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    advance.commit();
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }
}
