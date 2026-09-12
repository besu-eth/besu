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
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveHistoryReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeHistoryStore;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveNodeKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveReadTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveTrieNodeWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiTrieNodeStrategy;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for the bonsai archive trie-node write and proof-read pipeline.
 *
 * <p>Covers the full chain: ArchiveTrieNodeStrategy → ArchiveNodeHistoryStore →
 * ArchiveCoverageTracker → ArchiveReadTrieNodeStrategy (as would be used by
 * BonsaiArchiveReadWorldStateStorageCoordinator). Does NOT require a real block-processing stack.
 */
class BonsaiArchiveStateProofIntegrationTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveHistoryReader historyReader;
  private ArchiveTrieNodeStrategy archiveStrategy;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    final ArchiveCoverageTracker coverageTracker = new ArchiveCoverageTracker(storage);
    final ArchiveNodeHistoryStore historyStore = new ArchiveNodeHistoryStore(storage);
    final BonsaiTrieNodeStrategy baseStrategy = new BonsaiTrieNodeStrategy();
    historyReader = new ArchiveHistoryReader(historyStore);
    final ArchiveTrieNodeWriter capture =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
    archiveStrategy = new ArchiveTrieNodeStrategy(baseStrategy, capture, () -> true);
  }

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  /** Branch node whose slot 0 holds a 33-byte hash ref derived from {@code seed}. */
  private static Bytes branchNode(final int seed) {
    final byte[] childRef = new byte[33];
    childRef[0] = (byte) 0xa0; // RLP string, 32 bytes
    for (int i = 1; i < 33; i++) {
      childRef[i] = (byte) (i + seed);
    }
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(Bytes.wrap(childRef));
    for (int i = 1; i < 16; i++) {
      out.writeNull();
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  /**
   * Branch node with all 16 child slots populated, where only slot 0 varies with {@code seed} — the
   * shape a real trie branch takes, and the case diff encoding is designed for: one changed child
   * ref out of sixteen.
   */
  private static Bytes fullBranchNode(final int seed) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(childRef(seed));
    for (int i = 1; i < 16; i++) {
      out.writeRaw(childRef(i));
    }
    out.writeBytes(Bytes.EMPTY);
    out.endList();
    return out.encoded();
  }

  /** A 33-byte raw-RLP hash ref derived from {@code seed}. */
  private static Bytes childRef(final int seed) {
    final byte[] ref = new byte[33];
    ref[0] = (byte) 0xa0; // RLP string, 32 bytes
    for (int i = 1; i < 33; i++) {
      ref[i] = (byte) (i + seed);
    }
    return Bytes.wrap(ref);
  }

  /** Leaf node {@code [path, value]} — the short-node shape of the codec's diff path. */
  private static Bytes leafNode(final Bytes path, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(path);
    out.writeBytes(value);
    out.endList();
    return out.encoded();
  }

  /**
   * Archives {@code node} at {@code location} as block {@code block}, then advances the stored
   * world block number exactly as a real block commit would, so the next call is treated as the
   * following block.
   */
  private void writeAccountBlock(final long block, final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    tx.put(
        TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();
  }

  private Optional<Bytes> readAccountNode(
      final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getFlatAccountTrieNode(location, nodeHash, storage);
  }

  private Optional<Bytes> readStorageNode(
      final Hash accountHash, final long block, final Bytes location, final Bytes32 nodeHash) {
    return new ArchiveReadTrieNodeStrategy(block, historyReader)
        .getFlatStorageTrieNode(accountHash, location, nodeHash, storage);
  }

  @Test
  void archivedNodeIsRetrievableViaProofLoader() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = branchNode(0);

    // Block 0 (no WORLD_BLOCK_NUMBER_KEY in storage → block is 0)
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(readAccountNode(0L, location, hash(node))).contains(node);
  }

  @Test
  void archivePathServesHistoricalNodeWhenLiveStateAdvanced() {
    final Bytes location = Bytes.of(0x0a);
    final Bytes nodeAtBlock0 = branchNode(0);
    final Bytes nodeAtBlock1 = branchNode(1);

    writeAccountBlock(0L, location, nodeAtBlock0);
    writeAccountBlock(1L, location, nodeAtBlock1);

    assertThat(readAccountNode(0L, location, hash(nodeAtBlock0))).contains(nodeAtBlock0);
    assertThat(readAccountNode(1L, location, hash(nodeAtBlock1))).contains(nodeAtBlock1);
  }

  @Test
  void progressCoversBlockAfterArchive() {
    final Bytes location = Bytes.of(0x00);
    final Bytes node = branchNode(0);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatAccountTrieNode(storage, tx, location, hash(node), node);
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();

    final ArchiveCoverageTracker loaded = new ArchiveCoverageTracker(storage);
    assertThat(loaded.hasArchiveBlock(0L)).isTrue();
    assertThat(loaded.hasArchiveBlock(1L)).isFalse(); // only block 0 was archived
  }

  @Test
  void proofLoaderReturnsEmptyForUnarchivedBlock() {
    // Nothing written to storage
    final Bytes location = Bytes.of(0x0f);
    final Bytes phantomNode = Bytes.fromHexString("0x9999");
    assertThat(readAccountNode(5L, location, hash(phantomNode))).isEmpty();
  }

  @Test
  void archivesAndRetrievesStorageTrieNode() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"));
    final Bytes location = Bytes.of(0x01);
    final Bytes node = branchNode(0);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatStorageTrieNode(storage, tx, accountHash, location, hash(node), node);
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(readStorageNode(accountHash, 0L, location, hash(node))).contains(node);
  }

  @Test
  void accountTrieLoaderIgnoresStorageTrieEntries() {
    final Hash accountHash =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1234567812345678123456781234567812345678123456781234567812345678"));
    final Bytes storageLocation = Bytes.of(0x02);
    final Bytes storageNode = branchNode(0);

    // Write only to storage-trie archive
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.putFlatStorageTrieNode(
        storage, tx, accountHash, storageLocation, hash(storageNode), storageNode);
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();

    // Account-trie reader must not return anything for the same location
    assertThat(readAccountNode(0L, storageLocation, hash(storageNode))).isEmpty();
  }

  @Test
  void everyVersionAcrossACheckpointWindowIsProvable() {
    final Bytes location = Bytes.of(0x0b);
    final int blocks = DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL * 2 + 3;
    for (int block = 0; block < blocks; block++) {
      writeAccountBlock(block, location, branchNode(block));
    }

    // Walking past two checkpoint rollovers, every historical version still proves out by hash.
    for (int block = 0; block < blocks; block++) {
      final Bytes expected = branchNode(block);
      assertThat(readAccountNode(block, location, hash(expected)))
          .as("block %d", block)
          .contains(expected);
    }
  }

  @Test
  void diffEncodedHistoryIsSmallerThanFullNodePerBlock() {
    final Bytes location = Bytes.of(0x0c);
    final int blocks = DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
    long archivedBytes = 0;
    long fullNodeBytes = 0;
    for (int block = 0; block < blocks; block++) {
      final Bytes node = fullBranchNode(block);
      writeAccountBlock(block, location, node);
      fullNodeBytes += node.size();
      archivedBytes +=
          storage
              .get(
                  TRIE_BRANCH_STORAGE_ARCHIVE,
                  ArchiveNodeKey.historyKey(ArchiveNodeKey.account(location), block)
                      .toArrayUnsafe())
              .orElseThrow()
              .length;
    }
    // A single-child-slot change encodes in a handful of bytes instead of a whole branch node.
    assertThat(archivedBytes).isLessThan(fullNodeBytes / 2);
  }

  @Test
  void shortNodeDiffsReconstructThroughTheProofLoader() {
    final Bytes location = Bytes.of(0x0d);
    final Bytes path = Bytes.of(0x20, 0x0a, 0x0b);
    final Bytes leafV0 = leafNode(path, Bytes.of(0x01));
    final Bytes leafV1 = leafNode(path, Bytes.of(0x02)); // value-only change
    final Bytes leafV2 = leafNode(Bytes.of(0x20, 0x0a, 0x0c), Bytes.of(0x02)); // path-only change

    writeAccountBlock(0L, location, leafV0);
    writeAccountBlock(1L, location, leafV1);
    writeAccountBlock(2L, location, leafV2);

    assertThat(readAccountNode(0L, location, hash(leafV0))).contains(leafV0);
    assertThat(readAccountNode(1L, location, hash(leafV1))).contains(leafV1);
    assertThat(readAccountNode(2L, location, hash(leafV2))).contains(leafV2);
  }

  @Test
  void deletedNodeIsAbsentFromLaterProofsButPresentInEarlierOnes() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = branchNode(0);
    writeAccountBlock(0L, location, node);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveStrategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.put(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(1L).toArrayUnsafe());
    archiveStrategy.onBeforeCommit(storage, tx);
    tx.commit();

    assertThat(readAccountNode(0L, location, hash(node))).contains(node);
    assertThat(readAccountNode(1L, location, hash(node))).isEmpty();
  }

  @Test
  void storageTrieDiffsAreIsolatedPerAccount() {
    final Hash accountA =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x1111111111111111111111111111111111111111111111111111111111111111"));
    final Hash accountB =
        Hash.wrap(
            Bytes32.fromHexString(
                "0x2222222222222222222222222222222222222222222222222222222222222222"));
    final Bytes location = Bytes.of(0x03);
    final Bytes aV0 = branchNode(10);
    final Bytes aV1 = branchNode(11);
    final Bytes bV0 = branchNode(20);

    for (long block = 0; block <= 1; block++) {
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      final Bytes aNode = block == 0 ? aV0 : aV1;
      archiveStrategy.putFlatStorageTrieNode(storage, tx, accountA, location, hash(aNode), aNode);
      if (block == 0) {
        archiveStrategy.putFlatStorageTrieNode(storage, tx, accountB, location, hash(bV0), bV0);
      }
      tx.put(
          TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY, Bytes.ofUnsignedLong(block).toArrayUnsafe());
      archiveStrategy.onBeforeCommit(storage, tx);
      tx.commit();
    }

    // A's diff at block 1 must not disturb B, whose last write was at block 0.
    assertThat(readStorageNode(accountA, 1L, location, hash(aV1))).contains(aV1);
    assertThat(readStorageNode(accountB, 1L, location, hash(bV0))).contains(bV0);
  }
}
