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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeWriterTest {

  private static final Bytes LOCATION = Bytes.of(0x01);
  private static final Bytes ROOT_LOCATION = Bytes.EMPTY;

  private static Bytes branchNode(final int seed) {
    final byte[] childRef = new byte[33];
    childRef[0] = (byte) 0xa0;
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

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveTrieNodeWriter trieNodeWriter;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    trieNodeWriter =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            Executors.newFixedThreadPool(2),
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
  }

  private void enqueueAndCommit(
      final long block,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes newNode,
      final Bytes priorNode) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    trieNodeWriter.capture(naturalKey, location, block, newNode, priorNode, tx);
    trieNodeWriter.onBeforeCommit(tx);
    tx.commit();
  }

  @Test
  void creationIsStoredAsFull() {
    final Bytes node = branchNode(0);
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, node, null);

    final var entry = historyStore.getLatestBefore(nk, 0L).orElseThrow();
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.counter()).isZero();
    assertThat(coverageTracker.hasArchiveBlock(0L)).isTrue();
  }

  @Test
  void updateOnContiguousChainIsDiff() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, LOCATION, branchNode(1), branchNode(0));

    final var diff = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(diff.block()).isEqualTo(1L);
    assertThat(diff.counter()).isEqualTo(1);
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.codecEntry().isDeletion()).isFalse();
  }

  @Test
  void deletionWithPriorStoresTombstone() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, LOCATION, null, branchNode(0));

    final var tombstone = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(tombstone.block()).isEqualTo(1L);
    assertThat(tombstone.codecEntry().isDeletion()).isTrue();
  }

  @Test
  void nonContiguousUpdateForcesFullCheckpoint() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    // block 5 does not follow block 0 — chain is not contiguous
    enqueueAndCommit(5L, nk, LOCATION, branchNode(5), branchNode(4));

    final var entry = historyStore.getLatestBefore(nk, 5L).orElseThrow();
    assertThat(entry.block()).isEqualTo(5L);
    assertThat(entry.codecEntry().isFull()).isTrue();
    assertThat(entry.counter()).isZero();
  }

  @Test
  void rootSparseChange_producesDiffBetweenCheckpoints() {
    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, branchNode(1), branchNode(0));

    final var creation = historyStore.getLatestBefore(nk, 0L).orElseThrow();
    assertThat(creation.codecEntry().isFull()).isTrue();
    assertThat(creation.counter()).isZero();

    final var diff = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(diff.codecEntry().isFull()).isFalse();
    assertThat(diff.codecEntry().isDeletion()).isFalse();
    assertThat(diff.counter()).isEqualTo(1);
  }

  @Test
  void rootDrasticChange_staysFullEveryBlock() {
    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, disjointNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, disjointNode(1), disjointNode(0));
    enqueueAndCommit(2L, nk, ROOT_LOCATION, disjointNode(2), disjointNode(1));

    for (long block = 0; block <= 2; block++) {
      assertThat(historyStore.getLatestBefore(nk, block).orElseThrow().codecEntry().isFull())
          .as("drastic root change at block %s must fall back to FULL", block)
          .isTrue();
    }
  }

  @Test
  void midChainFullFallback_resetsCounterToZero() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    enqueueAndCommit(0L, nk, LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, LOCATION, disjointNode(1), branchNode(0));

    final var entry1 = historyStore.getLatestBefore(nk, 1L).orElseThrow();
    assertThat(entry1.codecEntry().isFull())
        .as("drastic change must produce a FULL entry")
        .isTrue();
    assertThat(entry1.counter())
        .as("FULL entry produced by encodeDiff fallback must have counter == 0")
        .isZero();
  }

  @Test
  void storageRootSparseChange_producesDiff() {
    final Bytes accountHash = Bytes32.leftPad(Bytes.of(0x04));
    final Bytes nk = ArchiveNodeKey.storage(accountHash, ROOT_LOCATION);
    enqueueAndCommit(0L, nk, ROOT_LOCATION, branchNode(0), null);
    enqueueAndCommit(1L, nk, ROOT_LOCATION, branchNode(1), branchNode(0));

    assertThat(historyStore.getLatestBefore(nk, 0L).orElseThrow().codecEntry().isFull()).isTrue();
    assertThat(historyStore.getLatestBefore(nk, 1L).orElseThrow().codecEntry().isFull()).isFalse();
  }

  @Test
  void rollbackDiscardsCaptureBuffer() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    trieNodeWriter.capture(nk, LOCATION, 0L, branchNode(0), null, tx);
    trieNodeWriter.onRollback(tx);
    tx.rollback();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isEmpty();
  }

  @Test
  void foreignTransactionIgnoredOnRollback() {
    // Simulates commitTrieLogOnly() mid-block: a different updater's onRollback arrives while
    // the owning updater's buffer is still live. The buffer must not be discarded.
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    trieNodeWriter.capture(nk, LOCATION, 0L, branchNode(0), null, tx);

    final SegmentedKeyValueStorageTransaction foreignTx = storage.startTransaction();
    trieNodeWriter.onRollback(foreignTx); // must be ignored
    foreignTx.rollback();

    // Buffer is still intact — flush with the owning tx.
    trieNodeWriter.onBeforeCommit(tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isPresent();
  }

  @Test
  void interleavedTransactionsOnOneThreadFlushIndependently() {
    // Transaction identity — not thread identity — scopes a capture buffer: two transactions live
    // at once on the same thread must each keep their own captures and flush only their own.
    final Bytes locA = Bytes.of(0x0a);
    final Bytes locB = Bytes.of(0x0b);
    final Bytes nkA = ArchiveNodeKey.account(locA);
    final Bytes nkB = ArchiveNodeKey.account(locB);

    final SegmentedKeyValueStorageTransaction txA = storage.startTransaction();
    final SegmentedKeyValueStorageTransaction txB = storage.startTransaction();
    trieNodeWriter.capture(nkA, locA, 0L, branchNode(0), null, txA);
    trieNodeWriter.capture(nkB, locB, 1L, branchNode(1), null, txB);

    trieNodeWriter.onBeforeCommit(txA);
    txA.commit();
    assertThat(historyStore.getLatestBefore(nkA, 0L)).isPresent();
    assertThat(historyStore.getLatestBefore(nkB, 1L)).isEmpty();

    trieNodeWriter.onBeforeCommit(txB);
    txB.commit();
    assertThat(historyStore.getLatestBefore(nkB, 1L)).isPresent();
  }

  @Test
  void checkpointIntervalForDepth_mapsDepthToTier() {
    // depths 0-2 (root + trie levels 1-2) use the shallow interval
    // depth >= 3 uses the deep interval
    final int shallow = 24;
    final int deep = 8;
    final ArchiveTrieNodeWriter writer =
        new ArchiveTrieNodeWriter(
            historyStore, coverageTracker, Executors.newFixedThreadPool(1), shallow, deep);
    assertThat(writer.checkpointIntervalForDepth(0)).isEqualTo(shallow);
    assertThat(writer.checkpointIntervalForDepth(1)).isEqualTo(shallow);
    assertThat(writer.checkpointIntervalForDepth(2)).isEqualTo(shallow);
    assertThat(writer.checkpointIntervalForDepth(3)).isEqualTo(deep);
    assertThat(writer.checkpointIntervalForDepth(5)).isEqualTo(deep);
    assertThat(writer.checkpointIntervalForDepth(32)).isEqualTo(deep);
  }

  @Test
  void shallowNodeCheckpointsAtShallowIntervalNotDeepInterval() {
    // A shallow node (LOCATION has size 1, i.e. depth 1) must checkpoint on the shallow interval
    final int shallowInterval = 8;
    final int deepInterval = 4;
    final ArchiveTrieNodeWriter writer =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            Executors.newFixedThreadPool(1),
            shallowInterval,
            deepInterval);

    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    Bytes prior = null;
    for (int block = 0; block <= shallowInterval; block++) {
      final Bytes node = branchNode(block);
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      writer.capture(nk, LOCATION, block, node, prior, tx);
      writer.onBeforeCommit(tx);
      tx.commit();
      prior = node;
    }

    assertThat(historyStore.getLatestBefore(nk, deepInterval).orElseThrow().codecEntry().isFull())
        .as("shallow node must NOT checkpoint at the deep interval (%d)", deepInterval)
        .isFalse();
    assertThat(
            historyStore.getLatestBefore(nk, shallowInterval).orElseThrow().codecEntry().isFull())
        .as("shallow node must checkpoint at the shallow interval (%d)", shallowInterval)
        .isTrue();
  }

  // ~530-byte node in which exactly one byte changes per block — a small, diff-friendly per-block
  // delta typical of a real (deep) trie node's churn, where the wider interval genuinely wins.
  private static Bytes smallDeltaNode(final int block) {
    final byte[] b = new byte[530];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31);
    }
    b[7] = (byte) block;
    return Bytes.wrap(b);
  }

  // ~530-byte node in which every byte differs from the previous block — the patch is >= the full
  // node, so encodeDiff falls back to a FULL every block regardless of interval. This is the tiny
  // local-QBFT case: shallow nodes cover a large fraction of the small keyspace and change
  // drastically each block, so raising the checkpoint interval stores nothing extra AND saves
  // nothing.
  private static Bytes drasticNode(final int block) {
    final byte[] b = new byte[530];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31 + block * 131 + 1);
    }
    return Bytes.wrap(b);
  }

  // 64-byte node fully determined by seed; consecutive seeds share no bytes, so encodeDiff's
  // patch is >= the node and it falls back to FULL. Models dense (mainnet-shaped) root churn.
  private static Bytes disjointNode(final int seed) {
    final byte[] b = new byte[64];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (i * 31 + seed * 131 + 1);
    }
    return Bytes.wrap(b);
  }

  /**
   * Replays an identical mutation sequence for a node at {@code location} on a fresh capture
   * instance, then sums the on-disk stored-value bytes across all blocks. Stored value = 1 counter
   * byte + the codec entry.
   */
  private long replayAndSumStoredBytes(
      final Bytes location, final int blocks, final IntFunction<Bytes> nodeAt) {
    final Bytes nk = ArchiveNodeKey.account(location);
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    final ArchiveTrieNodeWriter localCapture =
        new ArchiveTrieNodeWriter(
            historyStore,
            coverageTracker,
            pool,
            DEFAULT_BONSAI_ARCHIVE_SHALLOW_CHECKPOINT_INTERVAL,
            DEFAULT_BONSAI_ARCHIVE_DEEP_CHECKPOINT_INTERVAL);
    Bytes prior = null;
    for (int block = 0; block < blocks; block++) {
      final Bytes node = nodeAt.apply(block);
      final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
      localCapture.capture(nk, location, block, node, prior, tx);
      localCapture.onBeforeCommit(tx);
      tx.commit();
      prior = node;
    }
    pool.shutdown();
    long total = 0;
    for (int block = 0; block < blocks; block++) {
      total +=
          1 + historyStore.getLatestBefore(nk, (long) block).orElseThrow().rawEntryBytes().size();
    }
    return total;
  }

  @Test
  void widerShallowIntervalReducesStoredBytes_forSmallPerBlockDeltas() {
    // Shallow location (size 1 -> interval 32) vs deep location (size 3 -> interval 16), same
    // 128-block sequence with a 1-byte-per-block delta on a ~530-byte node.
    final int blocks = 128;
    final long shallowBytes =
        replayAndSumStoredBytes(Bytes.of(0x0a), blocks, ArchiveTrieNodeWriterTest::smallDeltaNode);
    final long deepBytes =
        replayAndSumStoredBytes(
            Bytes.of(0x0b, 0x0c, 0x0d), blocks, ArchiveTrieNodeWriterTest::smallDeltaNode);

    // Monotonic invariant: a wider interval can never store MORE (encodeDiff never exceeds a FULL).
    assertThat(shallowBytes)
        .as("interval-32 (%s B) must never exceed interval-16 (%s B)", shallowBytes, deepBytes)
        .isLessThanOrEqualTo(deepBytes);
    // With small per-block deltas the wider interval genuinely wins: fewer forced FULL checkpoints.
    assertThat(shallowBytes)
        .as(
            "interval-32 (%s B) should be strictly smaller than interval-16 (%s B) for small deltas",
            shallowBytes, deepBytes)
        .isLessThan(deepBytes);
  }

  @Test
  void intervalHasNoEffectOnStoredBytes_whenEveryBlockChangesDrastically() {
    // When each block's node shares nothing with the previous, encodeDiff falls back to FULL every
    // block, so interval-32 and interval-16 store byte-for-byte identically. This is why the local
    // QBFT network (tiny trie, drastically-churning shallow nodes) shows no logical benefit — and
    // why a small on-disk *increase* there is RocksDB blob/compaction noise, not this change.
    final int blocks = 128;
    final long shallowBytes =
        replayAndSumStoredBytes(Bytes.of(0x0a), blocks, ArchiveTrieNodeWriterTest::drasticNode);
    final long deepBytes =
        replayAndSumStoredBytes(
            Bytes.of(0x0b, 0x0c, 0x0d), blocks, ArchiveTrieNodeWriterTest::drasticNode);

    assertThat(shallowBytes)
        .as("drastic churn: interval-32 (%s B) == interval-16 (%s B)", shallowBytes, deepBytes)
        .isEqualTo(deepBytes);
  }

  @Test
  void chunkBoundaryFlushesAllNodes() {
    // 65 creations in one block: the first 64 are auto-submitted as a chunk during enqueue, the
    // 65th is submitted in onBeforeCommit. All 65 must appear in the archive.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2)); // +2 avoids ROOT_LOCATION and LOCATION
      trieNodeWriter.capture(ArchiveNodeKey.account(loc), loc, 0L, branchNode(i), null, tx);
    }
    trieNodeWriter.onBeforeCommit(tx);
    tx.commit();

    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2));
      assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(loc), 0L)).isPresent();
    }
  }
}
