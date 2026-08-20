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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveTrieNodeCaptureTest {

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

  private static Bytes32 hashOf(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private SegmentedKeyValueStorage storage;
  private ArchiveNodeHistoryStore historyStore;
  private ArchiveCoverageTracker coverageTracker;
  private ArchiveTrieNodeCapture capture;

  @BeforeEach
  void setUp() {
    storage =
        new SegmentedInMemoryKeyValueStorage(
            List.of(TRIE_BRANCH_STORAGE, TRIE_BRANCH_STORAGE_ARCHIVE));
    historyStore = new ArchiveNodeHistoryStore(storage);
    coverageTracker = new ArchiveCoverageTracker(storage);
    capture =
        new ArchiveTrieNodeCapture(historyStore, coverageTracker, Executors.newFixedThreadPool(2));
  }

  private void enqueueAndCommit(
      final long block,
      final Bytes naturalKey,
      final Bytes location,
      final Bytes newNode,
      final Bytes priorNode) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(naturalKey, location, block, null, Bytes32.ZERO, newNode, priorNode, tx);
    capture.onBeforeCommit(tx);
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
  void rootLocationAlwaysStoredFull() {
    final Bytes nk = ArchiveNodeKey.account(ROOT_LOCATION);
    for (int block = 0; block < 3; block++) {
      enqueueAndCommit(
          block, nk, ROOT_LOCATION, branchNode(block), block == 0 ? null : branchNode(block - 1));
    }
    for (int block = 0; block < 3; block++) {
      final var entry = historyStore.getLatestBefore(nk, block).orElseThrow();
      assertThat(entry.codecEntry().isFull()).isTrue();
      assertThat(entry.counter()).isZero();
    }
  }

  @Test
  void rollbackDiscardsCaptureBuffer() {
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(nk, LOCATION, 0L, null, Bytes32.ZERO, branchNode(0), null, tx);
    capture.onRollback(tx);
    tx.rollback();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isEmpty();
  }

  @Test
  void foreignTransactionIgnoredOnRollback() {
    // Simulates commitTrieLogOnly() mid-block: a different updater's onRollback arrives while
    // the owning updater's buffer is still live. The buffer must not be discarded.
    final Bytes nk = ArchiveNodeKey.account(LOCATION);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    capture.enqueue(nk, LOCATION, 0L, null, Bytes32.ZERO, branchNode(0), null, tx);

    final SegmentedKeyValueStorageTransaction foreignTx = storage.startTransaction();
    capture.onRollback(foreignTx); // must be ignored
    foreignTx.rollback();

    // Buffer is still intact — flush with the owning tx.
    capture.onBeforeCommit(tx);
    tx.commit();

    assertThat(historyStore.getLatestBefore(nk, 0L)).isPresent();
  }

  @Test
  void chunkBoundaryFlushesAllNodes() {
    // 65 creations in one block: the first 64 are auto-submitted as a chunk during enqueue, the
    // 65th is submitted in onBeforeCommit. All 65 must appear in the archive.
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2)); // +2 avoids ROOT_LOCATION and LOCATION
      capture.enqueue(
          ArchiveNodeKey.account(loc),
          loc,
          0L,
          null,
          hashOf(branchNode(i)),
          branchNode(i),
          null,
          tx);
    }
    capture.onBeforeCommit(tx);
    tx.commit();

    for (int i = 0; i < 65; i++) {
      final Bytes loc = Bytes.of((byte) (i + 2));
      assertThat(historyStore.getLatestBefore(ArchiveNodeKey.account(loc), 0L)).isPresent();
    }
  }
}
