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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArchiveCoverageTrackerTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveCoverageTracker progress;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    progress = new ArchiveCoverageTracker(storage);
  }

  @Test
  void coversNothingWhenNoProgressRecorded() {
    assertThat(progress.hasArchiveBlock(0)).isFalse();
    assertThat(progress.hasArchiveBlock(10)).isFalse();
  }

  @Test
  void coversRecordedBlockAfterRecord() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 5L);
    tx.commit();

    assertThat(progress.hasArchiveBlock(5L)).isTrue();
    assertThat(progress.hasArchiveBlock(6L)).isFalse();
  }

  @Test
  void indexStartTracksFirstRecordedBlock() {
    record(5L);
    record(6L);
    record(7L);

    // Covered range is [5, 7]
    assertThat(progress.hasArchiveBlock(5L)).isTrue();
    assertThat(progress.hasArchiveBlock(7L)).isTrue();
    assertThat(progress.hasArchiveBlock(4L)).isFalse();
    assertThat(progress.hasArchiveBlock(8L)).isFalse();
  }

  @Test
  void progressIsReadFromStorageNotInMemory() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 3L);
    tx.commit();

    // A fresh instance reading the same storage sees the same progress
    final ArchiveCoverageTracker anotherView = new ArchiveCoverageTracker(storage);
    assertThat(anotherView.hasArchiveBlock(3L)).isTrue();
    assertThat(anotherView.hasArchiveBlock(4L)).isFalse();
  }

  @Test
  void uncommittedRecordNotVisible() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 7L);
    // NOT committed

    assertThat(progress.hasArchiveBlock(7L)).isFalse();
  }

  @Test
  void gapInArchivedBlocksResetsRangeToAvoidFalseCoverage() {
    // Blocks 1-2 archived normally.
    record(1L);
    record(2L);

    // Blocks 3-5 are skipped (gate was closed).  Record resumes at 6.
    record(6L);

    // New range starts at 6; the gap blocks must not be claimed.
    assertThat(progress.hasArchiveBlock(6L)).isTrue();
    assertThat(progress.hasArchiveBlock(3L)).isFalse();
    assertThat(progress.hasArchiveBlock(4L)).isFalse();
    assertThat(progress.hasArchiveBlock(5L)).isFalse();
  }

  private void record(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, block);
    tx.commit();
  }
}
