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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.ArchiveCoverageTracker.Coverage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  private static Hash blockHash(final long block) {
    return Hash.hash(Bytes.ofUnsignedLong(block));
  }

  private void commitRecord(final long block) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, block, blockHash(block));
    tx.commit();
  }

  @Test
  void coversRecordedBlockAfterRecord() {
    commitRecord(0L);

    assertThat(progress.hasArchiveBlock(0L)).isTrue();
    assertThat(progress.hasArchiveBlock(1L)).isFalse();
  }

  @Test
  void coverageSpansContiguousRecords() {
    commitRecord(0L);
    commitRecord(1L);
    commitRecord(2L);

    assertThat(progress.hasArchiveBlock(0L)).isTrue();
    assertThat(progress.hasArchiveBlock(2L)).isTrue();
    assertThat(progress.hasArchiveBlock(3L)).isFalse();
  }

  @Test
  void progressIsReadFromStorageNotInMemory() {
    commitRecord(0L);
    commitRecord(1L);

    final ArchiveCoverageTracker anotherView = new ArchiveCoverageTracker(storage);
    assertThat(anotherView.hasArchiveBlock(1L)).isTrue();
    assertThat(anotherView.hasArchiveBlock(2L)).isFalse();
  }

  @Test
  void uncommittedRecordNotVisible() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 0L, blockHash(0L));
    // NOT committed

    assertThat(progress.hasArchiveBlock(0L)).isFalse();
  }

  // 16 bytes is the legacy hash-less format.
  @ParameterizedTest
  @ValueSource(ints = {4, 16})
  void malformedCoverageTreatedAsNone(final int size) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TRIE_BRANCH_STORAGE_ARCHIVE, ArchiveCoverageTracker.COVERAGE_KEY, new byte[size]);
    tx.commit();

    assertThat(progress.getCoverage()).isEmpty();
    assertThat(progress.hasArchiveBlock(0L)).isFalse();
  }

  @Test
  void recordRejectsGap() {
    commitRecord(0L);
    assertThatThrownBy(() -> commitRecord(2L)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void recordRejectsNonGenesisFirstBlock() {
    assertThatThrownBy(() -> commitRecord(5L)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void coverageReportsWindow() {
    commitRecord(0L);
    commitRecord(1L);
    assertThat(progress.getCoverage()).contains(new Coverage(0L, 1L, blockHash(1L)));
  }

  @Test
  void recordStoresStartLastAndHash() {
    commitRecord(0L);

    final byte[] raw =
        storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, ArchiveCoverageTracker.COVERAGE_KEY).orElseThrow();
    assertThat(raw).hasSize(48);
    assertThat(new ArchiveCoverageTracker(storage).getCoverage())
        .contains(new Coverage(0L, 0L, blockHash(0L)));
  }
}
