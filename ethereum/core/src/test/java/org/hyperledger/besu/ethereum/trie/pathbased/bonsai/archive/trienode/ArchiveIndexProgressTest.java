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

class ArchiveIndexProgressTest {

  private SegmentedKeyValueStorage storage;
  private ArchiveIndexProgress progress;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage(List.of(TRIE_BRANCH_STORAGE_ARCHIVE));
    progress = new ArchiveIndexProgress(storage);
  }

  @Test
  void coversNothingWhenNoProgressRecorded() {
    assertThat(progress.isBlockIndexed(0)).isFalse();
    assertThat(progress.isBlockIndexed(10)).isFalse();
  }

  @Test
  void coversRecordedBlockAfterRecord() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 5L);
    tx.commit();

    assertThat(progress.isBlockIndexed(5L)).isTrue();
    assertThat(progress.isBlockIndexed(6L)).isFalse();
  }

  @Test
  void indexStartTracksFirstRecordedBlock() {
    // Blocks are always archived in ascending order.
    final SegmentedKeyValueStorageTransaction tx1 = storage.startTransaction();
    progress.record(tx1, 5L);
    tx1.commit();

    final SegmentedKeyValueStorageTransaction tx2 = storage.startTransaction();
    progress.record(tx2, 10L);
    tx2.commit();

    // Covered range is [5, 10]
    assertThat(progress.isBlockIndexed(5L)).isTrue();
    assertThat(progress.isBlockIndexed(10L)).isTrue();
    assertThat(progress.isBlockIndexed(4L)).isFalse();
    assertThat(progress.isBlockIndexed(11L)).isFalse();
  }

  @Test
  void progressIsReadFromStorageNotInMemory() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 3L);
    tx.commit();

    // A fresh instance reading the same storage sees the same progress
    final ArchiveIndexProgress anotherView = new ArchiveIndexProgress(storage);
    assertThat(anotherView.isBlockIndexed(3L)).isTrue();
    assertThat(anotherView.isBlockIndexed(4L)).isFalse();
  }

  @Test
  void uncommittedRecordNotVisible() {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.record(tx, 7L);
    // NOT committed

    assertThat(progress.isBlockIndexed(7L)).isFalse();
  }
}
