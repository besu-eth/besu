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

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.junit.jupiter.api.Test;

class TrieNodeHistoryProgressTest {

  @Test
  void freshInstanceNeverCoversAnything() {
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    assertThat(progress.covers(0L)).isFalse();
    assertThat(progress.covers(1_000_000L)).isFalse();
  }

  @Test
  void windowGrowsBlockByBlockAsWritesAdvance() {
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    progress.setIndexStartBlock(10L);
    progress.setLastIndexedBlock(10L);
    assertThat(progress.covers(10L)).isTrue();
    assertThat(progress.covers(11L)).isFalse();

    progress.setLastIndexedBlock(11L);
    assertThat(progress.covers(11L)).isTrue();
    assertThat(progress.covers(9L)).isFalse(); // below indexStartBlock
  }

  @Test
  void setLastIndexedBlockIsMonotonicNonDecreasing() {
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    progress.setLastIndexedBlock(100L);
    progress.setLastIndexedBlock(50L); // no-op, would move backward
    assertThat(progress.lastIndexedBlock()).isEqualTo(100L);
  }

  @Test
  void setIndexStartBlockIsMonotonicNonIncreasing() {
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    progress.setIndexStartBlock(50L);
    progress.setIndexStartBlock(100L); // no-op, would move forward
    assertThat(progress.indexStartBlock()).isEqualTo(50L);
  }

  @Test
  void serializationRoundTripsIncludingSentinelValues() {
    final TrieNodeHistoryProgress fresh = new TrieNodeHistoryProgress();
    assertThat(TrieNodeHistoryProgress.fromBytes(fresh.toBytes()).covers(0L)).isFalse();

    final TrieNodeHistoryProgress advanced = new TrieNodeHistoryProgress();
    advanced.setIndexStartBlock(5L);
    advanced.setLastIndexedBlock(20L);
    final TrieNodeHistoryProgress restored = TrieNodeHistoryProgress.fromBytes(advanced.toBytes());
    assertThat(restored.indexStartBlock()).isEqualTo(5L);
    assertThat(restored.lastIndexedBlock()).isEqualTo(20L);
  }

  @Test
  void loadReturnsFreshInstanceWhenNothingPersistedYet() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    assertThat(TrieNodeHistoryProgress.load(storage).covers(0L)).isFalse();
  }

  @Test
  void saveThenLoadRoundTrips() {
    final SegmentedKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    final TrieNodeHistoryProgress progress = new TrieNodeHistoryProgress();
    progress.setIndexStartBlock(7L);
    progress.setLastIndexedBlock(42L);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    progress.save(tx);
    tx.commit();

    final TrieNodeHistoryProgress restored = TrieNodeHistoryProgress.load(storage);
    assertThat(restored.indexStartBlock()).isEqualTo(7L);
    assertThat(restored.lastIndexedBlock()).isEqualTo(42L);
  }
}
