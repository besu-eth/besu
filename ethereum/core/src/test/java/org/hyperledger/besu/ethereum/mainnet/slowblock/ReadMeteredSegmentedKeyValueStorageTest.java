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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockDiskReadCounters.Snapshot;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReadMeteredSegmentedKeyValueStorageTest {

  private static final byte[] KEY = new byte[] {1, 2, 3};
  private static final byte[] VALUE = new byte[] {4, 5, 6};

  private ReadMeteredSegmentedKeyValueStorage storage;

  @BeforeEach
  void setUp() {
    SlowBlockDiskReadCounters.reset();
    storage = new ReadMeteredSegmentedKeyValueStorage(new SegmentedInMemoryKeyValueStorage());
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE, KEY, VALUE);
    tx.commit();
  }

  @AfterEach
  void tearDown() {
    SlowBlockDiskReadCounters.reset();
  }

  @Test
  void pointReadsAreCountedAndStillReturnTheValue() {
    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    assertThat(storage.get(ACCOUNT_INFO_STATE, KEY)).contains(VALUE);
    assertThat(storage.containsKey(ACCOUNT_INFO_STATE, KEY)).isTrue();
    assertThat(storage.get(ACCOUNT_STORAGE_STORAGE, KEY)).isEmpty();

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(2, 1, 0));
  }

  @Test
  void snapshotsAreSnappedAndStayMetered() {
    // BonsaiSnapshotWorldStateKeyValueStorage casts the composed storage without checking, so a
    // decorator that dropped SnappableKeyValueStorage, or returned an unsnapped wrapper, would
    // blow up with a ClassCastException at snapshot time.
    assertThat(storage).isInstanceOf(SnappableKeyValueStorage.class);

    final SnappedKeyValueStorage snapshot = storage.takeSnapshot();
    assertThat(snapshot).isInstanceOf(SegmentedKeyValueStorage.class);
    assertThat(snapshot.getSnapshotTransaction()).isNotNull();

    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    assertThat(snapshot.get(ACCOUNT_INFO_STATE, KEY)).contains(VALUE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(1, 0, 0));
  }

  @Test
  void writeAndStreamPathsAreNotCountedAsReads() {
    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    final SegmentedKeyValueStorageTransaction tx = storage.startLowPriorityTransaction();
    tx.put(ACCOUNT_STORAGE_STORAGE, KEY, VALUE);
    tx.commit();
    assertThat(storage.streamKeys(ACCOUNT_INFO_STATE)).hasSize(1);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(0, 0, 0));
  }
}
