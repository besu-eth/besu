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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;

import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockDiskReadCounters.Snapshot;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SlowBlockDiskReadCountersTest {

  @BeforeEach
  @AfterEach
  void resetCounters() {
    SlowBlockDiskReadCounters.reset();
  }

  @Test
  void readsFromUnmarkedThreadsAreIgnored() {
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    SlowBlockDiskReadCounters.recordRead(ACCOUNT_INFO_STATE);
    SlowBlockDiskReadCounters.recordRead(ACCOUNT_STORAGE_STORAGE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(0, 0, 0));
  }

  @Test
  void readsFromMarkedThreadsAreCountedPerSegment() {
    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    SlowBlockDiskReadCounters.recordRead(ACCOUNT_INFO_STATE);
    SlowBlockDiskReadCounters.recordRead(ACCOUNT_STORAGE_STORAGE);
    SlowBlockDiskReadCounters.recordRead(ACCOUNT_STORAGE_STORAGE);
    SlowBlockDiskReadCounters.recordRead(CODE_STORAGE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(1, 2, 1));
  }

  @Test
  void nonStateSegmentsAreNotStateReads() {
    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    SlowBlockDiskReadCounters.recordRead(TRIE_BRANCH_STORAGE);
    SlowBlockDiskReadCounters.recordRead(TRIE_LOG_STORAGE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(0, 0, 0));
  }

  @Test
  void unmarkingStopsCounting() {
    SlowBlockDiskReadCounters.markExecutionReader();
    SlowBlockDiskReadCounters.unmarkExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    SlowBlockDiskReadCounters.recordRead(ACCOUNT_INFO_STATE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(0, 0, 0));
  }

  @Test
  void theMarkIsPerThreadSoPrefetchAndBackgroundReadsAreExcluded() throws InterruptedException {
    SlowBlockDiskReadCounters.markExecutionReader();
    final Snapshot before = SlowBlockDiskReadCounters.snapshot();

    // stands in for the prefetch or state-root pools, which are deliberately left unmarked
    final CountDownLatch done = new CountDownLatch(1);
    final Thread unmarked =
        new Thread(
            () -> {
              SlowBlockDiskReadCounters.recordRead(ACCOUNT_INFO_STATE);
              SlowBlockDiskReadCounters.recordRead(ACCOUNT_STORAGE_STORAGE);
              done.countDown();
            });
    unmarked.start();
    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    unmarked.join();

    SlowBlockDiskReadCounters.recordRead(ACCOUNT_INFO_STATE);

    assertThat(SlowBlockDiskReadCounters.snapshot().since(before)).isEqualTo(new Snapshot(1, 0, 0));
  }
}
