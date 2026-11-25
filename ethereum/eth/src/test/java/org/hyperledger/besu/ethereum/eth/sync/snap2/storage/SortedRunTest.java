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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

import org.hyperledger.besu.plugin.services.storage.SortedWriter;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SortedRunTest {

  private final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
  private final SortedIngestStats stats = new SortedIngestStats();

  // with a file, and with a run too small for one
  @ParameterizedTest
  @ValueSource(longs = {1, Long.MAX_VALUE})
  void aKeyHandedOverAgainKeepsItsLaterValue(final long minFileBytes) {
    final SortedRun run = new SortedRun(storage, ACCOUNT_INFO_STATE, minFileBytes, stats);
    run.put(key(1), value(1));
    run.put(key(2), value(2));
    run.put(key(2), value(3));
    run.put(key(1), value(4));
    run.end().ifPresent(SortedWriter::commit);

    assertThat(stats.outOfOrderEntries()).isEqualTo(2);
    assertThat(stored(1)).isEqualTo(Bytes.wrap(value(4)));
    assertThat(stored(2)).isEqualTo(Bytes.wrap(value(3)));
  }

  @Test
  void reportsWhatItHoldsUntilItIsWorthAFile() {
    final AtomicLong held = new AtomicLong();
    // entries of 3 bytes, a file from 10
    final SortedRun file = new SortedRun(storage, ACCOUNT_INFO_STATE, 10, stats, held::addAndGet);
    for (int key = 1; key <= 3; key++) {
      file.put(key(key), value(key));
    }
    assertThat(held).hasValue(9);
    file.put(key(4), value(4));
    assertThat(held).hasValue(0);
    file.put(key(5), value(5));
    assertThat(held).hasValue(0);
    file.end().ifPresent(SortedWriter::commit);

    final SortedRun small = new SortedRun(storage, ACCOUNT_INFO_STATE, 10, stats, held::addAndGet);
    small.put(key(6), value(6));
    assertThat(held).hasValue(3);
    assertThat(small.end()).isEmpty();
    assertThat(held).hasValue(0);

    final SortedRun dropped =
        new SortedRun(storage, ACCOUNT_INFO_STATE, 10, stats, held::addAndGet);
    dropped.put(key(7), value(7));
    dropped.discard();
    assertThat(held).hasValue(0);
  }

  private Bytes stored(final int key) {
    return storage.get(ACCOUNT_INFO_STATE, key(key)).map(Bytes::wrap).orElseThrow();
  }

  private static byte[] key(final int key) {
    return new byte[] {(byte) key};
  }

  private static byte[] value(final int value) {
    return new byte[] {(byte) value, (byte) value};
  }
}
