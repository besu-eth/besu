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

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A run of entries of one segment in ascending key order, written to a sorted file once it holds
 * enough to be worth one. A run that ends smaller goes through the regular write path, as does an
 * entry that is not after the one before.
 */
final class SortedRun {

  private final SegmentedKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final long minFileBytes;
  private final SortedIngestStats stats;
  private final List<byte[]> keys = new ArrayList<>();
  private final List<byte[]> values = new ArrayList<>();
  private SortedWriter writer;
  private byte[] lastKey;
  private long buffered;
  private long size;

  SortedRun(
      final SegmentedKeyValueStorage storage,
      final SegmentIdentifier segment,
      final long minFileBytes,
      final SortedIngestStats stats) {
    this.storage = storage;
    this.segment = segment;
    this.minFileBytes = minFileBytes;
    this.stats = stats;
  }

  void put(final byte[] key, final byte[] value) {
    if (lastKey != null && Arrays.compareUnsigned(key, lastKey) <= 0) {
      stats.outOfOrder();
      writeRegular(List.of(key), List.of(value));
      return;
    }
    lastKey = key;
    size += key.length + value.length;
    if (writer != null) {
      writer.put(key, value);
      return;
    }
    keys.add(key);
    values.add(value);
    buffered += key.length + value.length;
    if (buffered >= minFileBytes) {
      writer = storage.startSortedWriter(segment);
      for (int i = 0; i < keys.size(); i++) {
        writer.put(keys.get(i), values.get(i));
      }
      keys.clear();
      values.clear();
      buffered = 0;
    }
  }

  /** Returns the bytes of the entries of the run. */
  long size() {
    return size;
  }

  /** Returns the bytes held in memory until the run is worth a file. */
  long buffered() {
    return buffered;
  }

  /**
   * Ends the run.
   *
   * @return the writer of its file, to be committed, or empty if its entries were written through
   *     the regular write path
   */
  Optional<SortedWriter> end() {
    if (writer != null) {
      stats.file(size);
      return Optional.of(writer);
    }
    writeRegular(keys, values);
    keys.clear();
    values.clear();
    buffered = 0;
    return Optional.empty();
  }

  /** Drops the run, nothing of what is not written yet is kept. */
  void discard() {
    if (writer != null) {
      writer.rollback();
    }
    keys.clear();
    values.clear();
    buffered = 0;
  }

  private void writeRegular(final List<byte[]> regularKeys, final List<byte[]> regularValues) {
    final RegularWriter regular = new RegularWriter(storage, segment, stats);
    for (int i = 0; i < regularKeys.size(); i++) {
      regular.put(regularKeys.get(i), regularValues.get(i));
    }
    regular.commit();
  }
}
