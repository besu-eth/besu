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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

/** Writes entries of one segment through the regular write path, in bounded batches. */
final class RegularWriter {

  private final SegmentedKeyValueStorage storage;
  private final SegmentIdentifier segment;
  private final SortedIngestStats stats;
  private SegmentedKeyValueStorageTransaction tx;
  private long pending;

  RegularWriter(
      final SegmentedKeyValueStorage storage,
      final SegmentIdentifier segment,
      final SortedIngestStats stats) {
    this.storage = storage;
    this.segment = segment;
    this.stats = stats;
  }

  void put(final byte[] key, final byte[] value) {
    if (tx == null) {
      // the sync is the only writer: no conflict checks
      tx = storage.startBatchTransaction();
    }
    tx.put(segment, key, value);
    pending += key.length + value.length;
    if (pending >= AbstractSyncStateStorage.BYTES_PER_COMMIT) {
      commit();
    }
  }

  void commit() {
    if (tx != null) {
      tx.commit();
      stats.regular(pending);
      tx = null;
      pending = 0;
    }
  }
}
