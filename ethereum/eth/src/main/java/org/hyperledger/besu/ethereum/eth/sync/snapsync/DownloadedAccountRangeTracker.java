/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which account-hash intervals have been persisted during snap/2 leaf download. Intervals
 * are identified by a {@code rangeStart} (Bytes32 start hash). Account leaves and trie nodes are
 * committed immediately, so we only record which intervals are present in the database.
 */
public class DownloadedAccountRangeTracker {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadedAccountRangeTracker.class);

  private final ConcurrentSkipListMap<Bytes32, Bytes32> persistedRanges =
      new ConcurrentSkipListMap<>();

  /**
   * Register an account-hash interval whose account leaves have been downloaded and persisted.
   *
   * @param rangeStart the start of the interval (inclusive)
   * @param rangeEnd the end of the interval (inclusive)
   */
  public synchronized void registerPersisted(final Bytes32 rangeStart, final Bytes32 rangeEnd) {
    assertNoOverlap(rangeStart, rangeEnd);
    persistedRanges.put(rangeStart, rangeEnd);
    LOG.atDebug()
        .setMessage("Registered persisted account range: [{},{}]")
        .addArgument(rangeStart)
        .addArgument(rangeEnd)
        .log();
  }

  /**
   * Check whether an account hash falls within any persisted interval. Used for selective BAL
   * application: only accounts whose hash is in a persisted interval have downloaded state and can
   * be updated by BAL.
   */
  public boolean isAccountHashPersisted(final Bytes32 accountHash) {
    final Entry<Bytes32, Bytes32> entry = persistedRanges.floorEntry(accountHash);
    return entry != null && accountHash.compareTo(entry.getValue()) <= 0;
  }

  /** Return an unmodifiable snapshot of persisted ranges for external consumers (e.g. BAL). */
  public NavigableMap<Bytes32, Bytes32> getPersistedRanges() {
    return Collections.unmodifiableNavigableMap(new ConcurrentSkipListMap<>(persistedRanges));
  }

  public long persistedRangeCount() {
    return persistedRanges.size();
  }

  /** Clear all tracked state. */
  public synchronized void clear() {
    persistedRanges.clear();
  }

  private void assertNoOverlap(final Bytes32 start, final Bytes32 end) {
    for (var entry : persistedRanges.entrySet()) {
      // start <= existingEnd && existingStart <= end
      if (start.compareTo(entry.getValue()) <= 0 && entry.getKey().compareTo(end) <= 0) {
        final String message =
            String.format(
                "Overlapping persisted range detected: [%s,%s] vs existing [%s,%s]",
                start, end, entry.getKey(), entry.getValue());
        LOG.error(message);
        throw new IllegalStateException(message);
      }
    }
  }
}
