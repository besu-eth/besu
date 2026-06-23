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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which storage slot hash intervals have been persisted per account. Used for selective BAL
 * application on partially-downloaded accounts. Slots outside tracked intervals have not been
 * persisted yet.
 */
public class DownloadedStorageRangeTracker {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadedStorageRangeTracker.class);

  private final ConcurrentHashMap<Bytes32, ConcurrentSkipListMap<Bytes32, Bytes32>>
      accountStorageRanges = new ConcurrentHashMap<>();

  /**
   * Register a storage slot hash interval whose slots have been downloaded and persisted for the
   * given account. Intervals are guaranteed non-overlapping by the storage range continuation
   * protocol (continuations always start past the last received key).
   */
  public synchronized void registerSlotRange(
      final Bytes32 accountHash, final Bytes32 startSlot, final Bytes32 endSlot) {
    if (startSlot.compareTo(endSlot) > 0) {
      throw new IllegalArgumentException(
          "startSlot must be <= endSlot: " + startSlot + " > " + endSlot);
    }
    final ConcurrentSkipListMap<Bytes32, Bytes32> ranges =
        accountStorageRanges.computeIfAbsent(accountHash, k -> new ConcurrentSkipListMap<>());
    assertNoOverlap(ranges, startSlot, endSlot);
    ranges.put(startSlot, endSlot);
  }

  /**
   * Check whether a specific storage slot hash has been persisted for the given account. Returns
   * true if the slot falls within any registered interval.
   */
  public boolean isSlotHashDownloaded(final Bytes32 accountHash, final Bytes32 slotHash) {
    final NavigableMap<Bytes32, Bytes32> ranges = accountStorageRanges.get(accountHash);
    if (ranges == null) {
      return false;
    }
    final Entry<Bytes32, Bytes32> entry = ranges.floorEntry(slotHash);
    return entry != null && slotHash.compareTo(entry.getValue()) <= 0;
  }

  /** Returns an unmodifiable snapshot of completed slot intervals for a given account. */
  public NavigableMap<Bytes32, Bytes32> getCompletedSlotRanges(final Bytes32 accountHash) {
    final NavigableMap<Bytes32, Bytes32> ranges = accountStorageRanges.get(accountHash);
    if (ranges == null) {
      return Collections.emptyNavigableMap();
    }
    return Collections.unmodifiableNavigableMap(new ConcurrentSkipListMap<>(ranges));
  }

  /** Remove all tracked storage intervals for a given account (e.g. during reorg cleanup). */
  public synchronized void removeAccount(final Bytes32 accountHash) {
    accountStorageRanges.remove(accountHash);
  }

  /** Clear all tracked state. */
  public synchronized void clear() {
    accountStorageRanges.clear();
  }

  private void assertNoOverlap(
      final ConcurrentSkipListMap<Bytes32, Bytes32> ranges,
      final Bytes32 start,
      final Bytes32 end) {
    for (var entry : ranges.entrySet()) {
      // start <= existingEnd && existingStart <= end
      if (start.compareTo(entry.getValue()) <= 0 && entry.getKey().compareTo(end) <= 0) {
        final String message =
            String.format(
                "Overlapping storage slot range detected for account: [%s,%s] vs existing [%s,%s]",
                start, end, entry.getKey(), entry.getValue());
        LOG.error(message);
        throw new IllegalStateException(message);
      }
    }
  }
}
