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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which account-hash intervals have been fully downloaded (account leaves + all storage and
 * code for accounts within those intervals). Intervals are identified by a {@code rangeStart}
 * (Bytes32 start hash). An interval moves from pending to completed when all storage and code child
 * requests spawned from accounts in that interval have finished.
 */
public class DownloadedAccountRangeTracker {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadedAccountRangeTracker.class);

  private final ConcurrentSkipListMap<Bytes32, PendingRange> pendingRanges =
      new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<Bytes32, Bytes32> completedRanges =
      new ConcurrentSkipListMap<>();

  private static class PendingRange {
    volatile Bytes32 endInclusive;
    final AtomicInteger pendingChildRequests;

    PendingRange(final Bytes32 endInclusive, final int initialChildCount) {
      this.endInclusive = endInclusive;
      this.pendingChildRequests = new AtomicInteger(initialChildCount);
    }
  }

  /**
   * Register an account-hash interval whose account leaves have been downloaded and persisted. The
   * interval becomes "completed" (BAL-eligible) only after {@code initialChildCount} child storage
   * and code requests have finished.
   *
   * @param rangeStart the start of the interval (inclusive)
   * @param rangeEnd the end of the interval (inclusive)
   * @param initialChildCount the number of child storage and code requests spawned from accounts
   *     within this range
   */
  public synchronized void registerPending(
      final Bytes32 rangeStart, final Bytes32 rangeEnd, final int initialChildCount) {
    if (initialChildCount < 0) {
      throw new IllegalArgumentException("Initial child count cannot be negative");
    }
    assertNoOverlap(rangeStart, rangeEnd);
    if (initialChildCount == 0) {
      completedRanges.put(rangeStart, rangeEnd);
      LOG.atDebug()
          .setMessage("Account range completed immediately: [{},{}] (no children)")
          .addArgument(rangeStart)
          .addArgument(rangeEnd)
          .log();
    } else {
      pendingRanges.put(rangeStart, new PendingRange(rangeEnd, initialChildCount));
      LOG.atDebug()
          .setMessage("Registered pending account range: [{},{}] (pending children: {})")
          .addArgument(rangeStart)
          .addArgument(rangeEnd)
          .addArgument(initialChildCount)
          .log();
    }
  }

  /** Add a newly-spawned child request to an existing pending range. */
  public void addPendingChild(final Bytes32 rangeStart) {
    adjustPendingChildren(rangeStart, 1);
  }

  /**
   * Adjust the pending child count for a range by {@code delta}. A positive delta adds child work
   * (e.g. storage continuations), a negative delta removes it (child completed). When the count
   * reaches zero, the range is promoted to completed.
   */
  public synchronized void adjustPendingChildren(final Bytes32 rangeStart, final int delta) {
    final PendingRange range = pendingRanges.get(rangeStart);
    if (range == null) {
      throw new IllegalStateException("No pending range found for start key " + rangeStart);
    }
    final int remaining = range.pendingChildRequests.addAndGet(delta);
    if (remaining < 0) {
      throw new IllegalStateException(
          String.format(
              "Pending child count cannot be negative, but was %d for range %s",
              remaining, rangeStart));
    }
    if (remaining == 0) {
      pendingRanges.remove(rangeStart);
      completedRanges.put(rangeStart, range.endInclusive);
      LOG.atDebug()
          .setMessage("Account range completed (delta {}): [{},{}]")
          .addArgument(delta)
          .addArgument(rangeStart)
          .addArgument(range.endInclusive)
          .log();
    }
  }

  /**
   * Notify the tracker that a child storage or code request belonging to {@code rangeStart} has
   * finished. When the last pending child for a range completes, the range is promoted to
   * completed.
   */
  public void onChildCompleted(final Bytes32 rangeStart) {
    adjustPendingChildren(rangeStart, -1);
  }

  /**
   * Check whether an account hash falls within any completed interval. Used for selective BAL
   * application: only accounts whose hash is in a completed interval have fully-downloaded state
   * and can be updated by BAL.
   */
  public boolean isAccountHashDownloaded(final Bytes32 accountHash) {
    final Entry<Bytes32, Bytes32> entry = completedRanges.floorEntry(accountHash);
    return entry != null && accountHash.compareTo(entry.getValue()) <= 0;
  }

  /**
   * Check whether an account hash falls within any pending interval. The account has been persisted
   * but still has outstanding storage or code child requests.
   */
  public boolean isAccountHashPending(final Bytes32 accountHash) {
    final Entry<Bytes32, PendingRange> entry = pendingRanges.floorEntry(accountHash);
    return entry != null && accountHash.compareTo(entry.getValue().endInclusive) <= 0;
  }

  /** Return an unmodifiable snapshot of completed ranges for external consumers (e.g. BAL). */
  public NavigableMap<Bytes32, Bytes32> getCompletedRanges() {
    return Collections.unmodifiableNavigableMap(new ConcurrentSkipListMap<>(completedRanges));
  }

  public long pendingRangeCount() {
    return pendingRanges.size();
  }

  public long completedRangeCount() {
    return completedRanges.size();
  }

  /** Clear all tracked state. */
  public synchronized void clear() {
    pendingRanges.clear();
    completedRanges.clear();
  }

  private void assertNoOverlap(final Bytes32 start, final Bytes32 end) {
    for (var entry : completedRanges.entrySet()) {
      // start <= existingEnd && existingStart <= end
      if (start.compareTo(entry.getValue()) <= 0 && entry.getKey().compareTo(end) <= 0) {
        final String message =
            String.format(
                "Overlapping completed range detected: [%s,%s] vs existing [%s,%s]",
                start, end, entry.getKey(), entry.getValue());
        LOG.error(message);
        throw new IllegalStateException(message);
      }
    }
    for (var entry : pendingRanges.entrySet()) {
      // start <= existingEnd && existingStart <= end
      if (start.compareTo(entry.getValue().endInclusive) <= 0
          && entry.getKey().compareTo(end) <= 0) {
        final String message =
            String.format(
                "Overlapping pending range: [%s,%s] vs existing [%s,%s]",
                start, end, entry.getKey(), entry.getValue().endInclusive);
        LOG.error(message);
        throw new IllegalStateException(message);
      }
    }
  }
}
