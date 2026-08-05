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

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters for world-state reads that cross the RocksDB boundary, which is what
 * slow-block metrics call a cache miss.
 *
 * <p>Storage is a process singleton with no per-block seam, so reads are attributed by thread
 * instead: the block-import thread and the block-processing CPU pool are marked as execution
 * readers, and only their reads are counted. Prefetch, background state-root and RPC threads are
 * deliberately left unmarked — a prefetched key still crosses the boundary again when execution
 * reads it, so counting prefetch would turn real misses into phantom hits.
 *
 * <p>A block's misses are the delta between a snapshot taken when the block starts and one taken
 * after it persists; only one block is imported at a time, so the window is well defined.
 */
public final class SlowBlockDiskReadCounters {

  private static final ThreadLocal<Boolean> EXECUTION_READER = new ThreadLocal<>();

  private static final LongAdder ACCOUNTS = new LongAdder();
  private static final LongAdder STORAGE_SLOTS = new LongAdder();
  private static final LongAdder CODE = new LongAdder();

  private SlowBlockDiskReadCounters() {}

  /** Marks the calling thread so its world-state reads count as block-execution reads. */
  public static void markExecutionReader() {
    EXECUTION_READER.set(Boolean.TRUE);
  }

  /** Clears the block-execution mark from the calling thread. */
  public static void unmarkExecutionReader() {
    EXECUTION_READER.remove();
  }

  /**
   * Records a read that crossed the storage boundary, ignoring it unless the calling thread is
   * marked as a block-execution reader.
   *
   * @param segment the segment that was read
   */
  public static void recordRead(final SegmentIdentifier segment) {
    if (EXECUTION_READER.get() == null) {
      return;
    }
    if (segment == KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE) {
      ACCOUNTS.increment();
    } else if (segment == KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE) {
      STORAGE_SLOTS.increment();
    } else if (segment == KeyValueSegmentIdentifier.CODE_STORAGE) {
      CODE.increment();
    }
    // trie node and non world-state segments are not state reads
  }

  /**
   * Takes a snapshot of the running totals.
   *
   * @return the current counts
   */
  public static Snapshot snapshot() {
    return new Snapshot(ACCOUNTS.sum(), STORAGE_SLOTS.sum(), CODE.sum());
  }

  /** Resets the running totals. Test support only. */
  static void reset() {
    ACCOUNTS.reset();
    STORAGE_SLOTS.reset();
    CODE.reset();
    EXECUTION_READER.remove();
  }

  /**
   * A point-in-time reading of the counters.
   *
   * @param accounts account reads that crossed the storage boundary
   * @param storageSlots storage slot reads that crossed the storage boundary
   * @param code code reads that crossed the storage boundary
   */
  public record Snapshot(long accounts, long storageSlots, long code) {

    /**
     * Reads made since an earlier snapshot.
     *
     * @param earlier the snapshot to subtract
     * @return the delta
     */
    public Snapshot since(final Snapshot earlier) {
      return new Snapshot(
          accounts - earlier.accounts, storageSlots - earlier.storageSlots, code - earlier.code);
    }
  }
}
