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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * The read/write sets of one block's speculative runs, by transaction index: the storage slots each
 * run read and wrote, indexed by slot. A run that read a slot at a value other than the one the
 * transactions before it now leave there has its read/write set invalidated, and can be refreshed:
 * run again on those values. The transactions block processing executes itself report their writes,
 * which take the place of what their read/write sets predicted.
 *
 * <p>Workers store read/write sets and a refresh worker indexes reports under a lock. Block
 * processing only reads the latest read/write set of a position and queues reports, so it never
 * waits for the lock.
 */
final class BlockReadWriteSets {

  /** Writers or readers indexed for one slot; past it, the slot is no longer tracked. */
  static final int MAX_INDEXED_PER_SLOT = 64;

  /** Refreshes one block may run. */
  static final int MAX_REFRESHES_PER_BLOCK = 128;

  /** A storage slot of an account. The slot key compares by its hash. */
  record Cell(Address address, StorageSlotKey slot) {}

  /** A slot a run read: the value it started from, and the parent state's value. */
  record Read(Cell cell, UInt256 value, UInt256 parent) {}

  /** A slot a run or block processing left at a value other than the one it started from. */
  record Write(Cell cell, UInt256 value) {}

  /** What a speculative run read and wrote. */
  static final class ReadWriteSet {
    final ParallelizedTransactionContext context;
    final Read[] reads;
    final Write[] writes;
    final Set<Address> cleared;
    final boolean refreshed;
    final boolean refreshable;

    @VisibleForTesting
    ReadWriteSet(
        final ParallelizedTransactionContext context,
        final Read[] reads,
        final Write[] writes,
        final Set<Address> cleared,
        final boolean refreshed,
        final boolean refreshable) {
      this.context = context;
      this.reads = reads;
      this.writes = writes;
      this.cleared = cleared;
      this.refreshed = refreshed;
      this.refreshable = refreshable;
    }

    /**
     * The read/write set of a finished run. Every slot of the run's state was loaded, so each
     * counts as read; it started from its seed, its chain predecessor's value or the parent's, in
     * that order. The mining beneficiary's slots are left out, as in the collision check.
     *
     * @param context the finished run
     * @param miningBeneficiary the mining beneficiary of the block
     * @param seeds the values a refresh started from, or null for a run that is no refresh
     * @return the read/write set
     */
    static ReadWriteSet of(
        final ParallelizedTransactionContext context,
        final Address miningBeneficiary,
        final @Nullable Map<Address, Map<StorageSlotKey, UInt256>> seeds) {
      final PathBasedWorldStateUpdateAccumulator<?> accumulator = context.transactionAccumulator();
      final PathBasedWorldStateUpdateAccumulator<?> predecessor =
          context.chainPredecessorAccumulator();
      final List<Read> reads = new ArrayList<>();
      final List<Write> writes = new ArrayList<>();

      for (final var account : accumulator.getStorageToUpdate().entrySet()) {
        final Address address = account.getKey();
        if (address.equals(miningBeneficiary)) {
          continue;
        }

        final Map<StorageSlotKey, UInt256> seeded = seeds == null ? null : seeds.get(address);
        final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> before =
            predecessor == null ? null : predecessor.getStorageToUpdate().get(address);

        for (final var slot : account.getValue().entrySet()) {
          final BonsaiValue<UInt256> value = slot.getValue();
          final UInt256 parent = zeroIfNull(value.getPrior());
          final UInt256 start;
          if (seeded != null && seeded.containsKey(slot.getKey())) {
            start = zeroIfNull(seeded.get(slot.getKey()));
          } else if (before != null && before.containsKey(slot.getKey())) {
            start = zeroIfNull(before.get(slot.getKey()).getUpdated());
          } else {
            start = parent;
          }

          final Cell cell = new Cell(address, slot.getKey());
          reads.add(new Read(cell, start, parent));

          final UInt256 end = zeroIfNull(value.getUpdated());
          if (!end.equals(start)) {
            writes.add(new Write(cell, end));
          }
        }
      }

      final Set<Address> cleared = Set.copyOf(accumulator.getStorageToClear());
      return new ReadWriteSet(
          context,
          reads.toArray(Read[]::new),
          writes.toArray(Write[]::new),
          cleared,
          seeds != null,
          // a cleared storage is not visible slot by slot, and an invalid run leaves no state
          cleared.isEmpty() && !context.transactionProcessingResult().isInvalid());
    }
  }

  /** A transaction block processing executed itself, or took a read/write set of. */
  private record Report(
      int position, @Nullable List<RawWrite> writes, @Nullable Set<Address> cleared) {}

  /** A slot write of a transaction block processing executed, by the slot's key. */
  record RawWrite(Address address, UInt256 key, UInt256 value) {}

  private static final class Slot {
    final TreeMap<Integer, UInt256> writers = new TreeMap<>();
    final TreeMap<Integer, Read> readers = new TreeMap<>();
    boolean untracked;
  }

  private final int size;
  private final Object lock = new Object();
  private final AtomicReferenceArray<ReadWriteSet> latest;
  // guarded by lock
  private final ReadWriteSet[] indexed;
  private final Write[][] executedWrites;
  private final boolean[] executed;
  private final Map<Cell, Slot> slots = new HashMap<>();
  private final Set<Address> untrackedAccounts = new HashSet<>();
  private final BitSet invalidated;
  private int refreshesRun;

  private final ConcurrentLinkedQueue<Report> reported = new ConcurrentLinkedQueue<>();
  private final Semaphore changed = new Semaphore(0);
  private volatile boolean finished;
  private volatile int blockProcessingAt = -1;

  BlockReadWriteSets(final int size) {
    this.size = size;
    this.latest = new AtomicReferenceArray<>(size);
    this.indexed = new ReadWriteSet[size];
    this.executedWrites = new Write[size][];
    this.executed = new boolean[size];
    this.invalidated = new BitSet(size);
  }

  int size() {
    return size;
  }

  /** The read/write set stored last for the position, refreshed or not. */
  @Nullable ReadWriteSet latest(final int position) {
    return position < size ? latest.get(position) : null;
  }

  /** The position block processing has reached, or -1 before it reached the first. */
  int blockProcessingAt() {
    return blockProcessingAt;
  }

  /** Records that block processing reached the position. */
  void blockProcessingReached(final int position) {
    blockProcessingAt = position;
  }

  boolean isFinished() {
    return finished;
  }

  /** Ends the block: the refresh worker stops at its next check. */
  void finish() {
    finished = true;
    changed.release();
  }

  /**
   * Stores the read/write set of a run, unless block processing already executed the transaction,
   * or with {@code onlyIfEmpty} another read/write set is stored for it.
   *
   * @return whether it was stored
   */
  boolean store(final int position, final ReadWriteSet readWriteSet, final boolean onlyIfEmpty) {
    final boolean wake;
    synchronized (lock) {
      if (executed[position] || (onlyIfEmpty && latest.get(position) != null)) {
        return false;
      }

      latest.set(position, readWriteSet);
      wake = index(position, readWriteSet);
    }

    if (wake) {
      changed.release();
    }

    return true;
  }

  /** Queues that block processing took the read/write set stored for the position. */
  void reportTaken(final int position) {
    reported.add(new Report(position, null, null));
  }

  /** Queues the writes of a transaction block processing executed itself. */
  void reportExecuted(
      final int position, final List<RawWrite> writes, final Set<Address> clearedAccounts) {
    reported.add(new Report(position, writes, clearedAccounts));
    changed.release();
  }

  /** Waits for an invalidation, a report or the end of the block, or a short while. */
  void waitForWork() throws InterruptedException {
    if (changed.tryAcquire(2, TimeUnit.MILLISECONDS)) {
      changed.drainPermits();
    }
  }

  /** Indexes what block processing reported, in the order it processed the transactions. */
  void applyReported() {
    Report report;
    while ((report = reported.poll()) != null) {
      apply(report);
    }
  }

  private void apply(final Report report) {
    final int position = report.position();
    if (position >= size) {
      return;
    }

    synchronized (lock) {
      executed[position] = true;
      invalidated.clear(position);

      if (report.writes() == null) {
        // taken: the writes are the ones its read/write set predicted, its reads no longer matter
        final ReadWriteSet readWriteSet = indexed[position];
        if (readWriteSet != null) {
          unindexReads(position, readWriteSet);
          indexed[position] = null;
          executedWrites[position] = readWriteSet.writes;
        }
        return;
      }

      if (report.cleared() != null) {
        for (final Address address : report.cleared()) {
          untrack(address);
        }
      }

      final Write[] writes = new Write[report.writes().size()];
      for (int i = 0; i < writes.length; i++) {
        final RawWrite raw = report.writes().get(i);
        writes[i] = new Write(new Cell(raw.address(), new StorageSlotKey(raw.key())), raw.value());
      }

      replace(position, writes, null);
    }
  }

  /**
   * Takes the first invalidated position after {@code after}, dropping the ones up to it, while the
   * block has refreshes left.
   *
   * @return the position, or -1 if none
   */
  int takeInvalidated(final int after) {
    synchronized (lock) {
      if (refreshesRun >= MAX_REFRESHES_PER_BLOCK) {
        return -1;
      }

      if (after >= 0) {
        invalidated.clear(0, Math.min(size, after + 1));
      }

      final int position = invalidated.nextSetBit(after + 1);
      if (position < 0) {
        return -1;
      }

      invalidated.clear(position);
      return position;
    }
  }

  /**
   * The seeds of a refresh of the read/write set: for each slot it read, the value the read/write
   * sets and writes before it now leave there. A refresh counts against the block's refreshes once
   * it has seeds.
   *
   * @return the seeds by account, or null if no slot it read moved or one of them is not tracked
   */
  @Nullable Map<Address, Map<StorageSlotKey, UInt256>> seedsFor(
      final int position, final ReadWriteSet readWriteSet) {
    synchronized (lock) {
      final Map<Address, Map<StorageSlotKey, UInt256>> seeds = new HashMap<>();
      boolean moved = false;

      for (final Read read : readWriteSet.reads) {
        final Slot slot = slots.get(read.cell());
        if (untrackedAccounts.contains(read.cell().address()) || (slot != null && slot.untracked)) {
          return null;
        }

        final UInt256 before = slot == null ? null : valueBefore(slot, position);
        if (before != null) {
          seeds
              .computeIfAbsent(read.cell().address(), a -> new HashMap<>())
              .put(read.cell().slot(), before);
          moved |= !before.equals(read.value());
        } else {
          moved |= !read.parent().equals(read.value());
        }
      }

      if (!moved) {
        return null;
      }

      refreshesRun++;
      return seeds;
    }
  }

  // Called under the lock. Indexes the read/write set's writes and reads; marks it invalidated if
  // one of its reads no longer holds, and the later readers its writes invalidate.
  private boolean index(final int position, final ReadWriteSet readWriteSet) {
    for (final Address address : readWriteSet.cleared) {
      untrack(address);
    }

    boolean wake = replace(position, readWriteSet.writes, readWriteSet);
    boolean outdated = false;

    for (final Read read : readWriteSet.reads) {
      if (untrackedAccounts.contains(read.cell().address())) {
        continue;
      }
      final Slot slot = slots.computeIfAbsent(read.cell(), c -> new Slot());
      if (slot.untracked) {
        continue;
      }

      outdated |= !expected(slot, position, read).equals(read.value());
      if (slot.readers.size() < MAX_INDEXED_PER_SLOT) {
        slot.readers.put(position, read);
      }
    }

    if (outdated) {
      wake |= invalidate(position);
    }

    return wake;
  }

  // Called under the lock. Puts what the position now writes in place of what was indexed for it,
  // and marks the later readers of the slots either writes.
  private boolean replace(
      final int position, final Write[] writes, final @Nullable ReadWriteSet readWriteSet) {
    final ReadWriteSet previous = indexed[position];
    final Write[] previousWrites = previous != null ? previous.writes : executedWrites[position];

    if (previousWrites != null) {
      for (final Write write : previousWrites) {
        final Slot slot = slots.get(write.cell());
        if (slot != null) {
          slot.writers.remove(position);
        }
      }
    }

    if (previous != null) {
      unindexReads(position, previous);
    }

    indexed[position] = readWriteSet;
    executedWrites[position] = readWriteSet == null ? writes : null;

    for (final Write write : writes) {
      if (untrackedAccounts.contains(write.cell().address())) {
        continue;
      }
      final Slot slot = slots.computeIfAbsent(write.cell(), c -> new Slot());
      if (slot.untracked) {
        continue;
      }

      slot.writers.put(position, write.value());
      if (slot.writers.size() > MAX_INDEXED_PER_SLOT) {
        slot.untracked = true;
        slot.writers.clear();
        slot.readers.clear();
      }
    }

    boolean wake = false;
    final Set<Cell> visited = new HashSet<>();
    for (final Write write : writes) {
      if (visited.add(write.cell())) {
        wake |= invalidateReaders(write.cell(), position);
      }
    }

    if (previousWrites != null) {
      for (final Write write : previousWrites) {
        if (visited.add(write.cell())) {
          wake |= invalidateReaders(write.cell(), position);
        }
      }
    }

    return wake;
  }

  private void unindexReads(final int position, final ReadWriteSet readWriteSet) {
    for (final Read read : readWriteSet.reads) {
      final Slot slot = slots.get(read.cell());
      if (slot != null) {
        slot.readers.remove(position);
      }
    }
  }

  private void untrack(final Address address) {
    untrackedAccounts.add(address);
  }

  // Marks the later read/write sets whose runs read the slot at a value other than the one the
  // block now leaves before them.
  private boolean invalidateReaders(final Cell cell, final int position) {
    final Slot slot = slots.get(cell);
    if (slot == null || slot.untracked) {
      return false;
    }

    boolean wake = false;
    for (final var reader : slot.readers.tailMap(position, false).entrySet()) {
      final Read read = reader.getValue();
      if (!expected(slot, reader.getKey(), read).equals(read.value())) {
        wake |= invalidate(reader.getKey());
      }
    }

    return wake;
  }

  private boolean invalidate(final int position) {
    if (executed[position] || invalidated.get(position)) {
      return false;
    }

    invalidated.set(position);
    return true;
  }

  // the value the writes before the position leave, or the parent's if none writes the slot
  private static UInt256 expected(final Slot slot, final int position, final Read read) {
    final UInt256 before = valueBefore(slot, position);
    return before != null ? before : read.parent();
  }

  private static @Nullable UInt256 valueBefore(final Slot slot, final int position) {
    final Map.Entry<Integer, UInt256> writer = slot.writers.lowerEntry(position);
    return writer == null ? null : writer.getValue();
  }

  private static UInt256 zeroIfNull(final @Nullable UInt256 value) {
    return value == null ? UInt256.ZERO : value;
  }
}
