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

import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutable per-block aggregate of the slow-block metrics.
 *
 * <p>Almost everything here is written from the block-import thread: background transaction
 * recorders and read counters are merged in as their results are consumed, and the Block Access
 * List derived counts are set once the list is final. The handful of values produced by background
 * jobs (state-root hashing, prefetch) are {@link AtomicLong}s.
 *
 * <p>Cache hits are never counted directly: {@code hits = total logical reads - reads that crossed
 * the RocksDB boundary}. A negative result means read instrumentation drifted from the storage
 * decorator, so it is clamped to zero and logged rather than emitted.
 */
public class SlowBlockMetrics implements SlowBlockPersistTimings {

  private static final Logger LOG = LoggerFactory.getLogger(SlowBlockMetrics.class);

  private final long thresholdMs;

  // Block identity
  private long blockNumber;
  private String blockHash = "";
  private long gasUsed;
  private int txCount;

  // Timing, nanoseconds
  private long totalStartNanos;
  private long totalNanos;
  private long stateReadNanos;
  private long stateHashWaitNanos;
  private long commitNanos;

  // Total logical state reads
  private long readAccounts;
  private long readStorageSlots;
  private long readCode;
  private long readCodeBytes;

  // Reads that crossed the RocksDB boundary
  private long missAccounts;
  private long missStorageSlots;
  private long missCode;

  // Write events from the final block access list
  private long writeAccounts;
  private long writeStorageSlots;
  private long writeCode;
  private long writeCodeBytes;
  private long accountsDeleted;
  private long storageSlotsDeleted;

  // Unique touched state
  private long uniqueAccounts;
  private long uniqueStorageSlots;
  private final Set<Address> uniqueContracts = new HashSet<>();

  // EVM operation counts
  private long sload;
  private long sstore;
  private long calls;
  private long creates;

  // Besu BAL extension
  private final AtomicLong stateHashBackgroundNanos = new AtomicLong();
  private long txExecBackgroundNanos;
  private long txResultWaitNanos;
  private int replayedTxs;
  private final AtomicLong prefetchNanos = new AtomicLong();
  private final AtomicLong prefetchAccounts = new AtomicLong();
  private final AtomicLong prefetchStorageSlots = new AtomicLong();

  /**
   * Creates a per-block aggregate.
   *
   * @param thresholdMs blocks are logged when total processing time in ms is at least this value
   */
  public SlowBlockMetrics(final long thresholdMs) {
    this.thresholdMs = thresholdMs;
  }

  /**
   * Records the block identity and starts the total-time clock.
   *
   * @param blockNumber the block number
   * @param blockHash the block hash, hex encoded
   * @param gasUsed the block's gas used
   * @param txCount the number of transactions in the block
   */
  public void startBlock(
      final long blockNumber, final String blockHash, final long gasUsed, final int txCount) {
    this.blockNumber = blockNumber;
    this.blockHash = blockHash;
    this.gasUsed = gasUsed;
    this.txCount = txCount;
    this.totalStartNanos = System.nanoTime();
  }

  /** Stops the total-time clock. */
  public void endBlock() {
    endBlock(System.nanoTime() - totalStartNanos);
  }

  void endBlock(final long elapsedNanos) {
    this.totalNanos = elapsedNanos;
  }

  /**
   * Whether this block's total processing time reached the configured threshold.
   *
   * @return true when the block should be logged
   */
  public boolean meetsThreshold() {
    return totalNanos / 1_000_000L >= thresholdMs;
  }

  void addEvmCounts(
      final long sloadCount, final long sstoreCount, final long callCount, final long createCount) {
    sload += sloadCount;
    sstore += sstoreCount;
    calls += callCount;
    creates += createCount;
  }

  void addContractsExecuted(final Collection<Address> contracts) {
    uniqueContracts.addAll(contracts);
  }

  void addStateReads(
      final long accounts, final long storageSlots, final long code, final long codeBytes) {
    readAccounts += accounts;
    readStorageSlots += storageSlots;
    readCode += code;
    readCodeBytes += codeBytes;
  }

  void addStateReadNanos(final long nanos) {
    stateReadNanos += nanos;
  }

  /**
   * Merges the results of one completed execution — a background transaction, or everything the
   * block-level path ran.
   *
   * @param recorder the EVM counters for that execution
   * @param reads the logical state reads for that execution, may be null
   */
  public void mergeExecution(final SlowBlockTxRecorder recorder, final SlowBlockReadCounts reads) {
    if (recorder != null) {
      recorder.mergeInto(this);
    }
    if (reads != null) {
      reads.mergeInto(this);
    }
  }

  /**
   * Records reads that crossed the RocksDB boundary during this block, from which cache hits are
   * derived.
   *
   * @param accounts account reads
   * @param storageSlots storage slot reads
   * @param code code reads
   */
  public void setCacheMisses(final long accounts, final long storageSlots, final long code) {
    this.missAccounts = accounts;
    this.missStorageSlots = storageSlots;
    this.missCode = code;
  }

  /**
   * Records the write events and unique touched state derived from the final block access list.
   *
   * @param uniqueAccounts distinct accounts appearing in the list
   * @param uniqueStorageSlots distinct storage slots read or written
   * @param writeAccounts account change entries (balance, nonce and code changes)
   * @param writeStorageSlots storage change entries
   * @param writeCode code change entries
   * @param writeCodeBytes bytes of code written
   * @param accountsDeleted accounts whose final state is empty
   * @param storageSlotsDeleted storage slots whose final written value is zero
   */
  public void setBlockAccessListDerived(
      final long uniqueAccounts,
      final long uniqueStorageSlots,
      final long writeAccounts,
      final long writeStorageSlots,
      final long writeCode,
      final long writeCodeBytes,
      final long accountsDeleted,
      final long storageSlotsDeleted) {
    this.uniqueAccounts = uniqueAccounts;
    this.uniqueStorageSlots = uniqueStorageSlots;
    this.writeAccounts = writeAccounts;
    this.writeStorageSlots = writeStorageSlots;
    this.writeCode = writeCode;
    this.writeCodeBytes = writeCodeBytes;
    this.accountsDeleted = accountsDeleted;
    this.storageSlotsDeleted = storageSlotsDeleted;
  }

  /**
   * Records the import thread's wait for the block's state root.
   *
   * @param nanos elapsed nanoseconds
   */
  @Override
  public void setStateHashWaitNanos(final long nanos) {
    this.stateHashWaitNanos = nanos;
  }

  /**
   * Records the time spent committing the world state to storage.
   *
   * @param nanos elapsed nanoseconds
   */
  @Override
  public void setCommitNanos(final long nanos) {
    this.commitNanos = nanos;
  }

  /**
   * Records how long the background state-root computation actually took. Last write wins: the
   * parallel-to-serial fallback re-enters block processing and submits a second computation.
   *
   * @param nanos elapsed nanoseconds
   */
  public void setStateHashBackgroundNanos(final long nanos) {
    stateHashBackgroundNanos.set(nanos);
  }

  /**
   * Adds one background transaction's execution time, excluding the time it spent queued.
   *
   * @param nanos elapsed nanoseconds
   */
  public void addBackgroundExecutionNanos(final long nanos) {
    txExecBackgroundNanos += nanos;
  }

  /**
   * Adds time the import thread spent waiting for a background transaction result.
   *
   * @param nanos elapsed nanoseconds
   */
  public void addResultWaitNanos(final long nanos) {
    txResultWaitNanos += nanos;
  }

  /** Records that a transaction had to be re-executed on the import thread. */
  public void incrementReplayedTxs() {
    replayedTxs++;
  }

  /**
   * Records what the block access list prefetcher fetched, and how long it took.
   *
   * @param nanos elapsed nanoseconds
   * @param accounts accounts prefetched
   * @param storageSlots storage slots prefetched
   */
  public void setPrefetch(final long nanos, final long accounts, final long storageSlots) {
    prefetchNanos.set(nanos);
    prefetchAccounts.set(accounts);
    prefetchStorageSlots.set(storageSlots);
  }

  long blockNumber() {
    return blockNumber;
  }

  String blockHash() {
    return blockHash;
  }

  long gasUsed() {
    return gasUsed;
  }

  int txCount() {
    return txCount;
  }

  long totalNanos() {
    return totalNanos;
  }

  /**
   * Wall-clock time on the import thread outside the state-root wait and the storage commit. Phase
   * timings deliberately do not sum to the total: under BAL, execution and hashing overlap.
   */
  long executionNanos() {
    return Math.max(0L, totalNanos - stateHashWaitNanos - commitNanos);
  }

  long stateReadNanos() {
    return stateReadNanos;
  }

  long stateHashWaitNanos() {
    return stateHashWaitNanos;
  }

  long commitNanos() {
    return commitNanos;
  }

  double mgasPerSecond() {
    if (totalNanos <= 0L) {
      return 0.0;
    }
    return (gasUsed / 1_000_000.0) / (totalNanos / 1_000_000_000.0);
  }

  long readAccounts() {
    return readAccounts;
  }

  long readStorageSlots() {
    return readStorageSlots;
  }

  long readCode() {
    return readCode;
  }

  long readCodeBytes() {
    return readCodeBytes;
  }

  long missAccounts() {
    return missAccounts;
  }

  long missStorageSlots() {
    return missStorageSlots;
  }

  long missCode() {
    return missCode;
  }

  long hitAccounts() {
    return derivedHits("account", readAccounts, missAccounts);
  }

  long hitStorageSlots() {
    return derivedHits("storage", readStorageSlots, missStorageSlots);
  }

  long hitCode() {
    return derivedHits("code", readCode, missCode);
  }

  private long derivedHits(final String kind, final long totalReads, final long misses) {
    final long hits = totalReads - misses;
    if (hits < 0L) {
      LOG.debug(
          "Slow block {}: more {} reads crossed the storage boundary ({}) than were counted as"
              + " logical reads ({}); clamping derived cache hits to zero",
          blockNumber,
          kind,
          misses,
          totalReads);
      return 0L;
    }
    return hits;
  }

  long writeAccounts() {
    return writeAccounts;
  }

  long writeStorageSlots() {
    return writeStorageSlots;
  }

  long writeCode() {
    return writeCode;
  }

  long writeCodeBytes() {
    return writeCodeBytes;
  }

  long accountsDeleted() {
    return accountsDeleted;
  }

  long storageSlotsDeleted() {
    return storageSlotsDeleted;
  }

  long uniqueAccounts() {
    return uniqueAccounts;
  }

  long uniqueStorageSlots() {
    return uniqueStorageSlots;
  }

  long uniqueContracts() {
    return uniqueContracts.size();
  }

  long sload() {
    return sload;
  }

  long sstore() {
    return sstore;
  }

  long calls() {
    return calls;
  }

  long creates() {
    return creates;
  }

  long stateHashBackgroundNanos() {
    return stateHashBackgroundNanos.get();
  }

  long txExecBackgroundNanos() {
    return txExecBackgroundNanos;
  }

  long txResultWaitNanos() {
    return txResultWaitNanos;
  }

  int replayedTxs() {
    return replayedTxs;
  }

  long prefetchNanos() {
    return prefetchNanos.get();
  }

  long prefetchAccounts() {
    return prefetchAccounts.get();
  }

  long prefetchStorageSlots() {
    return prefetchStorageSlots.get();
  }
}
