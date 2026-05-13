/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.util.log.LogUtil;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives the backward header download pipeline by combining the source-side responsibility of
 * emitting descending block numbers with the import-side responsibility of validating and storing
 * the resulting header batches.
 *
 * <p>This consolidation co-locates state that is intrinsically shared between the two faces of the
 * pipeline (e.g. the stop block, the currently-expected child header) so that anchor-reorg recovery
 * logic (Task C1) can be added with minimal cross-class plumbing.
 */
public class BackwardHeaderDriver implements Iterator<Long>, Consumer<List<BlockHeader>> {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardHeaderDriver.class);
  private static final int LOG_DELAY_SECONDS = 30;
  private static final int RECOVERY_WARN_EVERY_N_BATCHES = 3;

  // Source-side state
  private final AtomicLong currentBlock;
  private final int batchSize;
  private volatile long stopBlock;

  // Import-side state
  private final MutableBlockchain blockchainStorage;
  private final long lowestHeaderToImport;
  private final Hash anchorHash;
  private final long totalHeaders;
  private final AtomicBoolean isTimeToLog = new AtomicBoolean(true);
  private final boolean previousPivotWasSafe;
  private final Supplier<String> finalizationStatusSupplier;
  private volatile BlockHeader currentChildHeader;
  private volatile BlockHeader lowestImportedHeader;

  // Anchor-reorg recovery coordination (Task C1)
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition decided = lock.newCondition();
  // Guarded by lock for writes; read on the source thread via the volatile qualifier.
  private volatile boolean done = false;
  // Written under lock; volatile so the recovery / milestone / success log lines can read it
  // without acquiring the lock.
  private volatile int extraBatchesRequested = 0;
  // Written under lock, read without lock by getMatchedAncestor().
  private volatile BlockHeader matchedAncestor;
  private volatile boolean recoveryMode = false;

  /**
   * Creates a new BackwardHeaderDriver. Stores the pivot header synchronously as the first imported
   * header (matching the prior {@code ImportHeadersStep} behavior).
   *
   * @param batchSize the number of blocks per batch (also the stride of the descending iterator)
   * @param anchorHeader the anchor (checkpoint) header; the lowest header to import is {@code
   *     anchorHeader.getNumber() + 1}, and its parent hash must equal the anchor's hash
   * @param pivotHeader the pivot header at the top of the range to import
   * @param blockchain the blockchain to which headers will be stored
   * @param previousPivotWasSafe whether the previous pivot selection used a safe/finalized source;
   *     surfaced via {@link #previousPivotWasSafe()} for downstream recovery logic (Task C1)
   * @param finalizationStatusSupplier supplies the "CL finalization status" log triage tag emitted
   *     alongside the recovery-start, milestone, and recovery-success log lines (Task D1)
   */
  public BackwardHeaderDriver(
      final int batchSize,
      final BlockHeader anchorHeader,
      final BlockHeader pivotHeader,
      final MutableBlockchain blockchain,
      final boolean previousPivotWasSafe,
      final Supplier<String> finalizationStatusSupplier) {
    this.batchSize = batchSize;
    this.blockchainStorage = blockchain;
    this.previousPivotWasSafe = previousPivotWasSafe;
    this.finalizationStatusSupplier = finalizationStatusSupplier;

    final long anchorNumber = anchorHeader.getNumber();
    final long pivotNumber = pivotHeader.getNumber();

    this.lowestHeaderToImport = anchorNumber + 1;
    this.anchorHash = anchorHeader.getHash();
    this.totalHeaders = pivotNumber - anchorNumber;

    // Source-side: iterator emits pivot.number - 1, then strides down by batchSize.
    this.currentBlock = new AtomicLong(pivotNumber - 1);
    this.stopBlock = lowestHeaderToImport;

    // Import-side: pivot becomes the initial "child" and the initial lowest imported.
    this.currentChildHeader = pivotHeader;
    this.lowestImportedHeader = pivotHeader;

    LOG.debug(
        "BackwardHeaderDriver: pivot={}, anchor={}, batchSize={}",
        pivotNumber,
        anchorNumber,
        batchSize);

    // Store the pivot block header as the first imported header.
    this.blockchainStorage.storeBlockHeaders(List.of(pivotHeader));
  }

  @Override
  public boolean hasNext() {
    // Fast path: there is more above the boundary, no need to coordinate.
    // `done` is checked first so a completed driver short-circuits even if the source thread has
    // not yet decremented currentBlock past stopBlock.
    if (!done && currentBlock.get() >= stopBlock) {
      return true;
    }
    lock.lock();
    try {
      while (!done && currentBlock.get() < stopBlock) {
        decided.await();
      }
      return !done;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Long next() {
    final long block = currentBlock.getAndUpdate(current -> current - batchSize);

    if (block >= stopBlock) {
      return block;
    }
    LOG.debug("BackwardHeaderDriver exhausted at block {} (stopBlock={})", block, stopBlock);
    throw new NoSuchElementException("BackwardHeaderDriver exhausted at block " + block);
  }

  @Override
  public void accept(final List<BlockHeader> blockHeaders) {
    if (!blockHeaders.getFirst().getHash().equals(currentChildHeader.getParentHash())) {
      final String message =
          "Received invalid header list: expected hash "
              + currentChildHeader.getParentHash()
              + " for block number "
              + (currentChildHeader.getNumber() - 1)
              + " ,but got "
              + blockHeaders.getFirst().getHash()
              + " from block with number "
              + blockHeaders.getFirst().getNumber();
      LOG.warn(message);
      throw new IllegalStateException(message);
    }

    currentChildHeader = blockHeaders.getLast();

    final boolean atBoundary =
        currentChildHeader.getNumber() == lowestHeaderToImport || recoveryMode;

    if (atBoundary) {
      final Hash parentHash = currentChildHeader.getParentHash();
      if (parentHash.equals(anchorHash)) {
        // Happy-path match: parent links to the original anchor. No recovery occurred. Do not
        // populate matchedAncestor — the existing blockDownloadAnchor is already correct.
        blockchainStorage.storeBlockHeaders(blockHeaders);
        lowestImportedHeader = blockHeaders.getLast();
        lock.lock();
        try {
          done = true;
          decided.signalAll();
        } finally {
          lock.unlock();
        }
        return;
      }
      if (blockchainStorage.getBlockHeader(parentHash).isPresent()) {
        // Recovery match: parent is a header stored canonically from a prior cycle. Populate
        // matchedAncestor so downstream code can rewrite the Stage 2 anchor.
        blockchainStorage.storeBlockHeaders(blockHeaders);
        lowestImportedHeader = blockHeaders.getLast();
        final BlockHeader ancestor;
        lock.lock();
        try {
          matchedAncestor = blockchainStorage.getBlockHeader(parentHash).orElseThrow();
          ancestor = matchedAncestor;
          done = true;
          decided.signalAll();
        } finally {
          lock.unlock();
        }
        emitRecoverySuccessLog(ancestor);
        return;
      }

      // No match: store the canonical batch and extend the walk by one batch.
      blockchainStorage.storeBlockHeaders(blockHeaders);
      lowestImportedHeader = blockHeaders.getLast();
      // First-time entry into recovery: emit the recovery-start log at appropriate severity.
      // This MUST fire before extendWalkByOneBatch() sets recoveryMode to true so the log
      // fires exactly once on first entry.
      if (!recoveryMode) {
        emitRecoveryStartLog();
      }
      extendWalkByOneBatch();
      // Milestone log: every N-th extra batch, emit a WARN. extraBatchesRequested is volatile
      // and was just bumped inside extendWalkByOneBatch().
      final int extras = extraBatchesRequested;
      if (extras > 0 && extras % RECOVERY_WARN_EVERY_N_BATCHES == 0) {
        emitRecoveryMilestoneLog(extras);
      }
      return;
    }

    // Not at the boundary: normal mid-walk store.
    blockchainStorage.storeBlockHeaders(blockHeaders);
    lowestImportedHeader = blockHeaders.getLast();

    if (isTimeToLog.get()) {
      final long downloadedHeaders =
          totalHeaders - (blockHeaders.getFirst().getNumber() - lowestHeaderToImport);
      final double headersPercent = (double) (downloadedHeaders) / totalHeaders * 100;
      LogUtil.throttledLog(
          LOG::info,
          String.format("Header import progress %.2f%%", headersPercent),
          isTimeToLog,
          LOG_DELAY_SECONDS);
    }
  }

  /**
   * Lowers {@link #stopBlock} by one batch, enters recovery mode, and signals waiters. The
   * read-modify-write of the volatile {@code stopBlock} is performed while holding {@link #lock};
   * the volatile qualifier only guarantees that the unsynchronized read of {@code stopBlock} on the
   * source thread (in {@link #hasNext()}) observes the latest value.
   */
  @SuppressWarnings("NonAtomicVolatileUpdate")
  private void extendWalkByOneBatch() {
    lock.lock();
    try {
      recoveryMode = true;
      extraBatchesRequested++;
      stopBlock -= batchSize;
      decided.signalAll();
      LOG.debug(
          "BackwardHeaderDriver: extending walk by one batch (extraBatches={}, stopBlock={})",
          extraBatchesRequested,
          stopBlock);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Emits the one-time recovery-start log. Severity is ERROR if the previous pivot was safe (the
   * rare CL-level event we want to surface) and WARN otherwise (head-fallback recovery is more
   * routine).
   */
  private void emitRecoveryStartLog() {
    final String msg =
        String.format(
            "Anchor mismatch at #%d: previous pivot was %s. Entering recovery. previousAnchor=%s, "
                + "rejectedParentFromBatch=%s. CL finalization status: %s",
            currentChildHeader.getNumber(),
            previousPivotWasSafe ? "safe block" : "head-fallback",
            anchorHash,
            currentChildHeader.getParentHash(),
            finalizationStatusSupplier.get());
    if (previousPivotWasSafe) {
      LOG.error(msg);
    } else {
      LOG.warn(msg);
    }
  }

  /** Emits the periodic recovery-progress WARN every {@link #RECOVERY_WARN_EVERY_N_BATCHES}. */
  private void emitRecoveryMilestoneLog(final int extras) {
    final int extraHeaders = extras * batchSize;
    LOG.warn(
        "Anchor recovery still walking after {} extra batches (~{} headers below previous anchor). "
            + "currentLowestHeader=#{} ({}). CL finalization status: {}",
        extras,
        extraHeaders,
        currentChildHeader.getNumber(),
        currentChildHeader.getHash(),
        finalizationStatusSupplier.get());
  }

  /**
   * Emits the recovery-success log. Severity is ERROR if the previous pivot was safe (rare CL-level
   * event) and INFO otherwise. Only called on the recovery-match path; the happy-path match (parent
   * == anchorHash) does not emit a recovery log.
   */
  private void emitRecoverySuccessLog(final BlockHeader ancestor) {
    final long delta = (lowestHeaderToImport - 1) - ancestor.getNumber();
    final String successMsg =
        String.format(
            "Anchor recovery succeeded after %d extra batch(es). previousAnchor=%s (was-safe=%s), "
                + "matchedAncestor=%s (#%d), depthBelowPreviousAnchor=%d. "
                + "CL finalization status: %s",
            extraBatchesRequested,
            anchorHash,
            previousPivotWasSafe,
            ancestor.getHash(),
            ancestor.getNumber(),
            delta,
            finalizationStatusSupplier.get());
    if (previousPivotWasSafe) {
      LOG.error(successMsg);
    } else {
      LOG.info(successMsg);
    }
  }

  /**
   * Returns the lowest header that has been imported so far (i.e. the header with the smallest
   * block number that has been stored).
   *
   * @return the lowest imported header
   */
  public BlockHeader getLowestImportedHeader() {
    return lowestImportedHeader;
  }

  /**
   * Returns the canonical ancestor found by recovery, if recovery fired. Empty on the happy path
   * (parent links to the original anchor as expected) — in that case the existing {@code
   * ChainSyncState.blockDownloadAnchor} is already correct and no recovery-driven update is needed.
   * Presence of a value is therefore equivalent to "recovery fired and succeeded."
   *
   * @return the matched ancestor header, or empty if recovery did not fire
   */
  public Optional<BlockHeader> getMatchedAncestor() {
    return Optional.ofNullable(matchedAncestor);
  }

  /**
   * Returns whether the previous pivot selection used a safe/finalized source.
   *
   * @return {@code true} if the previous pivot was selected from a safe source
   */
  public boolean previousPivotWasSafe() {
    return previousPivotWasSafe;
  }
}
