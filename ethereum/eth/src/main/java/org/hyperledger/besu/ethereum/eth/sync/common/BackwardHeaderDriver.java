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
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.util.log.LogUtil;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives the backward header download pipeline by combining the source-side responsibility of
 * emitting descending block numbers with the import-side responsibility of validating and storing
 * the resulting header batches.
 */
public class BackwardHeaderDriver implements Iterator<Long>, Consumer<List<BlockHeader>> {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardHeaderDriver.class);
  private static final int LOG_DELAY_SECONDS = 30;
  private static final int RECOVERY_WARN_EVERY_N_BATCHES = 3;

  // Source-side state
  private final AtomicLong currentBlock;
  private final int batchSize;

  // Import-side state
  private final MutableBlockchain blockchainStorage;
  private final long lowestHeaderToImport;
  private final Hash anchorHash;
  private final long totalHeaders;
  private final AtomicBoolean isTimeToLog = new AtomicBoolean(true);
  private final boolean previousPivotWasSafe;
  private final Supplier<String> finalizationStatusSupplier;
  private final LabelledMetric<Counter> recoveryEventCounter;
  private final LongConsumer recoveryDepthSetter;
  private volatile BlockHeader currentChildHeader;
  private volatile BlockHeader lowestImportedHeader;

  // Anchor-reorg recovery coordination. The import side puts one decision onto the queue per
  // boundary batch: {@code true} = walk one more batch (EXTEND); {@code false} = finished
  // (STOP). The source side blocks on {@code decisions.take()} in hasNext() once it reaches
  // the original anchor and caches the result in {@code held} until next() emits the
  // corresponding batch.
  private final BlockingQueue<Boolean> decisions = new LinkedBlockingQueue<>();
  // Cached decision; accessed only on the source thread (set in hasNext, cleared in next).
  private Boolean held;
  // Number of extra batches stored below the original anchor; written by accept(), read by
  // emit*Log helpers on the same thread.
  private int extraBatchesRequested = 0;
  // Written by accept() on the recovery-match path, read by getMatchedAncestor() on the
  // downloader thread after the pipeline future completes.
  private volatile BlockHeader matchedAncestor;
  // Set on the first recovery extension and never reset (recovery is monotonic).
  private boolean recoveryMode = false;
  // Short-circuits Phase 1 in hasNext() once accept() has decided STOP. In production this is
  // redundant with currentBlock having dropped below lowestHeaderToImport (because next() runs
  // before accept() for the boundary batch); for callers that bypass the iterator and feed a
  // multi-batch range straight into accept(), this flag is what makes hasNext() return false.
  private volatile boolean stopped = false;

  /**
   * Creates a new BackwardHeaderDriver. Stores the pivot header synchronously as the first imported
   * header.
   *
   * @param batchSize the number of blocks per batch (also the stride of the descending iterator)
   * @param anchorHeader the anchor (checkpoint) header; the lowest header to import is {@code
   *     anchorHeader.getNumber() + 1}, and its parent hash must equal the anchor's hash
   * @param pivotHeader the pivot header at the top of the range to import
   * @param blockchain the blockchain to which headers will be stored
   * @param previousPivotWasSafe whether the previous pivot selection used a safe/finalized source;
   *     surfaced via {@link #previousPivotWasSafe()} for downstream recovery logic
   * @param finalizationStatusSupplier supplies the "CL finalization status" log triage tag emitted
   *     alongside the recovery-start, milestone, and recovery-success log lines
   * @param recoveryEventCounter labeled counter for anchor-mismatch recovery events (labels: {@code
   *     result} ∈ {started, succeeded}, {@code previous_pivot_trust} ∈ {safe, head_fallback})
   * @param recoveryDepthSetter setter for the "last recovery depth" gauge value; receives the
   *     number of extra batches walked below the original anchor when recovery succeeds
   */
  public BackwardHeaderDriver(
      final int batchSize,
      final BlockHeader anchorHeader,
      final BlockHeader pivotHeader,
      final MutableBlockchain blockchain,
      final boolean previousPivotWasSafe,
      final Supplier<String> finalizationStatusSupplier,
      final LabelledMetric<Counter> recoveryEventCounter,
      final LongConsumer recoveryDepthSetter) {
    this.batchSize = batchSize;
    this.blockchainStorage = blockchain;
    this.previousPivotWasSafe = previousPivotWasSafe;
    this.finalizationStatusSupplier = finalizationStatusSupplier;
    this.recoveryEventCounter = recoveryEventCounter;
    this.recoveryDepthSetter = recoveryDepthSetter;

    final long anchorNumber = anchorHeader.getNumber();
    final long pivotNumber = pivotHeader.getNumber();

    this.lowestHeaderToImport = anchorNumber + 1;
    this.anchorHash = anchorHeader.getHash();
    this.totalHeaders = pivotNumber - anchorNumber;

    this.currentBlock = new AtomicLong(pivotNumber - 1);

    this.currentChildHeader = pivotHeader;
    this.lowestImportedHeader = pivotHeader;

    LOG.debug(
        "BackwardHeaderDriver: pivot={}, anchor={}, batchSize={}",
        pivotNumber,
        anchorNumber,
        batchSize);

    this.blockchainStorage.storeBlockHeaders(List.of(pivotHeader));
  }

  @Override
  public boolean hasNext() {
    if (stopped) {
      return false;
    }
    // Phase 1: normal walk down to the original anchor — no coordination needed, the iterator
    // emits descending block numbers as long as we are still above the boundary.
    if (currentBlock.get() >= lowestHeaderToImport) {
      return true;
    }
    // Phase 2: at or past the boundary. Wait for the import side to hand us a Decision via the
    // queue (EXTEND = walk one more batch; STOP = finished). Cache the decision in `held` so
    // this call stays idempotent; next() consumes it.
    if (held == null) {
      try {
        held = decisions.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    // held is non-null here (the if-block above guarantees this); auto-unbox to boolean.
    return held;
  }

  @Override
  public Long next() {
    final long block = currentBlock.getAndUpdate(current -> current - batchSize);
    if (block >= lowestHeaderToImport) {
      // Phase 1 emit, still above the original boundary.
      return block;
    }
    // Phase 2 emit — must have a live extend permission cached by hasNext(). Consume it so the
    // next hasNext() blocks again waiting for the next batch's decision.
    if (held != null && held) {
      held = null;
      return block;
    }
    LOG.debug("BackwardHeaderDriver exhausted at block {}", block);
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
        stopped = true;
        decisions.add(false);
        return;
      }
      final Optional<BlockHeader> storedAncestor = blockchainStorage.getBlockHeader(parentHash);
      if (storedAncestor.isPresent()) {
        // Recovery match: parent is a header stored canonically from a prior cycle. Populate
        // matchedAncestor so downstream code can rewrite the Stage 2 anchor.
        // (Reaching block 0's parent always lands here — genesis is canonically stored — so the
        // genesis floor is implicit; no explicit clamp is needed.)
        blockchainStorage.storeBlockHeaders(blockHeaders);
        lowestImportedHeader = blockHeaders.getLast();
        matchedAncestor = storedAncestor.get();
        stopped = true;
        decisions.add(false);
        emitRecoverySuccessLog(matchedAncestor);
        return;
      }

      // No match: store the canonical batch and extend the walk by one batch.
      blockchainStorage.storeBlockHeaders(blockHeaders);
      lowestImportedHeader = blockHeaders.getLast();
      // First-time entry into recovery: emit the recovery-start log at appropriate severity
      // before flipping recoveryMode so the log fires exactly once on first entry.
      if (!recoveryMode) {
        emitRecoveryStartLog();
        recoveryMode = true;
      }
      extraBatchesRequested++;
      decisions.add(true);
      LOG.debug(
          "BackwardHeaderDriver: extending walk by one batch (extraBatches={})",
          extraBatchesRequested);
      if (extraBatchesRequested % RECOVERY_WARN_EVERY_N_BATCHES == 0) {
        emitRecoveryMilestoneLog(extraBatchesRequested);
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
    recoveryEventCounter.labels("started", trustLabel()).inc();
  }

  /** Emits the periodic recovery-progress WARN every {@link #RECOVERY_WARN_EVERY_N_BATCHES}. */
  private void emitRecoveryMilestoneLog(final int extras) {
    final int extraHeaders = extras * batchSize;
    final long hoursOfHistory = extraHeaders / 300; // mainnet: ~300 blocks/hour at 12 s slot
    LOG.warn(
        "Anchor recovery still walking after {} extra batches (~{} hr of mainnet history below previous anchor). "
            + "currentLowestHeader=#{} ({}). CL finalization status: {}",
        extras,
        hoursOfHistory,
        lowestImportedHeader.getNumber(),
        lowestImportedHeader.getHash(),
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
    recoveryEventCounter.labels("succeeded", trustLabel()).inc();
    recoveryDepthSetter.accept(extraBatchesRequested);
  }

  /**
   * Returns the metric label for the previous pivot's trust level.
   *
   * @return {@code "safe"} if the previous pivot was selected from a safe/finalized source,
   *     otherwise {@code "head_fallback"}
   */
  private String trustLabel() {
    return previousPivotWasSafe ? "safe" : "head_fallback";
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
