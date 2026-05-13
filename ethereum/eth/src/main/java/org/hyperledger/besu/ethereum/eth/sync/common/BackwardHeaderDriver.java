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
  private volatile BlockHeader currentChildHeader;
  private volatile BlockHeader lowestImportedHeader;

  // Anchor-reorg recovery coordination (Task C1)
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition decided = lock.newCondition();
  // Guarded by lock for writes; read on the source thread via the volatile qualifier.
  private volatile boolean done = false;
  // Guarded by lock.
  private int extraBatchesRequested = 0;
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
   */
  public BackwardHeaderDriver(
      final int batchSize,
      final BlockHeader anchorHeader,
      final BlockHeader pivotHeader,
      final MutableBlockchain blockchain,
      final boolean previousPivotWasSafe) {
    this.batchSize = batchSize;
    this.blockchainStorage = blockchain;
    this.previousPivotWasSafe = previousPivotWasSafe;

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
      if (matchesStoredCanonicalAncestor(parentHash)) {
        // Match: store this batch and signal done.
        blockchainStorage.storeBlockHeaders(blockHeaders);
        lowestImportedHeader = blockHeaders.getLast();
        lock.lock();
        try {
          matchedAncestor = blockchainStorage.getBlockHeader(parentHash).orElse(null);
          done = true;
          decided.signalAll();
        } finally {
          lock.unlock();
        }
        return;
      }

      // No match: store the canonical batch and extend the walk by one batch.
      blockchainStorage.storeBlockHeaders(blockHeaders);
      lowestImportedHeader = blockHeaders.getLast();
      extendWalkByOneBatch();
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
   * Returns whether the supplied parent hash links to an ancestor that the driver should treat as
   * the matched canonical ancestor and stop the backward walk.
   *
   * <p>A match occurs when either:
   *
   * <ul>
   *   <li>the parent hash equals the original anchor hash supplied at construction (the
   *       pre-recovery happy path), or
   *   <li>a header with that hash is already present in {@link #blockchainStorage} (a header from a
   *       previous sync cycle that survived the reorg).
   * </ul>
   */
  private boolean matchesStoredCanonicalAncestor(final Hash parentHash) {
    if (parentHash.equals(anchorHash)) {
      return true;
    }
    return blockchainStorage.getBlockHeader(parentHash).isPresent();
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
   * Returns the matched ancestor discovered during anchor-reorg recovery, if any.
   *
   * <p>Populated when the backward walk reaches a header whose parent is either the original anchor
   * (the happy path) or an already-stored canonical ancestor discovered during recovery.
   *
   * @return the matched ancestor header, or empty if the walk has not yet identified one
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
