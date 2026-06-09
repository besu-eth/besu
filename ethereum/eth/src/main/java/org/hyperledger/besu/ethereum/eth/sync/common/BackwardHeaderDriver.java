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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

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
  private volatile BlockHeader lowestImportedHeader;

  private final BlockingQueue<Boolean> decisions = new LinkedBlockingQueue<>();
  private Boolean held;
  private int extraBatchesRequested = 0;
  private volatile BlockHeader matchedAncestor;
  private boolean recoveryMode = false;
  private volatile boolean stopped = false;

  /**
   * Creates a new BackwardHeaderDriver. Stores the pivot header synchronously as the first imported
   * header.
   *
   * @param batchSize    the number of blocks per batch
   * @param anchorHeader the anchor (checkpoint) header
   * @param pivotHeader  the pivot header at the top of the range to import
   * @param blockchain   the blockchain to which headers will be stored
   */
  public BackwardHeaderDriver(
      final int batchSize,
      final BlockHeader anchorHeader,
      final BlockHeader pivotHeader,
      final MutableBlockchain blockchain) {
    this.batchSize = batchSize;
    this.blockchainStorage = blockchain;

    final long anchorNumber = anchorHeader.getNumber();
    final long pivotNumber = pivotHeader.getNumber();

    this.lowestHeaderToImport = anchorNumber + 1;
    this.anchorHash = anchorHeader.getHash();
    this.totalHeaders = pivotNumber - anchorNumber;

    this.currentBlock = new AtomicLong(pivotNumber - 1);

    this.lowestImportedHeader = pivotHeader;

    this.blockchainStorage.storeBlockHeaders(List.of(pivotHeader));

    LOG.debug(
        "BackwardHeaderDriver: pivot={}, anchor={}, batchSize={}",
        pivotNumber,
        anchorNumber,
        batchSize);
  }

  /* In the pipeline, hasNext() and next() are only ever called by the single source thread.
   * accept() runs on the completer thread but never touches {@code held}; it communicates
   * back via {@link #decisions}.
   */

  @Override
  public boolean hasNext() {
    if (stopped) {
      return false;
    }
    if (currentBlock.get() >= lowestHeaderToImport) {
      return true;
    }
    // After downloading to the anchor we finish or go into recovery mode, see accept()
    if (held == null) {
      try {
        held = decisions.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return held;
  }

  @Override
  public Long next() {
    final long block = currentBlock.getAndUpdate(current -> current - batchSize);
    if (block >= lowestHeaderToImport) {
      // Phase 1 emit, still above the original anchor.
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
    if (!blockHeaders.getFirst().getHash().equals(lowestImportedHeader.getParentHash())) {
      final String message =
          "Received invalid header list: expected hash "
              + lowestImportedHeader.getParentHash()
              + " for block number "
              + (lowestImportedHeader.getNumber() - 1)
              + " ,but got "
              + blockHeaders.getFirst().getHash()
              + " from block with number "
              + blockHeaders.getFirst().getNumber();
      LOG.warn(message);
      throw new IllegalStateException(message);
    }

    blockchainStorage.storeBlockHeaders(blockHeaders);

    lowestImportedHeader = blockHeaders.getLast();
    long lowestImportedHeaderNumber = lowestImportedHeader.getNumber();

    if (recoveryMode) {
      // Original anchor hash did not match. Look up the block at the parent's expected height; if
      // its hash matches the parent hash the downloaded headers connect to the existing chain.
      final long parentNumber = lowestImportedHeaderNumber - 1;
      final Optional<BlockHeader> potentialParent = blockchainStorage.getBlockHeader(parentNumber);
      if (potentialParent.isPresent()
          && potentialParent.get().getHash().equals(lowestImportedHeader.getParentHash())) {
        matchedAncestor = potentialParent.get();
        stopped = true;
        decisions.add(false);
        emitRecoverySuccessLog(matchedAncestor);
        return;
      }
      if (parentNumber == 0) {
        // Genesis floor reached without a canonical match: The chain from the CL provided pivot
        // down to the genesis did not connect. Fail with a non-retryable exception.
        LOG.error(
            "Backward header download reached block number 1 with hash {}, but it's parent hash {} is not matching the genesis hash {}.",
            lowestImportedHeader.getBlockHash(),
            lowestImportedHeader.getParentHash(),
            potentialParent.orElseThrow().getBlockHash());
        stopped = true;
        decisions.add(false);
        throw new WrongChainException(
            "Backward header download reached genesis without matching parent hash.");
      }
      startOrExtendRecovery();
      return;
    }

    final long batchTop = blockHeaders.getFirst().getNumber();
    if (lowestImportedHeaderNumber == lowestHeaderToImport) {
      if (lowestImportedHeader.getParentHash().equals(anchorHash)) {
        stopped = true;
        decisions.add(false);
      } else {
        emitRecoveryStartLog(lowestImportedHeader);
        recoveryMode = true;
        startOrExtendRecovery();
      }
      return;
    }

    if (!recoveryMode && isTimeToLog.get()) {
      final long downloadedHeaders = totalHeaders - (batchTop - lowestHeaderToImport);
      final double headersPercent = (double) (downloadedHeaders) / totalHeaders * 100;
      LogUtil.throttledLog(
          LOG::info,
          String.format("Header import progress %.2f%%", headersPercent),
          isTimeToLog,
          LOG_DELAY_SECONDS);
    }
  }

  private void startOrExtendRecovery() {
    extraBatchesRequested++;
    decisions.add(true);
    LOG.debug(
        "BackwardHeaderDriver: extending walk by one batch (extraBatches={})",
        extraBatchesRequested);
    if (extraBatchesRequested % RECOVERY_WARN_EVERY_N_BATCHES == 0) {
      emitRecoveryMilestoneLog(extraBatchesRequested);
    }
  }

  private void emitRecoveryStartLog(final BlockHeader boundaryHeader) {
    LOG.warn(
        "Anchor mismatch at #{}. Entering recovery. previousAnchor={}, rejectedParentFromBatch={}.",
        boundaryHeader.getNumber(),
        anchorHash,
        boundaryHeader.getParentHash());
  }

  private void emitRecoveryMilestoneLog(final int extras) {
    final int extraHeaders = extras * batchSize;
    final long hoursOfHistory = extraHeaders / 300; // mainnet: ~300 blocks/hour at 12 s slot
    LOG.warn(
        "Anchor recovery still walking after {} extra batches (~{} hr of mainnet history below previous anchor). "
            + "currentLowestHeader=#{} ({}).",
        extras,
        hoursOfHistory,
        lowestImportedHeader.getNumber(),
        lowestImportedHeader.getHash());
  }

  private void emitRecoverySuccessLog(final BlockHeader ancestor) {
    final long delta = (lowestHeaderToImport - 1) - ancestor.getNumber();
    LOG.debug(
        "Anchor recovery succeeded after {} extra batch(es). previousAnchor={}, matchedAncestor={} (#{}), depthBelowPreviousAnchor={}.",
        extraBatchesRequested,
        anchorHash,
        ancestor.getHash(),
        ancestor.getNumber(),
        delta);
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
   * (parent links to the original anchor as expected)
   *
   * @return the matched ancestor header, or empty if recovery did not fire
   */
  public Optional<BlockHeader> getMatchedAncestor() {
    return Optional.ofNullable(matchedAncestor);
  }
}
