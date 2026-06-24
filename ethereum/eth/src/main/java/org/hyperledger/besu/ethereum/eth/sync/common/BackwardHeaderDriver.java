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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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

  private final long checkpointFloorNumber;
  private final Hash checkpointFloorHash;
  private final AtomicBoolean isTimeToLog = new AtomicBoolean(true);
  private volatile BlockHeader lowestImportedHeader;

  private volatile CompletableFuture<Boolean> decision = new CompletableFuture<>();
  private int extraBatchesRequested = 0;
  private volatile BlockHeader matchedAncestor;
  private boolean recoveryMode = false;
  private volatile boolean stopped = false;

  /**
   * Creates a new BackwardHeaderDriver. Stores the pivot header synchronously as the first imported
   * header.
   *
   * @param batchSize the number of blocks per batch
   * @param anchorHeader the anchor header where Stage 1 stops (for this cycle)
   * @param pivotHeader the pivot header at the top of the range to import
   * @param checkpointHeader the trusted body checkpoint
   * @param blockchain the blockchain to which headers will be stored
   */
  public BackwardHeaderDriver(
      final int batchSize,
      final BlockHeader anchorHeader,
      final BlockHeader pivotHeader,
      final BlockHeader checkpointHeader,
      final MutableBlockchain blockchain) {
    this.batchSize = batchSize;
    this.blockchainStorage = blockchain;

    final long anchorNumber = anchorHeader.getNumber();
    final long pivotNumber = pivotHeader.getNumber();

    this.lowestHeaderToImport = anchorNumber + 1;
    this.anchorHash = anchorHeader.getHash();
    this.totalHeaders = pivotNumber - anchorNumber;

    this.checkpointFloorNumber = checkpointHeader.getNumber();
    this.checkpointFloorHash = checkpointHeader.getHash();

    this.currentBlock = new AtomicLong(pivotNumber - 1);

    this.lowestImportedHeader = pivotHeader;

    this.blockchainStorage.storeBlockHeaders(List.of(pivotHeader));

    LOG.debug(
        "BackwardHeaderDriver: pivot={}, anchor={}, batchSize={}",
        pivotNumber,
        anchorNumber,
        batchSize);
  }

  @Override
  public boolean hasNext() {
    if (stopped) {
      return false;
    }
    if (currentBlock.get() >= lowestHeaderToImport) {
      return true;
    }
    // Below the original anchor: wait for the completer to decide extend-or-stop, see accept()
    try {
      return decision.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (final ExecutionException e) {
      // The decision is only ever completed normally; treat anything else as "stop".
      return false;
    }
  }

  @Override
  public Long next() {
    final long block = currentBlock.getAndUpdate(current -> current - batchSize);
    if (block >= lowestHeaderToImport) {
      // Phase 1 emit, still above the original anchor.
      return block;
    }
    // Recovery-mode emit. hasNext() must have observed an "extend" decision. Install a fresh
    // future so the next hasNext() blocks again until the completer decides on this batch.
    if (decision.getNow(Boolean.FALSE)) {
      decision = new CompletableFuture<>();
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

    lowestImportedHeader = blockHeaders.getLast();
    long lowestImportedHeaderNumber = lowestImportedHeader.getNumber();

    if (recoveryMode) {
      // Original anchor hash did not match. Check whether the parent hash of the lowest downloaded
      // header connects to the existing chain. Recovery must reconnect at or above the trusted
      // checkpoint.
      final long parentNumber = lowestImportedHeaderNumber - 1;
      final Optional<BlockHeader> potentialParent = blockchainStorage.getBlockHeader(parentNumber);
      if (parentNumber >= checkpointFloorNumber
          && potentialParent.isPresent()
          && potentialParent.get().getHash().equals(lowestImportedHeader.getParentHash())) {
        blockchainStorage.storeBlockHeaders(blockHeaders);
        matchedAncestor = potentialParent.get();
        stopped = true;
        decision.complete(false);
        emitRecoverySuccessLog(matchedAncestor);
        return;
      }
      if (parentNumber == 0) {
        // Genesis floor reached without a canonical match: the pivot's chain does not connect to
        // our genesis.
        stopped = true;
        decision.complete(false);
        LOG.error(
            "Backward header download reached block number 1 with hash {}, but it's parent hash {} is not matching the genesis hash {}.",
            lowestImportedHeader.getBlockHash(),
            lowestImportedHeader.getParentHash(),
            potentialParent
                .map(BlockHeader::getHash)
                .orElseThrow(() -> new RuntimeException("NO GENESIS HEADER AVAILABLE.")));
        throw new WrongChainException(
            "Backward header download reached genesis without matching parent hash.");
      }
      if (parentNumber <= checkpointFloorNumber) {
        // Recovery reached the trusted checkpoint without reconnecting to the canonical chain
        // above it: the pivot is not on the checkpoint's chain.
        stopped = true;
        decision.complete(false);
        final String message =
            "Anchor recovery reached the trusted checkpoint #"
                + checkpointFloorNumber
                + " ("
                + checkpointFloorHash
                + ") without reconnecting to the canonical chain above it. The pivot is not on "
                + "the checkpoint's chain; stopping snap sync.";
        LOG.error(message);
        throw new CheckpointReorgException(message);
      }
      blockchainStorage.storeBlockHeaders(blockHeaders);
      startOrExtendRecovery();
      return;
    }

    blockchainStorage.storeBlockHeaders(blockHeaders);

    if (lowestImportedHeaderNumber == lowestHeaderToImport) {
      if (lowestImportedHeader.getParentHash().equals(anchorHash)) {
        LOG.info("Header import progress 100.00%");
        stopped = true;
        decision.complete(false);
      } else {
        if (lowestHeaderToImport == 1) {
          stopped = true;
          decision.complete(false);
          throw new WrongChainException(
              "Backward header download reached genesis boundary without matching parent hash.");
        }
        emitRecoveryStartLog(lowestImportedHeader);
        recoveryMode = true;
        currentBlock.set(lowestHeaderToImport - 1);
        startOrExtendRecovery();
      }
      return;
    }

    if (!recoveryMode && isTimeToLog.get()) {
      final long downloadedHeaders =
          totalHeaders - (lowestImportedHeaderNumber - lowestHeaderToImport);
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
    decision.complete(true);
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
