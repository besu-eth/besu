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
    return currentBlock.get() >= stopBlock;
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

    if (currentChildHeader.getNumber() == lowestHeaderToImport) {
      if (!currentChildHeader.getParentHash().equals(anchorHash)) {
        // B1 preserves the existing throw; Task C1 will replace this with recovery logic.
        throw new IllegalStateException(
            "The lower header parent hash does not match the checkpoint hash");
      }
    }

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
   * <p>Always {@link Optional#empty()} in Task B1; populated by recovery logic introduced in Task
   * C1.
   *
   * @return the matched ancestor header, or empty if recovery has not produced one
   */
  public Optional<BlockHeader> getMatchedAncestor() {
    return Optional.empty();
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
