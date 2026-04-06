/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task attempts to find a non-controversial pivot block by confirming the pivot block number
 * with a minimum number of peers. If a particular pivot block cannot be confirmed, the pivot block
 * number is pushed back and the confirmation process repeats. If a maximum number of retries is
 * reached, the task fails with a {@code FastSyncException} containing {@code
 * FastSyncError.PIVOT_BLOCK_HEADER_MISMATCH}.
 */
public class PivotBlockRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(PivotBlockRetriever.class);
  public static final int MAX_QUERY_RETRIES_PER_PEER = 5;
  private static final int DEFAULT_MAX_PIVOT_BLOCK_RESETS = 250;
  private static final int SUSPICIOUS_NUMBER_OF_RETRIES = 5;

  private final EthContext ethContext;
  private final ProtocolSchedule protocolSchedule;

  // The number of peers we need to query to confirm our pivot block
  private final int peersToQuery;
  // The max times to push the pivot block number back when peers can't agree on a pivot
  private final int maxPivotBlockResets;
  // How far to push back the pivot block when we retry on pivot disagreement
  private final long pivotBlockNumberResetDelta;

  // The current pivot block number, gets pushed back if peers disagree on the pivot block
  AtomicLong pivotBlockNumber;

  private final CompletableFuture<PivotSyncState> result = new CompletableFuture<>();
  private final Map<Long, PivotBlockConfirmer> confirmationTasks = new ConcurrentHashMap<>();

  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  PivotBlockRetriever(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final long pivotBlockNumber,
      final int peersToQuery,
      final long pivotBlockNumberResetDelta,
      final int maxPivotBlockResets) {
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.pivotBlockNumber = new AtomicLong(pivotBlockNumber);
    this.peersToQuery = peersToQuery;
    this.pivotBlockNumberResetDelta = pivotBlockNumberResetDelta;
    this.maxPivotBlockResets = maxPivotBlockResets;
  }

  public PivotBlockRetriever(
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final long pivotBlockNumber,
      final int peersToQuery,
      final long pivotBlockNumberResetDelta) {
    this(
        protocolSchedule,
        ethContext,
        pivotBlockNumber,
        peersToQuery,
        pivotBlockNumberResetDelta,
        DEFAULT_MAX_PIVOT_BLOCK_RESETS);
  }

  public CompletableFuture<PivotSyncState> downloadPivotBlockHeader() {
    if (isStarted.compareAndSet(false, true)) {
      LOG.info("Retrieve a pivot block that can be confirmed by at least {} peers.", peersToQuery);
      confirmBlock(pivotBlockNumber.get());
    }

    return result;
  }

  private void confirmBlock(final long blockNumber) {
    final PivotBlockConfirmer pivotBlockConfirmationTask =
        new PivotBlockConfirmer(protocolSchedule, ethContext, pivotBlockNumber.get(), peersToQuery);
    final PivotBlockConfirmer preexistingTask =
        confirmationTasks.putIfAbsent(blockNumber, pivotBlockConfirmationTask);
    if (preexistingTask != null) {
      // We already set up a task to confirm this block
      return;
    }

    pivotBlockConfirmationTask.confirmPivotBlock().whenComplete(this::handleConfirmationResult);
  }

  private void handleConfirmationResult(final PivotSyncState fastSyncState, final Throwable error) {
    if (error != null) {
      final Throwable rootCause = ExceptionUtils.rootCause(error);
      if (rootCause instanceof PivotBlockConfirmer.ContestedPivotBlockException) {
        final long blockNumber =
            ((PivotBlockConfirmer.ContestedPivotBlockException) error).getBlockNumber();
        handleContestedPivotBlock(blockNumber);
      } else {
        LOG.error("Encountered error while requesting pivot block header", error);
        result.completeExceptionally(error);
      }
      return;
    }

    result.complete(fastSyncState);
  }

  private void handleContestedPivotBlock(final long contestedBlockNumber) {
    if (pivotBlockNumber.compareAndSet(
        contestedBlockNumber, contestedBlockNumber - pivotBlockNumberResetDelta)) {
      LOG.info("Received conflicting pivot blocks for {}.", contestedBlockNumber);

      final int retryCount = confirmationTasks.size();

      if ((retryCount % SUSPICIOUS_NUMBER_OF_RETRIES) == 0) {
        LOG.warn("{} attempts have failed to find a fast sync pivot block", retryCount);
      }

      if (retryCount > maxPivotBlockResets
          || pivotBlockNumber.get() <= BlockHeader.GENESIS_BLOCK_NUMBER) {
        LOG.info("Max retries reached, cancel pivot block download.");
        // Pivot block selection has failed
        result.completeExceptionally(new SyncException(SyncError.PIVOT_BLOCK_HEADER_MISMATCH));
        return;
      } else {
        LOG.info("Move pivot block back to {} and retry.", pivotBlockNumber);
      }

      confirmBlock(pivotBlockNumber.get());
    }
  }
}
