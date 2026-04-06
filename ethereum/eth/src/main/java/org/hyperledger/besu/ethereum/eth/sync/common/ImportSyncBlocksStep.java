/*
 * Copyright contributors to Hyperledger Besu.
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

import static org.hyperledger.besu.util.log.LogUtil.throttledLog;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportSyncBlocksStep implements Consumer<List<SyncBlockWithReceipts>> {
  private static final Logger LOG = LoggerFactory.getLogger(ImportSyncBlocksStep.class);
  private static final int PRINT_DELAY_SECONDS = 30;

  protected final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final SyncState syncState;
  private final long startBlock;
  private long accumulatedTime = 0L;
  private OptionalLong logStartBlock = OptionalLong.empty();
  private final boolean transactionIndexingEnabled;
  private final AtomicBoolean shouldLog = new AtomicBoolean(true);
  private final long pivotHeaderNumber;

  public ImportSyncBlocksStep(
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final SyncState syncState,
      final long startBlock,
      final long pivotHeaderNumber,
      final boolean transactionIndexingEnabled) {
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.syncState = syncState;
    this.startBlock = startBlock;
    this.pivotHeaderNumber = pivotHeaderNumber;
    this.transactionIndexingEnabled = transactionIndexingEnabled;
  }

  @Override
  public void accept(final List<SyncBlockWithReceipts> blocksWithReceipts) {
    final long startTime = System.nanoTime();
    protocolContext
        .getBlockchain()
        .unsafeImportSyncBodiesAndReceipts(blocksWithReceipts, transactionIndexingEnabled);
    if (logStartBlock.isEmpty()) {
      logStartBlock = OptionalLong.of(blocksWithReceipts.getFirst().getNumber());
    }
    final long lastBlock = blocksWithReceipts.getLast().getNumber();
    int peerCount = -1; // ethContext is not available in tests
    if (ethContext != null && ethContext.getEthPeers().peerCount() >= 0) {
      peerCount = ethContext.getEthPeers().peerCount();
    }
    final long endTime = System.nanoTime();
    accumulatedTime += TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);

    syncState.setSyncProgress(startBlock, lastBlock, pivotHeaderNumber);

    if (shouldLog.get()) {
      final long blocksPercent = getBlocksPercent(lastBlock, pivotHeaderNumber);
      throttledLog(
          LOG::info,
          String.format(
              "Block import progress: %s of %s (%s%%), Peer count: %s",
              lastBlock, pivotHeaderNumber, blocksPercent, peerCount),
          shouldLog,
          PRINT_DELAY_SECONDS);
      LOG.debug(
          "Completed importing chain segment {} to {} ({} blocks in {}ms), Peer count: {}",
          logStartBlock.getAsLong(),
          lastBlock,
          lastBlock - logStartBlock.getAsLong() + 1,
          accumulatedTime,
          peerCount);
      accumulatedTime = 0L;
      logStartBlock = OptionalLong.empty();
    }
  }

  @VisibleForTesting
  protected static long getBlocksPercent(final long lastBlock, final long totalBlocks) {
    if (totalBlocks == 0) {
      return 0;
    }
    return (100 * lastBlock / totalBlocks);
  }
}
