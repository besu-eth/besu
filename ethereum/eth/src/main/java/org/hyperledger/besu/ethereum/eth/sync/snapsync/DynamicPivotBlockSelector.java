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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotUpdateListener;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the pivot block for snap sync dynamically. Asks the underlying {@link PivotSyncActions}
 * for a fresh pivot at most once per configured interval; if the result differs from the current
 * pivot it downloads the new header and switches.
 */
public class DynamicPivotBlockSelector {

  public static final BiConsumer<BlockHeader, Boolean> doNothingOnPivotChange = (___, __) -> {};

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPivotBlockSelector.class);
  private static final Duration CHECK_INTERVAL = Duration.ofMinutes(1);

  private final AtomicBoolean isTimeToCheckAgain = new AtomicBoolean(true);

  private final EthContext ethContext;
  private final PivotSyncActions syncActions;
  private final SnapSyncProcessState syncState;
  private final PivotUpdateListener pivotUpdateListener;

  private Optional<BlockHeader> lastPivotBlockFound = Optional.empty();

  public DynamicPivotBlockSelector(
      final EthContext ethContext,
      final PivotSyncActions fastSyncActions,
      final SnapSyncProcessState fastSyncState,
      final PivotUpdateListener pivotUpdateListener) {
    this.ethContext = ethContext;
    this.syncActions = fastSyncActions;
    this.syncState = fastSyncState;
    this.pivotUpdateListener = pivotUpdateListener;
  }

  public void check(final BiConsumer<BlockHeader, Boolean> onNewPivotBlock) {
    if (!isTimeToCheckAgain.compareAndSet(true, false)) {
      return;
    }

    boolean cycleSucceeded = false;
    try {
      CompletableFuture.completedFuture(PivotSyncState.EMPTY_SYNC_STATE)
          .thenCompose(syncActions::selectPivotBlock)
          .thenCompose(
              fss -> {
                if (isSamePivotBlock(fss)) {
                  LOG.atDebug()
                      .setMessage("New pivot {} equals current pivot, nothing to do")
                      .addArgument(fss::getPivotBlockHash)
                      .log();
                  return CompletableFuture.completedFuture(null);
                }
                return downloadNewPivotBlock(fss);
              })
          .get();
      cycleSucceeded = true;
    } catch (Exception e) {
      LOG.debug("Exception while searching for new pivot", e);
    }

    switchToNewPivotBlock(onNewPivotBlock);
    scheduleNextCheck(cycleSucceeded);
  }

  private CompletableFuture<Void> downloadNewPivotBlock(final PivotSyncState fss) {
    return syncActions
        .downloadPivotBlockHeader(fss)
        .thenAccept(
            fssWithHeader -> {
              lastPivotBlockFound = fssWithHeader.getPivotBlockHeader();
              LOG.atDebug()
                  .setMessage("Found new pivot block {}")
                  .addArgument(this::logLastPivotBlockFound)
                  .log();
            })
        .orTimeout(20, TimeUnit.SECONDS);
  }

  private boolean isSamePivotBlock(final PivotSyncState fss) {
    final Optional<BlockHeader> currentPivot = syncState.getPivotBlockHeader();
    if (currentPivot.isEmpty()) {
      return false;
    }
    if (fss.hasPivotBlockHash()) {
      return currentPivot.get().getHash().equals(fss.getPivotBlockHash().get());
    }
    return fss.getPivotBlockNumber().isPresent()
        && fss.getPivotBlockNumber().getAsLong() == currentPivot.get().getNumber();
  }

  private void scheduleNextCheck(final boolean delayNextCheck) {
    if (delayNextCheck) {
      ethContext
          .getScheduler()
          .scheduleFutureTask(
              () -> {
                LOG.debug("Is time to check the pivot again");
                isTimeToCheckAgain.set(true);
              },
              CHECK_INTERVAL);
    } else {
      isTimeToCheckAgain.set(true);
    }
  }

  public void switchToNewPivotBlock(final BiConsumer<BlockHeader, Boolean> onSwitchDone) {
    lastPivotBlockFound.ifPresentOrElse(
        blockHeader -> {
          LOG.atDebug()
              .setMessage("Setting new pivot block {} with state root {}")
              .addArgument(blockHeader::toLogString)
              .addArgument(blockHeader.getStateRoot())
              .log();
          syncState.setCurrentHeader(blockHeader);
          if (pivotUpdateListener != null) {
            pivotUpdateListener.onPivotUpdated(blockHeader);
            LOG.trace("Notified chain downloader of pivot update: {}", blockHeader.getNumber());
          }
          lastPivotBlockFound = Optional.empty();
          onSwitchDone.accept(blockHeader, true);
        },
        () -> onSwitchDone.accept(syncState.getPivotBlockHeader().orElseThrow(), false));
  }

  public boolean isBlockchainBehind() {
    return syncActions.isBlockchainBehind(syncState.getPivotBlockNumber().orElse(0L));
  }

  private String logLastPivotBlockFound() {
    return lastPivotBlockFound.map(BlockHeader::toLogString).orElse("empty");
  }
}
