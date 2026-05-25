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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.slf4j.LoggerFactory.getLogger;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.MaxRetriesReachedException;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

public class BackwardSyncAlgorithm implements BesuEvents.InitialSyncCompletionListener {
  private static final Logger LOG = getLogger(BackwardSyncAlgorithm.class);

  private final BackwardSyncContext context;
  private final FinalBlockConfirmation finalBlockConfirmation;
  private final AtomicReference<CountDownLatch> latch =
      new AtomicReference<>(new CountDownLatch(1));
  private volatile boolean finished = false;

  public BackwardSyncAlgorithm(
      final BackwardSyncContext context, final FinalBlockConfirmation finalBlockConfirmation) {
    this.context = context;
    this.finalBlockConfirmation = finalBlockConfirmation;
  }

  public CompletableFuture<Void> executeBackwardsSync(final Void unused) {
    final CompletableFuture<Void> nextStep = pickNextStep();
    if (finished) {
      return nextStep;
    }
    return nextStep.thenCompose(this::executeBackwardsSync);
  }

  public CompletableFuture<Void> pickNextStep() {
    final Optional<Hash> firstHash = context.getBackwardChain().getFirstHashToAppend();
    if (!context.isReady()) {
      return waitForReady();
    }
    if (firstHash.isPresent()) {
      return handleSyncStep(firstHash.get());
    }
    final Optional<BlockHeader> maybeFirstAncestorHeader =
        context.getBackwardChain().getFirstAncestorHeader();
    if (maybeFirstAncestorHeader.isEmpty()) {
      this.finished = true;
      LOG.info("Current backward sync session is done");
      context.getBackwardChain().clear();
      return CompletableFuture.completedFuture(null);
    }

    final MutableBlockchain blockchain = context.getProtocolContext().getBlockchain();
    final BlockHeader firstAncestorHeader = maybeFirstAncestorHeader.get();
    final BlockHeader chainHeader = blockchain.getChainHeadHeader();
    if (blockchain.contains(firstAncestorHeader.getHash())
        && firstAncestorHeader.getNumber() <= chainHeader.getNumber()) {
      return executeProcessKnownAncestors();
    }

    if (chainHeader.getNumber() > firstAncestorHeader.getNumber()) {
      LOG.atDebug()
          .setMessage("Backward reached below current chain head {} : {}")
          .addArgument(() -> blockchain.getChainHead().toLogString())
          .addArgument(firstAncestorHeader::toLogString)
          .log();
    }

    if (finalBlockConfirmation.ancestorHeaderReached(firstAncestorHeader)) {
      LOG.atDebug()
          .setMessage("Backward sync reached ancestor header with {}, starting forward sync")
          .addArgument(firstAncestorHeader::toLogString)
          .log();
      return executeForwardAsync();
    }

    return executeBackwardAsync(firstAncestorHeader);
  }

  private CompletableFuture<Void> handleSyncStep(final Hash firstHash) {
    final CompletableFuture<Void> syncStep = new CompletableFuture<>();
    executeSyncStep(firstHash)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                handleSyncStepError(error, firstHash, syncStep);
              } else {
                handleSyncStepSuccess(result, firstHash, syncStep);
              }
            });
    return syncStep;
  }

  private void handleSyncStepSuccess(
      final Block result, final Hash firstHash, final CompletableFuture<Void> syncStep) {
    if (result == null) {
      LOG.atWarn().setMessage("Unexpected null result in for hash {}").addArgument(firstHash).log();
      syncStep.completeExceptionally(new BackwardSyncException("Unexpected null result", true));
    } else {
      LOG.atDebug()
          .setMessage("Backward sync target block is {}")
          .addArgument(result::toLogString)
          .log();
      context.getBackwardChain().removeFromHashToAppend(firstHash);
      context.getStatus().updateTargetHeight(result.getHeader().getNumber());
      syncStep.complete(null);
    }
  }

  private void handleSyncStepError(
      final Throwable error, final Hash firstHash, final CompletableFuture<Void> syncStep) {
    if (error instanceof CompletionException
        && error.getCause() instanceof MaxRetriesReachedException) {
      handleEthPeerMaxRetriesException(firstHash);
      syncStep.complete(null);
    } else {
      syncStep.completeExceptionally(error);
    }
  }

  private void handleEthPeerMaxRetriesException(final Hash firstHash) {
    context.getBackwardChain().removeFromHashToAppend(firstHash);
    LOG.atWarn()
        .setMessage(
            "Unable to retrieve block {} from any peer, with {} peers available. Could be a reorged block. Waiting for the next block from the consensus client to try again.")
        .addArgument(firstHash)
        .addArgument(context.getEthContext().getEthPeers().peerCount())
        .log();
    LOG.atDebug().setMessage("Removing hash {} from hashesToAppend").addArgument(firstHash).log();
  }

  @VisibleForTesting
  public CompletableFuture<Void> executeProcessKnownAncestors() {
    return new ProcessKnownAncestorsStep(context, context.getBackwardChain()).executeAsync();
  }

  @VisibleForTesting
  public CompletableFuture<Block> executeSyncStep(final Hash hash) {
    return new SyncStepStep(context, context.getBackwardChain()).executeAsync(hash);
  }

  @VisibleForTesting
  protected CompletableFuture<Void> executeBackwardAsync(final BlockHeader firstHeader) {
    return new BackwardSyncStep(context, context.getBackwardChain()).executeAsync(firstHeader);
  }

  @VisibleForTesting
  protected CompletableFuture<Void> executeForwardAsync() {
    return new ForwardSyncStep(context, context.getBackwardChain()).executeAsync();
  }

  @VisibleForTesting
  protected CompletableFuture<Void> waitForReady() {
    final long idTTD = context.getSyncState().subscribeTTDReached(reached -> countDownIfReady());
    final long idIS = context.getSyncState().subscribeCompletionReached(this);
    return CompletableFuture.runAsync(() -> checkReadiness(idTTD, idIS));
  }

  private void checkReadiness(final long idTTD, final long idIS) {
    try {
      if (!context.isReady()) {
        LOG.debug("Waiting for preconditions...");
        final boolean await = latch.get().await(2, TimeUnit.MINUTES);
        if (await) {
          LOG.debug("Preconditions meet, ensure at least one peer is connected");
          context
              .getEthContext()
              .getEthPeers()
              .waitForPeer((peer) -> true)
              .orTimeout(5, TimeUnit.SECONDS)
              .get();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BackwardSyncException(
          "Wait for TTD preconditions interrupted (" + e.getMessage() + ")");
    } catch (ExecutionException e) {
      throw new BackwardSyncException(
          "Error while waiting for at least one connected peer (" + e.getMessage() + ")", true);
    } finally {
      context.getSyncState().unsubscribeTTDReached(idTTD);
      context.getSyncState().unsubscribeInitialConditionReached(idIS);
    }
  }

  private void countDownIfReady() {
    if (context.isReady()) {
      latch.get().countDown();
    }
  }

  @Override
  public void onInitialSyncCompleted() {
    countDownIfReady();
  }

  @Override
  public void onInitialSyncRestart() {
    latch.set(new CountDownLatch(1));
  }
}
