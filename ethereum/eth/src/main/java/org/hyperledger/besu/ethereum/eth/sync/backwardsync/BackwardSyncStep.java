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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackwardSyncStep {
  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncStep.class);
  private final BackwardSyncContext context;
  private final BackwardChain backwardChain;

  public BackwardSyncStep(final BackwardSyncContext context, final BackwardChain backwardChain) {
    this.context = context;
    this.backwardChain = backwardChain;
  }

  public CompletableFuture<Void> executeAsync(final BlockHeader firstHeader) {
    return CompletableFuture.supplyAsync(() -> firstHeader)
        .thenApply(this::possibleRestoreOldNodes)
        .thenCompose(this::requestHeaders)
        .thenApply(this::saveHeaders);
  }

  @VisibleForTesting
  protected Hash possibleRestoreOldNodes(final BlockHeader firstAncestor) {
    Hash lastHash = firstAncestor.getParentHash();
    Optional<BlockHeader> iterator = backwardChain.getHeader(lastHash);
    while (iterator.isPresent()) {
      backwardChain.prependAncestorsHeader(iterator.get(), true);
      lastHash = iterator.get().getParentHash();
      iterator = backwardChain.getHeader(lastHash);
    }
    return lastHash;
  }

  @VisibleForTesting
  protected CompletableFuture<List<BlockHeader>> requestHeaders(final Hash hash) {
    final Optional<BlockHeader> blockHeader =
        context.getProtocolContext().getBlockchain().getBlockHeader(hash);
    if (blockHeader.isPresent()) {
      LOG.debug(
          "Hash {} already present in local blockchain no need to request headers from peers",
          hash);
      return CompletableFuture.completedFuture(List.of(blockHeader.get()));
    }

    final int batchSize = context.getBatchSize();
    LOG.trace("Requesting headers for hash {}, with batch size {}", hash, batchSize);

    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      hash,
                      0,
                      batchSize,
                      0,
                      Direction.REVERSE,
                      context.getEthContext().getEthPeers().peerCount(),
                      context.getProtocolSchedule());
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  context.getEthContext().getPeerTaskExecutor().execute(task);
              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                throw new RuntimeException("Unable to retrieve headers");
              }
              return CompletableFuture.completedFuture(taskResult.result().get());
            })
        .thenApply(
            blockHeaders -> {
              LOG.atDebug()
                  .setMessage("Got headers {} -> {}")
                  .addArgument(blockHeaders.getFirst()::getNumber)
                  .addArgument(blockHeaders.getLast()::getNumber)
                  .log();
              return blockHeaders;
            });
  }

  @VisibleForTesting
  protected Void saveHeader(final BlockHeader blockHeader) {
    backwardChain.prependAncestorsHeader(blockHeader);
    return null;
  }

  @VisibleForTesting
  protected Void saveHeaders(final List<BlockHeader> blockHeaders) {
    for (BlockHeader blockHeader : blockHeaders) {
      saveHeader(blockHeader);
    }

    if (!blockHeaders.isEmpty()) {
      logProgress(blockHeaders.get(blockHeaders.size() - 1).getNumber());
    }
    return null;
  }

  private void logProgress(final long currLowestDownloadedHeight) {
    final long targetHeight = context.getStatus().getTargetChainHeight();
    final long initialHeight = context.getStatus().getInitialChainHeight();
    final long estimatedTotal = targetHeight - initialHeight;
    final long downloaded = targetHeight - currLowestDownloadedHeight;

    final float completedPercentage = 100.0f * downloaded / estimatedTotal;

    if (completedPercentage < 100.0f) {
      if (context.getStatus().progressLogDue()) {
        LOG.info(
            String.format(
                "Backward sync phase 1 of 2, %.2f%% completed, downloaded %d headers of at least %d. Peers: %d",
                completedPercentage,
                downloaded,
                estimatedTotal,
                context.getEthContext().getEthPeers().peerCount()));
      }
    } else {
      LOG.info(
          String.format(
              "Backward sync phase 1 of 2 completed, downloaded a total of %d headers. Peers: %d",
              downloaded, context.getEthContext().getEthPeers().peerCount()));
    }
  }
}
