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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardSyncStep {

  private static final Logger LOG = LoggerFactory.getLogger(ForwardSyncStep.class);

  private final BackwardSyncContext context;
  private final BackwardChain backwardChain;
  private final BackwardSyncBalImporter balImporter;

  public ForwardSyncStep(final BackwardSyncContext context, final BackwardChain backwardChain) {
    this(context, backwardChain, new BackwardSyncBalImporter(context));
  }

  @VisibleForTesting
  ForwardSyncStep(
      final BackwardSyncContext context,
      final BackwardChain backwardChain,
      final BackwardSyncBalImporter balImporter) {
    this.context = context;
    this.backwardChain = backwardChain;
    this.balImporter = balImporter;
  }

  public CompletableFuture<Void> executeAsync() {
    return CompletableFuture.supplyAsync(
            () -> backwardChain.getFirstNAncestorHeaders(context.getBatchSize()))
        .thenCompose(this::possibleRequestBodies);
  }

  @VisibleForTesting
  public CompletableFuture<Void> possibleRequestBodies(final List<BlockHeader> blockHeaders) {
    if (blockHeaders.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.atDebug()
          .setMessage("Requesting {} blocks {}->{} ({})")
          .addArgument(blockHeaders::size)
          .addArgument(() -> blockHeaders.getFirst().getNumber())
          .addArgument(() -> blockHeaders.getLast().getNumber())
          .addArgument(() -> blockHeaders.getFirst().getHash().getBytes().toHexString())
          .log();
      final CompletableFuture<Map<Hash, BlockAccessList>> firstWindowBals =
          balImporter.prefetchFirstWindow(blockHeaders);
      return requestBodies(blockHeaders)
          .handle(
              (blocks, throwable) -> {
                if (throwable != null) {
                  firstWindowBals.cancel(false);
                  context.halveBatchSize();
                  LOG.atDebug()
                      .setMessage(
                          "Getting {} blocks from peers failed with reason {}, reducing batch size to {}")
                      .addArgument(blockHeaders::size)
                      .addArgument(throwable::getMessage)
                      .addArgument(context::getBatchSize)
                      .log();
                  return null;
                }
                // a block that cannot be saved is not a failed download, retrying it right away
                // repeats the failure, so the sync session decides whether and when to retry
                balImporter.importBlocks(blocks, firstWindowBals);
                return null;
              });
    }
  }

  @VisibleForTesting
  protected CompletableFuture<List<Block>> requestBodies(final List<BlockHeader> blockHeaders) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetBodiesFromPeerTask task =
                  new GetBodiesFromPeerTask(
                      blockHeaders,
                      context.getProtocolSchedule(),
                      context.getEthContext().getEthPeers().peerCount());
              PeerTaskExecutorResult<List<Block>> taskResult =
                  context.getEthContext().getPeerTaskExecutor().execute(task);
              if (taskResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
                  && taskResult.result().isPresent()) {
                return CompletableFuture.completedFuture(taskResult.result().get());
              } else {
                return CompletableFuture.failedFuture(
                    new RuntimeException(taskResult.responseCode().toString()));
              }
            })
        .thenApply(
            blocks -> {
              LOG.debug("Got {} blocks from peers", blocks.size());
              blocks.sort(Comparator.comparing(block -> block.getHeader().getNumber()));
              return blocks;
            });
  }
}
