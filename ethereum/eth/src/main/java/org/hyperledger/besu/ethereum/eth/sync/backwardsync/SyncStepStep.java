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
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.MaxRetriesReachedException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;

public class SyncStepStep {
  private static final Logger LOG = getLogger(SyncStepStep.class);

  private final BackwardSyncContext context;
  private final long recoveryGeneration;
  private final BackwardChain backwardChain;

  public SyncStepStep(final BackwardSyncContext context, final BackwardChain backwardChain) {
    this.context = context;
    this.recoveryGeneration = context.recoveryGeneration();
    this.backwardChain = backwardChain;
  }

  public CompletableFuture<Block> executeAsync(final Hash hash) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              Block block;
              try {
                block = requestBlock(hash);
              } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(
                    new CompletionException(new MaxRetriesReachedException()));
              }
              saveBlock(block);
              return CompletableFuture.completedFuture(block);
            });
  }

  private Block requestBlock(final Hash targetHash) {
    LOG.debug("Fetching block by hash {} from peers", targetHash);
    GetHeadersFromPeerTask headersFromPeerTask =
        new GetHeadersFromPeerTask(
            targetHash,
            1,
            0,
            Direction.FORWARD,
            Math.max(1, context.getEthContext().getEthPeers().peerCount()),
            context.getProtocolSchedule());
    PeerTaskExecutorResult<List<BlockHeader>> headerExecutorResult =
        context.getEthContext().getPeerTaskExecutor().execute(headersFromPeerTask);
    if (headerExecutorResult.result().isEmpty()
        || headerExecutorResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
      throw new RuntimeException(new InvalidPeerTaskResponseException());
    } else {
      GetBodiesFromPeerTask bodiesTask =
          new GetBodiesFromPeerTask(
              headerExecutorResult.result().get(), context.getProtocolSchedule());
      PeerTaskExecutorResult<List<Block>> blockExecutorResult =
          context.getEthContext().getPeerTaskExecutor().execute(bodiesTask);
      if (blockExecutorResult.result().isEmpty()
          || blockExecutorResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
        throw new RuntimeException(new InvalidPeerTaskResponseException());
      }
      return blockExecutorResult.result().get().getFirst();
    }
  }

  private Block saveBlock(final Block block) {
    context.ensureCurrentGeneration(recoveryGeneration);
    LOG.atDebug().setMessage("Appending fetched block {}").addArgument(block::toLogString).log();
    backwardChain.appendTrustedBlock(block);
    return block;
  }
}
