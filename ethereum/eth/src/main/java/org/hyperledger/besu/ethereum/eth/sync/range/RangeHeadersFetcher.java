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
package org.hyperledger.besu.ethereum.eth.sync.range;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask.Direction;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeHeadersFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(RangeHeadersFetcher.class);

  private final SynchronizerConfiguration syncConfig;
  private final ProtocolSchedule protocolSchedule;
  private final EthContext ethContext;
  // The range we're aiming to reach at the end of this sync.
  private final PivotSyncState fastSyncState;

  public RangeHeadersFetcher(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext) {
    this(syncConfig, protocolSchedule, ethContext, new PivotSyncState());
  }

  public RangeHeadersFetcher(
      final SynchronizerConfiguration syncConfig,
      final ProtocolSchedule protocolSchedule,
      final EthContext ethContext,
      final PivotSyncState fastSyncState) {
    this.syncConfig = syncConfig;
    this.protocolSchedule = protocolSchedule;
    this.ethContext = ethContext;
    this.fastSyncState = fastSyncState;
  }

  public CompletableFuture<List<BlockHeader>> getNextRangeHeaders(
      final EthPeer peer, final BlockHeader previousRangeHeader) {
    LOG.atTrace()
        .setMessage("Requesting next range headers from peer {}")
        .addArgument(peer.getLoggableId())
        .log();
    final int skip = syncConfig.getDownloaderChainSegmentSize() - 1;
    final int maximumHeaderRequestSize = syncConfig.getDownloaderHeaderRequestSize();
    final long previousRangeNumber = previousRangeHeader.getNumber();

    final int additionalHeaderCount;
    final Optional<BlockHeader> finalRangeHeader = fastSyncState.getPivotBlockHeader();
    if (finalRangeHeader.isPresent()) {
      final BlockHeader targetHeader = finalRangeHeader.get();
      final long blocksUntilTarget = targetHeader.getNumber() - previousRangeNumber;
      if (blocksUntilTarget <= 0) {
        LOG.atTrace()
            .setMessage("Requesting next range headers: no blocks until target: {}")
            .addArgument(blocksUntilTarget)
            .log();
        return completedFuture(emptyList());
      }
      final long maxHeadersToRequest = blocksUntilTarget / (skip + 1);
      additionalHeaderCount = (int) Math.min(maxHeadersToRequest, maximumHeaderRequestSize);
      if (additionalHeaderCount == 0) {
        LOG.atTrace()
            .setMessage(
                "Requesting next range headers: additional header count is 0, blocks until target: {}")
            .addArgument(blocksUntilTarget)
            .log();
        return completedFuture(singletonList(targetHeader));
      }
    } else {
      additionalHeaderCount = maximumHeaderRequestSize;
    }

    return requestHeaders(peer, previousRangeHeader, additionalHeaderCount, skip);
  }

  private CompletableFuture<List<BlockHeader>> requestHeaders(
      final EthPeer peer,
      final BlockHeader referenceHeader,
      final int headerCount,
      final int skip) {
    LOG.atTrace()
        .setMessage("Requesting {} range headers, starting from {}, {} blocks apart")
        .addArgument(headerCount)
        .addArgument(referenceHeader.getNumber())
        .addArgument(skip)
        .log();
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      referenceHeader.getHash(),
                      referenceHeader.getNumber(),
                      // + 1 because lastHeader will be returned as well.
                      headerCount + 1,
                      skip,
                      Direction.FORWARD,
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  ethContext.getPeerTaskExecutor().executeAgainstPeer(task, peer);
              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                LOG.warn(
                    "Unsuccessfully used peer task system to fetch headers. Response code was {}",
                    taskResult.responseCode());
                return CompletableFuture.failedFuture(
                    new RuntimeException(
                        "Unable to retrieve headers. Response code was "
                            + taskResult.responseCode()));
              }
              return CompletableFuture.completedFuture(taskResult.result().get());
            })
        .thenApply(
            headers -> {
              if (headers.size() < headerCount) {
                LOG.atTrace()
                    .setMessage(
                        "Peer {} returned fewer headers than requested. Expected: {}, Actual: {}")
                    .addArgument(peer)
                    .addArgument(headerCount)
                    .addArgument(headers.size())
                    .log();
              }
              return stripExistingRangeHeaders(referenceHeader, headers);
            });
  }

  private List<BlockHeader> stripExistingRangeHeaders(
      final BlockHeader lastHeader, final List<BlockHeader> headers) {
    if (!headers.isEmpty() && headers.get(0).equals(lastHeader)) {
      return headers.subList(1, headers.size());
    }
    return headers;
  }

  public boolean nextRangeEndsAtChainHead(
      final EthPeer peer, final BlockHeader previousRangeHeader) {
    final Optional<BlockHeader> finalRangeHeader = fastSyncState.getPivotBlockHeader();
    if (finalRangeHeader.isPresent()) {
      return false;
    }
    final int skip = syncConfig.getDownloaderChainSegmentSize() - 1;
    final long peerEstimatedHeight = peer.chainState().getEstimatedHeight();
    final long previousRangeNumber = previousRangeHeader.getNumber();
    return previousRangeNumber + skip >= peerEstimatedHeight;
  }
}
