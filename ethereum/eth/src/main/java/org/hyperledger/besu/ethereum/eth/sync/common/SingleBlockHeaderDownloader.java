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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for downloading single block headers by hash from peers. This class encapsulates the
 * dependencies required for header download operations and supports dependency injection.
 */
public class SingleBlockHeaderDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(SingleBlockHeaderDownloader.class);

  private final EthContext ethContext;
  private final ProtocolSchedule protocolSchedule;

  /**
   * Creates a new SingleBlockHeaderDownloader instance.
   *
   * @param ethContext the Ethereum context for peer communication
   * @param protocolSchedule the protocol schedule for validation
   */
  public SingleBlockHeaderDownloader(
      final EthContext ethContext, final ProtocolSchedule protocolSchedule) {
    this.ethContext = ethContext;
    this.protocolSchedule = protocolSchedule;
  }

  /**
   * Downloads a single block header by hash from peers.
   *
   * @param hash the hash of the block header to download
   * @return a CompletableFuture that completes with the downloaded BlockHeader
   */
  public CompletableFuture<BlockHeader> downloadBlockHeader(final Hash hash) {
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      hash,
                      0,
                      1,
                      0,
                      GetHeadersFromPeerTask.Direction.FORWARD,
                      ethContext.getEthPeers().peerCount(),
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  ethContext.getPeerTaskExecutor().execute(task);
              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                return CompletableFuture.failedFuture(
                    new RuntimeException("Unable to retrieve header"));
              }
              return CompletableFuture.completedFuture(taskResult.result().get().getFirst());
            })
        .whenComplete(
            (blockHeader, throwable) -> {
              if (throwable != null) {
                LOG.debug("Error downloading block header by hash {}", hash);
              } else {
                LOG.atDebug()
                    .setMessage("Successfully downloaded block header by hash {}")
                    .addArgument(blockHeader::toLogString)
                    .log();
              }
            });
  }
}
