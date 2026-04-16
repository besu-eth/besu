/*
 * Copyright 2019 ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.peervalidation;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractPeerBlockValidator implements PeerValidator {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPeerBlockValidator.class);
  static long DEFAULT_CHAIN_HEIGHT_ESTIMATION_BUFFER = 10L;

  private final ProtocolSchedule protocolSchedule;
  private final PeerTaskExecutor peerTaskExecutor;

  final long blockNumber;
  // Wait for peer's chainhead to advance some distance beyond blockNumber before validating
  private final long chainHeightEstimationBuffer;

  AbstractPeerBlockValidator(
      final ProtocolSchedule protocolSchedule,
      final PeerTaskExecutor peerTaskExecutor,
      final long blockNumber,
      final long chainHeightEstimationBuffer) {
    checkArgument(chainHeightEstimationBuffer >= 0);
    this.protocolSchedule = protocolSchedule;
    this.peerTaskExecutor = peerTaskExecutor;
    this.blockNumber = blockNumber;
    this.chainHeightEstimationBuffer = chainHeightEstimationBuffer;
  }

  @Override
  public CompletableFuture<Boolean> validatePeer(
      final EthContext ethContext, final EthPeer ethPeer) {
    return ethContext
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetHeadersFromPeerTask task =
                  new GetHeadersFromPeerTask(
                      blockNumber,
                      1,
                      0,
                      GetHeadersFromPeerTask.Direction.FORWARD,
                      protocolSchedule);
              PeerTaskExecutorResult<List<BlockHeader>> taskResult =
                  peerTaskExecutor.executeAgainstPeer(task, ethPeer);
              CompletableFuture<Boolean> resultFuture;
              if (taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
                  || taskResult.result().isEmpty()) {
                resultFuture = CompletableFuture.completedFuture(false);
              } else {
                resultFuture =
                    CompletableFuture.completedFuture(
                        validateBlockHeaders(ethPeer, taskResult.result().get()));
              }
              return resultFuture;
            });
  }

  private Boolean validateBlockHeaders(final EthPeer ethPeer, final List<BlockHeader> headers) {
    boolean isValid;
    if (headers.isEmpty()) {
      if (blockIsRequired()) {
        // If no headers are returned, fail
        LOG.debug(
            "Peer {} is invalid because required block ({}) is unavailable.", ethPeer, blockNumber);
        isValid = false;
      } else {
        LOG.debug(
            "Peer {} deemed valid because unavailable block ({}) is not required.",
            ethPeer,
            blockNumber);
        isValid = true;
      }
    } else {
      final BlockHeader header = headers.getFirst();
      isValid = validateBlockHeader(ethPeer, header);
    }
    return isValid;
  }

  abstract boolean validateBlockHeader(EthPeer ethPeer, BlockHeader header);

  @Override
  public boolean canBeValidated(final EthPeer ethPeer) {
    return ethPeer.chainState().getEstimatedHeight() >= (blockNumber + chainHeightEstimationBuffer);
  }

  protected boolean blockIsRequired() {
    return true;
  }

  @Override
  public Duration nextValidationCheckTimeout(final EthPeer ethPeer) {
    if (!ethPeer.chainState().hasEstimatedHeight()) {
      return Duration.ofSeconds(30);
    }
    final long distanceToBlock = blockNumber - ethPeer.chainState().getEstimatedHeight();
    if (distanceToBlock < 100_000L) {
      return Duration.ofMinutes(1);
    }
    // If the peer is trailing behind, give it some time to catch up before trying again.
    return Duration.ofMinutes(10);
  }
}
