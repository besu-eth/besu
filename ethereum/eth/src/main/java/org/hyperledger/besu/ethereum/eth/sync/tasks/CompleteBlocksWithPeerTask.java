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
package org.hyperledger.besu.ethereum.eth.sync.tasks;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a set of headers, "completes" them by repeatedly requesting additional data (bodies) needed
 * to create the blocks that correspond to the supplied headers.
 */
public class CompleteBlocksWithPeerTask {
  private static final Logger LOG = LoggerFactory.getLogger(CompleteBlocksWithPeerTask.class);

  private final ProtocolSchedule protocolSchedule;
  private final List<BlockHeader> headersToGet = new ArrayList<>();
  private final PeerTaskExecutor peerTaskExecutor;

  private final Block[] result;
  private final int resultSize;
  private int nextIndex = 0;
  private int remainingBlocks;

  public CompleteBlocksWithPeerTask(
      final ProtocolSchedule protocolSchedule,
      final List<BlockHeader> headers,
      final PeerTaskExecutor peerTaskExecutor) {
    checkArgument(!headers.isEmpty(), "Must supply a non-empty headers list");
    this.protocolSchedule = protocolSchedule;
    this.peerTaskExecutor = peerTaskExecutor;

    resultSize = headers.size();
    result = new Block[resultSize];
    remainingBlocks = resultSize;

    for (int i = 0; i < resultSize; i++) {
      final BlockHeader header = headers.get(i);
      if (BlockHeader.hasEmptyBlock(header)) {
        final Block emptyBlock =
            new Block(header, createEmptyBodyBasedOnProtocolSchedule(protocolSchedule, header));
        result[i] = emptyBlock;
        remainingBlocks--;
      } else {
        headersToGet.add(header);
      }
    }
    this.nextIndex = findNextIndex(0);
  }

  private BlockBody createEmptyBodyBasedOnProtocolSchedule(
      final ProtocolSchedule protocolSchedule, final BlockHeader header) {
    return new BlockBody(
        Collections.emptyList(),
        Collections.emptyList(),
        isWithdrawalsEnabled(protocolSchedule, header)
            ? Optional.of(Collections.emptyList())
            : Optional.empty());
  }

  private boolean isWithdrawalsEnabled(
      final ProtocolSchedule protocolSchedule, final BlockHeader header) {
    return protocolSchedule.getByBlockHeader(header).getWithdrawalsProcessor().isPresent();
  }

  /**
   * Retrieves all remaining blocks from connected peers. Subsequent calls will have no affect.
   *
   * @return A List of all blocks for the headers supplied when constructing this
   *     CompleteBlocksWithPeerTask
   */
  public List<Block> retrieveBlocksFromPeers() {
    while (remainingBlocks > 0) {
      LOG.atDebug()
          .setMessage("Requesting {} bodies from peer")
          .addArgument(headersToGet.size())
          .log();
      final GetBodiesFromPeerTask task = new GetBodiesFromPeerTask(headersToGet, protocolSchedule);
      final PeerTaskExecutorResult<List<Block>> executionResult = peerTaskExecutor.execute(task);
      if (executionResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
          && executionResult.result().isPresent()) {
        final List<Block> blockList = executionResult.result().get();
        LOG.atDebug()
            .setMessage("Received {} bodies out of {} from peer")
            .addArgument(blockList.size())
            .addArgument(headersToGet.size())
            .log();
        blockList.forEach(
            block -> {
              remainingBlocks--;
              result[nextIndex] = block;
              headersToGet.removeFirst();
              nextIndex = findNextIndex(nextIndex + 1);
            });
      } else {
        throw new RuntimeException(
            "Unable to retrieve blocks for block numbers: "
                + headersToGet.getFirst().getNumber()
                + " to "
                + headersToGet.getLast().getNumber());
      }
    }
    return List.of(result);
  }

  private int findNextIndex(final int startIndex) {
    for (int i = startIndex; i < resultSize; i++) {
      if (result[i] == null) {
        return i;
      }
    }
    return -1; // This only happens when we have finished processing all headers
  }
}
