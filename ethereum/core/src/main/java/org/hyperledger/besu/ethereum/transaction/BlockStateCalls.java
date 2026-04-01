/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.transaction;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError;
import org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallException;
import org.hyperledger.besu.plugin.data.BlockOverrides;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A class that manages a chain of BlockStateCalls. It fills gaps between blocks and sets the
 * correct block number and timestamp when they are not set.
 */
public class BlockStateCalls {
  private static final long MAX_BLOCK_CALL_SIZE = 256;

  /** Standard Ethereum slot duration used for eth_simulateV1 timestamp auto-increment. */
  private static final long SLOT_DURATION_SECONDS = 12;

  /**
   * Normalizes a list of BlockStateCalls by filling gaps and setting the correct block number and
   * timestamp.
   *
   * @param blockStateCalls the list of BlockStateCalls to normalize
   * @param header the initial block header
   * @return a normalized list of BlockStateCalls
   */
  public static List<BlockStateCall> fillBlockStateCalls(
      final List<? extends BlockStateCall> blockStateCalls, final BlockHeader header) {
    long lastPresentBlockNumber = findLastBlockNumber(blockStateCalls);
    if (lastPresentBlockNumber > header.getNumber() + MAX_BLOCK_CALL_SIZE) {
      String errorMessage =
          String.format(
              "Block number %d exceeds the limit of %d (header: %d + MAX_BLOCK_CALL_SIZE: %d)",
              lastPresentBlockNumber,
              header.getNumber() + MAX_BLOCK_CALL_SIZE,
              header.getNumber(),
              MAX_BLOCK_CALL_SIZE);
      throw new BlockStateCallException(errorMessage, BlockStateCallError.TOO_MANY_BLOCK_CALLS);
    }
    List<BlockStateCall> filledCalls = new ArrayList<>();
    long currentBlock = header.getNumber();
    long currentTimestamp = header.getTimestamp();
    for (BlockStateCall blockStateCall : blockStateCalls) {
      long nextBlockNumber =
          blockStateCall.getBlockOverrides().getBlockNumber().orElse(currentBlock + 1);
      List<BlockStateCall> intermediateBlocks =
          new ArrayList<>(
              generateIntermediateBlocks(nextBlockNumber, currentBlock, currentTimestamp));
      // Add intermediate blocks
      for (BlockStateCall intermediateBlock : intermediateBlocks) {
        add(filledCalls, intermediateBlock, currentBlock, currentTimestamp);
        currentBlock = intermediateBlock.getBlockOverrides().getBlockNumber().orElseThrow();
        currentTimestamp = intermediateBlock.getBlockOverrides().getTimestamp().orElseThrow();
      }
      // set the block number and timestamp if they are not set
      if (blockStateCall.getBlockOverrides().getBlockNumber().isEmpty()) {
        blockStateCall.getBlockOverrides().setBlockNumber(currentBlock + 1);
      }
      if (blockStateCall.getBlockOverrides().getTimestamp().isEmpty()) {
        blockStateCall.getBlockOverrides().setTimestamp(currentTimestamp + SLOT_DURATION_SECONDS);
      }
      // Add the current block
      add(filledCalls, blockStateCall, currentBlock, currentTimestamp);
      currentBlock = blockStateCall.getBlockOverrides().getBlockNumber().orElseThrow();
      currentTimestamp = blockStateCall.getBlockOverrides().getTimestamp().orElseThrow();
    }
    return filledCalls;
  }

  private static void add(
      final List<BlockStateCall> blockStateCalls,
      final BlockStateCall blockStateCall,
      final long currentBlockNumber,
      final long currentTimestamp) {
    long blockNumber = blockStateCall.getBlockOverrides().getBlockNumber().orElseThrow();
    long timestamp = blockStateCall.getBlockOverrides().getTimestamp().orElseThrow();
    if (blockNumber <= currentBlockNumber) {
      throw new BlockStateCallException(
          String.format(
              "Block number is invalid. Trying to add a call at block number %s, while current block number is %s.",
              blockNumber, currentBlockNumber),
          BlockStateCallError.BLOCK_NUMBERS_NOT_ASCENDING);
    }
    if (timestamp <= currentTimestamp) {
      throw new BlockStateCallException(
          String.format(
              "Timestamp is invalid. Trying to add a call at timestamp %s, while current timestamp is %s.",
              timestamp, currentTimestamp),
          BlockStateCallError.TIMESTAMPS_NOT_ASCENDING);
    }
    blockStateCalls.add(blockStateCall);
  }

  private static List<BlockStateCall> generateIntermediateBlocks(
      final long targetBlockNumber, final long startBlockNumber, final long startTimestamp) {
    List<BlockStateCall> intermediateBlocks = new ArrayList<>();
    long blockNumberDiff = targetBlockNumber - startBlockNumber;
    for (long i = 1; i < blockNumberDiff; i++) {
      long nextBlockNumber = startBlockNumber + i;
      long nextTimestamp = startTimestamp + SLOT_DURATION_SECONDS * i;
      var nextBlockOverrides =
          BlockOverrides.builder().blockNumber(nextBlockNumber).timestamp(nextTimestamp).build();
      intermediateBlocks.add(new BlockStateCall(nextBlockOverrides));
    }
    return intermediateBlocks;
  }

  private static long findLastBlockNumber(final List<? extends BlockStateCall> blockStateCalls) {
    var lastPresentBlockNumber =
        blockStateCalls.stream()
            .map(blockStateCall -> blockStateCall.getBlockOverrides().getBlockNumber())
            .filter(Optional::isPresent)
            .mapToLong(Optional::get)
            .max()
            .orElse(-1);
    long callsAfterLastPresentBlockNumber =
        blockStateCalls.stream()
            .filter(
                blockStateCall ->
                    blockStateCall.getBlockOverrides().getBlockNumber().orElse(Long.MAX_VALUE)
                        > lastPresentBlockNumber)
            .count();
    return lastPresentBlockNumber + callsAfterLastPresentBlockNumber;
  }
}
