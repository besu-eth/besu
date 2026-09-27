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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.function.LongPredicate;

/**
 * Finds the lowest block for which this node still holds block data. Chain pruning removes bodies
 * from the bottom up while retaining headers, so the boundary is the first block that still has a
 * body. Shared so that the eth/69 advertised range, the pruner's start point and any backlog
 * calculation agree on one definition.
 */
public final class BlockDataBoundary {

  private BlockDataBoundary() {}

  /**
   * Binary-searches for the lowest block that still has data, per the caller's own test of what
   * "has data" means. Assumes a contiguous boundary, which contiguous pruning gives; an interrupted
   * parallel offline prune can leave holes, in which case the result is an arbitrary point within
   * them.
   *
   * @param hasBlockData tests whether a given block number still has data; see {@link
   *     #blockDataIn(BlockchainStorage)} for the standard storage-backed test
   * @param chainHeadBlockNumber the current chain head number, the upper bound of the search
   * @return the lowest block number with data, {@code 0} when the chain is intact from genesis, or
   *     empty when no block has data
   */
  public static Optional<Long> firstBlockWithBody(
      final LongPredicate hasBlockData, final long chainHeadBlockNumber) {
    long low = 1;
    long high = chainHeadBlockNumber;
    while (low < high) {
      final long mid = (low + high) / 2;
      if (hasBlockData.test(mid)) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    if (!hasBlockData.test(low)) {
      return Optional.empty();
    }
    return Optional.of(
        low == BlockHeader.GENESIS_BLOCK_NUMBER + 1 ? BlockHeader.GENESIS_BLOCK_NUMBER : low);
  }

  /**
   * The standard "this block still has data" test: a canonical hash for the number, and a body
   * under that hash. This is semantically the same test as {@code DefaultBlockchain}'s own {@code
   * getBlockByNumber(blockNumber).isPresent()} route, which reads through its instance caches
   * rather than storage directly; the two are meant to agree.
   *
   * @param storage the blockchain storage to read through
   * @return a predicate over block numbers
   */
  public static LongPredicate blockDataIn(final BlockchainStorage storage) {
    return blockNumber ->
        storage.getBlockHash(blockNumber).flatMap(storage::getBlockBody).isPresent();
  }
}
