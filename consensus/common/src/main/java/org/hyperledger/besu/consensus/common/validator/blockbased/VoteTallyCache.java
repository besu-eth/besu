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
package org.hyperledger.besu.consensus.common.validator.blockbased;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.consensus.common.BlockInterface;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jspecify.annotations.Nullable;

class VoteTallyCache {

  private final Blockchain blockchain;
  private final EpochManager epochManager;
  private final VoteTallyUpdater voteTallyUpdater;

  private final Cache<Hash, VoteTally> voteTallyCache =
      CacheBuilder.newBuilder().maximumSize(100).build();
  private final BlockInterface blockInterface;

  VoteTallyCache(
      final Blockchain blockchain,
      final VoteTallyUpdater voteTallyUpdater,
      final EpochManager epochManager,
      final BlockInterface blockInterface) {

    checkNotNull(blockchain);
    checkNotNull(voteTallyUpdater);
    checkNotNull(epochManager);
    checkNotNull(blockInterface);
    this.blockchain = blockchain;
    this.voteTallyUpdater = voteTallyUpdater;
    this.epochManager = epochManager;
    this.blockInterface = blockInterface;
  }

  VoteTally getVoteTallyAtHead() {
    return getVoteTallyAfterBlock(blockchain.getChainHeadHeader());
  }

  /**
   * Determines the VoteTally for a given block header, by back-tracing the blockchain to a
   * previously cached value or epoch block. Then appyling votes in each intermediate header such
   * that representative state can be provided. This function assumes the vote cast in {@code
   * header} is applied, thus the voteTally returned contains the group of validators who are
   * permitted to partake in the next block's creation.
   *
   * @param header the header of the block after which the VoteTally is to be returned
   * @return The Vote Tally (and therefore validators) following the application of all votes upto
   *     and including the requested header.
   */
  VoteTally getVoteTallyAfterBlock(final BlockHeader header) {
    try {
      return voteTallyCache.get(header.getHash(), () -> populateCacheUptoAndIncluding(header));
    } catch (final ExecutionException ex) {
      throw new RuntimeException("Unable to determine a VoteTally object for the requested block.");
    }
  }

  private VoteTally populateCacheUptoAndIncluding(final BlockHeader start) {
    BlockHeader header = start;
    final Deque<BlockHeader> intermediateBlocks = new ArrayDeque<>();
    VoteTally voteTally = null;

    while (true) { // Will run into an epoch block (and thus a VoteTally) to break loop.
      intermediateBlocks.push(header);
      voteTally = getValidatorsAfter(header);
      if (voteTally != null) {
        break;
      }

      header =
          blockchain
              .getBlockHeader(header.getParentHash())
              .orElseThrow(
                  () ->
                      new NoSuchElementException(
                          "Supplied block was on a orphaned chain, unable to generate VoteTally."));
    }
    return constructMissingCacheEntries(intermediateBlocks, voteTally);
  }

  protected @Nullable VoteTally getValidatorsAfter(final BlockHeader header) {
    if (epochManager.isEpochBlock(header.getNumber())) {
      return new VoteTally(blockInterface.validatorsInBlock(header));
    }

    return voteTallyCache.getIfPresent(header.getParentHash());
  }

  private VoteTally constructMissingCacheEntries(
      final Deque<BlockHeader> headers, final VoteTally tally) {
    final VoteTally mutableVoteTally = tally.copy();
    while (!headers.isEmpty()) {
      final BlockHeader h = headers.pop();
      voteTallyUpdater.updateForBlock(h, mutableVoteTally);
      voteTallyCache.put(h.getHash(), mutableVoteTally.copy());
    }
    return mutableVoteTally;
  }
}
