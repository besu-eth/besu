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
package org.hyperledger.besu.chainimport;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes an imported proof-of-stake block the chain head with a fork choice update, as the consensus
 * client would. Used from one thread at a time.
 *
 * <p>{@link MergeMiningCoordinator#updateForkChoice} does not check how the head, finalized and
 * safe blocks relate; for the Engine API, {@code EngineForkchoiceUpdatedV1.isValidForkchoiceState}
 * does. The update built here keeps to the same rules: the head descends from the finalized block,
 * and from the safe block. Unlike the Engine API, it never changes the finalized block, and moves
 * the safe block back rather than rejecting the update. If those rules change, change this too.
 */
class PosHeadUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(PosHeadUpdater.class);

  private final MutableBlockchain blockchain;
  private final MergeMiningCoordinator coordinator;

  /**
   * By ancestor hash, the last block checked against it. A side chain is imported parent first, so
   * a check is answered from the parent's result instead of walking back to the ancestor for every
   * block, which would be quadratic in the length of the side chain.
   */
  private final Map<Hash, AncestryCheck> lastChecks = new HashMap<>();

  /** The last block left as a side block, so that a side chain is logged once, not per block. */
  private Hash lastSideBlock = null;

  private record AncestryCheck(Hash block, boolean descends) {}

  PosHeadUpdater(final MutableBlockchain blockchain, final MergeMiningCoordinator coordinator) {
    this.blockchain = blockchain;
    this.coordinator = coordinator;
  }

  /**
   * Makes the block the chain head, unless it does not descend from the finalized block: it then
   * stays a side block, as the Engine API would not make it the head either.
   *
   * @param header the block to make the chain head
   * @return true if the block is now the chain head, false if it stays a side block
   * @throws IllegalStateException if the fork choice update fails
   */
  boolean makeHead(final BlockHeader header) {
    final Optional<Hash> finalized = blockchain.getFinalized();
    if (!descendsFrom(finalized, header)) {
      if (!header.getParentHash().equals(lastSideBlock)) {
        LOG.warn(
            "Block {} does not descend from the finalized block {}, so it stays a side block",
            header.toLogString(),
            finalized.map(Hash::toHexString).orElse("none"));
      }
      lastSideBlock = header.getHash();
      return false;
    }
    // Hash.ZERO leaves the finalized block as it is: updateForkChoice ignores a block it cannot
    // find
    final ForkchoiceResult result =
        coordinator.updateForkChoice(header, Hash.ZERO, safeBlockFor(header));
    if (result.getStatus() != ForkchoiceResult.Status.VALID) {
      throw new IllegalStateException(
          "Unable to make block "
              + header.getNumber()
              + " the chain head: "
              + result.getErrorMessage().orElse(result.getStatus().name()));
    }
    return true;
  }

  /**
   * The safe block for the fork choice update. {@link Hash#ZERO} leaves the safe block as it is, as
   * updateForkChoice ignores a block it cannot find; the Engine API instead reads it as no safe
   * block. When the new head does not descend from the safe block, move it back to the finalized
   * block, or to genesis, so that it stays on the canonical chain.
   */
  private Hash safeBlockFor(final BlockHeader newHead) {
    return blockchain
        .getSafeBlock()
        .filter(safe -> !descendsFrom(Optional.of(safe), newHead))
        .map(unused -> blockchain.getFinalized().orElse(blockchain.getGenesisBlock().getHash()))
        .orElse(Hash.ZERO);
  }

  /** Whether the block is, or descends from, the ancestor; true when there is no ancestor. */
  private boolean descendsFrom(final Optional<Hash> ancestorHash, final BlockHeader header) {
    final Optional<BlockHeader> ancestor = ancestorHash.flatMap(blockchain::getBlockHeader);
    if (ancestor.isEmpty()) {
      return true;
    }
    final Hash ancestorBlockHash = ancestor.get().getHash();
    final AncestryCheck last = lastChecks.get(ancestorBlockHash);
    final boolean descends;
    if (header.getHash().equals(ancestorBlockHash)) {
      descends = true;
    } else if (last != null && last.block().equals(header.getParentHash())) {
      descends = last.descends();
    } else {
      descends = coordinator.isDescendantOf(ancestor.get(), header);
    }
    lastChecks.put(ancestorBlockHash, new AncestryCheck(header.getHash(), descends));
    return descends;
  }
}
