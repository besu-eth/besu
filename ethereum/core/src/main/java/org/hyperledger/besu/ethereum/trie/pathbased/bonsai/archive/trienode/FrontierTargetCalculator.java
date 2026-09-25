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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

/**
 * Computes the block number the trie-node archive roller trails: the finalized block if a finality
 * signal exists (QBFT instant finality, PoS ~2 epochs behind head), otherwise {@code head -
 * maxLayersToLoad} for chains without finality (Clique/PoW). Every block at or below this target is
 * beyond any possible reorg, so the roller never observes a competing fork.
 */
public final class FrontierTargetCalculator {
  private FrontierTargetCalculator() {}

  public static long computeFrontierTarget(
      final Blockchain blockchain, final long maxLayersToLoad) {
    return blockchain
        .getFinalized()
        .flatMap(blockchain::getBlockHeader)
        .map(BlockHeader::getNumber)
        .orElseGet(() -> Math.max(0L, blockchain.getChainHeadBlockNumber() - maxLayersToLoad));
  }
}
