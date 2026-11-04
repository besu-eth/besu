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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner;

import java.util.Optional;

/**
 * The earliest block this node can serve bodies and receipts for, as advertised in the eth/69
 * {@code Status} and {@code BlockRangeUpdate} messages. Combines the blockchain's own view, which
 * is cached and so goes stale as pruning advances, with the pruner's forward-looking bound.
 */
public class BlockRangeProvider {
  private final Blockchain blockchain;
  private final Optional<ChainDataPruner> chainDataPruner;

  public BlockRangeProvider(
      final Blockchain blockchain, final Optional<ChainDataPruner> chainDataPruner) {
    this.blockchain = blockchain;
    this.chainDataPruner = chainDataPruner;
  }

  /**
   * The earliest block number to advertise, never above the chain head.
   *
   * @return the earliest block number, or empty when no block has a body yet
   */
  public Optional<Long> getEarliestBlockNumber() {
    return getEarliestBlockNumber(blockchain.getChainHeadBlockNumber());
  }

  /**
   * Computes a pruning-aware range boundary against the same head used by the outgoing message.
   *
   * @param head captured head number
   * @return earliest available block at or below that head
   */
  public Optional<Long> getEarliestBlockNumber(final long head) {
    return blockchain
        .getEarliestBlockNumber(head)
        .map(
            fromChain ->
                chainDataPruner
                    .flatMap(pruner -> pruner.getEarliestRetainedBlock(head))
                    .map(fromPruner -> Math.max(fromChain, fromPruner))
                    .orElse(fromChain))
        .map(earliest -> Math.min(earliest, head));
  }
}
