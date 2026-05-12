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
package org.hyperledger.besu.ethereum.vm;

import static org.hyperledger.besu.datatypes.Hash.ZERO;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.BlockHashOperation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Calculates and caches block hashes by number following the chain for a specific branch. This is
 * used by {@link BlockHashOperation} and ensures that the correct block hash is returned even when
 * the block being imported is on a fork.
 *
 * <p>A new {@link BlockchainBasedBlockHashLookup} must be created for each block being processed
 * but should be reused for all transactions within that block.
 *
 * <p>Parallel worker forks (preload or transaction execution) share the same hash cache via {@link
 * #forkForParallelWorker} so work done by one worker is visible to others; each fork keeps its own
 * {@code searchStartHeader} cursor when walking parent headers.
 */
public class BlockchainBasedBlockHashLookup implements BlockHashLookup {
  /** Block header for the block being executed; used to fork fresh lookups for parallel preload. */
  private final ProcessableBlockHeader anchorHeader;

  private ProcessableBlockHeader searchStartHeader;
  private final Blockchain blockchain;
  private final ConcurrentMap<Long, Hash> hashByNumber;

  public BlockchainBasedBlockHashLookup(
      final ProcessableBlockHeader currentBlock, final Blockchain blockchain) {
    this(currentBlock, blockchain, new ConcurrentHashMap<>());
  }

  private BlockchainBasedBlockHashLookup(
      final ProcessableBlockHeader currentBlock,
      final Blockchain blockchain,
      final ConcurrentMap<Long, Hash> hashByNumber) {
    this.anchorHeader = currentBlock;
    this.searchStartHeader = currentBlock;
    this.blockchain = blockchain;
    this.hashByNumber = hashByNumber;
    hashByNumber.putIfAbsent(currentBlock.getNumber() - 1, currentBlock.getParentHash());
  }

  @Override
  public BlockHashLookup forkForParallelWorker() {
    return new BlockchainBasedBlockHashLookup(anchorHeader, blockchain, hashByNumber);
  }

  @Override
  public Hash apply(final MessageFrame frame, final Long blockNumber) {
    final Hash cachedHash = hashByNumber.get(blockNumber);
    if (cachedHash != null) {
      return cachedHash;
    }
    while (searchStartHeader != null && searchStartHeader.getNumber() - 1 > blockNumber) {
      searchStartHeader = blockchain.getBlockHeader(searchStartHeader.getParentHash()).orElse(null);
      if (searchStartHeader != null) {
        hashByNumber.put(searchStartHeader.getNumber() - 1, searchStartHeader.getParentHash());
      }
    }
    return hashByNumber.getOrDefault(blockNumber, ZERO);
  }
}
