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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads stored block headers from the blockchain database in forward direction. Used in the second
 * stage to supply headers for bodies and receipts download. Thread-safe for parallel consumption.
 */
public class BlockHeaderSource implements Iterator<List<BlockHeader>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockHeaderSource.class);

  private final Blockchain blockchain;
  private final long pivotBlockNumber;
  private final int batchSize;

  private final AtomicLong currentBlockNumber;

  /**
   * Creates a new BlockHeaderSource.
   *
   * @param blockchain the blockchain to read headers from
   * @param anchorBlockNumber the block number before the block to start with
   * @param pivotBlockNumber the block number to stop at (inclusive)
   * @param batchSize the number of headers to return per batch
   */
  public BlockHeaderSource(
      final Blockchain blockchain,
      final long anchorBlockNumber,
      final long pivotBlockNumber,
      final int batchSize) {
    this.blockchain = blockchain;
    this.pivotBlockNumber = pivotBlockNumber;
    this.batchSize = batchSize;
    this.currentBlockNumber = new AtomicLong(anchorBlockNumber + 1);

    LOG.debug(
        "BlockHeaderSource created: start={}, end={}, batchSize={}",
        anchorBlockNumber,
        pivotBlockNumber,
        batchSize);
  }

  @Override
  public boolean hasNext() {
    return currentBlockNumber.get() <= pivotBlockNumber;
  }

  @Override
  public List<BlockHeader> next() {
    if (currentBlockNumber.get() > pivotBlockNumber) {
      LOG.debug("BlockHeaderSource exhausted at block {}", currentBlockNumber);
      throw new NoSuchElementException(
          "BlockHeaderSource exhausted at block " + currentBlockNumber);
    }

    long start = currentBlockNumber.getAndAdd(batchSize);
    final int actualLength = (int) Math.min(batchSize, pivotBlockNumber - start + 1);

    LOG.trace(
        "BlockHeaderSource reading batch: {} blocks from block number {}", actualLength, start);

    return blockchain.getBlockHeaders(start, actualLength);
  }
}
