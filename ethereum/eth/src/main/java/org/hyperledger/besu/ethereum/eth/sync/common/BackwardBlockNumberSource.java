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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates block numbers in descending order for backward header sync. Thread-safe for parallel
 * consumption.
 */
public class BackwardBlockNumberSource implements Iterator<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(BackwardBlockNumberSource.class);

  private final AtomicLong currentBlock;
  private final int batchSize;
  private final long stopBlock;

  /**
   * Creates a new BackwardHeaderSource.
   *
   * @param batchSize the number of blocks in each batch
   * @param lowerBlockNumber lowerBlockNumber +1 is the lowest header to download
   * @param upperBlockNumber the highest header to download
   */
  public BackwardBlockNumberSource(
      final int batchSize, final long lowerBlockNumber, final long upperBlockNumber) {

    this.currentBlock = new AtomicLong(upperBlockNumber);
    this.stopBlock = lowerBlockNumber;
    this.batchSize = batchSize;

    LOG.debug(
        "BackwardHeaderSource: upperBlockNumber={}, lowerBlockNumber={}, batchSize={}",
        upperBlockNumber,
        lowerBlockNumber,
        batchSize);
  }

  @Override
  public boolean hasNext() {
    return currentBlock.get() >= stopBlock;
  }

  @Override
  public Long next() {
    final long block = currentBlock.getAndUpdate(current -> current - batchSize);

    if (block >= stopBlock) {
      return block;
    } else {
      LOG.debug("BackwardHeaderSource exhausted at block {} (stopBlock={})", block, stopBlock);
      throw new NoSuchElementException("BackwardHeaderSource exhausted at block " + block);
    }
  }
}
