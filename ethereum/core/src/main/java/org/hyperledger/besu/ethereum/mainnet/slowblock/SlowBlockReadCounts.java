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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.AccumulatorReadObserver;

import java.util.concurrent.atomic.LongAdder;

/**
 * Total logical state reads seen by one world-state accumulator: one instance per background
 * transaction, plus one for the block accumulator.
 *
 * <p>Counters are {@link LongAdder}s because the accumulator's {@code commit()} resolves original
 * storage values from a parallel stream, so reads are not strictly single-threaded.
 */
public class SlowBlockReadCounts implements AccumulatorReadObserver {

  private final LongAdder accounts = new LongAdder();
  private final LongAdder storageSlots = new LongAdder();
  private final LongAdder code = new LongAdder();
  private final LongAdder codeBytes = new LongAdder();
  private final LongAdder readNanos = new LongAdder();

  @Override
  public void onAccountRead() {
    accounts.increment();
  }

  @Override
  public void onStorageRead() {
    storageSlots.increment();
  }

  @Override
  public void onCodeRead(final int size) {
    code.increment();
    if (size > 0) {
      codeBytes.add(size);
    }
  }

  @Override
  public void addFallThroughReadNanos(final long nanos) {
    readNanos.add(nanos);
  }

  /**
   * Folds these counts into the per-block aggregate.
   *
   * @param metrics the block aggregate to merge into
   */
  public void mergeInto(final SlowBlockMetrics metrics) {
    metrics.addStateReads(accounts.sum(), storageSlots.sum(), code.sum(), codeBytes.sum());
    metrics.addStateReadNanos(readNanos.sum());
  }

  /**
   * Returns logical account loads seen so far.
   *
   * @return the account read count
   */
  public long accounts() {
    return accounts.sum();
  }

  /**
   * Returns logical storage slot loads seen so far.
   *
   * @return the storage read count
   */
  public long storageSlots() {
    return storageSlots.sum();
  }

  /**
   * Returns logical code loads seen so far.
   *
   * @return the code read count
   */
  public long code() {
    return code.sum();
  }

  /**
   * Returns bytes of code loaded so far.
   *
   * @return the code byte count
   */
  public long codeBytes() {
    return codeBytes.sum();
  }

  /**
   * Returns time spent in reads that fell through to the underlying world state.
   *
   * @return elapsed nanoseconds
   */
  public long readNanos() {
    return readNanos.sum();
  }
}
