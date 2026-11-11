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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Counts the operations that are currently inside a native RocksDB call, so that closing the
 * database can wait for them to finish. RocksJava does not check its handles, so a call that runs
 * after the database is closed crashes the JVM.
 */
final class ActiveOperationCounter {
  // One shared counter would make concurrent reads slow each other down, so threads are spread
  // over several counters, placed 128 bytes apart so that no two share a CPU cache line.
  private static final int NUMBER_OF_COUNTERS = 64;
  private static final int LONGS_PER_COUNTER = 16;

  private final AtomicLongArray counters =
      new AtomicLongArray(NUMBER_OF_COUNTERS * LONGS_PER_COUNTER);

  /** Counts an operation started by the calling thread. */
  void increment() {
    counters.incrementAndGet(indexForCurrentThread());
  }

  /** Counts an operation as finished. Must be called on the thread that called increment(). */
  void decrement() {
    counters.decrementAndGet(indexForCurrentThread());
  }

  /**
   * Waits until no operation is running.
   *
   * @param timeout how long to wait at most
   * @return false if an operation was still running when the timeout ran out
   */
  boolean awaitZero(final Duration timeout) {
    final long deadline = System.nanoTime() + timeout.toNanos();
    for (int counter = 0; counter < NUMBER_OF_COUNTERS; counter++) {
      while (counters.get(counter * LONGS_PER_COUNTER) != 0) {
        if (System.nanoTime() - deadline > 0) {
          return false;
        }
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      }
    }
    return true;
  }

  private static int indexForCurrentThread() {
    final int counter = (int) (Thread.currentThread().threadId() % NUMBER_OF_COUNTERS);
    return counter * LONGS_PER_COUNTER;
  }
}
