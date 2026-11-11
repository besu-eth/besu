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
 * Counts the operations that are inside a native RocksDB call, so that closing the database can
 * wait for them. RocksJava does not check its handles, so a call that runs while the database is
 * closed crashes the JVM.
 */
final class InFlightOperations {
  // a counter per stripe of threads, 128 bytes apart, so that readers do not share a cache line
  private static final int STRIPES = 64;
  private static final int STRIDE = 16;

  private final AtomicLongArray counts = new AtomicLongArray(STRIPES * STRIDE);

  /**
   * Counts an operation of the calling thread.
   *
   * @return the slot to pass to {@link #exit(int)}
   */
  int enter() {
    final int slot = (int) (Thread.currentThread().threadId() & (STRIPES - 1)) * STRIDE;
    counts.getAndIncrement(slot);
    return slot;
  }

  /**
   * Ends an operation counted by {@link #enter()}.
   *
   * @param slot the slot returned by {@link #enter()}
   */
  void exit(final int slot) {
    counts.getAndDecrement(slot);
  }

  /**
   * Waits until no operation is counted.
   *
   * @param timeout how long to wait
   * @return false if an operation was still counted when the timeout ran out
   */
  boolean awaitNone(final Duration timeout) {
    final long deadline = System.nanoTime() + timeout.toNanos();
    for (int slot = 0; slot < counts.length(); slot += STRIDE) {
      while (counts.get(slot) != 0) {
        if (System.nanoTime() - deadline > 0) {
          return false;
        }
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      }
    }
    return true;
  }
}
