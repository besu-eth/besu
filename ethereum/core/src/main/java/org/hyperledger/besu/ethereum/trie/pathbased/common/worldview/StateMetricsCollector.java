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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview;

/**
 * Interface for collecting state-layer metrics during block execution.
 *
 * <p>Implementations are threaded through the world state object graph (PathBasedWorldState →
 * accumulator → accounts) so that state operations can record metrics without relying on
 * ThreadLocal statics.
 */
public interface StateMetricsCollector {

  /** Increments the account read counter. */
  void incrementAccountReads();

  /** Increments the storage read counter. */
  void incrementStorageReads();

  /**
   * Sets the final unique account write count for the block.
   *
   * @param count the number of unique accounts written
   */
  void setAccountWrites(int count);

  /**
   * Sets the final unique storage slot write count for the block.
   *
   * @param count the number of unique storage slots written
   */
  void setStorageWrites(int count);

  /** Increments the account destruct counter. */
  void incrementAccountDestructs();

  /**
   * Adds to the storage slots deleted counter.
   *
   * @param count the number of storage slots deleted
   */
  void addStorageDeletes(int count);

  /** Increments the code read counter. */
  void incrementCodeReads();

  /**
   * Adds bytes read for code.
   *
   * @param bytes the number of bytes read
   */
  void addCodeBytesRead(long bytes);

  /** Increments the code cache hit counter. */
  void incrementCodeCacheHits();

  /** Increments the code cache miss counter. */
  void incrementCodeCacheMisses();

  /** Increments the account cache hit counter. */
  void incrementAccountCacheHits();

  /** Increments the account cache miss counter. */
  void incrementAccountCacheMisses();

  /** Increments the storage cache hit counter. */
  void incrementStorageCacheHits();

  /** Increments the storage cache miss counter. */
  void incrementStorageCacheMisses();

  /**
   * Adds elapsed time for a state read operation.
   *
   * @param nanos elapsed time in nanoseconds
   */
  void addStateReadTime(long nanos);

  /** A no-op implementation that discards all metrics. */
  StateMetricsCollector NOOP =
      new StateMetricsCollector() {
        @Override
        public void incrementAccountReads() {}

        @Override
        public void incrementStorageReads() {}

        @Override
        public void setAccountWrites(final int count) {}

        @Override
        public void setStorageWrites(final int count) {}

        @Override
        public void incrementAccountDestructs() {}

        @Override
        public void addStorageDeletes(final int count) {}

        @Override
        public void incrementCodeReads() {}

        @Override
        public void addCodeBytesRead(final long bytes) {}

        @Override
        public void incrementCodeCacheHits() {}

        @Override
        public void incrementCodeCacheMisses() {}

        @Override
        public void incrementAccountCacheHits() {}

        @Override
        public void incrementAccountCacheMisses() {}

        @Override
        public void incrementStorageCacheHits() {}

        @Override
        public void incrementStorageCacheMisses() {}

        @Override
        public void addStateReadTime(final long nanos) {}
      };
}
