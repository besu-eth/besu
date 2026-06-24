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
package org.hyperledger.besu.ethereum.trie.pathbased.common.code;

import static org.hyperledger.besu.metrics.BesuMetricCategory.BONSAI_CACHE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.util.cache.MemoryBoundCache;

/** The path-based bytecode cache. */
public class PathBasedCodeCache implements org.hyperledger.besu.evm.internal.CodeCache {

  private final MemoryBoundCache<Hash, Code> cache;

  // metrics
  private long lastRequestCount = 0;
  private long lastRequestTimestamp = System.nanoTime();

  /** Instantiates a new Code cache. */
  public PathBasedCodeCache() {
    // Initialize the cache with a maximum size of 256 MB and a custom memory footprint estimator
    this.cache = new MemoryBoundCache<>(256 * 1024 * 1024, CodeMemoryFootprint::estimate);
  }

  /**
   * Sets up the metrics system for the code cache.
   *
   * @param metricsSystem the metrics system to use
   */
  public void setupMetricsSystem(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createLongGauge(
        BONSAI_CACHE,
        "code_cache_size",
        "Current number of entries in the code cache",
        cache::estimatedSize);

    metricsSystem.createGauge(
        BONSAI_CACHE, "code_cache_hit_rate", "Hit rate of the code cache", cache::hitRate);

    metricsSystem.createLongGauge(
        BONSAI_CACHE,
        "code_cache_evictions",
        "Total number of evictions from the code cache",
        cache::evictionCount);

    metricsSystem.createLongGauge(
        BONSAI_CACHE,
        "code_cache_eviction_weight",
        "Total weight of evictions from the code cache",
        cache::evictionWeight);

    metricsSystem.createGauge(
        BONSAI_CACHE,
        "code_cache_lookups_per_second",
        "Estimated number of code cache lookups per second",
        () -> {
          long now = System.nanoTime();
          long currentCount = cache.requestCount();

          long deltaRequests = currentCount - lastRequestCount;
          long deltaTimeNanos = now - lastRequestTimestamp;

          lastRequestCount = currentCount;
          lastRequestTimestamp = now;

          if (deltaTimeNanos == 0) {
            return 0.0;
          }
          return (deltaRequests * 1_000_000_000.0) / deltaTimeNanos;
        });
  }

  /**
   * Gets the code if present in the cache.
   *
   * @param codeHash the code hash
   * @return the code if present
   */
  @Override
  public Code getIfPresent(final Hash codeHash) {
    return cache.getIfPresent(codeHash);
  }

  /**
   * Put the code into the cache.
   *
   * @param codeHash the code hash
   * @param code the code
   */
  @Override
  public void put(final Hash codeHash, final Code code) {
    cache.put(codeHash, code);
  }
}
