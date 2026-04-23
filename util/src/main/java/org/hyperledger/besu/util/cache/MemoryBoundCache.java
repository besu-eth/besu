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
package org.hyperledger.besu.util.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jspecify.annotations.Nullable;

/**
 * A memory-bound cache that uses Caffeine to limit the size of the cache based on the memory
 * footprint of the key-value pairs.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public class MemoryBoundCache<K, V> {
  private final Cache<K, V> cache;

  /**
   * Constructs a MemoryBoundCache with a specified maximum size in bytes and a memory footprint
   * calculator.
   *
   * @param maxBytes the maximum size of the cache in bytes
   * @param memoryFootprintCalculator a function that calculates the memory footprint of a key-value
   *     pair
   */
  public MemoryBoundCache(
      final long maxBytes, final BiToIntFunction<K, V> memoryFootprintCalculator) {
    this.cache =
        Caffeine.newBuilder()
            .maximumWeight(maxBytes)
            .weigher(memoryFootprintCalculator::applyAsInt)
            .recordStats()
            .build();
  }

  /**
   * Puts a key-value pair into the cache.
   *
   * @param key the key to be associated with the value
   * @param value the value to be stored in the cache
   */
  public void put(final K key, final V value) {
    cache.put(key, value);
  }

  /**
   * Retrieves a value from the cache if present.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with the key, or null if not present
   */
  public @Nullable V getIfPresent(final K key) {
    return cache.getIfPresent(key);
  }

  /**
   * Estimates the number of entries in the cache
   *
   * @return the estimated number of entries in the cache
   */
  public long estimatedSize() {
    return cache.estimatedSize();
  }

  /**
   * Gets the hit rate of the cache.
   *
   * @return the hit rate as a double value
   */
  public double hitRate() {
    return cache.stats().hitRate();
  }

  /**
   * Gets the total number of evictions from the cache.
   *
   * @return the total number of evictions
   */
  public long evictionCount() {
    return cache.stats().evictionCount();
  }

  /**
   * Gets the total weight of evictions from the cache.
   *
   * @return the total weight of evictions
   */
  public long evictionWeight() {
    return cache.stats().evictionWeight();
  }

  /**
   * Gets the total number of requests made to the cache.
   *
   * @return the total number of requests
   */
  public long requestCount() {
    return cache.stats().requestCount();
  }
}
