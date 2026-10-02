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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * Bounded cache with LRU eviction at block granularity.
 *
 * <p>Each entry records the generation (the cache version, bumped once per committed block) it was
 * last used at. Reads and writes never evict, so the block's threads pay no eviction or lock cost;
 * {@link #evict()} runs once per block, off the hot path, and drops the least recently used entries
 * down to the bound. Until then the cache may hold more than its bound.
 *
 * <p>LRU rather than frequency-based: values prefetched for a block must survive until the block
 * reads them, and committed trie nodes matter to the next few blocks only. A frequency filter
 * rejects both on admission.
 */
final class BlockLruCache<V> {

  /** Ages beyond this share one bucket; those entries are the first to go anyway. */
  private static final int MAX_TRACKED_AGE = 255;

  private final ConcurrentHashMap<CacheKey, Entry<V>> map;
  private final long maxSize;
  private final long overflowSize;
  private final LongSupplier generation;

  private static final class Entry<V> {
    private final V value;
    private volatile long lastUsed;

    private Entry(final V value, final long lastUsed) {
      this.value = value;
      this.lastUsed = lastUsed;
    }
  }

  /**
   * @param maxSize number of entries kept after an eviction
   * @param initialCapacity initial capacity of the backing map
   * @param generation current generation, used to date accesses
   */
  BlockLruCache(final long maxSize, final int initialCapacity, final LongSupplier generation) {
    this.map = new ConcurrentHashMap<>(initialCapacity);
    this.maxSize = maxSize;
    this.overflowSize = maxSize + Math.max(1, maxSize / 2);
    this.generation = generation;
  }

  /** Returns the value and marks it used, or null. */
  V getIfPresent(final CacheKey key) {
    final Entry<V> entry = map.get(key);
    if (entry == null) {
      return null;
    }
    touch(entry);
    return entry.value;
  }

  /** Returns the value without marking it used, or null. */
  V peek(final CacheKey key) {
    final Entry<V> entry = map.get(key);
    return entry == null ? null : entry.value;
  }

  void put(final CacheKey key, final V value) {
    map.put(key, new Entry<>(value, generation.getAsLong()));
  }

  /**
   * Atomically replaces the value with {@code remapping(current)}; {@code current} is null when
   * absent. Returning {@code current} keeps the entry and marks it used.
   */
  void compute(final CacheKey key, final UnaryOperator<V> remapping) {
    map.compute(
        key,
        (k, entry) -> {
          final V current = entry == null ? null : entry.value;
          final V updated = remapping.apply(current);
          if (updated == null) {
            return null;
          }
          if (entry != null && updated == current) {
            touch(entry);
            return entry;
          }
          return new Entry<>(updated, generation.getAsLong());
        });
  }

  private void touch(final Entry<V> entry) {
    final long now = generation.getAsLong();
    // skip the write when already current, so hits on hot entries don't bounce the cache line
    if (entry.lastUsed != now) {
      entry.lastUsed = now;
    }
  }

  long size() {
    return map.mappingCount();
  }

  /** Whether the cache has grown far enough past its bound not to wait for the end of the block. */
  boolean isOverflowing() {
    return map.mappingCount() > overflowSize;
  }

  void clear() {
    map.clear();
  }

  /**
   * Evicts the least recently used entries until at most {@code maxSize} remain. Entries last used
   * at the same generation are evicted in no particular order.
   *
   * @return the number of entries evicted
   */
  int evict() {
    final long excess = map.mappingCount() - maxSize;
    if (excess <= 0) {
      return 0;
    }
    final long now = generation.getAsLong();
    final long[] countByAge = new long[MAX_TRACKED_AGE + 1];
    map.values().forEach(entry -> countByAge[age(entry, now)]++);

    // evict every entry older than cutoffAge, then evictAtCutoff entries of exactly that age
    int cutoffAge = MAX_TRACKED_AGE;
    long evictAtCutoff = excess;
    while (cutoffAge > 0 && countByAge[cutoffAge] < evictAtCutoff) {
      evictAtCutoff -= countByAge[cutoffAge];
      cutoffAge--;
    }

    int evicted = 0;
    final Iterator<Map.Entry<CacheKey, Entry<V>>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<CacheKey, Entry<V>> mapEntry = it.next();
      final int age = age(mapEntry.getValue(), now);
      if (age > cutoffAge || (age == cutoffAge && evictAtCutoff-- > 0)) {
        if (map.remove(mapEntry.getKey(), mapEntry.getValue())) {
          evicted++;
        }
      }
    }
    return evicted;
  }

  private static int age(final Entry<?> entry, final long now) {
    return (int) Math.min(MAX_TRACKED_AGE, Math.max(0, now - entry.lastUsed));
  }
}
