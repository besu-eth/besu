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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BlockLruCacheTest {

  private final AtomicLong generation = new AtomicLong();
  private final BlockLruCache<String> cache = new BlockLruCache<>(2, 4, generation::get);

  @Test
  void writesNeverEvict() {
    cache.put(key(1), "a");
    cache.put(key(2), "b");
    cache.put(key(3), "c");

    assertThat(cache.size()).isEqualTo(3);
    assertThat(cache.peek(key(1))).isEqualTo("a");
  }

  @Test
  void evictDropsLeastRecentlyUsedGenerationsFirst() {
    cache.put(key(1), "a");
    generation.incrementAndGet();
    cache.put(key(2), "b");
    generation.incrementAndGet();
    cache.put(key(3), "c");

    assertThat(cache.evict()).isEqualTo(1);
    assertThat(cache.peek(key(1))).isNull();
    assertThat(cache.peek(key(2))).isEqualTo("b");
    assertThat(cache.peek(key(3))).isEqualTo("c");
  }

  @Test
  void readMarksEntryAsRecentlyUsed() {
    cache.put(key(1), "a");
    cache.put(key(2), "b");
    generation.incrementAndGet();
    assertThat(cache.getIfPresent(key(1))).isEqualTo("a");
    cache.put(key(3), "c");

    cache.evict();

    assertThat(cache.peek(key(2))).isNull();
    assertThat(cache.peek(key(1))).isEqualTo("a");
    assertThat(cache.peek(key(3))).isEqualTo("c");
  }

  @Test
  void peekDoesNotMarkEntryAsRecentlyUsed() {
    cache.put(key(1), "a");
    cache.put(key(2), "b");
    generation.incrementAndGet();
    cache.peek(key(1));
    cache.put(key(3), "c");
    cache.put(key(4), "d");

    cache.evict();

    assertThat(cache.peek(key(1))).isNull();
    assertThat(cache.peek(key(2))).isNull();
  }

  @Test
  void evictWithinOneGenerationKeepsBound() {
    for (int i = 0; i < 10; i++) {
      cache.put(key(i), "v" + i);
    }

    assertThat(cache.evict()).isEqualTo(8);
    assertThat(cache.size()).isEqualTo(2);
  }

  @Test
  void computeKeepingCurrentValueMarksItUsed() {
    cache.put(key(1), "a");
    cache.put(key(2), "b");
    generation.incrementAndGet();
    cache.compute(key(1), current -> current);
    cache.put(key(3), "c");

    cache.evict();

    assertThat(cache.peek(key(1))).isEqualTo("a");
    assertThat(cache.peek(key(2))).isNull();
  }

  @Test
  void computeReplacesOrRemoves() {
    cache.compute(key(1), current -> current == null ? "a" : current + "!");
    cache.compute(key(1), current -> current == null ? "a" : current + "!");
    assertThat(cache.peek(key(1))).isEqualTo("a!");

    cache.compute(key(1), current -> null);
    assertThat(cache.peek(key(1))).isNull();
  }

  @Test
  void isOverflowingPastOneAndAHalfTimesTheBound() {
    final BlockLruCache<String> bounded = new BlockLruCache<>(4, 4, generation::get);
    for (int i = 0; i < 6; i++) {
      bounded.put(key(i), "v");
    }
    assertThat(bounded.isOverflowing()).isFalse();

    bounded.put(key(6), "v");
    assertThat(bounded.isOverflowing()).isTrue();
  }

  private static CacheKey key(final int i) {
    return CacheKey.of(Bytes.of(i));
  }
}
