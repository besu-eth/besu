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

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * No-op implementation of FlatDbCacheManager that bypasses caching entirely. Used when caching is
 * disabled in configuration.
 */
public interface FlatDbCacheManager {

  FlatDbCacheManager NO_OP_CACHE = new FlatDbCacheManager() {};

  default long getCurrentVersion() {
    return 0;
  }

  default long incrementAndGetVersion() {
    return 0;
  }

  /**
   * While a head commit is publishing storage/cache, readers must ignore the cross-block cache
   * entirely (no hits, no miss inserts) and read storage only. Nested calls are reference-counted.
   */
  default void beginCommitCacheBypass() {
    // No-op
  }

  /** Ends a matching {@link #beginCommitCacheBypass()}. */
  default void endCommitCacheBypass() {
    // No-op
  }

  default boolean isCommitCacheBypassActive() {
    return false;
  }

  /**
   * Commits storage and publishes the matching cache writes as one step with respect to other
   * publishes and {@link #invalidateAll(LongConsumer)}.
   *
   * <p>The storage commit runs first, then a new version is allocated and handed to {@code
   * publisher}, which must publish the committed writes at exactly that version. Readers bypass the
   * cache for the whole sequence. Concurrent calls are serialized, so version order always matches
   * storage commit order.
   *
   * @param storageCommit commits the underlying storage transaction
   * @param publisher receives the new version; publishes the committed writes at that version
   */
  default void commitAndPublish(final Runnable storageCommit, final LongConsumer publisher) {
    // No cache to publish into: commit only, without serializing concurrent commits
    storageCommit.run();
  }

  /**
   * Turns the cache on (e.g. once the initial sync is done). Any leftover entry is dropped first.
   */
  default void enable() {
    // No-op
  }

  /**
   * Turns the cache off (e.g. while snap syncing): reads go to storage, nothing is cached and
   * commits are not serialized. All entries are dropped.
   */
  default void disable() {
    // No-op
  }

  default boolean isEnabled() {
    return false;
  }

  /**
   * Drops every cached entry. The version is advanced first (and handed to {@code onNewVersion}) so
   * that a read which loaded a pre-clear value and has not inserted it yet is rejected instead of
   * repopulating the cache right after the clear.
   *
   * @param onNewVersion receives the version allocated for the clear
   */
  default void invalidateAll(final LongConsumer onNewVersion) {
    // No-op
  }

  default void clear(final SegmentIdentifier segment) {
    // No-op
  }

  default void scheduleAsyncMaintenance() {
    // No-op
  }

  default Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {
    // Always bypass cache and go directly to storage
    return storageGetter.get();
  }

  /**
   * Batch read through the cache.
   *
   * <p>The returned list is always aligned with {@code keys}, but an element may be {@code null}
   * (not {@link Optional#empty()}) when the value is unknown: the batch fetcher does not support
   * multi-get in the current flat-db mode, or returned a list of a different size. {@code
   * Optional.empty()} means the key is known to be absent. Callers must handle both.
   */
  default List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {
    final List<Optional<Bytes>> fetched = batchFetcher.apply(keys);
    if (fetched.size() != keys.size()) {
      return Collections.nCopies(keys.size(), null);
    }
    return fetched;
  }

  default void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    // No-op
  }

  default void removeFromCache(
      final SegmentIdentifier segment, final Bytes key, final long version) {
    // No-op
  }

  default long getCacheSize(final SegmentIdentifier segment) {
    return 0;
  }

  default boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    return false;
  }

  default Optional<VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final Bytes key) {
    return Optional.empty();
  }

  /** Value wrapper with version and removal flag. */
  final class VersionedValue {
    final Bytes value;
    final long version;
    final boolean isRemoval;

    VersionedValue(final Bytes value, final long version, final boolean isRemoval) {
      this.value = value;
      this.version = version;
      this.isRemoval = isRemoval;
    }

    public Bytes getValue() {
      return value;
    }

    public long getVersion() {
      return version;
    }

    public boolean isRemoval() {
      return isRemoval;
    }
  }
}

/**
 * Holds bytes by reference and a precomputed hash; callers must not mutate the source array after
 * handing it off.
 */
final class CacheKey {
  private final byte[] data;
  private final int hashCode;

  static CacheKey of(final Bytes bytes) {
    return new CacheKey(bytes.toArrayUnsafe());
  }

  private CacheKey(final byte[] data) {
    this.data = data;
    this.hashCode = Arrays.hashCode(data);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheKey that)) return false;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
