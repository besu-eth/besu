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
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * No-op implementation of CacheManager that bypasses caching entirely. Used when caching is
 * disabled in configuration.
 */
public interface CacheManager {

  CacheManager NO_OP_CACHE = new CacheManager() {};

  default long getCurrentVersion() {
    return 0;
  }

  default long incrementAndGetVersion() {
    return 0;
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

  default List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {
    // Always bypass cache and go directly to storage
    return batchFetcher.apply(keys);
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
    if (!(o instanceof CacheKey)) return false;
    return Arrays.equals(data, ((CacheKey) o).data);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
