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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cross block cache implementation using Caffeine. */
public class CrossBlockCacheManager implements CacheManager, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CrossBlockCacheManager.class);

  /** How a segment uses the {@code version} argument on reads and writes. */
  public enum SegmentCacheVersioningPolicy {
    /**
     * Cache entries track {@link #globalVersion}; hits require {@code versionedValue.version <=
     * readVersion}; inserts after miss only when {@code readVersion == globalVersion.get()}.
     */
    BLOCK_VERSIONED,
    /**
     * Entries not compare to block version; inserts after miss are always allowed for this segment.
     */
    UNVERSIONED
  }

  /** One versioned Caffeine cache plus its {@link SegmentCacheVersioningPolicy}. */
  private static final class SegmentCache {
    final SegmentCacheVersioningPolicy versioningPolicy;
    final Cache<ByteArrayWrapper, VersionedValue> data;

    SegmentCache(
        final SegmentCacheVersioningPolicy versioningPolicy,
        final Cache<ByteArrayWrapper, VersionedValue> data) {
      this.versioningPolicy = versioningPolicy;
      this.data = data;
    }

    boolean isUnversioned() {
      return versioningPolicy == SegmentCacheVersioningPolicy.UNVERSIONED;
    }
  }

  /** Default threshold of pending tasks before triggering automatic maintenance. */
  private static final int DEFAULT_DRAIN_THRESHOLD = 1000;

  /**
   * An executor that queues maintenance tasks instead of running them immediately. This prevents
   * Caffeine's scheduleDrainBuffers from impacting read/write performance. When the number of
   * pending tasks exceeds a configurable threshold, an async maintenance is submitted.
   */
  private static class ThresholdDrainExecutor implements java.util.concurrent.Executor {
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final int drainThreshold;
    private final Runnable onThresholdReached;

    ThresholdDrainExecutor(final int drainThreshold, final Runnable onThresholdReached) {
      this.drainThreshold = drainThreshold;
      this.onThresholdReached = onThresholdReached;
    }

    @Override
    public void execute(final Runnable command) {
      tasks.add(command);
      if (pendingCount.incrementAndGet() >= drainThreshold) {
        onThresholdReached.run();
      }
    }

    /**
     * Execute all pending maintenance tasks.
     *
     * @return the number of tasks that were drained
     */
    public int drain() {
      int drained = 0;
      Runnable task;
      while ((task = tasks.poll()) != null) {
        task.run();
        drained++;
      }
      pendingCount.addAndGet(-drained);
      return drained;
    }

    /**
     * @return the approximate number of pending tasks
     */
    public int getPendingCount() {
      return pendingCount.get();
    }
  }

  private final AtomicLong globalVersion = new AtomicLong(0);
  private final Map<SegmentIdentifier, SegmentCache> segmentCaches;
  private final ThresholdDrainExecutor drainExecutor;
  private final ExecutorService maintenanceWorker;
  private final AtomicBoolean maintenanceScheduled = new AtomicBoolean(false);

  private final Counter cacheRequestCounter;
  private final Counter cacheHitCounter;
  private final Counter cacheMissCounter;
  private final Counter cacheInsertCounter;
  private final Counter cacheRemovalCounter;

  /**
   * Creates a new VersionedCacheManager with the default drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieBranchCacheSize maximum number of entries in the trie branch node cache
   * @param metricsSystem the metrics system for instrumentation
   */
  public CrossBlockCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieBranchCacheSize,
      final MetricsSystem metricsSystem) {
    this(
        accountCacheSize,
        storageCacheSize,
        trieBranchCacheSize,
        metricsSystem,
        DEFAULT_DRAIN_THRESHOLD);
  }

  /**
   * Creates a new VersionedCacheManager with a custom drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieBranchCacheSize maximum number of entries in the trie branch node cache
   * @param metricsSystem the metrics system for instrumentation
   * @param drainThreshold number of pending maintenance tasks before automatic drain is triggered
   */
  public CrossBlockCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieBranchCacheSize,
      final MetricsSystem metricsSystem,
      final int drainThreshold) {

    this.maintenanceWorker =
        Executors.newSingleThreadExecutor(
            r -> {
              final Thread t = new Thread(r, "cache-maintenance");
              t.setDaemon(true);
              return t;
            });

    this.drainExecutor = new ThresholdDrainExecutor(drainThreshold, this::scheduleAsyncMaintenance);

    this.segmentCaches = new HashMap<>();
    segmentCaches.put(
        ACCOUNT_INFO_STATE,
        new SegmentCache(
            SegmentCacheVersioningPolicy.BLOCK_VERSIONED, createCache(accountCacheSize)));
    segmentCaches.put(
        ACCOUNT_STORAGE_STORAGE,
        new SegmentCache(
            SegmentCacheVersioningPolicy.BLOCK_VERSIONED, createCache(storageCacheSize)));
    segmentCaches.put(
        TRIE_BRANCH_STORAGE,
        new SegmentCache(
            SegmentCacheVersioningPolicy.UNVERSIONED, createCache(trieBranchCacheSize)));

    this.cacheRequestCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_requests_total",
            "Total number of cache requests");

    this.cacheHitCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_hits_total", "Total number of cache hits");

    this.cacheMissCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_misses_total",
            "Total number of cache misses");

    this.cacheInsertCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_inserts_total",
            "Total number of cache insertions");

    this.cacheRemovalCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_removals_total",
            "Total number of cache removals");

    LOG.info(
        "Cache maintenance will trigger asynchronously after {} pending tasks", drainThreshold);
  }

  /** Whether a read may use {@code versionedValue} from the cache for this segment policy. */
  private static boolean cacheHitAcceptable(
      final boolean unversioned, final VersionedValue versionedValue, final long readVersion) {
    if (versionedValue == null) {
      return false;
    }
    if (unversioned) {
      return true;
    }
    return versionedValue.version <= readVersion;
  }

  /**
   * Whether a write may replace {@code existingValue} at {@code writeVersion} for this segment
   * policy.
   */
  private static boolean writeAcceptable(
      final boolean unversioned, final VersionedValue existingValue, final long writeVersion) {
    if (unversioned) {
      return true;
    }
    return existingValue == null || existingValue.version < writeVersion;
  }

  /** After a storage read on miss, whether the result may be written back to this segment cache. */
  private boolean shouldUpdateCacheAfterRead(final boolean unversioned, final long readVersion) {
    return unversioned || readVersion == globalVersion.get();
  }

  private Optional<SegmentCache> segmentCache(final SegmentIdentifier segment) {
    return Optional.ofNullable(segmentCaches.get(segment));
  }

  private Cache<ByteArrayWrapper, VersionedValue> createCache(final long maxSize) {
    return Caffeine.newBuilder()
        .initialCapacity((int) (maxSize * 0.1))
        .maximumSize(maxSize)
        .executor(drainExecutor)
        .build();
  }

  /**
   * Schedules an async maintenance if one is not already scheduled. Uses an AtomicBoolean to
   * prevent flooding the maintenance worker with redundant tasks.
   */
  private void scheduleAsyncMaintenance() {
    if (maintenanceScheduled.compareAndSet(false, true)) {
      try {
        maintenanceWorker.execute(
            () -> {
              try {
                doMaintenance();
              } finally {
                maintenanceScheduled.set(false);
              }
            });
      } catch (final Exception e) {
        maintenanceScheduled.set(false);
        LOG.warn("Failed to schedule async cache maintenance", e);
      }
    }
  }

  /** Performs the actual maintenance work: drains pending tasks and runs Caffeine's cleanUp. */
  private void doMaintenance() {
    try {
      final int drained = drainExecutor.drain();
      segmentCaches.values().forEach(sc -> sc.data.cleanUp());
      if (drained > 0) {
        LOG.trace("Cache maintenance drained {} tasks", drained);
      }
    } catch (final Exception e) {
      LOG.warn("Error during cache maintenance", e);
    }
  }

  /**
   * Trigger cache maintenance asynchronously. Can be called explicitly, for example after a block
   * import, for cleanup without blocking the caller. Also triggered automatically when the number
   * of pending tasks exceeds the configured threshold.
   */
  @Override
  public void performMaintenance() {
    scheduleAsyncMaintenance();
  }

  /**
   * Returns the approximate number of pending maintenance tasks.
   *
   * @return pending task count
   */
  public int getPendingMaintenanceCount() {
    return drainExecutor.getPendingCount();
  }

  /**
   * Shuts down the maintenance worker. Should be called when the cache manager is no longer needed.
   */
  @Override
  public void close() {
    LOG.info("Shutting down cache maintenance worker");
    maintenanceWorker.shutdown();
    try {
      if (!maintenanceWorker.awaitTermination(5, TimeUnit.SECONDS)) {
        maintenanceWorker.shutdownNow();
      }
    } catch (final InterruptedException e) {
      maintenanceWorker.shutdownNow();
      Thread.currentThread().interrupt();
    }
    // Final synchronous drain to process any remaining tasks
    doMaintenance();
  }

  @Override
  public long getCurrentVersion() {
    return globalVersion.get();
  }

  @Override
  public long incrementAndGetVersion() {
    return globalVersion.incrementAndGet();
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    segmentCache(segment).ifPresent(sc -> sc.data.invalidateAll());
  }

  @Override
  public Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final byte[] key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {

    final Optional<SegmentCache> osc = segmentCache(segment);

    cacheRequestCounter.inc();

    if (osc.isEmpty()) {
      cacheMissCounter.inc();
      return storageGetter.get();
    }

    final SegmentCache sc = osc.get();

    final boolean unversioned = sc.isUnversioned();
    final Cache<ByteArrayWrapper, VersionedValue> cache = sc.data;
    final ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
    final VersionedValue versionedValue = cache.getIfPresent(wrapper);

    if (cacheHitAcceptable(unversioned, versionedValue, version)) {
      cacheHitCounter.inc();
      return versionedValue.isRemoval
          ? Optional.empty()
          : Optional.of(Bytes.wrap(versionedValue.value));
    }

    cacheMissCounter.inc();
    final Optional<Bytes> result = storageGetter.get();

    if (shouldUpdateCacheAfterRead(unversioned, version)) {
      cacheInsertCounter.inc();
      final byte[] valueToCache = result.map(Bytes::toArrayUnsafe).orElse(null);
      final boolean isRemoval = result.isEmpty();

      cache
          .asMap()
          .compute(
              wrapper,
              (k, existingValue) ->
                  writeAcceptable(unversioned, existingValue, version)
                      ? new VersionedValue(valueToCache, version, isRemoval)
                      : existingValue);
    }

    return result;
  }

  @Override
  public List<Optional<byte[]>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<byte[]> keys,
      final long version,
      final Function<List<byte[]>, List<Optional<byte[]>>> batchFetcher) {

    final Optional<SegmentCache> osc = segmentCache(segment);

    if (osc.isEmpty()) {
      keys.forEach(k -> cacheMissCounter.inc());
      return batchFetcher.apply(keys);
    }

    final SegmentCache sc = osc.get();

    final boolean unversioned = sc.isUnversioned();
    final Cache<ByteArrayWrapper, VersionedValue> cache = sc.data;

    final List<Optional<byte[]>> results = new ArrayList<>(keys.size());
    final List<byte[]> keysToFetch = new ArrayList<>();
    final List<Integer> indicesToFetch = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final byte[] key = keys.get(i);
      cacheRequestCounter.inc();

      final ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
      final VersionedValue versionedValue = cache.getIfPresent(wrapper);

      if (cacheHitAcceptable(unversioned, versionedValue, version)) {
        cacheHitCounter.inc();
        results.add(
            versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.value));
      } else {
        cacheMissCounter.inc();
        results.add(null);
        keysToFetch.add(key);
        indicesToFetch.add(i);
      }
    }

    if (!keysToFetch.isEmpty()) {
      final List<Optional<byte[]>> fetchedValues = batchFetcher.apply(keysToFetch);
      final boolean writeBack = shouldUpdateCacheAfterRead(unversioned, version);

      for (int i = 0; i < fetchedValues.size(); i++) {
        final Optional<byte[]> fetchedValue = fetchedValues.get(i);
        final int resultIndex = indicesToFetch.get(i);
        final byte[] key = keysToFetch.get(i);

        results.set(resultIndex, fetchedValue);

        if (writeBack) {
          cacheInsertCounter.inc();
          final ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
          final byte[] valueToCache = fetchedValue.orElse(null);
          final boolean isRemoval = fetchedValue.isEmpty();

          cache
              .asMap()
              .compute(
                  wrapper,
                  (k, existingValue) ->
                      writeAcceptable(unversioned, existingValue, version)
                          ? new VersionedValue(valueToCache, version, isRemoval)
                          : existingValue);
        }
      }
    }

    return results;
  }

  @Override
  public void putInCache(
      final SegmentIdentifier segment, final byte[] key, final byte[] value, final long version) {
    segmentCache(segment)
        .ifPresent(
            sc -> {
              final boolean unversioned = sc.isUnversioned();
              final ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
              sc.data
                  .asMap()
                  .compute(
                      wrapper,
                      (k, existingValue) -> {
                        if (!writeAcceptable(unversioned, existingValue, version)) {
                          return existingValue;
                        }
                        cacheInsertCounter.inc();
                        return new VersionedValue(value, version, false);
                      });
            });
  }

  @Override
  public void removeFromCache(
      final SegmentIdentifier segment, final byte[] key, final long version) {
    segmentCache(segment)
        .ifPresent(
            sc -> {
              final boolean unversioned = sc.isUnversioned();
              final ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
              sc.data
                  .asMap()
                  .compute(
                      wrapper,
                      (k, existingValue) -> {
                        if (!writeAcceptable(unversioned, existingValue, version)) {
                          return existingValue;
                        }
                        cacheRemovalCounter.inc();
                        return new VersionedValue(null, version, true);
                      });
            });
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    return segmentCache(segment).map(sc -> sc.data.estimatedSize()).orElse(0L);
  }

  @Override
  public boolean isCached(final SegmentIdentifier segment, final byte[] key) {
    return segmentCache(segment)
        .map(sc -> sc.data.getIfPresent(new ByteArrayWrapper(key)) != null)
        .orElse(false);
  }

  @Override
  public Optional<VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final byte[] key) {
    return segmentCache(segment)
        .flatMap(sc -> Optional.ofNullable(sc.data.getIfPresent(new ByteArrayWrapper(key))));
  }
}
