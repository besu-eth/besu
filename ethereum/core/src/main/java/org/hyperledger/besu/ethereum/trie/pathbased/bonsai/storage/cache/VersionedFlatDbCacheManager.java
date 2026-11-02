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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Versioned cache implementation using Caffeine. */
public class VersionedFlatDbCacheManager implements FlatDbCacheManager, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(VersionedFlatDbCacheManager.class);

  /** Default threshold of pending tasks before triggering automatic maintenance. */
  private static final int DEFAULT_DRAIN_THRESHOLD = 1000;

  /** Upper bound for Caffeine {@code initialCapacity} (must fit in a positive int). */
  private static final long MAX_INITIAL_CAPACITY = Integer.MAX_VALUE;

  private static final long TRIE_NODE_CACHE_SIZE = 300_000;

  private static final long RECENT_WRITES_SIZE = 100_000;

  /**
   * A miss looks up every kept block, so only this many blocks are kept, however few entries they
   * wrote.
   */
  private static final int RECENT_BLOCKS = 16;

  /** Looked up in a block's writes to tell a key it did not write from one it removed. */
  private static final Bytes NOT_WRITTEN = Bytes.wrap(new byte[0]);

  private final AtomicLong globalVersion = new AtomicLong(0);

  /** Nested commit-bypass count; when readers ignore the cache entirely. */
  private final AtomicInteger commitCacheBypassCount = new AtomicInteger(0);

  private final Cache<CacheKey, VersionedValue> accountCache;
  private final Cache<CacheKey, VersionedValue> storageCache;

  /**
   * Trie nodes committed by recent blocks, keyed by hash, one map per block, the newest first.
   * Every node on a modified path gets a new hash, so the next block's walk down that path would
   * otherwise read back what was just written. The nodes are kept by how recently they were
   * written, not in a cache that admits what it has seen often: every node is new to it, so such a
   * cache turned away most of the nodes of a block that changes many accounts. The newest block's
   * nodes are kept whole, older blocks' while all add up to {@link #trieNodeCacheSize}, for at most
   * {@link #RECENT_BLOCKS} blocks.
   */
  private volatile List<Map<CacheKey, Bytes>> trieNodesByBlock = List.of();

  /**
   * The accounts and storage slots written by recent commits, the newest first, one per version
   * with no gap between them. They are looked up before the account and storage caches, which admit
   * what they have seen often and so turned away most of the accounts a block had just written once
   * they were full. Kept like {@link #trieNodesByBlock}, the newest commit's whole and older ones'
   * while all add up to {@link #RECENT_WRITES_SIZE}. Replaced under {@link #recentWritesLock}.
   */
  private volatile List<CommittedWrites> recentWrites = List.of();

  private final Object recentWritesLock = new Object();

  private final long trieNodeCacheSize;

  /**
   * Bumped by a clear, so that batches queued before it are dropped instead of refilling the cache.
   * Changed and checked under {@link #trieNodeLock}.
   */
  private final AtomicLong trieNodeGeneration = new AtomicLong(0);

  private final Object trieNodeLock = new Object();

  private final ThresholdDrainExecutor drainExecutor;
  private final ExecutorService maintenanceWorker;
  private final AtomicBoolean maintenanceScheduled = new AtomicBoolean(false);

  private final Counter cacheRequestCounter;
  private final Counter cacheHitCounter;
  private final Counter cacheMissCounter;
  private final Counter cacheInsertCounter;
  private final Counter cacheRemovalCounter;
  private final Counter trieNodeCacheHitCounter;
  private final Counter trieNodeCacheMissCounter;

  /**
   * Creates a new VersionedFlatDbCacheManager with the default drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param metricsSystem the metrics system for instrumentation
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize, final long storageCacheSize, final MetricsSystem metricsSystem) {
    this(accountCacheSize, storageCacheSize, metricsSystem, DEFAULT_DRAIN_THRESHOLD);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager with a custom drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param metricsSystem the metrics system for instrumentation
   * @param drainThreshold number of pending maintenance tasks before automatic drain is triggered
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final MetricsSystem metricsSystem,
      final int drainThreshold) {
    this(accountCacheSize, storageCacheSize, metricsSystem, drainThreshold, TRIE_NODE_CACHE_SIZE);
  }

  @VisibleForTesting
  VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final MetricsSystem metricsSystem,
      final int drainThreshold,
      final long trieNodeCacheSize) {

    requirePositiveCacheMaxSize("accountCacheSize", accountCacheSize);
    requirePositiveCacheMaxSize("storageCacheSize", storageCacheSize);
    this.trieNodeCacheSize = trieNodeCacheSize;

    this.maintenanceWorker =
        Executors.newSingleThreadExecutor(
            r -> {
              final Thread t = new Thread(r, "cache-maintenance");
              t.setDaemon(true);
              return t;
            });

    this.drainExecutor = new ThresholdDrainExecutor(drainThreshold, this::scheduleAsyncMaintenance);

    this.accountCache = createCache(accountCacheSize);
    this.storageCache = createCache(storageCacheSize);

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

    this.trieNodeCacheHitCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_trie_node_hits_total",
            "Total number of trie node cache hits");

    this.trieNodeCacheMissCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_trie_node_misses_total",
            "Total number of trie node cache misses");

    LOG.info(
        "Cache maintenance will trigger asynchronously after {} pending tasks", drainThreshold);
  }

  private <V> Cache<CacheKey, V> createCache(final long maxSize) {
    return Caffeine.newBuilder()
        .initialCapacity(initialCapacityFor(maxSize))
        .maximumSize(maxSize)
        .executor(drainExecutor)
        .build();
  }

  /**
   * Initial capacity ~10% of {@code maxSize}, at least 1, never exceeding {@link
   * Integer#MAX_VALUE}.
   */
  private static int initialCapacityFor(final long maxSize) {
    final long tenth = maxSize / 10;
    final long capped = Math.min(MAX_INITIAL_CAPACITY, tenth);
    return (int) Math.max(1L, capped);
  }

  private static void requirePositiveCacheMaxSize(final String name, final long maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got " + maxSize);
    }
  }

  private Cache<CacheKey, VersionedValue> cacheForSegment(final SegmentIdentifier segment) {
    if (segment == ACCOUNT_INFO_STATE) {
      return accountCache;
    }
    if (segment == ACCOUNT_STORAGE_STORAGE) {
      return storageCache;
    }
    return null;
  }

  /**
   * Schedules an async maintenance if one is not already scheduled. Uses an AtomicBoolean to
   * prevent flooding the maintenance worker with redundant tasks.
   */
  @Override
  public void scheduleAsyncMaintenance() {
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
      accountCache.cleanUp();
      storageCache.cleanUp();
      if (drained > 0) {
        LOG.trace("Cache maintenance drained {} tasks", drained);
      }
    } catch (final Exception e) {
      LOG.warn("Error during cache maintenance", e);
    }
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
  public void beginCommitCacheBypass() {
    commitCacheBypassCount.incrementAndGet();
  }

  @Override
  public void endCommitCacheBypass() {
    final int remaining = commitCacheBypassCount.decrementAndGet();
    if (remaining < 0) {
      commitCacheBypassCount.set(0);
      LOG.warn("endCommitCacheBypass() called without a matching begin");
    }
  }

  @Override
  public boolean isCommitCacheBypassActive() {
    return commitCacheBypassCount.get() > 0;
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      synchronized (trieNodeLock) {
        trieNodeGeneration.incrementAndGet();
        trieNodesByBlock = List.of();
      }
      return;
    }
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      synchronized (recentWritesLock) {
        recentWrites = List.of();
      }
      cache.invalidateAll();
    }
  }

  @Override
  public Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {

    cacheRequestCounter.inc();

    // During head commit publish, ignore the cache completely: no stale hits, no miss inserts.
    if (isCommitCacheBypassActive()) {
      cacheMissCounter.inc();
      return storageGetter.get();
    }

    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);

    if (cache == null) {
      cacheMissCounter.inc();
      return storageGetter.get();
    }

    final CacheKey cacheKey = CacheKey.of(key);
    final Bytes written = recentlyWritten(segment, cacheKey, version);
    if (written != NOT_WRITTEN) {
      cacheHitCounter.inc();
      return Optional.ofNullable(written);
    }

    final VersionedValue versionedValue = cache.getIfPresent(cacheKey);

    if (versionedValue != null && versionedValue.version <= version) {
      cacheHitCounter.inc();
      return versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue());
    }

    cacheMissCounter.inc();
    final Optional<Bytes> result = storageGetter.get();

    if (version == globalVersion.get()) {
      cacheInsertCounter.inc();
      final Bytes valueToCache = result.orElse(null);
      final boolean isRemoval = result.isEmpty();

      cache
          .asMap()
          .compute(
              cacheKey,
              (k, existingValue) -> {
                if (existingValue == null || existingValue.version < version) {
                  return new VersionedValue(valueToCache, version, isRemoval);
                }
                return existingValue;
              });
    }

    return result;
  }

  @Override
  public List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {

    if (isCommitCacheBypassActive()) {
      keys.forEach(
          k -> {
            cacheRequestCounter.inc();
            cacheMissCounter.inc();
          });
      final List<Optional<Bytes>> fetched = batchFetcher.apply(keys);
      if (fetched.size() != keys.size()) {
        return Collections.nCopies(keys.size(), null);
      }
      return fetched;
    }

    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);

    if (cache == null) {
      keys.forEach(k -> cacheMissCounter.inc());
      return batchFetcher.apply(keys);
    }

    final List<Optional<Bytes>> results = new ArrayList<>(keys.size());
    final List<Bytes> keysToFetch = new ArrayList<>();
    final List<Integer> indicesToFetch = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final Bytes key = keys.get(i);
      cacheRequestCounter.inc();

      final CacheKey cacheKey = CacheKey.of(key);
      final Bytes written = recentlyWritten(segment, cacheKey, version);
      if (written != NOT_WRITTEN) {
        cacheHitCounter.inc();
        results.add(Optional.ofNullable(written));
        continue;
      }

      final VersionedValue versionedValue = cache.getIfPresent(cacheKey);

      if (versionedValue != null && versionedValue.version <= version) {
        cacheHitCounter.inc();
        results.add(
            versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue()));
      } else {
        cacheMissCounter.inc();
        results.add(null);
        keysToFetch.add(key);
        indicesToFetch.add(i);
      }
    }

    if (!keysToFetch.isEmpty()) {
      final List<Optional<Bytes>> fetchedValues = batchFetcher.apply(keysToFetch);
      if (fetchedValues.size() != keysToFetch.size()) {
        return results;
      }

      final boolean shouldUpdateCache = version == globalVersion.get();

      for (int i = 0; i < fetchedValues.size(); i++) {
        final Optional<Bytes> fetchedValue = fetchedValues.get(i);
        final int resultIndex = indicesToFetch.get(i);
        final Bytes key = keysToFetch.get(i);

        results.set(resultIndex, fetchedValue);

        if (shouldUpdateCache && fetchedValue != null) {
          cacheInsertCounter.inc();
          final CacheKey cacheKey = CacheKey.of(key);
          final Bytes valueToCache = fetchedValue.orElse(null);
          final boolean isRemoval = fetchedValue.isEmpty();

          cache
              .asMap()
              .compute(
                  cacheKey,
                  (k, existingValue) -> {
                    if (existingValue == null || existingValue.version < version) {
                      return new VersionedValue(valueToCache, version, isRemoval);
                    }
                    return existingValue;
                  });
        }
      }
    }

    return results;
  }

  @Override
  public void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      cacheWrite(cache, CacheKey.of(key), value, version);
    }
  }

  @Override
  public void removeFromCache(
      final SegmentIdentifier segment, final Bytes key, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      cacheWrite(cache, CacheKey.of(key), null, version);
    }
  }

  @Override
  public void putCommittedWrites(
      final Map<SegmentIdentifier, Map<Bytes, Bytes>> writes, final long version) {
    final Map<CacheKey, Bytes> accounts = cacheWrites(writes, ACCOUNT_INFO_STATE, version);
    final Map<CacheKey, Bytes> storage = cacheWrites(writes, ACCOUNT_STORAGE_STORAGE, version);
    final CommittedWrites committed = new CommittedWrites(version, accounts, storage);
    synchronized (recentWritesLock) {
      final List<CommittedWrites> older = recentWrites;
      // a skipped version could hold a newer write than the older commits
      recentWrites =
          withNewestBlock(
              committed,
              !older.isEmpty() && older.getFirst().version() == version - 1 ? older : List.of(),
              CommittedWrites::size,
              RECENT_WRITES_SIZE);
    }
  }

  /** Caches a segment's writes and returns them by cache key, a null value for a removal. */
  private Map<CacheKey, Bytes> cacheWrites(
      final Map<SegmentIdentifier, Map<Bytes, Bytes>> writes,
      final SegmentIdentifier segment,
      final long version) {
    final Map<Bytes, Bytes> segmentWrites = writes.getOrDefault(segment, Map.of());
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    final Map<CacheKey, Bytes> byCacheKey = HashMap.newHashMap(segmentWrites.size());
    segmentWrites.forEach(
        (key, value) -> {
          final CacheKey cacheKey = CacheKey.of(key);
          cacheWrite(cache, cacheKey, value, version);
          byCacheKey.put(cacheKey, value);
        });
    return byCacheKey;
  }

  /** Caches a committed value, or its removal if null, unless a newer version is cached. */
  private void cacheWrite(
      final Cache<CacheKey, VersionedValue> cache,
      final CacheKey cacheKey,
      final Bytes value,
      final long version) {
    cache
        .asMap()
        .compute(
            cacheKey,
            (k, existingValue) -> {
              if (existingValue == null || existingValue.version < version) {
                if (value == null) {
                  cacheRemovalCounter.inc();
                  return new VersionedValue(null, version, true);
                }
                cacheInsertCounter.inc();
                return new VersionedValue(value, version, false);
              }
              return existingValue;
            });
  }

  /**
   * What the newest kept commit at or below {@code version} wrote to the key, null for a removal,
   * or {@link #NOT_WRITTEN} if none of them wrote it.
   */
  private Bytes recentlyWritten(
      final SegmentIdentifier segment, final CacheKey cacheKey, final long version) {
    for (final CommittedWrites committed : recentWrites) {
      if (committed.version() <= version) {
        final Bytes value = committed.of(segment).getOrDefault(cacheKey, NOT_WRITTEN);
        if (value != NOT_WRITTEN) {
          return value;
        }
      }
    }
    return NOT_WRITTEN;
  }

  @Override
  public Optional<Bytes> getTrieNode(final Bytes32 nodeHash) {
    final CacheKey key = CacheKey.of(nodeHash);
    for (final Map<CacheKey, Bytes> blockNodes : trieNodesByBlock) {
      final Bytes node = blockNodes.get(key);
      if (node != null) {
        trieNodeCacheHitCounter.inc();
        return Optional.of(node);
      }
    }
    trieNodeCacheMissCounter.inc();
    return Optional.empty();
  }

  /** Runs on the maintenance worker, so the committing thread does not pay for the inserts. */
  @Override
  public void putTrieNodes(final List<TrieNode> nodes) {
    if (nodes.isEmpty()) {
      return;
    }
    final long generation = trieNodeGeneration.get();
    try {
      maintenanceWorker.execute(
          () -> {
            synchronized (trieNodeLock) {
              if (generation == trieNodeGeneration.get()) {
                trieNodesByBlock = withBlockNodes(nodes);
              }
            }
            doMaintenance();
          });
    } catch (final RejectedExecutionException e) {
      LOG.debug("Cache maintenance worker is shut down, dropping committed trie nodes");
    }
  }

  /** The kept trie nodes with a block's committed nodes added as the newest. */
  private List<Map<CacheKey, Bytes>> withBlockNodes(final List<TrieNode> nodes) {
    final Map<CacheKey, Bytes> blockNodes = HashMap.newHashMap(nodes.size());
    nodes.forEach(node -> blockNodes.put(CacheKey.of(node.hash()), node.node()));
    return withNewestBlock(blockNodes, trieNodesByBlock, Map::size, trieNodeCacheSize);
  }

  /**
   * The newest block followed by the older ones while all their entries add up to at most {@code
   * maxSize}, for at most {@link #RECENT_BLOCKS} blocks.
   */
  private static <B> List<B> withNewestBlock(
      final B newest, final List<B> older, final ToLongFunction<B> size, final long maxSize) {
    final List<B> kept = new ArrayList<>();
    kept.add(newest);
    long total = size.applyAsLong(newest);
    for (final B block : older) {
      total += size.applyAsLong(block);
      if (total > maxSize || kept.size() == RECENT_BLOCKS) {
        break;
      }
      kept.add(block);
    }
    return List.copyOf(kept);
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      return trieNodesByBlock.stream().mapToLong(Map::size).sum();
    }
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    return cache != null ? cache.estimatedSize() : 0;
  }

  @Override
  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    return cache != null && cache.getIfPresent(CacheKey.of(key)) != null;
  }

  @Override
  public Optional<VersionedValue> getCachedValue(final SegmentIdentifier segment, final Bytes key) {
    final Cache<CacheKey, VersionedValue> cache = cacheForSegment(segment);
    return cache != null
        ? Optional.ofNullable(cache.getIfPresent(CacheKey.of(key)))
        : Optional.empty();
  }

  /** The accounts and storage slots one commit wrote, by cache key, a null value for a removal. */
  private record CommittedWrites(
      long version, Map<CacheKey, Bytes> accounts, Map<CacheKey, Bytes> storage) {

    Map<CacheKey, Bytes> of(final SegmentIdentifier segment) {
      return segment == ACCOUNT_INFO_STATE ? accounts : storage;
    }

    long size() {
      return (long) accounts.size() + storage.size();
    }
  }

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
  }
}
