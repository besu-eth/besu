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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static java.util.stream.Collectors.toUnmodifiableSet;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriority;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriorityContext;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBReadController;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.AbstractRocksIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RocksDb columnar key value snapshot. */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalAssignedToNull"})
public class RocksDBColumnarKeyValueSnapshot
    implements SegmentedKeyValueStorage, SnappedKeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBColumnarKeyValueSnapshot.class);
  private static final RocksDBReadController READ_CONTROLLER = RocksDBReadController.global();

  // Cache to store read results during snapshot access.
  // We use Optional<byte[]> as the value type to distinguish between:
  // - a key that maps to an actual zero value (represented as Optional.empty())
  // - a key that has not been read yet (not present in the cache)
  // - a key that has been read and has a non-zero value (Optional.of(bytes))
  private Optional<Cache<Bytes, Optional<byte[]>>> maybeCache = Optional.empty();

  /** The Db. */
  final OptimisticTransactionDB db;

  private final RocksDBSnapshot snapshot;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean isReadCacheEnabledForSnapshots;
  private final RocksDBMetrics metrics;
  private final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper;
  private final ReadOptions readOptions;
  private final ReadOptions lowPriorityReadOptions;

  /**
   * Instantiates a new RocksDb columnar key value snapshot.
   *
   * @param db the db
   * @param metrics the metrics
   */
  RocksDBColumnarKeyValueSnapshot(
      final OptimisticTransactionDB db,
      final boolean isReadCacheEnabledForSnapshots,
      final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper,
      final RocksDBMetrics metrics) {
    this.db = db;
    this.isReadCacheEnabledForSnapshots = isReadCacheEnabledForSnapshots;
    this.metrics = metrics;
    this.columnFamilyMapper = columnFamilyMapper;
    this.snapshot = new RocksDBSnapshot(db);
    this.readOptions =
        new ReadOptions()
            .setAsyncIo(true)
            .setVerifyChecksums(false)
            .setFillCache(true)
            .setSnapshot(snapshot.getSnapshot());
    this.lowPriorityReadOptions =
        new ReadOptions()
            .setAsyncIo(true)
            .setVerifyChecksums(false)
            .setFillCache(false)
            .setSnapshot(snapshot.getSnapshot());
    if (isReadCacheEnabledForSnapshots) {
      maybeCache =
          Optional.of(
              CacheBuilder.newBuilder()
                  .maximumSize(100_000)
                  .expireAfterAccess(5, TimeUnit.MINUTES)
                  .build());
    }
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    try (final OperationTimer.TimingContext ignored = metrics.getReadLatency().startTimer()) {
      final ColumnFamilyHandle handle = columnFamilyMapper.apply(segment);
      if (isReadCacheEnabledForSnapshots && segment.isEligibleToHighSpecFlag()) {
        return getFromCacheOrRead(segment.getId(), key, handle, maybeCache.get());
      } else {
        return READ_CONTROLLER.execute(() -> readSnapshotKey(handle, key));
      }
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<Optional<byte[]>> multiget(final SegmentIdentifier segment, final List<byte[]> keys)
      throws StorageException {
    throwIfClosed();
    if (keys.isEmpty()) {
      return List.of();
    }

    try (final OperationTimer.TimingContext ignored = metrics.getReadLatency().startTimer()) {
      final ColumnFamilyHandle handle = columnFamilyMapper.apply(segment);
      final RocksDBMultiGetPlan readPlan = createMultiGetPlan(keys);
      final List<byte[]> values = READ_CONTROLLER.execute(() -> readSnapshotKeys(handle, readPlan));
      return readPlan.restoreInputOrder(values);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private Optional<byte[]> readSnapshotKey(final ColumnFamilyHandle handle, final byte[] key)
      throws RocksDBException {
    return Optional.ofNullable(snapshot.get(handle, readOptionsForCurrentPriority(), key));
  }

  private List<byte[]> readSnapshotKeys(
      final ColumnFamilyHandle handle, final RocksDBMultiGetPlan readPlan) throws RocksDBException {
    return db.multiGetAsList(
        readOptionsForCurrentPriority(),
        Collections.nCopies(readPlan.keys().size(), handle),
        readPlan.keys());
  }

  private ReadOptions readOptionsForCurrentPriority() {
    return StorageReadPriorityContext.currentPriority() == StorageReadPriority.LOW
        ? lowPriorityReadOptions
        : readOptions;
  }

  private static RocksDBMultiGetPlan createMultiGetPlan(final List<byte[]> keys) {
    return RocksDBMultiGetPlan.preserveInputOrder(keys);
  }

  private Optional<byte[]> getFromCacheOrRead(
      final byte[] segmentId,
      final byte[] key,
      final ColumnFamilyHandle handle,
      final Cache<Bytes, Optional<byte[]>> cache)
      throws RocksDBException {
    final Bytes cacheKey = makeCacheKey(segmentId, key);
    Optional<byte[]> cached = cache.getIfPresent(cacheKey);
    if (cached == null) {
      final byte[] value =
          READ_CONTROLLER.execute(() -> snapshot.get(handle, readOptionsForCurrentPriority(), key));
      cached = Optional.ofNullable(value);
      cache.put(cacheKey, cached);
    }
    return cached;
  }

  private static Bytes makeCacheKey(final byte[] segmentId, final byte[] key) {
    final byte[] combined = new byte[segmentId.length + key.length];
    System.arraycopy(segmentId, 0, combined, 0, segmentId.length);
    System.arraycopy(key, 0, combined, segmentId.length, key.length);
    return Bytes.wrap(combined);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {

    try {
      return READ_CONTROLLER.execute(
          () -> {
            try (final RocksIterator rocksIterator =
                db.newIterator(
                    columnFamilyMapper.apply(segmentIdentifier), readOptionsForCurrentPriority())) {
              rocksIterator.seekForPrev(key.toArrayUnsafe());
              return Optional.of(rocksIterator)
                  .filter(AbstractRocksIterator::isValid)
                  .map(it -> new NearestKeyValue(Bytes.of(it.key()), Optional.of(it.value())));
            }
          });
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    try {
      return READ_CONTROLLER.execute(
          () -> {
            try (final RocksIterator rocksIterator =
                db.newIterator(
                    columnFamilyMapper.apply(segmentIdentifier), readOptionsForCurrentPriority())) {
              rocksIterator.seek(key.toArrayUnsafe());
              return Optional.of(rocksIterator)
                  .filter(AbstractRocksIterator::isValid)
                  .map(it -> new NearestKeyValue(Bytes.of(it.key()), Optional.of(it.value())));
            }
          });
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
    throwIfClosed();
    final RocksIterator rocksIterator =
        READ_CONTROLLER.executeUnchecked(
            () ->
                db.newIterator(columnFamilyMapper.apply(segment), readOptionsForCurrentPriority()));
    READ_CONTROLLER.executeUnchecked(
        () -> {
          rocksIterator.seekToFirst();
          return null;
        });
    return RocksDbIterator.create(rocksIterator).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey) {
    throwIfClosed();

    final RocksIterator rocksIterator =
        READ_CONTROLLER.executeUnchecked(
            () ->
                db.newIterator(columnFamilyMapper.apply(segment), readOptionsForCurrentPriority()));
    READ_CONTROLLER.executeUnchecked(
        () -> {
          rocksIterator.seek(startKey);
          return null;
        });
    return RocksDbIterator.create(rocksIterator).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey, final byte[] endKey) {
    throwIfClosed();
    final Bytes endKeyBytes = Bytes.wrap(endKey);

    final RocksIterator rocksIterator =
        READ_CONTROLLER.executeUnchecked(
            () ->
                db.newIterator(columnFamilyMapper.apply(segment), readOptionsForCurrentPriority()));
    READ_CONTROLLER.executeUnchecked(
        () -> {
          rocksIterator.seek(startKey);
          return null;
        });
    return RocksDbIterator.create(rocksIterator)
        .toStream()
        .takeWhile(e -> endKeyBytes.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
    throwIfClosed();

    final RocksIterator rocksIterator =
        READ_CONTROLLER.executeUnchecked(
            () ->
                db.newIterator(columnFamilyMapper.apply(segment), readOptionsForCurrentPriority()));
    READ_CONTROLLER.executeUnchecked(
        () -> {
          rocksIterator.seekToFirst();
          return null;
        });
    return RocksDbIterator.create(rocksIterator).toStreamKeys();
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throw new StorageException("delete is unsupported in snapshots");
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return streamKeys(segment).filter(returnCondition).collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return stream(segment)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    // snapshots are not mutable, return a no-op transaction:
    return noOpTx;
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    throw new UnsupportedOperationException(
        "RocksDBColumnarKeyValueSnapshot does not support clear");
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    return get(segment, key).isPresent();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      closed.set(true);
      readOptions.close();
      lowPriorityReadOptions.close();
      snapshot.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDBKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }

  @Override
  public SegmentedKeyValueStorageTransaction getSnapshotTransaction() {
    // snapshots are not mutable, return no-op transaction:
    return noOpTx;
  }

  static final SegmentedKeyValueStorageTransaction noOpTx =
      new SegmentedKeyValueStorageTransaction() {

        @Override
        public void put(
            final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
          // no-op
        }

        @Override
        public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
          // no-op
        }

        @Override
        public void commit() throws StorageException {
          // no-op
        }

        @Override
        public void rollback() {
          // no-op
        }

        @Override
        public void close() {
          // no-op
        }
      };
}
