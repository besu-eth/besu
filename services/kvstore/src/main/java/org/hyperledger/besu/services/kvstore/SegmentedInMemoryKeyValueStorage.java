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
package org.hyperledger.besu.services.kvstore;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hyperledger.besu.services.kvstore.KeyComparator.compareKeyLeftToRight;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/** Segmented in memory key value storage. */
public class SegmentedInMemoryKeyValueStorage
    implements SnappedKeyValueStorage, SnappableKeyValueStorage, SegmentedKeyValueStorage {
  /** protected access for the backing hash map. */
  final ConcurrentMap<SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>> hashValueStore;

  /** protected access to the rw lock. */
  protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * Create a navigable segment map, with a compatible Bytes comparator
   *
   * @return segment map
   */
  protected static NavigableMap<Bytes, Optional<byte[]>> newSegmentMap() {
    return newSegmentMap(Collections.emptyMap());
  }

  /**
   * Create and populate a navigable segment map, with a compatible Bytes comparator.
   *
   * @param sourceMap sourcemap to initialize the segmentmap with.
   * @return populated segment map
   */
  protected static NavigableMap<Bytes, Optional<byte[]>> newSegmentMap(
      final Map<Bytes, Optional<byte[]>> sourceMap) {

    Comparator<Bytes> byteWiseComparator =
        Comparator.comparing(Bytes::toArrayUnsafe, Arrays::compareUnsigned);

    NavigableMap<Bytes, Optional<byte[]>> segMap = new ConcurrentSkipListMap<>(byteWiseComparator);

    segMap.putAll(sourceMap);
    return segMap;
  }

  /** Instantiates a new In memory key value storage. */
  public SegmentedInMemoryKeyValueStorage() {
    this(new ConcurrentHashMap<>());
  }

  /**
   * Instantiates a new In memory key value storage.
   *
   * @param hashValueStore the hash value store
   */
  protected SegmentedInMemoryKeyValueStorage(
      final ConcurrentMap<SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>>
          hashValueStore) {
    this.hashValueStore = hashValueStore;
  }

  /**
   * Instantiates a new In memory key value storage with specific set of segments.
   *
   * @param segments the segments to be used
   */
  public SegmentedInMemoryKeyValueStorage(final List<SegmentIdentifier> segments) {
    this(
        segments.stream()
            .collect(
                Collectors
                    .<SegmentIdentifier, SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>>
                        toConcurrentMap(s -> s, s -> newSegmentMap())));
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    final Lock lock = rwLock.writeLock();
    lock.lock();
    try {
      Optional.ofNullable(hashValueStore.get(segmentIdentifier)).ifPresent(Map::clear);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return get(segmentIdentifier, key).isPresent();
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      return hashValueStore
          .computeIfAbsent(segmentIdentifier, s -> newSegmentMap())
          .getOrDefault(Bytes.wrap(key), Optional.empty());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(
        segmentIdentifier,
        e ->
            compareKeyLeftToRight(e.getKey(), key) <= 0
                && e.getKey().commonPrefixLength(key) >= e.getKey().size(),
        e -> compareKeyLeftToRight(e.getKey(), key) < 0,
        false);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(
        segmentIdentifier,
        e ->
            compareKeyLeftToRight(e.getKey(), key) >= 0
                && e.getKey().commonPrefixLength(key) >= e.getKey().size(),
        e -> compareKeyLeftToRight(e.getKey(), key) >= 0,
        true);
  }

  private Optional<NearestKeyValue> getNearest(
      final SegmentIdentifier segmentIdentifier,
      final Predicate<Map.Entry<Bytes, Optional<byte[]>>> samePrefixPredicate,
      final Predicate<Map.Entry<Bytes, Optional<byte[]>>> fallbackPredicate,
      final boolean useMin)
      throws StorageException {

    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      final Map<Bytes, Optional<byte[]>> segmentMap =
          this.hashValueStore.computeIfAbsent(segmentIdentifier, s -> newSegmentMap());

      final Function<Predicate<Map.Entry<Bytes, Optional<byte[]>>>, Optional<NearestKeyValue>>
          findNearest =
              (predicate) -> {
                final Stream<Map.Entry<Bytes, Optional<byte[]>>> filteredStream =
                    segmentMap.entrySet().stream().filter(predicate);
                // Depending on the useMin flag, find either the minimum or maximum entry according
                // to key order
                final Optional<Map.Entry<Bytes, Optional<byte[]>>> sortedStream =
                    useMin
                        ? filteredStream.min(
                            (t1, t2) -> compareKeyLeftToRight(t1.getKey(), t2.getKey()))
                        : filteredStream.max(
                            (t1, t2) -> compareKeyLeftToRight(t1.getKey(), t2.getKey()));
                return sortedStream.map(
                    entry -> new NearestKeyValue(entry.getKey(), entry.getValue()));
              };

      // First, attempt to find a key-value pair that matches the same prefix
      final Optional<NearestKeyValue> withSamePrefix = findNearest.apply(samePrefixPredicate);
      if (withSamePrefix.isPresent()) {
        return withSamePrefix;
      }
      // If a matching entry with a common prefix is not found, the next step is to search for the
      // nearest key that comes after or before the requested one.
      return findNearest.apply(fallbackPredicate);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return stream(segmentIdentifier)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getKey)
        .collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return stream(segmentIdentifier)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      return ImmutableSet.copyOf(
              hashValueStore.computeIfAbsent(segmentIdentifier, s -> newSegmentMap()).entrySet())
          .stream()
          .filter(bytesEntry -> bytesEntry.getValue().isPresent())
          .sorted(Map.Entry.comparingByKey())
          .map(
              bytesEntry ->
                  Pair.of(bytesEntry.getKey().toArrayUnsafe(), bytesEntry.getValue().get()));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    final Bytes startKeyBytes = Bytes.wrap(startKey);
    return stream(segmentIdentifier)
        .filter(e -> startKeyBytes.compareTo(Bytes.wrap(e.getKey())) <= 0);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    final Bytes startKeyHash = Bytes.wrap(startKey);
    final Bytes endKeyHash = Bytes.wrap(endKey);
    return stream(segmentIdentifier)
        .filter(e -> startKeyHash.compareTo(Bytes.wrap(e.getKey())) <= 0)
        .filter(e -> endKeyHash.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      return ImmutableMap.copyOf(
              hashValueStore.computeIfAbsent(segmentIdentifier, s -> newSegmentMap()))
          .entrySet()
          .stream()
          .filter(bytesEntry -> bytesEntry.getValue().isPresent())
          .map(bytesEntry -> bytesEntry.getKey().toArrayUnsafe());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    final Lock lock = rwLock.writeLock();
    if (lock.tryLock()) {
      try {
        Optional.ofNullable(hashValueStore.get(segmentIdentifier))
            .ifPresent(store -> store.remove(Bytes.wrap(key)));
      } finally {
        lock.unlock();
      }
      return true;
    }
    return false;
  }

  @Override
  public void close() {}

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() {
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new SegmentedInMemoryTransaction(), this::isClosed);
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public SegmentedInMemoryKeyValueStorage takeSnapshot() {
    // need to clone the submaps also:
    return new SegmentedInMemoryKeyValueStorage(
        hashValueStore.entrySet().stream()
            .collect(
                Collectors.toConcurrentMap(Map.Entry::getKey, e -> newSegmentMap(e.getValue()))));
  }

  @Override
  public SegmentedKeyValueStorageTransaction getSnapshotTransaction() {
    return startTransaction();
  }

  /** In memory transaction. */
  public class SegmentedInMemoryTransaction implements SegmentedKeyValueStorageTransaction {

    /** protected access to updatedValues map for the transaction. */
    protected Map<SegmentIdentifier, Map<Bytes, Optional<byte[]>>> updatedValues = new HashMap<>();

    /** protected access to deletedValues set for the transaction. */
    protected Map<SegmentIdentifier, Set<Bytes>> removedKeys = new HashMap<>();

    /** Default constructor */
    public SegmentedInMemoryTransaction() {}

    @Override
    public void put(
        final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
      updatedValues
          .computeIfAbsent(segmentIdentifier, __ -> new HashMap<>())
          .put(Bytes.wrap(key), Optional.of(value));
      removedKeys.computeIfAbsent(segmentIdentifier, __ -> new HashSet<>()).remove(Bytes.wrap(key));
    }

    @Override
    public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
      removedKeys.computeIfAbsent(segmentIdentifier, __ -> new HashSet<>()).add(Bytes.wrap(key));
      updatedValues
          .computeIfAbsent(segmentIdentifier, __ -> new HashMap<>())
          .remove(Bytes.wrap(key));
    }

    @Override
    public void commit() throws StorageException {
      final Lock lock = rwLock.writeLock();
      lock.lock();
      try {
        updatedValues.entrySet().stream()
            .forEach(
                entry ->
                    hashValueStore
                        .computeIfAbsent(entry.getKey(), __ -> newSegmentMap())
                        .putAll(entry.getValue()));

        removedKeys.entrySet().stream()
            .forEach(
                entry -> {
                  var keyset =
                      hashValueStore
                          .computeIfAbsent(entry.getKey(), __ -> newSegmentMap())
                          .keySet();
                  keyset.removeAll(entry.getValue());
                });

        updatedValues.clear();
        removedKeys.clear();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void rollback() {
      close();
    }

    @Override
    public void close() {
      updatedValues.clear();
      removedKeys.clear();
    }
  }

  /**
   * Dump the content of the store to the provided PrintStream.
   *
   * @param ps the PrintStream to dump the content to.
   */
  public void dump(final PrintStream ps) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      ImmutableSet.copyOf(hashValueStore.entrySet()).stream()
          .forEach(
              map -> {
                ps.println("Segment: " + map.getKey().getName());
                map.getValue().entrySet().stream()
                    .filter(bytesEntry -> bytesEntry.getValue().isPresent())
                    .forEach(
                        entry ->
                            ps.printf(
                                "  %s : %s%n",
                                entry.getKey().toHexString(),
                                Bytes.wrap(entry.getValue().get()).toHexString()));
              });
    } finally {
      lock.unlock();
    }
  }

  /**
   * Merge this storage's data to a SegmentedKeyValueStorageTransaction. This provides atomic
   * merging of all data from this storage to the target transaction.
   *
   * @param otherTransaction the transaction to merge data into
   */
  public void mergeTo(final SegmentedKeyValueStorageTransaction otherTransaction) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      hashValueStore.forEach(
          (segmentIdentifier, entries) -> {
            entries.forEach(
                (key, value) -> {
                  if (value.isPresent()) {
                    otherTransaction.put(segmentIdentifier, key.toArrayUnsafe(), value.get());
                  } else {
                    otherTransaction.remove(segmentIdentifier, key.toArrayUnsafe());
                  }
                });
          });
    } finally {
      lock.unlock();
    }
  }
}
