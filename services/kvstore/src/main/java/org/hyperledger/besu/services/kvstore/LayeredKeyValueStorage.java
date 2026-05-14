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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Key value storage which stores in memory all updates to a parent worldstate storage. */
public class LayeredKeyValueStorage extends SegmentedInMemoryKeyValueStorage
    implements SnappedKeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LayeredKeyValueStorage.class);

  private final SegmentedKeyValueStorage parent;

  /**
   * Instantiates a new Layered key value storage.
   *
   * @param parent the parent key value storage for this layered storage.
   */
  public LayeredKeyValueStorage(final SegmentedKeyValueStorage parent) {
    this(new ConcurrentHashMap<>(), parent);
  }

  /**
   * Constructor which takes an explicit backing map for the layered key value storage.
   *
   * @param map the backing map
   * @param parent the parent key value storage for this layered storage.
   */
  public LayeredKeyValueStorage(
      final ConcurrentMap<SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>> map,
      final SegmentedKeyValueStorage parent) {
    super(map);
    this.parent = parent;
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segmentId, final byte[] key)
      throws StorageException {
    return get(segmentId, key).isPresent();
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key)
      throws StorageException {
    throwIfClosed();

    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      Bytes wrapKey = Bytes.wrap(key);
      final Optional<byte[]> foundKey =
          hashValueStore.computeIfAbsent(segmentId, __ -> newSegmentMap()).get(wrapKey);
      if (foundKey == null) {
        return parent.get(segmentId, key);
      } else {
        return foundKey;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(
        key,
        k -> super.getNearestBefore(segmentIdentifier, k),
        k -> parent.getNearestBefore(segmentIdentifier, k),
        false);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBeforeMatchLength(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(
        key,
        k -> super.getNearestBeforeMatchLength(segmentIdentifier, k),
        k -> parent.getNearestBeforeMatchLength(segmentIdentifier, k),
        false);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(
        key,
        k -> super.getNearestAfter(segmentIdentifier, k),
        k -> parent.getNearestAfter(segmentIdentifier, k),
        true);
  }

  private Optional<NearestKeyValue> getNearest(
      final Bytes key,
      final Function<Bytes, Optional<NearestKeyValue>> ourNearestFunction,
      final Function<Bytes, Optional<NearestKeyValue>> parentNearestFunction,
      final boolean isAfter)
      throws StorageException {

    final Optional<NearestKeyValue> ourNearest = ourNearestFunction.apply(key);
    final Optional<NearestKeyValue> parentNearest = parentNearestFunction.apply(key);

    if (ourNearest.isPresent() && parentNearest.isPresent()) {
      return compareNearest(ourNearest, parentNearest, key, isAfter);
    } else if (ourNearest.isPresent()) {
      return ourNearest;
    } else {
      return parentNearest;
    }
  }

  private Optional<NearestKeyValue> compareNearest(
      final Optional<NearestKeyValue> ourNearest,
      final Optional<NearestKeyValue> parentNearest,
      final Bytes key,
      final boolean isAfter) {

    final int ourDistance = ourNearest.get().key().compareTo(key);
    final int parentDistance = parentNearest.get().key().compareTo(key);
    if (ourDistance == 0) {
      return ourNearest;
    } else if (parentDistance == 0) {
      return parentNearest;
    } else {
      final int ourCommonPrefixLength = ourNearest.get().key().commonPrefixLength(key);
      final int parentCommonPrefixLength = parentNearest.get().key().commonPrefixLength(key);
      if (ourCommonPrefixLength != parentCommonPrefixLength) {
        return ourCommonPrefixLength > parentCommonPrefixLength ? ourNearest : parentNearest;
      } else {
        // When searching for a key, if isAfter is true, we choose the next smallest key after our
        // target because both found keys are after it.
        // If isAfter is false, meaning we're doing a seekForPrev, we select the largest key that
        // comes before our target, as it's the nearest one.
        // For example : if the searched key is 0x0101 and we found 0x0001 and 0x0100 when isAfter
        // == false we will take 0x0100
        if (ourNearest.get().key().compareTo(parentNearest.get().key()) > 0) {
          return isAfter ? parentNearest : ourNearest;
        } else {
          return isAfter ? ourNearest : parentNearest;
        }
      }
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentId) {
    throwIfClosed();
    var ourLayerState = hashValueStore.computeIfAbsent(segmentId, s -> newSegmentMap());

    PeekingIterator<Map.Entry<Bytes, Optional<byte[]>>> ourIterator =
        new PeekingIterator<>(ourLayerState.entrySet().stream().iterator());
    PeekingIterator<Pair<byte[], byte[]>> parentIterator =
        new PeekingIterator<>(parent.stream(segmentId).iterator());

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                new LayeredIterator(ourIterator, parentIterator), ORDERED | SORTED | DISTINCT),
            false)
        .filter(e -> e.getValue() != null);
  }

  private static class LayeredIterator implements Iterator<Pair<byte[], byte[]>> {
    private final PeekingIterator<Map.Entry<Bytes, Optional<byte[]>>> ourIterator;
    private final PeekingIterator<Pair<byte[], byte[]>> parentIterator;

    LayeredIterator(
        final PeekingIterator<Map.Entry<Bytes, Optional<byte[]>>> ourIterator,
        final PeekingIterator<Pair<byte[], byte[]>> parentIterator) {
      this.ourIterator = ourIterator;
      this.parentIterator = parentIterator;
    }

    @Override
    public boolean hasNext() {
      return ourIterator.hasNext() || parentIterator.hasNext();
    }

    private Pair<byte[], byte[]> mapEntryToPair(final Map.Entry<Bytes, Optional<byte[]>> entry) {
      byte[] value = entry.getValue().orElse(null);
      return Pair.of(entry.getKey().toArrayUnsafe(), value);
    }

    @Override
    public Pair<byte[], byte[]> next() {
      var ourPeek = ourIterator.peek();
      var parentPeek = parentIterator.peek();

      if (ourPeek == null || parentPeek == null) {
        return ourPeek == null ? parentIterator.next() : mapEntryToPair(ourIterator.next());
      }

      // otherwise compare:
      int comparison = ourPeek.getKey().compareTo(Bytes.wrap(parentPeek.getKey()));
      if (comparison < 0) {
        return mapEntryToPair(ourIterator.next());
      } else if (comparison == 0) {
        // skip dupe key from parent, return ours:
        parentIterator.next();
        return mapEntryToPair(ourIterator.next());
      } else {
        return parentIterator.next();
      }
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentId, final byte[] startKey) {
    final Bytes startKeyBytes = Bytes.wrap(startKey);
    return stream(segmentId).filter(e -> startKeyBytes.compareTo(Bytes.wrap(e.getKey())) <= 0);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentId, final byte[] startKey, final byte[] endKey) {
    final Bytes startKeyBytes = Bytes.wrap(startKey);
    final Bytes endKeyBytes = Bytes.wrap(endKey);
    return stream(segmentId)
        .filter(e -> startKeyBytes.compareTo(Bytes.wrap(e.getKey())) <= 0)
        .filter(e -> endKeyBytes.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentId) {
    throwIfClosed();

    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      // copy of our in memory store to use for streaming and filtering:
      var ourLayerState =
          Optional.ofNullable(hashValueStore.get(segmentId))
              .map(HashMap::new)
              .orElse(new HashMap<>());

      return Streams.concat(
          ourLayerState.entrySet().stream()
              .filter(entry -> entry.getValue().isPresent())
              .map(bytesEntry -> bytesEntry.getKey().toArrayUnsafe())
          // since we are layered, concat a parent stream filtered by our map entries:
          ,
          parent.streamKeys(segmentId).filter(e -> !ourLayerState.containsKey(Bytes.of(e))));

    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentId, final byte[] key) {
    hashValueStore
        .computeIfAbsent(segmentId, __ -> newSegmentMap())
        .put(Bytes.wrap(key), Optional.empty());
    return true;
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() {
    throwIfClosed();

    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new SegmentedInMemoryTransaction() {
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

              // put empty rather than remove in order to not ask parent in case of deletion
              removedKeys.entrySet().stream()
                  .forEach(
                      segmentEntry ->
                          hashValueStore
                              .computeIfAbsent(segmentEntry.getKey(), __ -> newSegmentMap())
                              .putAll(
                                  segmentEntry.getValue().stream()
                                      .collect(
                                          Collectors.toMap(key -> key, __ -> Optional.empty()))));

              updatedValues.clear();
              removedKeys.clear();
            } finally {
              lock.unlock();
            }
          }
        },
        this::isClosed);
  }

  @Override
  public boolean isClosed() {
    return parent.isClosed();
  }

  @Override
  public SnappedKeyValueStorage clone() {
    return new LayeredKeyValueStorage(hashValueStore, parent);
  }

  private void throwIfClosed() {
    if (parent.isClosed()) {
      LOG.error("Attempting to use a closed RocksDBKeyValueStorage");
      throw new StorageException("Storage has been closed");
    }
  }

  private static class PeekingIterator<E> implements Iterator<E> {
    private final Iterator<E> iterator;
    private E next;

    public PeekingIterator(final Iterator<E> iterator) {
      this.iterator = iterator;
      this.next = iterator.hasNext() ? iterator.next() : null;
    }

    public E peek() {
      return next;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public E next() {
      E oldNext = next;
      next = iterator.hasNext() ? iterator.next() : null;
      return oldNext;
    }
  }
}
