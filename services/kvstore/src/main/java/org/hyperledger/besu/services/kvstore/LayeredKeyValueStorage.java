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
import static org.hyperledger.besu.services.kvstore.KeyComparator.UNSIGNED_KEY_ORDER;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Key value storage which stores in memory all updates to a parent worldstate storage. */
public class LayeredKeyValueStorage extends SegmentedInMemoryKeyValueStorage
    implements SnappedKeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LayeredKeyValueStorage.class);

  private final SegmentedKeyValueStorage parent;

  /**
   * The parent if it is exactly a {@link LayeredKeyValueStorage}, otherwise null. Lookups walk
   * these layers' maps directly, so a subclass parent is instead treated as the base storage and
   * queried through its own methods, keeping any overrides.
   */
  private final @Nullable LayeredKeyValueStorage parentLayer;

  /**
   * The first non-layered storage below this layer, where lookups end when no layer has a key.
   * Layers cannot be closed on their own, so its closed state is the closed state of the whole
   * chain.
   */
  private final SegmentedKeyValueStorage baseStorage;

  /**
   * Cached result of baseStorage.isClosed(). The closed state transitions from false to true
   * exactly once per process lifetime and never back, so caching the first true result is safe.
   */
  private volatile boolean closedCache = false;

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
    this.parentLayer =
        parent.getClass() == LayeredKeyValueStorage.class ? (LayeredKeyValueStorage) parent : null;
    this.baseStorage = parentLayer != null ? parentLayer.baseStorage : parent;
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
    final Optional<byte[]> found = getFromLayers(segmentId, Bytes.wrap(key));
    return found != null ? found : baseStorage.get(segmentId, key);
  }

  /**
   * Layer-local lookup that does not consult the parent.
   *
   * @param segmentId the segment identifier
   * @param key the key
   * @return empty if the key is absent on this layer; otherwise present with the layer value (an
   *     empty inner Optional means a deletion tombstone)
   */
  public Optional<Optional<byte[]>> peekThisLayer(
      final SegmentIdentifier segmentId, final byte[] key) {
    throwIfClosed();
    return Optional.ofNullable(getLocal(segmentId, Bytes.wrap(key)));
  }

  @Override
  public List<Optional<byte[]>> multiget(final SegmentIdentifier segment, final List<byte[]> keys)
      throws StorageException {
    throwIfClosed();
    if (keys.isEmpty()) {
      return List.of();
    }

    final List<Optional<byte[]>> results = new ArrayList<>(keys.size());
    final List<Integer> missingIndices = new ArrayList<>();
    final List<byte[]> missingKeys = new ArrayList<>();

    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      final NavigableMap<Bytes, Optional<byte[]>> segmentMap =
          hashValueStore.computeIfAbsent(segment, s -> newSegmentMap());
      for (int i = 0; i < keys.size(); i++) {
        final byte[] key = keys.get(i);
        final Optional<byte[]> localValue = segmentMap.get(Bytes.wrap(key));
        if (localValue != null) {
          results.add(localValue);
        } else {
          results.add(null);
          missingIndices.add(i);
          missingKeys.add(key);
        }
      }
    } finally {
      lock.unlock();
    }

    if (!missingKeys.isEmpty()) {
      final List<Optional<byte[]>> parentResults = parent.multiget(segment, missingKeys);
      for (int j = 0; j < missingIndices.size(); j++) {
        results.set(missingIndices.get(j), parentResults.get(j));
      }
    }

    return results;
  }

  /**
   * Bytes-keyed variant for callers that want to inject a function evaluated at the bottom of the
   * layer chain when no layer has a value for the key. Used by Bonsai to consult the cross-block
   * cache without going through the persistent storage twice.
   *
   * @param segmentId the segment identifier
   * @param key the key
   * @param cacheGetFunction function evaluated on the persistent parent when no layer holds the key
   * @return optional value as {@link Bytes}
   */
  public Optional<Bytes> get(
      final SegmentIdentifier segmentId,
      final Bytes key,
      final Function<SegmentedKeyValueStorage, Optional<Bytes>> cacheGetFunction) {
    throwIfClosed();
    // The layer maps compare keys via toArrayUnsafe(), which copies for keys such as
    // Bytes.concatenate results; materialize once rather than on every comparison in every layer.
    final Bytes arrayKey = Bytes.wrap(key.toArrayUnsafe());
    final Optional<byte[]> found = getFromLayers(segmentId, arrayKey);
    if (found != null) {
      return found.map(Bytes::wrap);
    }
    // a subclass layer below: let it check its own layers and apply the function to the
    // persistent storage beneath it, so the function is never handed a layer
    if (baseStorage instanceof LayeredKeyValueStorage layered) {
      return layered.get(segmentId, arrayKey, cacheGetFunction);
    }
    return cacheGetFunction != null
        ? cacheGetFunction.apply(baseStorage)
        : baseStorage.get(segmentId, arrayKey.toArrayUnsafe()).map(Bytes::wrap);
  }

  /**
   * Looks the key up in this layer and then each parent layer, stopping at the first layer that
   * holds it.
   *
   * @return null if no layer holds the key; otherwise the value, empty if the key was deleted
   */
  @SuppressWarnings("NullableOptional")
  private @Nullable Optional<byte[]> getFromLayers(
      final SegmentIdentifier segmentId, final Bytes key) {
    for (LayeredKeyValueStorage layer = this; layer != null; layer = layer.parentLayer) {
      final Optional<byte[]> found = layer.getLocal(segmentId, key);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  // null = not in this layer, empty = deleted in this layer (mirrors the layer map's get)
  @SuppressWarnings("NullableOptional")
  private @Nullable Optional<byte[]> getLocal(final SegmentIdentifier segmentId, final Bytes key) {
    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      final NavigableMap<Bytes, Optional<byte[]>> segment = hashValueStore.get(segmentId);
      return segment == null ? null : segment.get(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(segmentIdentifier, key, false);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {
    return getNearest(segmentIdentifier, key, true);
  }

  private Optional<NearestKeyValue> getNearest(
      final SegmentIdentifier segmentIdentifier, final Bytes key, final boolean isAfter)
      throws StorageException {
    throwIfClosed();
    // Each layer's own nearest, top first, then fold them onto the base storage's nearest from
    // the bottom up: the same result as each layer combining its nearest with its parent's.
    final List<Optional<NearestKeyValue>> layerNearest = new ArrayList<>();
    for (LayeredKeyValueStorage layer = this; layer != null; layer = layer.parentLayer) {
      layerNearest.add(layer.getLocalNearest(segmentIdentifier, key, isAfter));
    }
    Optional<NearestKeyValue> nearest =
        isAfter
            ? baseStorage.getNearestAfter(segmentIdentifier, key)
            : baseStorage.getNearestBefore(segmentIdentifier, key);
    for (int i = layerNearest.size() - 1; i >= 0; i--) {
      nearest = combineNearest(layerNearest.get(i), nearest, key, isAfter);
    }
    return nearest;
  }

  private Optional<NearestKeyValue> getLocalNearest(
      final SegmentIdentifier segmentIdentifier, final Bytes key, final boolean isAfter) {
    return isAfter
        ? super.getNearestAfter(segmentIdentifier, key)
        : super.getNearestBefore(segmentIdentifier, key);
  }

  private Optional<NearestKeyValue> combineNearest(
      final Optional<NearestKeyValue> ourNearest,
      final Optional<NearestKeyValue> parentNearest,
      final Bytes key,
      final boolean isAfter) {
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
    return streamRange(segmentId, null, null);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentId, final byte[] startKey) {
    return streamRange(segmentId, Bytes.wrap(startKey), null);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentId, final byte[] startKey, final byte[] endKey) {
    return streamRange(segmentId, Bytes.wrap(startKey), Bytes.wrap(endKey));
  }

  /**
   * Streams the merged entries of all layers and the base storage, optionally bounded so that each
   * layer and the base storage only read the requested range.
   *
   * @param startKey inclusive lower bound, or null for the start of the segment
   * @param endKey inclusive upper bound, or null for the end of the segment; requires startKey
   */
  private Stream<Pair<byte[], byte[]>> streamRange(
      final SegmentIdentifier segmentId,
      final @Nullable Bytes startKey,
      final @Nullable Bytes endKey) {
    throwIfClosed();
    if (startKey != null && endKey != null && UNSIGNED_KEY_ORDER.compare(startKey, endKey) > 0) {
      return Stream.empty();
    }
    // Merge all layers (top first) and the persistent parent in one pass rather than recursively.
    final List<Iterator<Map.Entry<Bytes, Optional<byte[]>>>> sources = new ArrayList<>();
    for (LayeredKeyValueStorage layer = this; layer != null; layer = layer.parentLayer) {
      final NavigableMap<Bytes, Optional<byte[]>> segment = layer.hashValueStore.get(segmentId);
      if (segment != null) {
        final NavigableMap<Bytes, Optional<byte[]>> range =
            startKey == null
                ? segment
                : endKey == null
                    ? segment.tailMap(startKey, true)
                    : segment.subMap(startKey, true, endKey, true);
        sources.add(range.entrySet().iterator());
      }
    }
    final Stream<Pair<byte[], byte[]>> baseStream =
        startKey == null
            ? baseStorage.stream(segmentId)
            : endKey == null
                ? baseStorage.streamFromKey(segmentId, startKey.toArrayUnsafe())
                : baseStorage.streamFromKey(
                    segmentId, startKey.toArrayUnsafe(), endKey.toArrayUnsafe());
    sources.add(
        baseStream
            .map(pair -> Map.entry(Bytes.wrap(pair.getKey()), Optional.ofNullable(pair.getValue())))
            .iterator());

    // iterator() drops the base stream's close handler (e.g. releasing a RocksDB iterator)
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                new LayeredMergeIterator(sources), ORDERED | SORTED | DISTINCT),
            false)
        .onClose(baseStream::close);
  }

  /** Merges sorted sources; higher layers win on duplicate keys and empty values are deletions. */
  private static class LayeredMergeIterator extends AbstractIterator<Pair<byte[], byte[]>> {

    private static final class Source {
      private final int rank;
      private final Iterator<Map.Entry<Bytes, Optional<byte[]>>> iterator;
      private Map.Entry<Bytes, Optional<byte[]>> head;

      Source(
          final int rank,
          final Iterator<Map.Entry<Bytes, Optional<byte[]>>> iterator,
          final Map.Entry<Bytes, Optional<byte[]>> head) {
        this.rank = rank;
        this.iterator = iterator;
        this.head = head;
      }
    }

    private final PriorityQueue<Source> queue =
        new PriorityQueue<>(
            Comparator.comparing((Source s) -> s.head.getKey(), UNSIGNED_KEY_ORDER)
                .thenComparingInt(s -> s.rank));

    LayeredMergeIterator(final List<Iterator<Map.Entry<Bytes, Optional<byte[]>>>> sources) {
      for (int i = 0; i < sources.size(); i++) {
        final Iterator<Map.Entry<Bytes, Optional<byte[]>>> source = sources.get(i);
        if (source.hasNext()) {
          queue.add(new Source(i, source, source.next()));
        }
      }
    }

    private void advance(final Source source) {
      if (source.iterator.hasNext()) {
        source.head = source.iterator.next();
        queue.add(source);
      }
    }

    @Override
    protected @Nullable Pair<byte[], byte[]> computeNext() {
      while (!queue.isEmpty()) {
        final Source top = queue.poll();
        final Map.Entry<Bytes, Optional<byte[]>> winner = top.head;
        advance(top);
        // drop the same key from lower-precedence sources
        while (!queue.isEmpty()
            && UNSIGNED_KEY_ORDER.compare(queue.peek().head.getKey(), winner.getKey()) == 0) {
          advance(queue.poll());
        }
        if (winner.getValue().isPresent()) {
          return Pair.of(winner.getKey().toArrayUnsafe(), winner.getValue().get());
        }
      }
      return endOfData();
    }
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentId) {
    throwIfClosed();
    // Collapse all layers into one key -> isPresent map; the highest layer with a key decides.
    final Map<Bytes, Boolean> layeredKeys = new HashMap<>();
    for (LayeredKeyValueStorage layer = this; layer != null; layer = layer.parentLayer) {
      final Lock lock = layer.rwLock.readLock();
      lock.lock();
      try {
        final NavigableMap<Bytes, Optional<byte[]>> segment = layer.hashValueStore.get(segmentId);
        if (segment != null) {
          segment.forEach((key, value) -> layeredKeys.putIfAbsent(key, value.isPresent()));
        }
      } finally {
        lock.unlock();
      }
    }

    return Streams.concat(
        layeredKeys.entrySet().stream()
            .filter(Map.Entry::getValue)
            .map(entry -> entry.getKey().toArrayUnsafe()),
        baseStorage.streamKeys(segmentId).filter(key -> !layeredKeys.containsKey(Bytes.wrap(key))));
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
    if (!closedCache && baseStorage.isClosed()) {
      closedCache = true;
    }
    return closedCache;
  }

  @Override
  public SnappedKeyValueStorage clone() {
    return new LayeredKeyValueStorage(hashValueStore, parent);
  }

  private void throwIfClosed() {
    if (isClosed()) {
      LOG.error("Attempting to use a closed RocksDBKeyValueStorage");
      throw new StorageException("Storage has been closed");
    }
  }
}
