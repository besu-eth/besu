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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.KeyComparator;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LayeredKeyValueStorageTest {

  @Mock private SegmentedKeyValueStorage parentStorage;

  private LayeredKeyValueStorage layeredKeyValueStorage;
  private SegmentIdentifier segmentId;

  @BeforeEach
  void setUp() {
    segmentId = mock(SegmentIdentifier.class);
    layeredKeyValueStorage = new LayeredKeyValueStorage(parentStorage);
  }

  @Test
  void shouldReturnEmptyStreamWhenParentAndLayerAreEmpty() {
    when(parentStorage.stream(segmentId)).thenReturn(Stream.empty());
    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);
    assertTrue(result.collect(Collectors.toList()).isEmpty());
  }

  private ConcurrentMap<SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>>
      createSegmentMap() {
    ConcurrentMap<SegmentIdentifier, NavigableMap<Bytes, Optional<byte[]>>> map =
        new ConcurrentHashMap<>();
    // same order as the production segment maps
    NavigableMap<Bytes, Optional<byte[]>> segmentMap =
        new TreeMap<>(KeyComparator.UNSIGNED_KEY_ORDER);
    map.put(segmentId, segmentMap);
    return map;
  }

  @Test
  void shouldReturnParentDataWhenLayerIsEmpty() {
    byte[] key1 = {1};
    byte[] value1 = {10};

    when(parentStorage.stream(segmentId)).thenReturn(Stream.of(Pair.of(key1, value1)));

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.collect(Collectors.toList());
    assertEquals(1, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
  }

  @Test
  void shouldReturnLayerDataWhenParentIsEmpty() {
    byte[] key1 = {1};
    byte[] value1 = {10};

    when(parentStorage.stream(segmentId)).thenReturn(Stream.empty());

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key1), Optional.of(value1));
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);
    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(1, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
  }

  @Test
  void isClosedReturnsFalseWhenRootIsOpen() {
    when(parentStorage.isClosed()).thenReturn(false);
    assertFalse(layeredKeyValueStorage.isClosed());
  }

  @Test
  void isClosedReturnsTrueWhenRootIsClosed() {
    when(parentStorage.isClosed()).thenReturn(true);
    assertTrue(layeredKeyValueStorage.isClosed());
  }

  /**
   * Verifies that isClosed() on a deep chain queries the root storage once and then answers from
   * its cached closed state, regardless of chain depth (besu-eth/besu#10498).
   */
  @Test
  void isClosedCachesResultOnDeepChainAndQueriesRootAtMostOnce() {
    // build a chain of depth 100: top -> layer99 -> ... -> layer1 -> parentStorage (the mock root)
    int depth = 100;
    LayeredKeyValueStorage top = layeredKeyValueStorage; // wraps parentStorage directly
    for (int i = 0; i < depth - 1; i++) {
      top = new LayeredKeyValueStorage(top);
    }

    when(parentStorage.isClosed()).thenReturn(true);

    // First call asks the root storage directly, without walking the layers, and caches the result
    assertTrue(top.isClosed());

    // Second call on the same instance must NOT reach the root again (cache hit)
    assertTrue(top.isClosed());

    // The root storage is queried once across both calls
    verify(parentStorage, times(1)).isClosed();
  }

  /**
   * isClosed() on an open chain must check the base storage directly rather than walking the
   * parents: get() calls it at every layer, so a walk made each key miss O(N²) in the chain depth.
   * A chain deep enough to overflow the stack if isClosed() recursed asserts it does not.
   */
  @Test
  void isClosedOnOpenDeepChainChecksBaseStorageDirectly() {
    LayeredKeyValueStorage top = layeredKeyValueStorage;
    for (int i = 0; i < 100_000; i++) {
      top = new LayeredKeyValueStorage(top);
    }
    when(parentStorage.isClosed()).thenReturn(false);

    assertFalse(top.isClosed());
    verify(parentStorage, times(1)).isClosed();
  }

  @Test
  void isClosedUsesOverrideOfSubclassParent() {
    final LayeredKeyValueStorage closedSubclass =
        new LayeredKeyValueStorage(parentStorage) {
          @Override
          public boolean isClosed() {
            return true;
          }
        };
    final LayeredKeyValueStorage top =
        new LayeredKeyValueStorage(new LayeredKeyValueStorage(closedSubclass));

    assertTrue(top.isClosed());
  }

  @Test
  void deepChainGetReachesParentAtConstantStackDepth() {
    byte[] key = {1};
    AtomicInteger maxDepth = new AtomicInteger();
    when(parentStorage.get(segmentId, key))
        .thenAnswer(
            __ -> {
              recordStackDepth(maxDepth);
              return Optional.of(key);
            });

    // A recursive implementation reaches the parent roughly one frame deeper per layer.
    assertEquals(
        parentStackDepth(1, maxDepth, top -> top.get(segmentId, key).isPresent() ? 1 : 0),
        parentStackDepth(500, maxDepth, top -> top.get(segmentId, key).isPresent() ? 1 : 0));
  }

  @Test
  void deepChainGetBytesVariantReachesCacheFunctionAtConstantStackDepth() {
    Bytes key = Bytes.of(1);
    AtomicInteger maxDepth = new AtomicInteger();
    Function<SegmentedKeyValueStorage, Optional<Bytes>> cacheGetFunction =
        storage -> {
          recordStackDepth(maxDepth);
          return Optional.of(key);
        };

    // A recursive implementation reaches the parent roughly one frame deeper per layer.
    assertEquals(
        parentStackDepth(
            1, maxDepth, top -> top.get(segmentId, key, cacheGetFunction).isPresent() ? 1 : 0),
        parentStackDepth(
            500, maxDepth, top -> top.get(segmentId, key, cacheGetFunction).isPresent() ? 1 : 0));
  }

  @Test
  void streamAndStreamKeysApplyPrecedenceAcrossLayers() {
    byte[] p = {0};
    byte[] a = {1};
    byte[] b = {2};
    byte[] c = {3};
    List<Pair<byte[], byte[]>> parentEntries =
        List.of(
            Pair.of(new byte[] {1}, p),
            Pair.of(new byte[] {2}, p),
            Pair.of(new byte[] {3}, p),
            Pair.of(new byte[] {4}, p));
    when(parentStorage.stream(segmentId)).thenAnswer(__ -> parentEntries.stream());
    when(parentStorage.streamKeys(segmentId))
        .thenAnswer(__ -> parentEntries.stream().map(Pair::getKey));

    LayeredKeyValueStorage bottom =
        layer(parentStorage, Map.of(2, Optional.of(a), 5, Optional.of(a), 6, Optional.of(a)));
    LayeredKeyValueStorage middle = layer(bottom, Map.of(3, Optional.empty(), 5, Optional.of(b)));
    LayeredKeyValueStorage top = layer(middle, Map.of(1, Optional.of(c), 6, Optional.empty()));

    // 1: top overrides parent; 2: bottom overrides parent; 3: deleted in middle;
    // 4: parent only; 5: middle overrides bottom; 6: deleted in top
    assertEquals(
        List.of(
            Pair.of(Bytes.of(1), Bytes.wrap(c)),
            Pair.of(Bytes.of(2), Bytes.wrap(a)),
            Pair.of(Bytes.of(4), Bytes.wrap(p)),
            Pair.of(Bytes.of(5), Bytes.wrap(b))),
        top.stream(segmentId)
            .map(e -> Pair.of(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue())))
            .toList());
    assertEquals(
        List.of(Bytes.of(1), Bytes.of(2), Bytes.of(4), Bytes.of(5)),
        top.streamKeys(segmentId).map(Bytes::wrap).sorted().toList());
  }

  @Test
  void closingStreamClosesBaseStorageStream() {
    // RocksDB streams release their native iterator in onClose
    AtomicInteger baseCloses = new AtomicInteger();
    when(parentStorage.stream(segmentId))
        .thenAnswer(
            __ ->
                Stream.of(Pair.of(new byte[] {1}, new byte[] {1}))
                    .onClose(baseCloses::incrementAndGet));
    when(parentStorage.streamFromKey(eq(segmentId), aryEq(new byte[] {0})))
        .thenAnswer(
            __ ->
                Stream.of(Pair.of(new byte[] {1}, new byte[] {1}))
                    .onClose(baseCloses::incrementAndGet));
    LayeredKeyValueStorage top =
        new LayeredKeyValueStorage(new LayeredKeyValueStorage(parentStorage));

    try (Stream<Pair<byte[], byte[]>> stream = top.stream(segmentId)) {
      assertEquals(1, stream.count());
    }
    assertEquals(1, baseCloses.get());

    try (Stream<Pair<byte[], byte[]>> stream = top.streamFromKey(segmentId, new byte[] {0})) {
      assertEquals(1, stream.count());
    }
    assertEquals(2, baseCloses.get());
  }

  @Test
  void streamFromKeyWithEndKeyReadsOnlyTheRangeFromLayersAndBaseStorage() {
    byte[] p = {0};
    byte[] a = {1};
    // the base storage is asked for the range only, and applies the bounds itself
    when(parentStorage.streamFromKey(eq(segmentId), aryEq(new byte[] {3}), aryEq(new byte[] {6})))
        .thenAnswer(
            __ ->
                Stream.of(
                    Pair.of(new byte[] {3}, p),
                    Pair.of(new byte[] {4}, p),
                    Pair.of(new byte[] {6}, p)));

    LayeredKeyValueStorage bottom =
        layer(parentStorage, Map.of(2, Optional.of(a), 5, Optional.of(a), 7, Optional.of(a)));
    LayeredKeyValueStorage top =
        layer(bottom, Map.of(1, Optional.of(a), 3, Optional.empty(), 6, Optional.of(a)));

    // 1, 2, 7: layer keys outside the range; 3: deleted at the start key;
    // 4: base only; 5: bottom only; 6: top overrides base at the inclusive end key
    assertEquals(
        List.of(
            Pair.of(Bytes.of(4), Bytes.wrap(p)),
            Pair.of(Bytes.of(5), Bytes.wrap(a)),
            Pair.of(Bytes.of(6), Bytes.wrap(a))),
        top.streamFromKey(segmentId, new byte[] {3}, new byte[] {6})
            .map(e -> Pair.of(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue())))
            .toList());
    verify(parentStorage, never()).stream(segmentId);
  }

  @Test
  void streamFromKeyReadsFromStartKeyInLayersAndBaseStorage() {
    byte[] p = {0};
    byte[] a = {1};
    when(parentStorage.streamFromKey(eq(segmentId), aryEq(new byte[] {3})))
        .thenAnswer(__ -> Stream.of(Pair.of(new byte[] {3}, p), Pair.of(new byte[] {8}, p)));

    LayeredKeyValueStorage top =
        layer(parentStorage, Map.of(2, Optional.of(a), 3, Optional.of(a), 9, Optional.of(a)));

    assertEquals(
        List.of(
            Pair.of(Bytes.of(3), Bytes.wrap(a)),
            Pair.of(Bytes.of(8), Bytes.wrap(p)),
            Pair.of(Bytes.of(9), Bytes.wrap(a))),
        top.streamFromKey(segmentId, new byte[] {3})
            .map(e -> Pair.of(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue())))
            .toList());
    verify(parentStorage, never()).stream(segmentId);
  }

  @Test
  void streamFromKeyWithStartAfterEndIsEmpty() {
    LayeredKeyValueStorage top = layer(parentStorage, Map.of(4, Optional.of(new byte[] {1})));

    assertEquals(0, top.streamFromKey(segmentId, new byte[] {5}, new byte[] {3}).count());
  }

  @Test
  void subclassParentLayerGetOverrideIsUsed() {
    byte[] overrideValue = {42};
    LayeredKeyValueStorage top =
        new LayeredKeyValueStorage(
            new LayeredKeyValueStorage(parentStorage) {
              @Override
              public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key) {
                return Optional.of(overrideValue);
              }
            });

    assertArrayEquals(overrideValue, top.get(segmentId, new byte[] {1}).orElseThrow());
  }

  @Test
  void cacheGetFunctionReceivesPersistentStorageBelowSubclassParentLayer() {
    byte[] subclassValue = {7};
    byte[] persistentValue = {9};
    var subclassMap = createSegmentMap();
    subclassMap.get(segmentId).put(Bytes.of(1), Optional.of(subclassValue));
    LayeredKeyValueStorage subclassParent =
        new LayeredKeyValueStorage(subclassMap, parentStorage) {};
    LayeredKeyValueStorage top = new LayeredKeyValueStorage(subclassParent);
    List<SegmentedKeyValueStorage> cacheFunctionArgs = new ArrayList<>();
    Function<SegmentedKeyValueStorage, Optional<Bytes>> cacheGetFunction =
        storage -> {
          cacheFunctionArgs.add(storage);
          return Optional.of(Bytes.wrap(persistentValue));
        };

    // held by the subclass layer: found without consulting the cache function
    assertEquals(
        Optional.of(Bytes.wrap(subclassValue)), top.get(segmentId, Bytes.of(1), cacheGetFunction));
    // held by no layer: the cache function must see the persistent storage, not a layer
    assertEquals(
        Optional.of(Bytes.wrap(persistentValue)),
        top.get(segmentId, Bytes.of(2), cacheGetFunction));
    assertEquals(List.of(parentStorage), cacheFunctionArgs);
  }

  @Test
  void getBytesVariantWithoutCacheFunctionReadsLayersThenBaseStorage() {
    byte[] layerValue = {1};
    byte[] baseValue = {2};
    when(parentStorage.get(eq(segmentId), aryEq(new byte[] {1, 2})))
        .thenReturn(Optional.of(baseValue));
    // never read when the delete is honoured; makes falling through to the base visible
    lenient()
        .when(parentStorage.get(eq(segmentId), aryEq(new byte[] {4})))
        .thenReturn(Optional.of(baseValue));
    LayeredKeyValueStorage bottom =
        layer(parentStorage, Map.of(3, Optional.of(layerValue), 4, Optional.of(layerValue)));
    LayeredKeyValueStorage top = layer(bottom, Map.of(4, Optional.empty()));

    // held by no layer: read from the base storage, here with a key that is not array-backed
    assertEquals(
        Optional.of(Bytes.wrap(baseValue)),
        top.get(segmentId, Bytes.concatenate(Bytes.of(1), Bytes.of(2)), null));
    // held by a lower layer
    assertEquals(Optional.of(Bytes.wrap(layerValue)), top.get(segmentId, Bytes.of(3), null));
    // deleted in the top layer, hiding the lower layer's value
    assertEquals(Optional.empty(), top.get(segmentId, Bytes.of(4), null));
  }

  @Test
  void peekThisLayerReadsOnlyThisLayer() {
    byte[] value = {1};
    LayeredKeyValueStorage bottom = layer(parentStorage, Map.of(1, Optional.of(value)));
    LayeredKeyValueStorage top = layer(bottom, Map.of(2, Optional.of(value), 3, Optional.empty()));

    // absent segment
    assertEquals(
        Optional.empty(), top.peekThisLayer(mock(SegmentIdentifier.class), new byte[] {2}));
    // absent on this layer, even though a lower layer has it
    assertEquals(Optional.empty(), top.peekThisLayer(segmentId, new byte[] {1}));
    // present
    assertArrayEquals(value, top.peekThisLayer(segmentId, new byte[] {2}).orElseThrow().get());
    // deletion tombstone
    assertEquals(Optional.of(Optional.empty()), top.peekThisLayer(segmentId, new byte[] {3}));
    verify(parentStorage, never()).get(eq(segmentId), any());
  }

  @Test
  void streamOverInMemoryBaseMergesKeysOfDifferentLengthsInUnsignedByteOrder() {
    byte[] p = {9};
    byte[] v = {7};
    SegmentedInMemoryKeyValueStorage base = new SegmentedInMemoryKeyValueStorage();
    var baseTx = base.startTransaction();
    baseTx.put(segmentId, new byte[] {1, 5}, p);
    baseTx.put(segmentId, new byte[] {2}, p);
    baseTx.commit();

    // the tombstone on 0x0105 must hide the base entry, which is only possible if the base streams
    // in the same unsigned byte order as the layers (0x0105 before 0x02); in numeric order the
    // base would stream 0x0105 after 0x02, past the point where the tombstone was merged
    LayeredKeyValueStorage top = new LayeredKeyValueStorage(base);
    var tx = top.startTransaction();
    tx.put(segmentId, Bytes.EMPTY.toArrayUnsafe(), v);
    tx.put(segmentId, new byte[] {0}, v);
    tx.remove(segmentId, new byte[] {1, 5});
    tx.commit();

    assertEquals(
        List.of(
            Pair.of(Bytes.EMPTY, Bytes.wrap(v)),
            Pair.of(Bytes.of(0), Bytes.wrap(v)),
            Pair.of(Bytes.of(2), Bytes.wrap(p))),
        top.stream(segmentId)
            .map(e -> Pair.of(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue())))
            .toList());
  }

  @Test
  void streamFromKeyOverInMemoryBaseUsesUnsignedByteOrderBounds() {
    byte[] p = {9};
    byte[] v = {7};
    SegmentedInMemoryKeyValueStorage base = new SegmentedInMemoryKeyValueStorage();
    var baseTx = base.startTransaction();
    baseTx.put(segmentId, new byte[] {0, 1}, p);
    baseTx.put(segmentId, new byte[] {1, 5}, p);
    baseTx.put(segmentId, new byte[] {3}, p);
    baseTx.commit();
    LayeredKeyValueStorage top = new LayeredKeyValueStorage(base);
    var tx = top.startTransaction();
    tx.put(segmentId, new byte[] {1}, v);
    tx.put(segmentId, new byte[] {2, 0}, v);
    tx.commit();

    // unsigned byte order: 0x0001 < 0x01 < 0x0105 < 0x0200 < 0x03
    assertEquals(
        List.of(Bytes.of(1), Bytes.of(1, 5), Bytes.of(2, 0)),
        top.streamFromKey(segmentId, new byte[] {1}, new byte[] {2, 0})
            .map(e -> Bytes.wrap(e.getKey()))
            .toList());
    assertEquals(
        List.of(Bytes.of(1), Bytes.of(1, 5), Bytes.of(2, 0), Bytes.of(3)),
        top.streamFromKey(segmentId, new byte[] {1}).map(e -> Bytes.wrap(e.getKey())).toList());
  }

  @Test
  void deepChainStreamReachesParentAtConstantStackDepth() {
    byte[] key = {1};
    AtomicInteger maxDepth = new AtomicInteger();
    when(parentStorage.stream(segmentId))
        .thenAnswer(
            __ -> {
              recordStackDepth(maxDepth);
              return Stream.of(Pair.of(key, key)).peek(e -> recordStackDepth(maxDepth));
            });

    // A recursive implementation reaches the parent roughly one frame deeper per layer.
    assertEquals(
        parentStackDepth(1, maxDepth, top -> top.stream(segmentId).toList().size()),
        parentStackDepth(500, maxDepth, top -> top.stream(segmentId).toList().size()));
  }

  @Test
  void deepChainStreamKeysReachesParentAtConstantStackDepth() {
    byte[] key = {1};
    AtomicInteger maxDepth = new AtomicInteger();
    when(parentStorage.streamKeys(segmentId))
        .thenAnswer(
            __ -> {
              recordStackDepth(maxDepth);
              return Stream.of(key).peek(e -> recordStackDepth(maxDepth));
            });

    // A recursive implementation reaches the parent roughly one frame deeper per layer.
    assertEquals(
        parentStackDepth(1, maxDepth, top -> top.streamKeys(segmentId).toList().size()),
        parentStackDepth(500, maxDepth, top -> top.streamKeys(segmentId).toList().size()));
  }

  @Test
  void shouldMultigetFromLayerAndParentInInputOrder() {
    byte[] key1 = {1};
    byte[] key2 = {2};
    byte[] key3 = {3};
    byte[] parentValue1 = {10};
    byte[] layerValue2 = {20};

    when(parentStorage.multiget(segmentId, List.of(key1, key3)))
        .thenReturn(List.of(Optional.of(parentValue1), Optional.empty()));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key2), Optional.of(layerValue2));
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    final List<Optional<byte[]>> result =
        layeredKeyValueStorage.multiget(segmentId, List.of(key1, key2, key3));

    assertEquals(3, result.size());
    assertTrue(result.get(0).isPresent());
    assertArrayEquals(parentValue1, result.get(0).get());
    assertTrue(result.get(1).isPresent());
    assertArrayEquals(layerValue2, result.get(1).get());
    assertTrue(result.get(2).isEmpty());
  }

  @Test
  void shouldNotDelegateLocalMultigetTombstonesToParent() {
    byte[] key1 = {1};
    byte[] key2 = {2};
    byte[] parentValue2 = {20};

    when(parentStorage.multiget(segmentId, List.of(key2)))
        .thenReturn(List.of(Optional.of(parentValue2)));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key1), Optional.empty());
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    final List<Optional<byte[]>> result =
        layeredKeyValueStorage.multiget(segmentId, List.of(key1, key2));

    assertEquals(2, result.size());
    assertTrue(result.get(0).isEmpty());
    assertTrue(result.get(1).isPresent());
    assertArrayEquals(parentValue2, result.get(1).get());
    verify(parentStorage).multiget(segmentId, List.of(key2));
  }

  @Test
  void shouldDefaultMultigetDelegateToGetForEachInputKey() {
    final SegmentedKeyValueStorage storage =
        mock(SegmentedKeyValueStorage.class, CALLS_REAL_METHODS);
    byte[] key1 = {1};
    byte[] key2 = {2};
    byte[] value1 = {10};

    doReturn(Optional.of(value1)).when(storage).get(segmentId, key1);
    doReturn(Optional.empty()).when(storage).get(segmentId, key2);

    final List<Optional<byte[]>> result = storage.multiget(segmentId, List.of(key1, key2, key1));

    assertEquals(3, result.size());
    assertTrue(result.get(0).isPresent());
    assertArrayEquals(value1, result.get(0).get());
    assertTrue(result.get(1).isEmpty());
    assertTrue(result.get(2).isPresent());
    assertArrayEquals(value1, result.get(2).get());
    verify(storage, times(2)).get(segmentId, key1);
    verify(storage).get(segmentId, key2);
  }

  @Test
  void shouldPreserveDuplicateMultigetKeysWhenDelegatingToParent() {
    byte[] key1 = {1};
    byte[] key2 = {2};
    byte[] parentValue1 = {10};
    byte[] layerValue2 = {20};

    when(parentStorage.multiget(segmentId, List.of(key1, key1)))
        .thenReturn(List.of(Optional.of(parentValue1), Optional.of(parentValue1)));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key2), Optional.of(layerValue2));
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    final List<Optional<byte[]>> result =
        layeredKeyValueStorage.multiget(segmentId, List.of(key1, key2, key1));

    assertEquals(3, result.size());
    assertArrayEquals(parentValue1, result.get(0).orElseThrow());
    assertArrayEquals(layerValue2, result.get(1).orElseThrow());
    assertArrayEquals(parentValue1, result.get(2).orElseThrow());
    verify(parentStorage).multiget(segmentId, List.of(key1, key1));
  }

  @Test
  void shouldNotDelegateParentLayerTombstonesDuringMultiget() {
    byte[] key1 = {1};
    byte[] key2 = {2};
    byte[] parentValue2 = {20};

    when(parentStorage.multiget(segmentId, List.of(key2)))
        .thenReturn(List.of(Optional.of(parentValue2)));

    var parentLayerMap = createSegmentMap();
    parentLayerMap.get(segmentId).put(Bytes.wrap(key1), Optional.empty());
    layeredKeyValueStorage =
        new LayeredKeyValueStorage(
            new ConcurrentHashMap<>(), new LayeredKeyValueStorage(parentLayerMap, parentStorage));

    final List<Optional<byte[]>> result =
        layeredKeyValueStorage.multiget(segmentId, List.of(key1, key2));

    assertEquals(2, result.size());
    assertTrue(result.get(0).isEmpty());
    assertArrayEquals(parentValue2, result.get(1).orElseThrow());
    verify(parentStorage).multiget(segmentId, List.of(key2));
  }

  private LayeredKeyValueStorage layer(
      final SegmentedKeyValueStorage parent, final Map<Integer, Optional<byte[]>> entries) {
    var map = createSegmentMap();
    entries.forEach((key, value) -> map.get(segmentId).put(Bytes.of(key), value));
    return new LayeredKeyValueStorage(map, parent);
  }

  /**
   * Runs a read on a chain of the given depth and returns how much deeper than the caller the stack
   * was, at most, when the parent storage was reached. Also checks the read asks the parent storage
   * whether it is closed only once, rather than once per layer.
   */
  private int parentStackDepth(
      final int depth,
      final AtomicInteger maxDepth,
      final ToIntFunction<LayeredKeyValueStorage> read) {
    LayeredKeyValueStorage top = new LayeredKeyValueStorage(parentStorage);
    for (int i = 1; i < depth; i++) {
      top = new LayeredKeyValueStorage(top);
    }
    clearInvocations(parentStorage);
    maxDepth.set(0);
    int callerDepth = Thread.currentThread().getStackTrace().length;
    assertEquals(1, read.applyAsInt(top));
    verify(parentStorage, times(1)).isClosed();
    return maxDepth.get() - callerDepth;
  }

  private static void recordStackDepth(final AtomicInteger maxDepth) {
    maxDepth.accumulateAndGet(Thread.currentThread().getStackTrace().length, Math::max);
  }
}
