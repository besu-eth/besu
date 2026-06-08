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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    NavigableMap<Bytes, Optional<byte[]>> segmentMap = new TreeMap<>();
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
  void shouldMergeParentAndLayerData() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};
    byte[] key3 = {3};
    byte[] value3 = {30};

    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, value1), Pair.of(key3, value3)));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key2), Optional.of(value2));
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(3, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
    assertArrayEquals(key2, resultList.get(1).getKey());
    assertArrayEquals(value2, resultList.get(1).getValue());
    assertArrayEquals(key3, resultList.get(2).getKey());
    assertArrayEquals(value3, resultList.get(2).getValue());
  }

  @Test
  void shouldPreferLayerDataOverParentDataForSameKey() {
    byte[] key = {1};
    byte[] parentValue = {10};
    byte[] layerValue = {20};

    when(parentStorage.stream(segmentId)).thenReturn(Stream.of(Pair.of(key, parentValue)));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key), Optional.of(layerValue));
    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(1, resultList.size());
    assertArrayEquals(key, resultList.get(0).getKey());
    // Layer value should be returned
    assertArrayEquals(layerValue, resultList.get(0).getValue());
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
  void shouldThrowWhenParentMultigetReturnsUnexpectedValueCount() {
    byte[] key1 = {1};
    byte[] key2 = {2};

    when(parentStorage.multiget(segmentId, List.of(key1, key2)))
        .thenReturn(List.of(Optional.empty()));

    assertThatThrownBy(() -> layeredKeyValueStorage.multiget(segmentId, List.of(key1, key2)))
        .isInstanceOf(StorageException.class)
        .hasMessageContaining("Parent multiget returned 1 values for 2 keys");
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

  @Test
  void shouldNotStreamKeyIfLayerKeyIsEmpty() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};

    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, value1), Pair.of(key2, value2)));

    var hashValueStore = createSegmentMap();
    hashValueStore.get(segmentId).put(Bytes.wrap(key1), Optional.empty());

    layeredKeyValueStorage = new LayeredKeyValueStorage(hashValueStore, parentStorage);

    var resultList = layeredKeyValueStorage.stream(segmentId).toList();
    assertEquals(1, resultList.size());
    assertArrayEquals(key2, resultList.get(0).getKey());
    assertArrayEquals(value2, resultList.get(0).getValue());
  }

  /**
   * Tests that the stream method correctly handles multiple layers where the current layer
   * overrides the parent layers.
   */
  @Test
  void shouldStreamWithMultipleLayersAndCurrentLayerOverrides() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};
    byte[] key3 = {3};
    byte[] value3 = {30};

    // Parent Layer 0
    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, null), Pair.of(key2, value2)));

    // Parent Layer 1
    var parentLayer1 = createSegmentMap();
    parentLayer1.get(segmentId).put(Bytes.wrap(key1), Optional.of(value1));
    parentLayer1.get(segmentId).put(Bytes.wrap(key2), Optional.of(value2));

    // Current Layer
    var currentLayer = createSegmentMap();
    currentLayer.get(segmentId).put(Bytes.wrap(key1), Optional.empty());
    currentLayer.get(segmentId).put(Bytes.wrap(key3), Optional.of(value3));

    layeredKeyValueStorage =
        new LayeredKeyValueStorage(
            currentLayer, new LayeredKeyValueStorage(parentLayer1, parentStorage));

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(2, resultList.size());
    assertArrayEquals(key2, resultList.get(0).getKey());
    assertArrayEquals(value2, resultList.get(0).getValue());
    assertArrayEquals(key3, resultList.get(1).getKey());
    assertArrayEquals(value3, resultList.get(1).getValue());
  }

  /**
   * Tests that the stream method correctly handles multiple layers where the current layer
   * overrides the parent layers with specific values.
   */
  @Test
  void shouldStreamWithMultipleLayersAndCurrentLayerOverridesWithValues() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};
    byte[] key3 = {3};
    byte[] value3 = {30};

    // Parent Layer 0
    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, value1), Pair.of(key2, value2)));

    // Parent Layer 1
    var parentLayer1 = createSegmentMap();
    parentLayer1.get(segmentId).put(Bytes.wrap(key1), Optional.empty());
    parentLayer1.get(segmentId).put(Bytes.wrap(key2), Optional.of(value2));

    // Current Layer
    var currentLayer = createSegmentMap();
    currentLayer.get(segmentId).put(Bytes.wrap(key1), Optional.of(value1));
    currentLayer.get(segmentId).put(Bytes.wrap(key3), Optional.of(value3));

    layeredKeyValueStorage =
        new LayeredKeyValueStorage(
            currentLayer, new LayeredKeyValueStorage(parentLayer1, parentStorage));

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(3, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
    assertArrayEquals(key2, resultList.get(1).getKey());
    assertArrayEquals(value2, resultList.get(1).getValue());
    assertArrayEquals(key3, resultList.get(2).getKey());
    assertArrayEquals(value3, resultList.get(2).getValue());
  }

  /**
   * Tests that the stream method correctly handles multiple layers where the current layer
   * overrides the parent layers with empty values.
   */
  @Test
  void shouldStreamWithMultipleLayersAndCurrentLayerOverridesWithEmptyValues() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};
    byte[] key3 = {3};
    byte[] value3 = {30};

    // Parent Layer 0
    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, null), Pair.of(key2, value2)));

    // Parent Layer 1
    var parentLayer1 = createSegmentMap();
    parentLayer1.get(segmentId).put(Bytes.wrap(key1), Optional.empty());
    parentLayer1.get(segmentId).put(Bytes.wrap(key2), Optional.of(value2));

    // Current Layer
    var currentLayer = createSegmentMap();
    currentLayer.get(segmentId).put(Bytes.wrap(key1), Optional.of(value1));
    currentLayer.get(segmentId).put(Bytes.wrap(key3), Optional.of(value3));

    layeredKeyValueStorage =
        new LayeredKeyValueStorage(
            currentLayer, new LayeredKeyValueStorage(parentLayer1, parentStorage));

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(3, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
    assertArrayEquals(key2, resultList.get(1).getKey());
    assertArrayEquals(value2, resultList.get(1).getValue());
    assertArrayEquals(key3, resultList.get(2).getKey());
    assertArrayEquals(value3, resultList.get(2).getValue());
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
   * Verifies that isClosed() on a deep chain only queries the root storage once after the closed
   * state has been cached, regardless of chain depth. This is the O(1) correctness test for the
   * volatile closedCache fix that eliminates the O(N) recursion hot path (besu-eth/besu#10498).
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

    // First call at the top of the chain should propagate down to the root and cache at each layer
    assertTrue(top.isClosed());

    // Second call on the same instance must NOT reach the root again (cache hit)
    assertTrue(top.isClosed());

    // The root mock should have been queried at most once across both top-level calls because the
    // intermediate layers cache the result and short-circuit before reaching the root
    verify(parentStorage, atMostOnce()).isClosed();
  }

  /**
   * Tests that the stream method correctly handles a parent layer and a current layer where the
   * current layer overrides the parent layer.
   */
  @Test
  void shouldStreamWithParentLayerAndCurrentLayerOverrides() {
    byte[] key1 = {1};
    byte[] value1 = {10};
    byte[] key2 = {2};
    byte[] value2 = {20};
    byte[] key3 = {3};
    byte[] value3 = {30};

    // Parent Layer 0
    when(parentStorage.stream(segmentId))
        .thenReturn(Stream.of(Pair.of(key1, null), Pair.of(key2, value2)));

    // Current Layer
    var currentLayer = createSegmentMap();
    currentLayer.get(segmentId).put(Bytes.wrap(key1), Optional.of(value1));
    currentLayer.get(segmentId).put(Bytes.wrap(key3), Optional.of(value3));

    layeredKeyValueStorage = new LayeredKeyValueStorage(currentLayer, parentStorage);

    Stream<Pair<byte[], byte[]>> result = layeredKeyValueStorage.stream(segmentId);

    List<Pair<byte[], byte[]>> resultList = result.toList();
    assertEquals(3, resultList.size());
    assertArrayEquals(key1, resultList.get(0).getKey());
    assertArrayEquals(value1, resultList.get(0).getValue());
    assertArrayEquals(key2, resultList.get(1).getKey());
    assertArrayEquals(value2, resultList.get(1).getValue());
    assertArrayEquals(key3, resultList.get(2).getKey());
    assertArrayEquals(value3, resultList.get(2).getValue());
  }
}
