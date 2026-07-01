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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
