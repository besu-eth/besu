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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

public class LayeredKeyValueStorageTest {

  private static final SegmentIdentifier SEGMENT =
      new SegmentIdentifier() {
        @Override
        public String getName() {
          return "test-segment";
        }

        @Override
        public byte[] getId() {
          return new byte[] {1};
        }

        @Override
        public boolean containsStaticData() {
          return false;
        }

        @Override
        public boolean isEligibleToHighSpecFlag() {
          return false;
        }
      };

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** In-memory parent that counts get() calls, standing in for the persistent (disk) storage. */
  private static class CountingInMemoryStorage extends SegmentedInMemoryKeyValueStorage {
    final AtomicInteger getCalls = new AtomicInteger();

    @Override
    public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key) {
      getCalls.incrementAndGet();
      return super.get(segmentId, key);
    }
  }

  @Test
  void getFromLayersOnlyReturnsValueFromOwnLayer() {
    final CountingInMemoryStorage parent = new CountingInMemoryStorage();
    final LayeredKeyValueStorage layer = new LayeredKeyValueStorage(parent);

    final var tx = layer.startTransaction();
    tx.put(SEGMENT, bytes("key"), bytes("layer-value"));
    tx.commit();

    assertThat(layer.getFromLayersOnly(SEGMENT, bytes("key"))).contains(bytes("layer-value"));
    assertThat(parent.getCalls.get()).isZero();
  }

  @Test
  void getFromLayersOnlyDoesNotFallThroughToNonLayeredParent() {
    final CountingInMemoryStorage parent = new CountingInMemoryStorage();
    final var parentTx = parent.startTransaction();
    parentTx.put(SEGMENT, bytes("key"), bytes("disk-value"));
    parentTx.commit();

    final LayeredKeyValueStorage layer = new LayeredKeyValueStorage(parent);

    // Regular get() falls through to the parent...
    assertThat(layer.get(SEGMENT, bytes("key"))).contains(bytes("disk-value"));
    // ...but the layers-only read must not touch it.
    parent.getCalls.set(0);
    assertThat(layer.getFromLayersOnly(SEGMENT, bytes("key"))).isEmpty();
    assertThat(parent.getCalls.get()).isZero();
  }

  @Test
  void getFromLayersOnlyReadsThroughNestedLayers() {
    final CountingInMemoryStorage parent = new CountingInMemoryStorage();
    final LayeredKeyValueStorage innerLayer = new LayeredKeyValueStorage(parent);
    final var tx = innerLayer.startTransaction();
    tx.put(SEGMENT, bytes("key"), bytes("inner-value"));
    tx.commit();

    final LayeredKeyValueStorage outerLayer = new LayeredKeyValueStorage(innerLayer);

    assertThat(outerLayer.getFromLayersOnly(SEGMENT, bytes("key"))).contains(bytes("inner-value"));
    assertThat(parent.getCalls.get()).isZero();
  }

  @Test
  void getFromLayersOnlyHonoursDeletionTombstone() {
    final CountingInMemoryStorage parent = new CountingInMemoryStorage();
    final var parentTx = parent.startTransaction();
    parentTx.put(SEGMENT, bytes("key"), bytes("disk-value"));
    parentTx.commit();

    final LayeredKeyValueStorage layer = new LayeredKeyValueStorage(parent);
    final var tx = layer.startTransaction();
    tx.remove(SEGMENT, bytes("key"));
    tx.commit();

    assertThat(layer.getFromLayersOnly(SEGMENT, bytes("key"))).isEmpty();
  }

  @Test
  void getFromLayersOnlyTombstoneInOuterLayerShadowsInnerLayerValue() {
    final CountingInMemoryStorage parent = new CountingInMemoryStorage();
    final LayeredKeyValueStorage innerLayer = new LayeredKeyValueStorage(parent);
    final var innerTx = innerLayer.startTransaction();
    innerTx.put(SEGMENT, bytes("key"), bytes("inner-value"));
    innerTx.commit();

    final LayeredKeyValueStorage outerLayer = new LayeredKeyValueStorage(innerLayer);
    final var outerTx = outerLayer.startTransaction();
    outerTx.remove(SEGMENT, bytes("key"));
    outerTx.commit();

    assertThat(outerLayer.getFromLayersOnly(SEGMENT, bytes("key"))).isEmpty();
  }
}
