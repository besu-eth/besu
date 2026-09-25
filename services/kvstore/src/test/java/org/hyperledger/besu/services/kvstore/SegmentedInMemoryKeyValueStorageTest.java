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
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class SegmentedInMemoryKeyValueStorageTest {

  @Test
  public void multigetPreservesInputOrderDuplicatesMissingAndSegmentIsolation() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    tx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
    tx.put(TestSegment.BAR, bytesOf(1), bytesOf(100));
    tx.commit();

    final List<Optional<byte[]>> values =
        storage.multiget(TestSegment.FOO, List.of(bytesOf(2), bytesOf(3), bytesOf(1), bytesOf(2)));

    assertThat(values).hasSize(4);
    assertValue(values.get(0), bytesOf(20));
    assertThat(values.get(1)).isEmpty();
    assertValue(values.get(2), bytesOf(10));
    assertValue(values.get(3), bytesOf(20));

    final List<Optional<byte[]>> barValues =
        storage.multiget(TestSegment.BAR, List.of(bytesOf(1), bytesOf(2)));
    assertValue(barValues.get(0), bytesOf(100));
    assertThat(barValues.get(1)).isEmpty();
  }

  @Test
  public void multigetReflectsCommittedRemovals() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();

    final SegmentedKeyValueStorageTransaction putTx = storage.startTransaction();
    putTx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    putTx.commit();

    final SegmentedKeyValueStorageTransaction removeTx = storage.startTransaction();
    removeTx.remove(TestSegment.FOO, bytesOf(1));
    removeTx.commit();

    assertThat(storage.multiget(TestSegment.FOO, List.of(bytesOf(1))).get(0)).isEmpty();
  }

  @Test
  public void snapshotMultigetIsIsolatedFromLaterWrites() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();

    final SegmentedKeyValueStorageTransaction initialTx = storage.startTransaction();
    initialTx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    initialTx.commit();

    final SegmentedInMemoryKeyValueStorage snapshot = storage.takeSnapshot();

    final SegmentedKeyValueStorageTransaction updateTx = storage.startTransaction();
    updateTx.put(TestSegment.FOO, bytesOf(1), bytesOf(11));
    updateTx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
    updateTx.commit();

    final List<Optional<byte[]>> snapshotValues =
        snapshot.multiget(TestSegment.FOO, List.of(bytesOf(1), bytesOf(2)));
    assertValue(snapshotValues.get(0), bytesOf(10));
    assertThat(snapshotValues.get(1)).isEmpty();

    final List<Optional<byte[]>> currentValues =
        storage.multiget(TestSegment.FOO, List.of(bytesOf(1), bytesOf(2)));
    assertValue(currentValues.get(0), bytesOf(11));
    assertValue(currentValues.get(1), bytesOf(20));
  }

  private static void assertValue(final Optional<byte[]> actual, final byte[] expected) {
    assertThat(actual).isPresent();
    assertThat(actual.get()).containsExactly(expected);
  }

  private static byte[] bytesOf(final int... values) {
    final byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return bytes;
  }

  private enum TestSegment implements SegmentIdentifier {
    FOO,
    BAR;

    @Override
    public String getName() {
      return name();
    }

    @Override
    public byte[] getId() {
      return name().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean containsStaticData() {
      return false;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return false;
    }
  }
}
