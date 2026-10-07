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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
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

  @Test
  public void rewriteReplacesDropsAndAddsEntries() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    putOneTwoThree(storage);

    storage.rewrite(
        TestSegment.FOO,
        (key, value) -> key[0] == 2 ? null : bytesOf(value[0], key[0]),
        List.of(Pair.of(bytesOf(9), bytesOf(9, 9))));

    assertValue(storage.get(TestSegment.FOO, bytesOf(1)), bytesOf(10, 1));
    assertThat(storage.get(TestSegment.FOO, bytesOf(2))).isEmpty();
    assertValue(storage.get(TestSegment.FOO, bytesOf(3)), bytesOf(30, 3));
    assertValue(storage.get(TestSegment.FOO, bytesOf(9)), bytesOf(9, 9));
    assertValue(storage.get(TestSegment.BAR, bytesOf(1)), bytesOf(100));
  }

  @Test
  public void rewriteDroppingEveryEntryLeavesOnlyTheAdditions() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    putOneTwoThree(storage);

    storage.rewrite(
        TestSegment.FOO, (key, value) -> null, List.of(Pair.of(bytesOf(9), bytesOf(9, 9))));

    assertThat(storage.stream(TestSegment.FOO).map(Pair::getKey).toList())
        .containsExactly(bytesOf(9));
  }

  @Test
  public void rewriteThatFailsLeavesTheSegmentAlone() {
    final SegmentedInMemoryKeyValueStorage storage = new SegmentedInMemoryKeyValueStorage();
    putOneTwoThree(storage);

    assertThatThrownBy(
            () ->
                storage.rewrite(
                    TestSegment.FOO,
                    (key, value) -> {
                      if (key[0] == 3) {
                        throw new IllegalStateException("no new value for this entry");
                      }
                      return bytesOf(0);
                    },
                    List.of(Pair.of(bytesOf(9), bytesOf(9, 9)))))
        .isInstanceOf(IllegalStateException.class);

    assertValue(storage.get(TestSegment.FOO, bytesOf(1)), bytesOf(10));
    assertValue(storage.get(TestSegment.FOO, bytesOf(2)), bytesOf(20));
    assertValue(storage.get(TestSegment.FOO, bytesOf(3)), bytesOf(30));
    assertThat(storage.get(TestSegment.FOO, bytesOf(9))).isEmpty();
  }

  @Test
  public void rewriteThatFailsRollsItsTransactionBack() {
    final List<String> ended = new ArrayList<>();
    final SegmentedInMemoryKeyValueStorage storage =
        new SegmentedInMemoryKeyValueStorage() {
          @Override
          public SegmentedKeyValueStorageTransaction startTransaction() {
            return new RecordingTransaction(super.startTransaction(), ended);
          }
        };
    putOneTwoThree(storage);
    ended.clear();

    assertThatThrownBy(
            () ->
                storage.rewrite(
                    TestSegment.FOO,
                    (key, value) -> {
                      throw new IllegalStateException("no new value for this entry");
                    },
                    List.of()))
        .isInstanceOf(IllegalStateException.class);

    assertThat(ended).containsExactly("rollback");
  }

  private record RecordingTransaction(
      SegmentedKeyValueStorageTransaction delegate, List<String> ended)
      implements SegmentedKeyValueStorageTransaction {

    @Override
    public void put(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
      delegate.put(segment, key, value);
    }

    @Override
    public void remove(final SegmentIdentifier segment, final byte[] key) {
      delegate.remove(segment, key);
    }

    @Override
    public void commit() {
      ended.add("commit");
      delegate.commit();
    }

    @Override
    public void rollback() {
      ended.add("rollback");
      delegate.rollback();
    }

    @Override
    public void close() {
      ended.add("close");
      delegate.close();
    }
  }

  private static void putOneTwoThree(final SegmentedInMemoryKeyValueStorage storage) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    tx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
    tx.put(TestSegment.FOO, bytesOf(3), bytesOf(30));
    tx.put(TestSegment.BAR, bytesOf(1), bytesOf(100));
    tx.commit();
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
