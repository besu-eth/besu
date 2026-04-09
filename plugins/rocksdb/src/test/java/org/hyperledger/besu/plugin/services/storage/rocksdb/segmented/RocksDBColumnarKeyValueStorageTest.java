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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.kvstore.AbstractKeyValueStorageTest;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public abstract class RocksDBColumnarKeyValueStorageTest extends AbstractKeyValueStorageTest {

  @Mock private ObservableMetricsSystem metricsSystemMock;
  @Mock private LabelledMetric<OperationTimer> labelledMetricOperationTimerMock;
  @Mock private LabelledMetric<Counter> labelledMetricCounterMock;
  @Mock private OperationTimer operationTimerMock;

  @TempDir public Path folder;

  @Test
  public void assertClear() throws Exception {
    final byte[] key = bytesFromHexString("0001");
    final byte[] val1 = bytesFromHexString("0FFF");
    final byte[] val2 = bytesFromHexString("1337");
    final SegmentedKeyValueStorage store = createSegmentedStore();
    var segment = TestSegment.FOO;
    KeyValueStorage duplicateSegmentRef =
        new SegmentedKeyValueStorageAdapter(TestSegment.FOO, store);

    final Consumer<byte[]> insert =
        value -> {
          final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
          tx.put(segment, key, value);
          tx.commit();
        };

    // insert val:
    insert.accept(val1);
    assertThat(store.get(segment, key).orElse(null)).isEqualTo(val1);
    assertThat(duplicateSegmentRef.get(key).orElse(null)).isEqualTo(val1);

    // clear and assert empty:
    store.clear(segment);
    assertThat(store.get(segment, key)).isEmpty();
    assertThat(duplicateSegmentRef.get(key)).isEmpty();

    // insert into empty:
    insert.accept(val2);
    assertThat(store.get(segment, key).orElse(null)).isEqualTo(val2);
    assertThat(duplicateSegmentRef.get(key).orElse(null)).isEqualTo(val2);

    store.close();
  }

  @Test
  public void twoSegmentsAreIndependent() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.BAR, bytesFromHexString("0001"), bytesFromHexString("0FFF"));
    tx.commit();

    final Optional<byte[]> result = store.get(TestSegment.FOO, bytesFromHexString("0001"));

    assertThat(result).isEmpty();

    store.close();
  }

  @Test
  public void canRemoveThroughSegmentIteration() throws Exception {
    // we're looping this in order to catch intermittent failures when rocksdb objects are not close
    // properly
    for (int i = 0; i < 50; i++) {
      final SegmentedKeyValueStorage store = createSegmentedStore();

      final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
      tx.put(TestSegment.FOO, bytesOf(1), bytesOf(1));
      tx.put(TestSegment.FOO, bytesOf(2), bytesOf(2));
      tx.put(TestSegment.FOO, bytesOf(3), bytesOf(3));
      tx.put(TestSegment.BAR, bytesOf(4), bytesOf(4));
      tx.put(TestSegment.BAR, bytesOf(5), bytesOf(5));
      tx.put(TestSegment.BAR, bytesOf(6), bytesOf(6));
      tx.commit();

      store.stream(TestSegment.FOO)
          .map(Pair::getKey)
          .forEach(
              key -> {
                if (!Arrays.equals(key, bytesOf(3))) store.tryDelete(TestSegment.FOO, key);
              });
      store.stream(TestSegment.BAR)
          .map(Pair::getKey)
          .forEach(
              key -> {
                if (!Arrays.equals(key, bytesOf(4))) store.tryDelete(TestSegment.BAR, key);
              });

      for (final var segment : Set.of(TestSegment.FOO, TestSegment.BAR)) {
        assertThat(store.stream(segment).count()).isEqualTo(1);
      }

      assertThat(store.get(TestSegment.FOO, bytesOf(1))).isEmpty();
      assertThat(store.get(TestSegment.FOO, bytesOf(2))).isEmpty();
      assertThat(store.get(TestSegment.FOO, bytesOf(3))).contains(bytesOf(3));

      assertThat(store.get(TestSegment.BAR, bytesOf(4))).contains(bytesOf(4));
      assertThat(store.get(TestSegment.BAR, bytesOf(5))).isEmpty();
      assertThat(store.get(TestSegment.BAR, bytesOf(6))).isEmpty();

      store.close();
    }
  }

  @Test
  public void canGetThroughSegmentIteration() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytesOf(1), bytesOf(1));
    tx.put(TestSegment.FOO, bytesOf(2), bytesOf(2));
    tx.put(TestSegment.FOO, bytesOf(3), bytesOf(3));
    tx.put(TestSegment.BAR, bytesOf(4), bytesOf(4));
    tx.put(TestSegment.BAR, bytesOf(5), bytesOf(5));
    tx.put(TestSegment.BAR, bytesOf(6), bytesOf(6));
    tx.commit();

    final Set<byte[]> gotFromFoo =
        store.getAllKeysThat(TestSegment.FOO, x -> Arrays.equals(x, bytesOf(3)));
    final Set<byte[]> gotFromBar =
        store.getAllKeysThat(
            TestSegment.BAR, x -> Arrays.equals(x, bytesOf(4)) || Arrays.equals(x, bytesOf(5)));
    final Set<byte[]> gotEmpty =
        store.getAllKeysThat(TestSegment.FOO, x -> Arrays.equals(x, bytesOf(0)));

    assertThat(gotFromFoo.size()).isEqualTo(1);
    assertThat(gotFromBar.size()).isEqualTo(2);
    assertThat(gotEmpty).isEmpty();

    assertThat(gotFromFoo).containsExactlyInAnyOrder(bytesOf(3));
    assertThat(gotFromBar).containsExactlyInAnyOrder(bytesOf(4), bytesOf(5));

    store.close();
  }

  @Test
  public void dbShouldIgnoreExperimentalSegmentsIfNotExisted(@TempDir final Path testPath)
      throws Exception {
    // Create new db should ignore experimental column family
    SegmentedKeyValueStorage store =
        createSegmentedStore(
            testPath,
            Arrays.asList(
                TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR, TestSegment.EXPERIMENTAL),
            List.of(TestSegment.EXPERIMENTAL));
    store.close();

    // new db will be backward compatible with db without knowledge of experimental column family
    store =
        createSegmentedStore(
            testPath,
            Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
            List.of());
    store.close();
  }

  @Test
  public void dbShouldNotIgnoreExperimentalSegmentsIfExisted(@TempDir final Path tempDir)
      throws Exception {
    final Path testPath = tempDir.resolve("testdb");
    // Create new db with experimental column family
    SegmentedKeyValueStorage store =
        createSegmentedStore(
            testPath,
            Arrays.asList(
                TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR, TestSegment.EXPERIMENTAL),
            List.of());
    store.close();

    // new db will not be backward compatible with db without knowledge of experimental column
    // family
    try {
      createSegmentedStore(
          testPath,
          Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
          List.of());
      fail("DB without knowledge of experimental column family should fail");
    } catch (StorageException e) {
      assertThat(e.getMessage()).contains("Unhandled column families");
    }

    // Even if the column family is marked as ignored, as long as it exists, it will not be ignored
    // and the db opens normally
    store =
        createSegmentedStore(
            testPath,
            Arrays.asList(
                TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR, TestSegment.EXPERIMENTAL),
            List.of(TestSegment.EXPERIMENTAL));
    store.close();
  }

  @Test
  public void dbWillBeBackwardIncompatibleAfterExperimentalSegmentsAreAdded(
      @TempDir final Path testPath) throws Exception {
    // Create new db should ignore experimental column family
    SegmentedKeyValueStorage store =
        createSegmentedStore(
            testPath,
            Arrays.asList(
                TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR, TestSegment.EXPERIMENTAL),
            List.of(TestSegment.EXPERIMENTAL));
    store.close();

    // new db will be backward compatible with db without knowledge of experimental column family
    store =
        createSegmentedStore(
            testPath,
            Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
            List.of());
    store.close();

    // Create new db without ignoring experimental column family will add column to db
    store =
        createSegmentedStore(
            testPath,
            Arrays.asList(
                TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR, TestSegment.EXPERIMENTAL),
            List.of());
    store.close();

    // Now, the db will be backward incompatible with db without knowledge of experimental column
    // family
    try {
      createSegmentedStore(
          testPath,
          Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
          List.of());
      fail("DB without knowledge of experimental column family should fail");
    } catch (StorageException e) {
      assertThat(e.getMessage()).contains("Unhandled column families");
    }
  }

  @Test
  public void createStoreMustCreateMetrics() throws Exception {
    // Prepare mocks
    when(labelledMetricOperationTimerMock.labels(any())).thenReturn(operationTimerMock);
    when(metricsSystemMock.createLabelledTimer(
            eq(BesuMetricCategory.KVSTORE_ROCKSDB), anyString(), anyString(), any()))
        .thenReturn(labelledMetricOperationTimerMock);
    when(metricsSystemMock.createLabelledCounter(
            eq(BesuMetricCategory.KVSTORE_ROCKSDB), anyString(), anyString(), any()))
        .thenReturn(labelledMetricCounterMock);
    // Prepare argument captors
    final ArgumentCaptor<String> labelledTimersMetricsNameArgs =
        ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> labelledTimersHelpArgs = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> labelledCountersMetricsNameArgs =
        ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> labelledCountersHelpArgs = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> longGaugesMetricsNameArgs = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> longGaugesHelpArgs = ArgumentCaptor.forClass(String.class);

    // Actual call

    try (final SegmentedKeyValueStorage store =
        createSegmentedStore(
            folder,
            metricsSystemMock,
            List.of(TestSegment.DEFAULT, TestSegment.FOO),
            List.of(TestSegment.EXPERIMENTAL))) {

      KeyValueStorage keyValueStorage = new SegmentedKeyValueStorageAdapter(TestSegment.FOO, store);

      // Assertions
      assertThat(keyValueStorage).isNotNull();
      verify(metricsSystemMock, times(4))
          .createLabelledTimer(
              eq(BesuMetricCategory.KVSTORE_ROCKSDB),
              labelledTimersMetricsNameArgs.capture(),
              labelledTimersHelpArgs.capture(),
              any());
      assertThat(labelledTimersMetricsNameArgs.getAllValues())
          .containsExactly(
              "read_latency_seconds",
              "remove_latency_seconds",
              "write_latency_seconds",
              "commit_latency_seconds");
      assertThat(labelledTimersHelpArgs.getAllValues())
          .containsExactly(
              "Latency for read from RocksDB.",
              "Latency of remove requests from RocksDB.",
              "Latency for write to RocksDB.",
              "Latency for commits to RocksDB.");

      verify(metricsSystemMock, times(2))
          .createLongGauge(
              eq(BesuMetricCategory.KVSTORE_ROCKSDB),
              longGaugesMetricsNameArgs.capture(),
              longGaugesHelpArgs.capture(),
              any(LongSupplier.class));
      assertThat(longGaugesMetricsNameArgs.getAllValues())
          .containsExactly("rocks_db_table_readers_memory_bytes", "rocks_db_files_size_bytes");
      assertThat(longGaugesHelpArgs.getAllValues())
          .containsExactly(
              "Estimated memory used for RocksDB index and filter blocks in bytes",
              "Estimated database size in bytes");

      verify(metricsSystemMock)
          .createLabelledCounter(
              eq(BesuMetricCategory.KVSTORE_ROCKSDB),
              labelledCountersMetricsNameArgs.capture(),
              labelledCountersHelpArgs.capture(),
              any());
      assertThat(labelledCountersMetricsNameArgs.getValue()).isEqualTo("rollback_count");
      assertThat(labelledCountersHelpArgs.getValue())
          .isEqualTo("Number of RocksDB transactions rolled back.");
    }
  }

  @Test
  public void lowPriorityTransactionCommitPersistsValues() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");
    final byte[] value = bytesFromHexString("0FFF");

    final SegmentedKeyValueStorageTransaction tx = store.startLowPriorityTransaction();
    tx.put(TestSegment.FOO, key, value);
    tx.commit();

    assertThat(store.get(TestSegment.FOO, key)).contains(value);
    store.close();
  }

  @Test
  public void lowPriorityTransactionRollbackDiscardsValues() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");
    final byte[] value = bytesFromHexString("0FFF");

    final SegmentedKeyValueStorageTransaction tx = store.startLowPriorityTransaction();
    tx.put(TestSegment.FOO, key, value);
    tx.rollback();

    assertThat(store.get(TestSegment.FOO, key)).isEmpty();
    store.close();
  }

  @Test
  public void lowPriorityTransactionThrowsAfterCommit() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    final SegmentedKeyValueStorageTransaction tx = store.startLowPriorityTransaction();
    tx.commit();

    assertThatThrownBy(
            () -> tx.put(TestSegment.FOO, bytesFromHexString("0001"), bytesFromHexString("0FFF")))
        .isInstanceOf(IllegalStateException.class);
    store.close();
  }

  public enum TestSegment implements SegmentIdentifier {
    DEFAULT("default".getBytes(StandardCharsets.UTF_8)),
    FOO(new byte[] {1}),
    BAR(new byte[] {2}),
    EXPERIMENTAL(new byte[] {3}),

    STATIC_DATA(new byte[] {4}, true, false);

    private final byte[] id;
    private final String nameAsUtf8;
    private final boolean containsStaticData;
    private final boolean eligibleToHighSpecFlag;

    TestSegment(final byte[] id) {
      this(id, false, false);
    }

    TestSegment(
        final byte[] id, final boolean containsStaticData, final boolean eligibleToHighSpecFlag) {
      this.id = id;
      this.nameAsUtf8 = new String(id, StandardCharsets.UTF_8);
      this.containsStaticData = containsStaticData;
      this.eligibleToHighSpecFlag = eligibleToHighSpecFlag;
    }

    @Override
    public String getName() {
      return nameAsUtf8;
    }

    @Override
    public byte[] getId() {
      return id;
    }

    @Override
    public boolean containsStaticData() {
      return containsStaticData;
    }

    @Override
    public boolean isEligibleToHighSpecFlag() {
      return eligibleToHighSpecFlag;
    }
  }

  protected abstract SegmentedKeyValueStorage createSegmentedStore() throws Exception;

  protected abstract SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments);

  protected abstract SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final MetricsSystem metricsSystem,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments);

  @Override
  protected KeyValueStorage createStore() throws Exception {
    return new SegmentedKeyValueStorageAdapter(TestSegment.FOO, createSegmentedStore());
  }
}
