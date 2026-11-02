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
import org.hyperledger.besu.plugin.services.storage.SegmentBulkLoad;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;

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
  public void multigetPreservesInputOrderAndMissingValues() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    tx.put(TestSegment.FOO, bytesOf(3), bytesOf(30));
    tx.put(TestSegment.BAR, bytesOf(2), bytesOf(20));
    tx.commit();

    final List<Optional<byte[]>> values =
        store.multiget(TestSegment.FOO, List.of(bytesOf(3), bytesOf(2), bytesOf(1)));
    assertThat(values).hasSize(3);
    assertThat(values.get(0)).isPresent();
    assertThat(values.get(0).get()).isEqualTo(bytesOf(30));
    assertThat(values.get(1)).isEmpty();
    assertThat(values.get(2)).isPresent();
    assertThat(values.get(2).get()).isEqualTo(bytesOf(10));

    store.close();
  }

  @Test
  public void multigetPreservesDuplicateKeysAndHandlesEmptyInput() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    assertThat(store.multiget(TestSegment.FOO, List.of())).isEmpty();

    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    tx.commit();

    final List<Optional<byte[]>> values =
        store.multiget(TestSegment.FOO, List.of(bytesOf(1), bytesOf(1)));

    assertThat(values).hasSize(2);
    assertThat(values.get(0)).isPresent();
    assertThat(values.get(0).get()).isEqualTo(bytesOf(10));
    assertThat(values.get(1)).isPresent();
    assertThat(values.get(1).get()).isEqualTo(bytesOf(10));

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
      verify(metricsSystemMock, times(5))
          .createLabelledTimer(
              eq(BesuMetricCategory.KVSTORE_ROCKSDB),
              labelledTimersMetricsNameArgs.capture(),
              labelledTimersHelpArgs.capture(),
              any());
      assertThat(labelledTimersMetricsNameArgs.getAllValues())
          .containsExactly(
              "read_latency_seconds",
              "multi_read_latency_seconds",
              "remove_latency_seconds",
              "write_latency_seconds",
              "commit_latency_seconds");
      assertThat(labelledTimersHelpArgs.getAllValues())
          .containsExactly(
              "Latency for read from RocksDB.",
              "Latency for multi read from RocksDB.",
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

  @Test
  public void batchTransactionCommitPersistsValues() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");
    final byte[] value = bytesFromHexString("0FFF");

    final SegmentedKeyValueStorageTransaction tx = store.startBatchTransaction();
    tx.put(TestSegment.FOO, key, value);
    tx.put(TestSegment.BAR, key, value);
    tx.remove(TestSegment.BAR, key);
    tx.commit();

    assertThat(store.get(TestSegment.FOO, key)).contains(value);
    assertThat(store.get(TestSegment.BAR, key)).isEmpty();
    store.close();
  }

  @Test
  public void batchTransactionRollbackDiscardsValues() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");

    final SegmentedKeyValueStorageTransaction tx = store.startBatchTransaction();
    tx.put(TestSegment.FOO, key, bytesFromHexString("0FFF"));
    tx.rollback();

    assertThat(store.get(TestSegment.FOO, key)).isEmpty();
    store.close();
  }

  @Test
  public void batchTransactionThrowsAfterCommit() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    final SegmentedKeyValueStorageTransaction tx = store.startBatchTransaction();
    tx.commit();

    assertThatThrownBy(
            () -> tx.put(TestSegment.FOO, bytesFromHexString("0001"), bytesFromHexString("0FFF")))
        .isInstanceOf(IllegalStateException.class);
    store.close();
  }

  @Test
  public void sortedWriterAddsItsEntriesAtOnceAsAFile() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
    writer.put(bytesFromHexString("0001"), bytesFromHexString("0A"));
    writer.put(bytesFromHexString("0002"), bytesFromHexString("0B"));
    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0001"))).isEmpty();

    writer.commit();

    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0001")))
        .contains(bytesFromHexString("0A"));
    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0002")))
        .contains(bytesFromHexString("0B"));
    // added as a file, not through the memtable
    final ColumnFamilyHandle foo = store.safeColumnHandle(TestSegment.FOO);
    assertThat(store.getDB().getProperty(foo, "rocksdb.num-entries-active-mem-table"))
        .isEqualTo("0");
    store.close();
  }

  @Test
  public void sortedWriterRefusesAKeyBelowThePreviousOne() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
    writer.put(bytesFromHexString("0002"), bytesFromHexString("0B"));

    assertThatThrownBy(() -> writer.put(bytesFromHexString("0001"), bytesFromHexString("0A")))
        .isInstanceOf(StorageException.class);
    writer.rollback();
    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0002"))).isEmpty();
    store.close();
  }

  @Test
  public void sortedWriterOverridesOlderValuesOfItsKeys() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytesFromHexString("0001"), bytesFromHexString("0A"));
    tx.commit();
    final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
    writer.put(bytesFromHexString("0001"), bytesFromHexString("0B"));

    writer.commit();

    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0001")))
        .contains(bytesFromHexString("0B"));
    store.close();
  }

  @Test
  public void sortedWriterDeletesItsFileOnceCommittedOrRolledBack(@TempDir final Path tempDir)
      throws Exception {
    final Path databaseDir = tempDir.resolve("db");
    final SegmentedKeyValueStorage store =
        createSegmentedStore(
            databaseDir,
            Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
            List.of());
    final Path sorted = RocksDBSortedWriter.directory(databaseDir);

    final SortedWriter committed = store.startSortedWriter(TestSegment.FOO);
    committed.put(bytesFromHexString("0001"), bytesFromHexString("0A"));
    assertThat(filesIn(sorted)).hasSize(1);
    committed.commit();
    assertThat(filesIn(sorted)).isEmpty();

    final SortedWriter rolledBack = store.startSortedWriter(TestSegment.FOO);
    rolledBack.put(bytesFromHexString("0002"), bytesFromHexString("0B"));
    rolledBack.rollback();
    assertThat(filesIn(sorted)).isEmpty();
    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0002"))).isEmpty();

    store.startSortedWriter(TestSegment.FOO).commit();
    assertThat(filesIn(sorted)).isEmpty();
    store.close();
  }

  @Test
  public void sortedWriterCannotBeUsedOnceCommitted() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
    writer.put(bytesFromHexString("0001"), bytesFromHexString("0A"));
    writer.commit();

    assertThatThrownBy(() -> writer.put(bytesFromHexString("0002"), bytesFromHexString("0B")))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(writer::commit).isInstanceOf(IllegalStateException.class);
    writer.rollback();
    assertThat(store.get(TestSegment.FOO, bytesFromHexString("0001")))
        .contains(bytesFromHexString("0A"));
    store.close();
  }

  @Test
  public void sortedWriterCannotCommitOnceTheStorageIsClosed(@TempDir final Path tempDir)
      throws Exception {
    final Path databaseDir = tempDir.resolve("db");
    final SegmentedKeyValueStorage store =
        createSegmentedStore(
            databaseDir,
            Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
            List.of());
    final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
    writer.put(bytesFromHexString("0001"), bytesFromHexString("0A"));
    store.close();

    assertThatThrownBy(writer::commit).isInstanceOf(IllegalStateException.class);
    assertThat(filesIn(RocksDBSortedWriter.directory(databaseDir))).isEmpty();
  }

  @Test
  public void openingTheDatabaseDeletesSortedFilesLeftBehind(@TempDir final Path tempDir)
      throws Exception {
    final Path databaseDir = tempDir.resolve("db");
    final List<SegmentIdentifier> segments =
        Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR);
    createSegmentedStore(databaseDir, segments, List.of()).close();
    final Path sorted = RocksDBSortedWriter.directory(databaseDir);
    Files.createDirectories(sorted.resolve("nested"));
    Files.write(sorted.resolve("left.sst"), new byte[] {1});
    Files.write(sorted.resolve("nested").resolve("left.sst"), new byte[] {1});

    createSegmentedStore(databaseDir, segments, List.of()).close();

    assertThat(sorted).doesNotExist();
  }

  private static List<Path> filesIn(final Path directory) throws IOException {
    try (final Stream<Path> files = Files.list(directory)) {
      return files.toList();
    }
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

  @Test
  public void rewriteReplacesTheSegmentContentInOneStep() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytes(1), bytes(1));
    tx.put(TestSegment.FOO, bytes(2), bytes(2));
    tx.put(TestSegment.FOO, bytes(3), bytes(3));
    tx.put(TestSegment.BAR, bytes(1), bytes(1));
    tx.commit();

    store.rewrite(
        TestSegment.FOO, RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey, ADDITION);

    assertRewritten(store);
    assertThat(store.get(TestSegment.BAR, bytes(1))).contains(bytes(1));
    assertThat(stagingDirectory(store)).doesNotExist();
    store.close();
  }

  @Test
  public void rewriteOfAnEmptySegmentOnlyAddsTheEntries() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    store.rewrite(
        TestSegment.FOO, RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey, ADDITION);

    assertThat(store.stream(TestSegment.FOO).map(Pair::getKey).toList()).containsExactly(bytes(9));
    store.close();
  }

  @Test
  public void rewriteDroppingEveryEntryLeavesOnlyTheAdditions() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    putOneTwoThree(store);

    store.rewrite(TestSegment.FOO, (key, value) -> null, ADDITION);

    assertThat(store.stream(TestSegment.FOO).map(Pair::getKey).toList()).containsExactly(bytes(9));
    store.close();
  }

  @Test
  public void rewriteOfTheDefaultSegmentIsRejected() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();

    assertThatThrownBy(() -> store.rewrite(TestSegment.DEFAULT, (key, value) -> value, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    store.close();
  }

  @Test
  public void rewriteSplitsTheSegmentOverSeveralFilesAndWriters() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    final int entries = 5_000;
    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    for (int i = 0; i < entries; i++) {
      tx.put(TestSegment.FOO, intKey(i), intKey(i));
    }
    tx.commit();
    // a few dozen entries fill a file, and every seventh entry is dropped
    final RocksDBSegmentRewrite rewrite =
        new RocksDBSegmentRewrite(
            (RocksDBColumnarKeyValueStorage) store, TestSegment.FOO, 4 << 10, 3);

    rewrite.writeFiles((key, value) -> intOf(key) % 7 == 0 ? null : intKey(intOf(value) + 1));
    try (final Stream<Path> staged = Files.list(stagingDirectory(store))) {
      assertThat(staged).hasSizeGreaterThan(50);
    }
    rewrite.complete(rewrite.markPending(ADDITION));

    final List<Pair<byte[], byte[]>> rewritten = store.stream(TestSegment.FOO).toList();
    final List<Integer> expected =
        IntStream.range(0, entries).filter(i -> i % 7 != 0).boxed().toList();
    assertThat(rewritten).hasSize(expected.size() + 1);
    for (int i = 0; i < expected.size(); i++) {
      assertThat(rewritten.get(i).getKey()).isEqualTo(intKey(expected.get(i)));
      assertThat(rewritten.get(i).getValue()).isEqualTo(intKey(expected.get(i) + 1));
    }
    assertThat(rewritten.getLast().getKey()).isEqualTo(bytes(9));
    assertThat(stagingDirectory(store)).doesNotExist();
    store.close();
  }

  @Test
  public void rewriteThatFailsWhileStagingLeavesTheSegmentAlone() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);

    assertThatThrownBy(
            () ->
                store.rewrite(
                    TestSegment.FOO,
                    (key, value) -> {
                      throw new IllegalStateException("no new value for this entry");
                    },
                    ADDITION))
        .isInstanceOf(StorageException.class)
        .hasRootCauseMessage("no new value for this entry");

    assertThat(store.stream(TestSegment.FOO).map(Pair::getValue).toList())
        .containsExactly(bytes(1), bytes(2), bytes(3));
    store.rewrite(
        TestSegment.FOO, RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey, ADDITION);
    assertRewritten(store);
    store.close();
  }

  @Test
  public void rewriteInterruptedBeforeTheSwapLeavesTheSegmentAlone() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    new RocksDBSegmentRewrite((RocksDBColumnarKeyValueStorage) store, TestSegment.FOO)
        .writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    store.close();

    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());

    assertThat(reopened.stream(TestSegment.FOO).map(Pair::getValue).toList())
        .containsExactly(bytes(1), bytes(2), bytes(3));
    assertThat(stagingDirectory(reopened)).doesNotExist();
    reopened.rewrite(
        TestSegment.FOO, RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey, ADDITION);
    assertRewritten(reopened);
    reopened.close();
  }

  @Test
  public void rewriteInterruptedAfterTheMarkerIsFinishedWhenOpenedAgain() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.markPending(ADDITION);
    store.close();

    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());

    assertRewritten(reopened);
    assertThat(stagingDirectory(reopened)).doesNotExist();
    reopened.close();
  }

  @Test
  public void rewriteInterruptedAfterTheDropIsFinishedWhenOpenedAgain() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.markPending(ADDITION);
    store.clear(TestSegment.FOO);
    store.close();

    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());

    assertRewritten(reopened);
    reopened.close();
  }

  @Test
  public void rewriteInterruptedAfterTheIngestionIsFinishedWhenOpenedAgain() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.swap(rewrite.markPending(ADDITION).files());
    store.close();

    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());

    assertRewritten(reopened);
    assertThat(stagingDirectory(reopened)).doesNotExist();
    reopened.close();
  }

  @Test
  public void rewriteInterruptedWhileTheIngestionRemovedItsFilesIsFinishedWhenOpenedAgain()
      throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.swap(rewrite.markPending(ADDITION).files());
    // the ingestion removes the files it was given one by one, and was stopped after the first
    final List<Path> staged = stagedFiles(store);
    assertThat(staged).hasSize(2);
    Files.createLink(
        stagingDirectory(store).resolve("ingest").resolve(staged.getLast().getFileName()),
        staged.getLast());
    store.close();

    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());

    assertRewritten(reopened);
    assertThat(stagingDirectory(reopened)).doesNotExist();
    reopened.close();
  }

  @Test
  public void rewriteRepeatedAfterTheDropFinishesTheSwap() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.markPending(ADDITION);
    store.clear(TestSegment.FOO);

    store.rewrite(
        TestSegment.FOO, RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey, ADDITION);

    assertRewritten(store);
    assertThat(stagingDirectory(store)).doesNotExist();
    store.close();
  }

  @Test
  public void rewriteRepeatedAfterTheIngestionDoesNotTransformTwice() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.swap(rewrite.markPending(ADDITION).files());

    store.rewrite(
        TestSegment.FOO,
        (key, value) -> {
          throw new AssertionError("the entries are transformed already");
        },
        ADDITION);

    assertRewritten(store);
    store.close();
  }

  @Test
  public void openFailsAndReleasesTheDatabaseWhenStagedFilesOfAPendingRewriteAreMissing()
      throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore(folder, SEGMENTS, List.of());
    putOneTwoThree(store);
    final RocksDBSegmentRewrite rewrite = oneFilePerEntry(store);
    rewrite.writeFiles(RocksDBColumnarKeyValueStorageTest::dropTwoAndAppendKey);
    rewrite.markPending(ADDITION);
    final Path missing = stagedFiles(store).getFirst();
    final Path kept = folder.resolveSibling(folder.getFileName() + "-kept.sst");
    Files.move(missing, kept);
    store.close();

    // the second attempt fails for the same reason, not because the first one kept the database
    for (int attempt = 0; attempt < 2; attempt++) {
      assertThatThrownBy(() -> createSegmentedStore(folder, SEGMENTS, List.of()))
          .isInstanceOf(StorageException.class)
          .hasMessageContaining("holds 1 of its 2 staged files");
    }

    // nothing was dropped on the way, the swap still goes through once the file is back
    Files.move(kept, missing);
    final SegmentedKeyValueStorage reopened = createSegmentedStore(folder, SEGMENTS, List.of());
    assertRewritten(reopened);
    reopened.close();
  }

  private static final List<SegmentIdentifier> SEGMENTS =
      List.of(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR);

  private static final List<Pair<byte[], byte[]>> ADDITION =
      List.of(Pair.of(bytes(9), bytes(9, 9)));

  private static byte[] dropTwoAndAppendKey(final byte[] key, final byte[] value) {
    return key[0] == 2 ? null : bytes(value[0], key[0]);
  }

  private static void putOneTwoThree(final SegmentedKeyValueStorage store) {
    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.FOO, bytes(1), bytes(1));
    tx.put(TestSegment.FOO, bytes(2), bytes(2));
    tx.put(TestSegment.FOO, bytes(3), bytes(3));
    tx.commit();
  }

  private static void assertRewritten(final SegmentedKeyValueStorage store) {
    assertThat(store.stream(TestSegment.FOO).map(Pair::getKey).toList())
        .containsExactly(bytes(1), bytes(3), bytes(9));
    assertThat(store.get(TestSegment.FOO, bytes(1))).contains(bytes(1, 1));
    assertThat(store.get(TestSegment.FOO, bytes(3))).contains(bytes(3, 3));
    assertThat(store.get(TestSegment.FOO, bytes(9))).contains(bytes(9, 9));
  }

  private static byte[] bytes(final int... values) {
    final byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return bytes;
  }

  private static byte[] intKey(final int value) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
  }

  private static int intOf(final byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  /** Every entry fills a file, so that the swap has more than one file to handle. */
  private static RocksDBSegmentRewrite oneFilePerEntry(final SegmentedKeyValueStorage store) {
    return new RocksDBSegmentRewrite((RocksDBColumnarKeyValueStorage) store, TestSegment.FOO, 1, 2);
  }

  private static List<Path> stagedFiles(final SegmentedKeyValueStorage store) throws IOException {
    try (final Stream<Path> files = Files.list(stagingDirectory(store))) {
      return files.filter(Files::isRegularFile).sorted().toList();
    }
  }

  private static Path stagingDirectory(final SegmentedKeyValueStorage store) {
    return RocksDBSegmentRewrite.stagingDirectory(
        ((RocksDBColumnarKeyValueStorage) store).configuration.getDatabaseDir(),
        TestSegment.FOO.getName());
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

  @Test
  public void bulkLoadHoldsBackCompactionUntilItIsClosed() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    // more files than the number in level 0 at which RocksDB stops all writes
    final int overlappingFiles = 50;
    final RocksDBSegmentBulkLoad bulkLoad =
        (RocksDBSegmentBulkLoad) store.startBulkLoad(List.of(TestSegment.FOO));

    try (bulkLoad) {
      assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isTrue();
      assertThat(isAutoCompactionDisabled(store, TestSegment.BAR)).isFalse();
      for (int i = 0; i < overlappingFiles; i++) {
        final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
        writer.put(bytesOf(1), bytesOf(i));
        writer.put(bytesOf(100), bytesOf(i));
        writer.commit();
      }

      // every file but the first holds keys of the files before it, and none was merged away
      assertThat(filesAtLevel(store, TestSegment.FOO, 0)).isEqualTo(overlappingFiles - 1);

      // the files in level 0 do not stop a regular write
      final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
      tx.put(TestSegment.FOO, bytesOf(50), bytesOf(5));
      assertThat(CompletableFuture.runAsync(tx::commit)).succeedsWithin(Duration.ofSeconds(30));
    }

    assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();
    // the limits on level 0 stay lifted until RocksDB has merged the files down
    bulkLoad.awaitBackgroundWork();
    assertThat(filesAtLevel(store, TestSegment.FOO, 0)).isLessThan(LEVEL0_SLOWDOWN_TRIGGER);
    assertThat(level0SlowdownTrigger(store, TestSegment.FOO)).isEqualTo(LEVEL0_SLOWDOWN_TRIGGER);
    assertThat(level0StopTrigger(store, TestSegment.FOO)).isEqualTo(LEVEL0_STOP_TRIGGER);
    assertThat(softPendingCompactionLimit(store, TestSegment.FOO))
        .isEqualTo(SOFT_PENDING_COMPACTION_LIMIT);
    assertThat(hardPendingCompactionLimit(store, TestSegment.FOO))
        .isEqualTo(HARD_PENDING_COMPACTION_LIMIT);
    assertThat(store.get(TestSegment.FOO, bytesOf(1))).contains(bytesOf(overlappingFiles - 1));
    assertThat(store.get(TestSegment.FOO, bytesOf(50))).contains(bytesOf(5));
    assertThat(store.get(TestSegment.FOO, bytesOf(100))).contains(bytesOf(overlappingFiles - 1));

    store.close();
  }

  @Test
  public void bulkLoadLetsOneSegmentGoBeforeTheOthers() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final int overlappingFiles = 50;
    final long readahead = store.getDB().getDBOptions().compactionReadaheadSize();
    final int backgroundJobs = store.getDB().getDBOptions().maxBackgroundJobs();
    final SegmentBulkLoad bulkLoad = store.startBulkLoad(List.of(TestSegment.FOO, TestSegment.BAR));

    try (bulkLoad) {
      for (int i = 0; i < overlappingFiles; i++) {
        final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
        writer.put(bytesOf(1), bytesOf(i));
        writer.put(bytesOf(100), bytesOf(i));
        writer.commit();
      }
      assertThat(filesAtLevel(store, TestSegment.FOO, 0)).isEqualTo(overlappingFiles - 1);

      assertThat(bulkLoad.end(TestSegment.FOO)).succeedsWithin(Duration.ofSeconds(30));

      assertThat(filesAtLevel(store, TestSegment.FOO, 0)).isLessThan(LEVEL0_SLOWDOWN_TRIGGER);
      assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();
      assertThat(level0SlowdownTrigger(store, TestSegment.FOO)).isEqualTo(LEVEL0_SLOWDOWN_TRIGGER);
      assertThat(level0StopTrigger(store, TestSegment.FOO)).isEqualTo(LEVEL0_STOP_TRIGGER);
      assertThat(softPendingCompactionLimit(store, TestSegment.FOO))
          .isEqualTo(SOFT_PENDING_COMPACTION_LIMIT);
      assertThat(store.getDB().getDBOptions().compactionReadaheadSize()).isEqualTo(readahead);
      assertThat(store.getDB().getDBOptions().maxBackgroundJobs()).isEqualTo(backgroundJobs);
      assertThat(isAutoCompactionDisabled(store, TestSegment.BAR)).isTrue();
      // a second end finds nothing to do
      assertThat(bulkLoad.end(TestSegment.FOO)).isCompleted();
    }

    assertThat(isAutoCompactionDisabled(store, TestSegment.BAR)).isFalse();
    assertThat(store.get(TestSegment.FOO, bytesOf(1))).contains(bytesOf(overlappingFiles - 1));
    assertThat(store.get(TestSegment.FOO, bytesOf(100))).contains(bytesOf(overlappingFiles - 1));

    store.close();
  }

  @Test
  public void bulkLoadLetsASegmentWithFewFilesGoAtOnce() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final SegmentBulkLoad bulkLoad = store.startBulkLoad(List.of(TestSegment.FOO));

    try (bulkLoad) {
      final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
      tx.put(TestSegment.FOO, bytesOf(1), bytesOf(1));
      tx.commit();

      assertThat(bulkLoad.end(TestSegment.FOO)).isCompleted();
      assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();
      // not part of the load
      assertThat(bulkLoad.end(TestSegment.BAR)).isCompleted();
      assertThat(isAutoCompactionDisabled(store, TestSegment.BAR)).isFalse();
    }
    assertThat(store.get(TestSegment.FOO, bytesOf(1))).contains(bytesOf(1));

    store.close();
  }

  @Test
  public void bulkLoadLeavesTheFilesAboveTheBottomLevelWhenClosed() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final RocksDBSegmentBulkLoad bulkLoad =
        (RocksDBSegmentBulkLoad) store.startBulkLoad(List.of(TestSegment.FOO));

    try (bulkLoad) {
      for (final int first : List.of(10, 20, 30)) {
        final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
        writer.put(bytesOf(first), bytesOf(first));
        writer.put(bytesOf(first + 5), bytesOf(first));
        writer.commit();
      }
      // holds a key inside the range of the first file, so it cannot join it in the bottom level
      final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
      writer.put(bytesOf(12), bytesOf(1));
      writer.commit();
    }
    bulkLoad.awaitBackgroundWork();

    assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();
    // a compaction of the whole segment would have merged the file above into the bottom level
    assertThat(filesAtLevel(store, TestSegment.FOO, BULK_LOAD_BOTTOM_LEVEL)).isEqualTo(3);
    assertThat(filesAtLevel(store, TestSegment.FOO, 0)).isEqualTo(1);
    assertThat(level0SlowdownTrigger(store, TestSegment.FOO)).isEqualTo(LEVEL0_SLOWDOWN_TRIGGER);
    assertThat(store.get(TestSegment.FOO, bytesOf(12))).contains(bytesOf(1));
    assertThat(store.get(TestSegment.FOO, bytesOf(15))).contains(bytesOf(10));

    store.close();
  }

  @Test
  public void storageClosedWhileLevel0IsMergedDownStopsTheWatch() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final RocksDBSegmentBulkLoad bulkLoad =
        (RocksDBSegmentBulkLoad) store.startBulkLoad(List.of(TestSegment.FOO));
    for (int i = 0; i < 50; i++) {
      final SortedWriter writer = store.startSortedWriter(TestSegment.FOO);
      writer.put(bytesOf(1), bytesOf(i));
      writer.put(bytesOf(100), bytesOf(i));
      writer.commit();
    }

    bulkLoad.close();
    store.close();

    // the watch stopped when the storage closed, with level 0 not merged down yet
    assertThat(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    bulkLoad.awaitBackgroundWork();
                  } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }))
        .succeedsWithin(Duration.ofSeconds(30));
  }

  @Test
  public void bulkLoadCanBeClosedTwice() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final SegmentBulkLoad bulkLoad = store.startBulkLoad(List.of(TestSegment.FOO));

    bulkLoad.close();
    bulkLoad.close();

    assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();

    store.close();
  }

  @Test
  public void bulkLoadKeepsLargeValuesInBlobFilesUntilItIsClosed() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final byte[] large = new byte[1024];
    Arrays.fill(large, (byte) 7);
    final SegmentBulkLoad bulkLoad =
        store.startBulkLoad(List.of(TestSegment.FOO), List.of(TestSegment.BAR));

    try (bulkLoad) {
      assertThat(blobFilesEnabled(store, TestSegment.BAR)).isTrue();
      assertThat(blobFilesEnabled(store, TestSegment.FOO)).isFalse();
      // only held back segments stop compacting
      assertThat(isAutoCompactionDisabled(store, TestSegment.BAR)).isFalse();
      final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
      tx.put(TestSegment.BAR, bytesOf(1), large);
      tx.commit();
      flush(store, TestSegment.BAR);
      assertThat(blobFiles(store, TestSegment.BAR)).isEqualTo(1);
      // compressed
      assertThat(blobFileBytes(store, TestSegment.BAR)).isLessThan(large.length);
    }

    assertThat(blobFilesEnabled(store, TestSegment.BAR)).isFalse();
    final SegmentedKeyValueStorageTransaction tx = store.startTransaction();
    tx.put(TestSegment.BAR, bytesOf(2), large);
    tx.commit();
    flush(store, TestSegment.BAR);
    assertThat(blobFiles(store, TestSegment.BAR)).isEqualTo(1);
    assertThat(store.get(TestSegment.BAR, bytesOf(1))).contains(large);
    assertThat(store.get(TestSegment.BAR, bytesOf(2))).contains(large);

    store.close();
  }

  @Test
  public void bulkLoadLetsASegmentWithSeparateValuesGoBeforeTheOthers() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final SegmentBulkLoad bulkLoad =
        store.startBulkLoad(List.of(TestSegment.FOO), List.of(TestSegment.BAR));

    try (bulkLoad) {
      assertThat(bulkLoad.end(TestSegment.BAR)).isCompleted();

      assertThat(blobFilesEnabled(store, TestSegment.BAR)).isFalse();
      assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isTrue();
    }

    store.close();
  }

  @Test
  public void bulkLoadRefusesASegmentBothHeldBackAndWithSeparateValues() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();

    assertThatThrownBy(
            () -> store.startBulkLoad(List.of(TestSegment.FOO), List.of(TestSegment.FOO)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(isAutoCompactionDisabled(store, TestSegment.FOO)).isFalse();

    store.close();
  }

  @Test
  public void bulkLoadClosedAfterTheStorageDoesNothing() throws Exception {
    final RocksDBColumnarKeyValueStorage store =
        (RocksDBColumnarKeyValueStorage) createSegmentedStore();
    final SegmentBulkLoad bulkLoad = store.startBulkLoad(List.of(TestSegment.FOO));
    store.close();

    bulkLoad.close();
  }

  private static final int BULK_LOAD_BOTTOM_LEVEL = 6;

  // the limits of RocksDB, which the storage does not change
  private static final int LEVEL0_SLOWDOWN_TRIGGER = 20;
  private static final int LEVEL0_STOP_TRIGGER = 36;
  private static final long SOFT_PENDING_COMPACTION_LIMIT = 64L << 30;
  private static final long HARD_PENDING_COMPACTION_LIMIT = 256L << 30;

  private static long filesAtLevel(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment, final int level)
      throws RocksDBException {
    return Long.parseLong(
        store
            .getDB()
            .getProperty(store.safeColumnHandle(segment), "rocksdb.num-files-at-level" + level));
  }

  private static boolean isAutoCompactionDisabled(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store.getDB().getOptions(store.safeColumnHandle(segment)).disableAutoCompactions();
  }

  private static boolean blobFilesEnabled(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store.getDB().getOptions(store.safeColumnHandle(segment)).enableBlobFiles();
  }

  private static long blobFiles(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return Long.parseLong(
        store.getDB().getProperty(store.safeColumnHandle(segment), "rocksdb.num-blob-files"));
  }

  private static long blobFileBytes(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return Long.parseLong(
        store.getDB().getProperty(store.safeColumnHandle(segment), "rocksdb.total-blob-file-size"));
  }

  private static void flush(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    try (final FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
      store.getDB().flush(options, store.safeColumnHandle(segment));
    }
  }

  private static int level0SlowdownTrigger(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store.getDB().getOptions(store.safeColumnHandle(segment)).level0SlowdownWritesTrigger();
  }

  private static int level0StopTrigger(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store.getDB().getOptions(store.safeColumnHandle(segment)).level0StopWritesTrigger();
  }

  private static long softPendingCompactionLimit(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store
        .getDB()
        .getOptions(store.safeColumnHandle(segment))
        .softPendingCompactionBytesLimit();
  }

  private static long hardPendingCompactionLimit(
      final RocksDBColumnarKeyValueStorage store, final SegmentIdentifier segment)
      throws RocksDBException {
    return store
        .getDB()
        .getOptions(store.safeColumnHandle(segment))
        .hardPendingCompactionBytesLimit();
  }
}
