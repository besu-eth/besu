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

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OptimisticTransactionDBRocksDBColumnarKeyValueStorageTest
    extends RocksDBColumnarKeyValueStorageTest {

  @Test
  public void snapshotMultigetReadsStableSnapshotState() throws Exception {
    final OptimisticRocksDBColumnarKeyValueStorage store =
        (OptimisticRocksDBColumnarKeyValueStorage) createSegmentedStore();

    final SegmentedKeyValueStorageTransaction initialTx = store.startTransaction();
    initialTx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    initialTx.commit();

    final RocksDBColumnarKeyValueSnapshot snapshot = store.takeSnapshot();
    try {
      final SegmentedKeyValueStorageTransaction updateTx = store.startTransaction();
      updateTx.put(TestSegment.FOO, bytesOf(1), bytesOf(11));
      updateTx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
      updateTx.commit();

      final List<Optional<byte[]>> values =
          snapshot.multiget(TestSegment.FOO, List.of(bytesOf(1), bytesOf(2), bytesOf(1)));

      assertThat(values).hasSize(3);
      assertThat(values.get(0)).isPresent();
      assertThat(values.get(0).get()).isEqualTo(bytesOf(10));
      assertThat(values.get(1)).isEmpty();
      assertThat(values.get(2)).isPresent();
      assertThat(values.get(2).get()).isEqualTo(bytesOf(10));
    } finally {
      snapshot.close();
      store.close();
    }
  }

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore() throws Exception {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder()
            .databaseDir(Files.createTempDirectory("segmentedStore"))
            .build(),
        Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
        List.of(),
        new NoOpMetricsSystem(),
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments) {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(path).build(),
        segments,
        ignorableSegments,
        new NoOpMetricsSystem(),
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final MetricsSystem metricsSystem,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments) {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(path).build(),
        segments,
        ignorableSegments,
        metricsSystem,
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }
}
