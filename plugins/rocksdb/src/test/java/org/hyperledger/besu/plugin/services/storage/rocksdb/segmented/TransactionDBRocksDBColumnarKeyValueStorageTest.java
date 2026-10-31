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

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionDBRocksDBColumnarKeyValueStorageTest
    extends RocksDBColumnarKeyValueStorageTest {

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore() throws Exception {
    return new TransactionDBRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(getTempSubFolder(folder)).build(),
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
    return new TransactionDBRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(folder).build(),
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
    return new TransactionDBRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(path).build(),
        segments,
        ignorableSegments,
        metricsSystem,
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  // a pessimistic database still locks the keys of a batch at commit
  @Test
  @Timeout(10)
  public void batchTransactionWaitsForTheKeysATransactionHolds() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");
    final SegmentedKeyValueStorageTransaction holder = store.startTransaction();
    holder.put(TestSegment.FOO, key, bytesFromHexString("1337"));

    final SegmentedKeyValueStorageTransaction batch = store.startBatchTransaction();
    batch.put(TestSegment.FOO, key, bytesFromHexString("0FFF"));
    // fails once the lock timeout runs out
    assertThatThrownBy(batch::commit).isInstanceOf(StorageException.class);

    holder.commit();
    assertThat(store.get(TestSegment.FOO, key)).contains(bytesFromHexString("1337"));
    store.close();
  }
}
