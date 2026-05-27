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

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageTransactionValidatorDecorator;

import java.util.List;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;

/** TransactionDB RocksDB Columnar key value storage */
public class TransactionDBRocksDBColumnarKeyValueStorage extends RocksDBColumnarKeyValueStorage {

  private final TransactionDB db;

  /**
   * The constructor of TransactionDBRocksDBColumnarKeyValueStorage
   *
   * @param configuration the RocksDB configuration
   * @param segments the segments
   * @param ignorableSegments the ignorable segments
   * @param metricsSystem the metrics system
   * @param rocksDBMetricsFactory the rocksdb metrics factory
   * @throws StorageException the storage exception
   */
  public TransactionDBRocksDBColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {
    super(configuration, segments, ignorableSegments, metricsSystem, rocksDBMetricsFactory);
    try {

      db =
          RocksDBOpener.openTransactionDBWithWarning(
              options,
              txOptions,
              configuration.getDatabaseDir().toString(),
              columnDescriptors,
              columnHandles);
      initMetrics();
      initColumnHandles();

    } catch (final RocksDBException e) {
      throw parseRocksDBException(e, segments, ignorableSegments);
    }
  }

  @Override
  RocksDB getDB() {
    return db;
  }

  /**
   * Start a transaction
   *
   * @return the new transaction started
   * @throws StorageException the storage exception
   */
  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    throwIfClosed();
    final WriteOptions writeOptions = new WriteOptions();
    writeOptions.setIgnoreMissingColumnFamilies(true);
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new RocksDBTransaction(
            this::safeColumnHandle, db.beginTransaction(writeOptions), writeOptions, metrics),
        this.closed::get);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startLowPriorityTransaction() throws StorageException {
    throwIfClosed();
    final WriteOptions writeOptions = new WriteOptions();
    writeOptions.setIgnoreMissingColumnFamilies(true);
    writeOptions.setLowPri(true);
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new RocksDBTransaction(
            this::safeColumnHandle, db.beginTransaction(writeOptions), writeOptions, metrics),
        this.closed::get);
  }
}
