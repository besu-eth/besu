/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.function.Function;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * A transaction that collects its writes in a batch and applies them atomically on commit.
 *
 * <p>Nothing reads through a transaction, so the conflict check of an optimistic transaction does
 * not protect a read-modify-write, and its per-key tracking and indexed batch only add work: on a
 * head world state commit, about half of it.
 */
public class RocksDBWriteBatchTransaction implements SegmentedKeyValueStorageTransaction {

  private final RocksDBMetrics metrics;
  private final RocksDB db;
  private final WriteBatch batch = new WriteBatch();
  private final WriteOptions options;
  private final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper;

  /**
   * Instantiates a new RocksDb write batch transaction.
   *
   * @param columnFamilyMapper mapper from segment identifier to column family handle
   * @param db the database the batch is written to
   * @param options the write options, closed with the transaction
   * @param metrics the metrics
   */
  public RocksDBWriteBatchTransaction(
      final Function<SegmentIdentifier, ColumnFamilyHandle> columnFamilyMapper,
      final RocksDB db,
      final WriteOptions options,
      final RocksDBMetrics metrics) {
    this.columnFamilyMapper = columnFamilyMapper;
    this.db = db;
    this.options = options;
    this.metrics = metrics;
  }

  @Override
  public void put(final SegmentIdentifier segmentId, final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      batch.put(columnFamilyMapper.apply(segmentId), key, value);
    } catch (final RocksDBException e) {
      throw RocksDBTransaction.storageException(e);
    }
  }

  @Override
  public void remove(final SegmentIdentifier segmentId, final byte[] key) {
    try (final OperationTimer.TimingContext ignored = metrics.getRemoveLatency().startTimer()) {
      batch.delete(columnFamilyMapper.apply(segmentId), key);
    } catch (final RocksDBException e) {
      throw RocksDBTransaction.storageException(e);
    }
  }

  @Override
  public void commit() throws StorageException {
    try (final OperationTimer.TimingContext ignored = metrics.getCommitLatency().startTimer()) {
      db.write(options, batch);
    } catch (final RocksDBException e) {
      throw RocksDBTransaction.storageException(e);
    } finally {
      close();
    }
  }

  @Override
  public void rollback() {
    metrics.getRollbackCount().inc();
    close();
  }

  @Override
  public void close() {
    batch.close();
    options.close();
  }
}
