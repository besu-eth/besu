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

import java.util.List;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

/**
 * Wraps and reference counts a Snapshot object from an OptimisticTransactionDB such that it can be
 * used as the basis of multiple RocksDBSnapshotTransaction's, and released once it is no longer in
 * use.
 */
class RocksDBSnapshot {

  private final OptimisticTransactionDB db;
  private final Snapshot dbSnapshot;

  RocksDBSnapshot(final OptimisticTransactionDB db) {
    this.db = db;
    this.dbSnapshot = db.getSnapshot();
  }

  synchronized Snapshot getSnapshot() {
    return dbSnapshot;
  }

  synchronized void close() {
    db.releaseSnapshot(dbSnapshot);
    dbSnapshot.close();
  }

  public byte[] get(
      final ColumnFamilyHandle columnFamilyHandle, final ReadOptions readOptions, final byte[] key)
      throws RocksDBException {
    return db.get(columnFamilyHandle, readOptions, key);
  }

  public List<byte[]> multiGet(
      final List<ColumnFamilyHandle> columnFamiliesHandle,
      final ReadOptions readOptions,
      final List<byte[]> keys)
      throws RocksDBException {
    return db.multiGetAsList(readOptions, columnFamiliesHandle, keys);
  }
}
