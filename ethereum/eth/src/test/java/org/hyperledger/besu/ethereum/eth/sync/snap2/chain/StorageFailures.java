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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

/** Storage failures as RocksDB reports them. */
public final class StorageFailures {

  private StorageFailures() {}

  /** A transaction RocksDB failed to check for conflicts, which a retry may commit. */
  public static StorageException notCommitted() {
    return failure(Status.Code.Busy);
  }

  /** A failure of the storage itself, which no retry fixes. */
  public static StorageException diskFailure() {
    return failure(Status.Code.IOError);
  }

  private static StorageException failure(final Status.Code code) {
    return new StorageException(
        new RocksDBException(code.name(), new Status(code, Status.SubCode.None, null)));
  }
}
