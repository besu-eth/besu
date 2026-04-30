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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class RocksDbStorageExceptionHelperTest {

  @Test
  public void shouldReturnBusyStatusCodeNameFromRocksDbException() {
    final StorageException storageException =
        new StorageException(
            new RocksDBException(new Status(Status.Code.Busy, Status.SubCode.None, "busy")));

    assertThat(RocksDbStorageExceptionHelper.getStatusCodeName(storageException))
        .contains("Busy");
    assertThat(RocksDbStorageExceptionHelper.isBusyException(storageException)).isTrue();
  }

  @Test
  public void shouldReturnEmptyWhenStorageExceptionHasNoCause() {
    assertThat(RocksDbStorageExceptionHelper.getStatusCodeName(new StorageException("missing")))
        .isEmpty();
  }

  @Test
  public void shouldReturnEmptyWhenCauseIsNotRocksDbException() {
    assertThat(
            RocksDbStorageExceptionHelper.getStatusCodeName(
                new StorageException(new IllegalStateException("not rocksdb"))))
        .isEmpty();
  }
}
