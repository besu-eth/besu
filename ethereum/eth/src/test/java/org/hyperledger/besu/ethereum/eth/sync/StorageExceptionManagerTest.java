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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

public class StorageExceptionManagerTest {

  @BeforeEach
  public void resetRetryableErrorCounter() throws ReflectiveOperationException {
    final Field retryableErrorCounterField =
        StorageExceptionManager.class.getDeclaredField("retryableErrorCounter");
    retryableErrorCounterField.setAccessible(true);
    ((AtomicLong) retryableErrorCounterField.get(null)).set(0L);
  }

  @Test
  public void shouldNotThrowWhenStorageExceptionCauseIsNull() {
    assertThat(StorageExceptionManager.canRetryOnError(new StorageException("missing cause")))
        .isFalse();
    assertThat(StorageExceptionManager.getRetryableErrorCounter()).isZero();
  }

  @Test
  public void shouldNotCountNonRetryableRocksDbErrors() {
    final RocksDBException rocksDbException =
        new RocksDBException(new Status(Status.Code.Aborted, Status.SubCode.None, "aborted"));

    assertThat(StorageExceptionManager.canRetryOnError(new StorageException(rocksDbException)))
        .isFalse();
    assertThat(StorageExceptionManager.getRetryableErrorCounter()).isZero();
  }

  @Test
  public void shouldCountRetryableRocksDbErrors() {
    final RocksDBException rocksDbException =
        new RocksDBException(new Status(Status.Code.Busy, Status.SubCode.None, "busy"));

    assertThat(StorageExceptionManager.canRetryOnError(new StorageException(rocksDbException)))
        .isTrue();
    assertThat(StorageExceptionManager.getRetryableErrorCounter()).isEqualTo(1L);
    assertThat(StorageExceptionManager.errorCountAtThreshold()).isTrue();
  }
}
