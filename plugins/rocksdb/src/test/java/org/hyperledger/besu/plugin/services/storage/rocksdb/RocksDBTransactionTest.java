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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

class RocksDBTransactionTest {

  @Test
  void isDiskFull_returnsTrueForStructuredIoErrorNoSpace() {
    final Status status = new Status(Status.Code.IOError, Status.SubCode.NoSpace, null);
    final RocksDBException e = new RocksDBException("unrelated message", status);

    assertThat(RocksDBTransaction.isDiskFull(e)).isTrue();
  }

  @Test
  void isDiskFull_returnsTrueWhenStatusNullAndMessageMatches() {
    final RocksDBException e = new RocksDBException("IO error: No space left on device");

    assertThat(e.getStatus()).isNull();
    assertThat(RocksDBTransaction.isDiskFull(e)).isTrue();
  }

  @Test
  void isDiskFull_returnsFalseWhenStatusNullAndMessageNull() {
    final RocksDBException e = new RocksDBException((String) null);

    assertThat(e.getStatus()).isNull();
    assertThat(e.getMessage()).isNull();
    assertThat(RocksDBTransaction.isDiskFull(e)).isFalse();
  }

  @Test
  void isDiskFull_returnsFalseForNonDiskFullRocksDBException() {
    final Status status = new Status(Status.Code.Corruption, Status.SubCode.None, null);
    final RocksDBException e = new RocksDBException("some other failure", status);

    assertThat(RocksDBTransaction.isDiskFull(e)).isFalse();
  }
}
