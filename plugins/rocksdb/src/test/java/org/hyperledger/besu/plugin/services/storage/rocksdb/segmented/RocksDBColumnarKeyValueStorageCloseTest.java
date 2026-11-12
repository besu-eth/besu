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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorageTest.TestSegment;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

class RocksDBColumnarKeyValueStorageCloseTest {

  private static final byte[] KEY = {1};
  private static final byte[] VALUE = {2};

  @TempDir Path folder;

  @Test
  void closeWaitsForAReadThatIsInsideRocksDb() throws Exception {
    final AtomicBoolean pauseNextRead = new AtomicBoolean(false);
    final CountDownLatch readPaused = new CountDownLatch(1);
    final CountDownLatch resumeRead = new CountDownLatch(1);
    final OptimisticRocksDBColumnarKeyValueStorage storage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(folder).build(),
            List.of(TestSegment.DEFAULT),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS) {
          @Override
          RocksDB getDB() {
            // the read has passed the closed check and is about to call into RocksDB
            if (pauseNextRead.compareAndSet(true, false)) {
              readPaused.countDown();
              Uninterruptibles.awaitUninterruptibly(resumeRead);
            }
            return super.getDB();
          }
        };
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(TestSegment.DEFAULT, KEY, VALUE);
    tx.commit();

    pauseNextRead.set(true);
    final CompletableFuture<Optional<byte[]>> read =
        CompletableFuture.supplyAsync(() -> storage.get(TestSegment.DEFAULT, KEY));
    assertThat(readPaused.await(10, TimeUnit.SECONDS)).isTrue();

    final CompletableFuture<Void> close = CompletableFuture.runAsync(storage::close);
    // closing the database now would free it under the read and crash the JVM
    assertThatThrownBy(() -> close.get(500, TimeUnit.MILLISECONDS))
        .isInstanceOf(TimeoutException.class);
    assertThat(storage.isClosed()).isTrue();

    resumeRead.countDown();
    assertThat(read.get(10, TimeUnit.SECONDS).orElseThrow()).containsExactly(VALUE);
    assertThat(close).succeedsWithin(Duration.ofSeconds(2));
    assertThatThrownBy(() -> storage.get(TestSegment.DEFAULT, KEY))
        .isInstanceOf(IllegalStateException.class);

    // RocksDB holds a lock on the directory until it is closed
    try (final OptimisticRocksDBColumnarKeyValueStorage reopened =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(folder).build(),
            List.of(TestSegment.DEFAULT),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      assertThat(reopened.get(TestSegment.DEFAULT, KEY).orElseThrow()).containsExactly(VALUE);
    }
  }
}
