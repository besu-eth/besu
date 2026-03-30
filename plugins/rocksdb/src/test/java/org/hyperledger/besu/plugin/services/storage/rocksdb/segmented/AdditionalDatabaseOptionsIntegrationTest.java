/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the License); you may not use this file except in compliance with
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
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbUtil;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Env;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;

/** Integration tests for {@code --Xplugin-rocksdb-additional-database-options}. */
public class AdditionalDatabaseOptionsIntegrationTest {

  @Test
  public void additionalDatabaseOptionsAreReflectedInOptionsFile(@TempDir final Path dbDir)
      throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final int maxFileOpeningThreads = 13;
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalDatabaseOptions(
                Optional.of("max_file_opening_threads=" + maxFileOpeningThreads + ";"))
            .build();

    try (OptimisticRocksDBColumnarKeyValueStorage store =
        new OptimisticRocksDBColumnarKeyValueStorage(
            configuration,
            List.of(
                RocksDBColumnarKeyValueStorageTest.TestSegment.DEFAULT,
                RocksDBColumnarKeyValueStorageTest.TestSegment.FOO),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      assertThat(store.getDB()).isNotNull();
    }

    final String optionsFileName =
        OptionsUtil.getLatestOptionsFileName(dbDir.toString(), Env.getDefault());
    final String content = Files.readString(dbDir.resolve(optionsFileName), StandardCharsets.UTF_8);
    assertThat(content).contains("max_file_opening_threads=" + maxFileOpeningThreads);
  }

  @Test
  public void unknownDatabaseOptionKeyFallsBackAndDbStillOpens(@TempDir final Path dbDir)
      throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalDatabaseOptions(
                Optional.of("besu_nonexistent_db_option_xyz=not_a_valid_value;"))
            .build();

    try (OptimisticRocksDBColumnarKeyValueStorage store =
        new OptimisticRocksDBColumnarKeyValueStorage(
            configuration,
            List.of(
                RocksDBColumnarKeyValueStorageTest.TestSegment.DEFAULT,
                RocksDBColumnarKeyValueStorageTest.TestSegment.FOO),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      assertThat(store.getDB()).isNotNull();
    }

    final String optionsFileName =
        OptionsUtil.getLatestOptionsFileName(dbDir.toString(), Env.getDefault());
    final String content = Files.readString(dbDir.resolve(optionsFileName), StandardCharsets.UTF_8);
    assertThat(content).doesNotContain("besu_nonexistent_db_option_xyz");
  }

  @Test
  public void unknownDbKeyCombinedWithValidMaxFileOpeningThreadsDropsEntireNativeString(
      @TempDir final Path dbDir) throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final int maxFileOpeningThreads = 17;
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalDatabaseOptions(
                Optional.of(
                    "max_file_opening_threads="
                        + maxFileOpeningThreads
                        + ";besu_nonexistent_db_option_xyz=1;"))
            .build();

    try (OptimisticRocksDBColumnarKeyValueStorage store =
        new OptimisticRocksDBColumnarKeyValueStorage(
            configuration,
            List.of(
                RocksDBColumnarKeyValueStorageTest.TestSegment.DEFAULT,
                RocksDBColumnarKeyValueStorageTest.TestSegment.FOO),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      assertThat(store.getDB()).isNotNull();
    }

    final String optionsFileName =
        OptionsUtil.getLatestOptionsFileName(dbDir.toString(), Env.getDefault());
    final String content = Files.readString(dbDir.resolve(optionsFileName), StandardCharsets.UTF_8);
    assertThat(content).doesNotContain("besu_nonexistent_db_option_xyz");
    // Invalid key makes getDBOptionsFromProps fail; Besu uses bare DBOptions for the whole string.
    assertThat(content).doesNotContain("max_file_opening_threads=" + maxFileOpeningThreads);
  }
}
