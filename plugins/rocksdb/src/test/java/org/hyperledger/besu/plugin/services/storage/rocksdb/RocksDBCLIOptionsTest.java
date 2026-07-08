/*
 * Copyright ConsenSys AG.
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
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.CACHE_CAPACITY_FLAG;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.MAX_OPEN_FILES_16GB;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.MAX_OPEN_FILES_32GB;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.MAX_OPEN_FILES_4GB;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.MAX_OPEN_FILES_8GB;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.MAX_OPEN_FILES_FLAG;

import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class RocksDBCLIOptionsTest {

  @Test
  public void defaultValues() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();

    new CommandLine(options).parseArgs();

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.getMaxOpenFiles())
        .isEqualTo(RocksDBCLIOptions.deriveMaxOpenFilesFromAvailableMemory());
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customBackgroundThreadCount() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();
    final int expectedBackgroundThreadCount = 99;

    new CommandLine(options)
        .parseArgs(
            RocksDBCLIOptions.BACKGROUND_THREAD_COUNT_FLAG, "" + expectedBackgroundThreadCount);

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(expectedBackgroundThreadCount);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.getMaxOpenFiles())
        .isEqualTo(RocksDBCLIOptions.deriveMaxOpenFilesFromAvailableMemory());
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customCacheCapacity() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();
    final long expectedCacheCapacity = 400050006000L;

    new CommandLine(options).parseArgs(CACHE_CAPACITY_FLAG, "" + expectedCacheCapacity);

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(expectedCacheCapacity);
    assertThat(configuration.getMaxOpenFiles())
        .isEqualTo(RocksDBCLIOptions.deriveMaxOpenFilesFromAvailableMemory());
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customMaxOpenFiles() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();
    final int expectedMaxOpenFiles = 65;

    new CommandLine(options).parseArgs(MAX_OPEN_FILES_FLAG, "" + expectedMaxOpenFiles);

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(expectedMaxOpenFiles);
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customIsHighSpec() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();

    new CommandLine(options).parseArgs(IS_HIGH_SPEC);

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.isHighSpec()).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void tableCacheWarmupEnabledByDefault() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();

    new CommandLine(options).parseArgs();

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration.isTableCacheWarmupEnabled()).isTrue();
  }

  @Test
  public void tableCacheWarmupCanBeDisabled() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();

    new CommandLine(options)
        .parseArgs(RocksDBCLIOptions.TABLE_CACHE_WARMUP_ENABLED_FLAG + "=false");

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration.isTableCacheWarmupEnabled()).isFalse();
  }

  @Test
  public void autoMaxOpenFilesUsesMemoryTiers() {
    final long gib = 1024L * 1024L * 1024L;

    assertThat(RocksDBCLIOptions.calculateMaxOpenFiles(2L * gib)).isEqualTo(DEFAULT_MAX_OPEN_FILES);
    assertThat(RocksDBCLIOptions.calculateMaxOpenFiles(4L * gib)).isEqualTo(MAX_OPEN_FILES_4GB);
    assertThat(RocksDBCLIOptions.calculateMaxOpenFiles(8L * gib)).isEqualTo(MAX_OPEN_FILES_8GB);
    assertThat(RocksDBCLIOptions.calculateMaxOpenFiles(16L * gib)).isEqualTo(MAX_OPEN_FILES_16GB);
    assertThat(RocksDBCLIOptions.calculateMaxOpenFiles(32L * gib)).isEqualTo(MAX_OPEN_FILES_32GB);
  }
}
