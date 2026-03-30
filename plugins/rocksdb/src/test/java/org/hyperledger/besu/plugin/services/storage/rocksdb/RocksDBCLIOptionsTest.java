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
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.ADDITIONAL_COLUMN_FAMILY_OPTIONS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.ADDITIONAL_DATABASE_OPTIONS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.CACHE_CAPACITY_FLAG;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.IS_HIGH_SPEC;
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
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(DEFAULT_MAX_OPEN_FILES);
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
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(DEFAULT_MAX_OPEN_FILES);
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
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(DEFAULT_MAX_OPEN_FILES);
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
  public void additionalColumnFamilyOptionsIncludesBlockTableKeys() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();
    new CommandLine(options)
        .parseArgs(
            ADDITIONAL_COLUMN_FAMILY_OPTIONS,
            "write_buffer_size=1000000;block_based_table_factory.prepopulate_block_cache=kFlushOnly;");

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration.getAdditionalColumnFamilyOptions())
        .hasValue(
            "write_buffer_size=1000000;block_based_table_factory.prepopulate_block_cache=kFlushOnly;");
  }

  @Test
  public void additionalDatabaseOptions() {
    final RocksDBCLIOptions options = RocksDBCLIOptions.create();
    new CommandLine(options).parseArgs(ADDITIONAL_DATABASE_OPTIONS, "max_file_opening_threads=16;");

    final RocksDBFactoryConfiguration configuration = options.toDomainObject();
    assertThat(configuration.getAdditionalDatabaseOptions())
        .hasValue("max_file_opening_threads=16;");
  }
}
