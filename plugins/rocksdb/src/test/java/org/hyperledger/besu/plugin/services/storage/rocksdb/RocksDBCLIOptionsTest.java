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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import picocli.CommandLine;

public class RocksDBCLIOptionsTest {

  private static final long GIB = 1024L * 1024L * 1024L;

  @Test
  public void defaultValues() {
    final RocksDBFactoryConfiguration configuration = toDomainObjectWithAvailableMemory(8L * GIB);

    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(MAX_OPEN_FILES_8GB);
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customBackgroundThreadCount() {
    final int expectedBackgroundThreadCount = 99;

    final RocksDBFactoryConfiguration configuration =
        toDomainObjectWithAvailableMemory(
            8L * GIB,
            RocksDBCLIOptions.BACKGROUND_THREAD_COUNT_FLAG,
            "" + expectedBackgroundThreadCount);

    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(expectedBackgroundThreadCount);
    assertThat(configuration.getCacheCapacity()).isEqualTo(DEFAULT_CACHE_CAPACITY);
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(MAX_OPEN_FILES_8GB);
    assertThat(configuration.isHighSpec()).isEqualTo(DEFAULT_IS_HIGH_SPEC);
  }

  @Test
  public void customCacheCapacity() {
    final long expectedCacheCapacity = 400050006000L;

    final RocksDBFactoryConfiguration configuration =
        toDomainObjectWithAvailableMemory(
            8L * GIB, CACHE_CAPACITY_FLAG, "" + expectedCacheCapacity);

    assertThat(configuration).isNotNull();
    assertThat(configuration.getBackgroundThreadCount()).isEqualTo(DEFAULT_BACKGROUND_THREAD_COUNT);
    assertThat(configuration.getCacheCapacity()).isEqualTo(expectedCacheCapacity);
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(MAX_OPEN_FILES_8GB);
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
    assertThat(options.getResolvedMaxOpenFiles()).isEqualTo(expectedMaxOpenFiles);
    assertThat(options.isMaxOpenFilesExplicitlySet()).isTrue();
  }

  @Test
  public void derivedMaxOpenFilesIsNotExplicitlySet() {
    final RocksDBCLIOptions options = toOptionsWithAvailableMemory(8L * GIB);

    assertThat(options.getResolvedMaxOpenFiles()).isEqualTo(MAX_OPEN_FILES_8GB);
    assertThat(options.isMaxOpenFilesExplicitlySet()).isFalse();
  }

  @Test
  public void resolvedMaxOpenFilesIsCachedAcrossCalls() {
    final RocksDBCLIOptions options = toOptionsWithAvailableMemory(8L * GIB);

    final int resolvedForDisplay = options.getResolvedMaxOpenFiles();
    final int resolvedForRocksDb = options.toDomainObject().getMaxOpenFiles();

    assertThat(resolvedForDisplay).isEqualTo(resolvedForRocksDb);
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
  public void autoMaxOpenFilesUsesMemoryTiers() {
    assertMaxOpenFilesDerivedFromAvailableMemory(2L * GIB, DEFAULT_MAX_OPEN_FILES);
    assertMaxOpenFilesDerivedFromAvailableMemory(4L * GIB, MAX_OPEN_FILES_4GB);
    assertMaxOpenFilesDerivedFromAvailableMemory(8L * GIB, MAX_OPEN_FILES_8GB);
    assertMaxOpenFilesDerivedFromAvailableMemory(16L * GIB, MAX_OPEN_FILES_16GB);
    assertMaxOpenFilesDerivedFromAvailableMemory(32L * GIB, MAX_OPEN_FILES_32GB);
  }

  private static RocksDBFactoryConfiguration toDomainObjectWithAvailableMemory(
      final long freeMemoryBytes, final String... cliArgs) {
    return toOptionsWithAvailableMemory(freeMemoryBytes, cliArgs).toDomainObject();
  }

  private static RocksDBCLIOptions toOptionsWithAvailableMemory(
      final long freeMemoryBytes, final String... cliArgs) {
    try (MockedStatic<ManagementFactory> managementFactory = mockStatic(ManagementFactory.class)) {
      final OperatingSystemMXBean osBean = mock(OperatingSystemMXBean.class);
      when(osBean.getFreeMemorySize()).thenReturn(freeMemoryBytes);
      managementFactory.when(ManagementFactory::getOperatingSystemMXBean).thenReturn(osBean);

      final RocksDBCLIOptions options = RocksDBCLIOptions.create();
      new CommandLine(options).parseArgs(cliArgs);
      return options;
    }
  }

  private static void assertMaxOpenFilesDerivedFromAvailableMemory(
      final long freeMemoryBytes, final int expectedMaxOpenFiles) {
    final RocksDBFactoryConfiguration configuration =
        toDomainObjectWithAvailableMemory(freeMemoryBytes);
    assertThat(configuration.getMaxOpenFiles()).isEqualTo(expectedMaxOpenFiles);
  }
}
