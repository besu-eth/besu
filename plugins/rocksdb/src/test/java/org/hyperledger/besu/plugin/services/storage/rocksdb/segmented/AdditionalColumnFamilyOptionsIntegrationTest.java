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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;

/**
 * Verifies additional column-family option strings against RocksDB and against Besu's storage
 * wrapper. Memtable options such as {@code write_buffer_size} survive Besu's Java overlay; some
 * block-table factory settings from the native string may be reset when Besu reapplies its table
 * configuration in Java (see {@link
 * #prepopulateBlockCacheFromNativeStringSurvivesWithoutBesuTableOverlay}).
 *
 * <p>Database-level strings are covered by {@link AdditionalDatabaseOptionsIntegrationTest}.
 */
public class AdditionalColumnFamilyOptionsIntegrationTest {

  @Test
  public void getColumnFamilyOptionsFromPropsThenBesuTtlAndLz4KeepsOtherNativeSettings()
      throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final Properties props = new Properties();
    props.setProperty("write_buffer_size", "7654321");
    props.setProperty("block_based_table_factory.prepopulate_block_cache", "kFlushOnly");
    try (ColumnFamilyOptions opts =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), props)) {
      assertThat(opts).isNotNull();
      opts.setTtl(0).setCompressionType(CompressionType.LZ4_COMPRESSION);
      assertThat(opts.writeBufferSize()).isEqualTo(7_654_321L);
    }
  }

  @Test
  public void prepopulateBlockCacheFromNativeStringSurvivesWithoutBesuTableOverlay(
      @TempDir final Path dbDir) throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final Properties props = new Properties();
    props.setProperty("block_based_table_factory.prepopulate_block_cache", "kFlushOnly");

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (ColumnFamilyOptions cfOpts =
            ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), props);
        DBOptions dbOpts =
            new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        RocksDB db =
            RocksDB.open(
                dbOpts,
                dbDir.toString(),
                List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)),
                handles)) {
      assertThat(db).isNotNull();
    } finally {
      for (ColumnFamilyHandle h : handles) {
        h.close();
      }
    }

    final String optionsFileName =
        OptionsUtil.getLatestOptionsFileName(dbDir.toString(), Env.getDefault());
    final String content = Files.readString(dbDir.resolve(optionsFileName), StandardCharsets.UTF_8);
    assertThat(content)
        .containsIgnoringCase("prepopulate_block_cache")
        .containsIgnoringCase("kFlushOnly");
  }

  @Test
  public void additionalColumnFamilyOptionsAreVisibleInLiveDbAndOptionsFile(
      @TempDir final Path dbDir) throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final long writeBufferSize = 2_014_040L;
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalColumnFamilyOptions(
                Optional.of(
                    "write_buffer_size="
                        + writeBufferSize
                        + ";block_based_table_factory.prepopulate_block_cache=kFlushOnly;"))
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

      final RocksDB db = store.getDB();
      final var segmentRef =
          store.columnHandlesBySegmentIdentifier.get(
              RocksDBColumnarKeyValueStorageTest.TestSegment.DEFAULT);
      assertThat(segmentRef).isNotNull();
      final long liveWriteBuffer = db.getOptions(segmentRef.get()).writeBufferSize();
      assertThat(liveWriteBuffer).isEqualTo(writeBufferSize);
    }

    final String optionsFileName =
        OptionsUtil.getLatestOptionsFileName(dbDir.toString(), Env.getDefault());
    final String optionsContent =
        Files.readString(dbDir.resolve(optionsFileName), StandardCharsets.UTF_8);
    assertThat(optionsContent).contains("write_buffer_size=" + writeBufferSize);
    // Besu reapplies block table settings in Java; JNI resets prepopulate_block_cache to default.
    assertThat(optionsContent).containsIgnoringCase("prepopulate_block_cache=kDisable");
  }

  @Test
  public void unknownColumnFamilyOptionKeyFallsBackAndStorageStillOpens(@TempDir final Path dbDir)
      throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalColumnFamilyOptions(Optional.of("besu_nonexistent_cf_option_xyz=1;"))
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
    assertThat(content).doesNotContain("besu_nonexistent_cf_option_xyz");
  }

  @Test
  public void unknownCfKeyInStringInvalidatesEntireAdditionalColumnFamilyOptions(
      @TempDir final Path dbDir) throws Exception {
    RocksDbUtil.loadNativeLibrary();
    final long requestedWriteBufferSize = 1_919_191L;
    final var configuration =
        new RocksDBConfigurationBuilder()
            .databaseDir(dbDir)
            .additionalColumnFamilyOptions(
                Optional.of(
                    "write_buffer_size="
                        + requestedWriteBufferSize
                        + ";besu_nonexistent_cf_option_xyz=1;"))
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

      final var segmentRef =
          store.columnHandlesBySegmentIdentifier.get(
              RocksDBColumnarKeyValueStorageTest.TestSegment.DEFAULT);
      assertThat(segmentRef).isNotNull();
      final long liveWriteBuffer = store.getDB().getOptions(segmentRef.get()).writeBufferSize();
      assertThat(liveWriteBuffer).isNotEqualTo(requestedWriteBufferSize);
      // Bare ColumnFamilyOptions default when getColumnFamilyOptionsFromProps returns null (RocksDB
      // 9.7.x).
      assertThat(liveWriteBuffer).isEqualTo(67_108_864L);
    }
  }
}
