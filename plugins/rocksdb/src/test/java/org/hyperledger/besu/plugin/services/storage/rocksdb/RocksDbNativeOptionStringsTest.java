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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;

public class RocksDbNativeOptionStringsTest {

  @Test
  public void parseSemicolonKeyValueStringSplitsPairs() {
    final var props = RocksDbNativeOptionStrings.parseSemicolonKeyValueString("a=1;b=two;");
    assertThat(props.getProperty("a")).isEqualTo("1");
    assertThat(props.getProperty("b")).isEqualTo("two");
  }

  @Test
  public void parseSemicolonKeyValueStringPreservesKeyOrderInStringPropertyNames() {
    final var props = RocksDbNativeOptionStrings.parseSemicolonKeyValueString("z=1;a=2;m=3;");
    assertThat(new ArrayList<>(props.stringPropertyNames())).containsExactly("z", "a", "m");
  }

  @Test
  public void parseSemicolonKeyValueStringKeepsAllKeysIncludingBlockTable() {
    final var props =
        RocksDbNativeOptionStrings.parseSemicolonKeyValueString(
            "write_buffer_size=1000000;block_based_table_factory.block_size=8192;block_based_table_factory.prepopulate_block_cache=kFlushOnly;ttl=0");
    assertThat(props.getProperty("write_buffer_size")).isEqualTo("1000000");
    assertThat(props.getProperty("ttl")).isEqualTo("0");
    assertThat(props.getProperty("block_based_table_factory.prepopulate_block_cache"))
        .isEqualTo("kFlushOnly");
    assertThat(props.getProperty("block_based_table_factory.block_size")).isEqualTo("8192");
  }

  @Test
  public void insertionOrderedPropertiesKeepsBesuBlockTableKeyCallOrder() {
    final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps =
        new RocksDbNativeOptionStrings.InsertionOrderedProperties();
    cfProps.setProperty("block_based_table_factory.index_type", "kTwoLevelIndexSearch");
    cfProps.setProperty("block_based_table_factory.format_version", "5");
    cfProps.setProperty("block_based_table_factory.filter_policy", "bloomfilter:10:false");
    cfProps.setProperty("block_based_table_factory.partition_filters", "true");
    cfProps.setProperty("block_based_table_factory.cache_index_and_filter_blocks", "false");
    cfProps.setProperty("block_based_table_factory.block_size", "32768");
    cfProps.setProperty("block_based_table_factory.block_cache", "8388608");
    cfProps.setProperty("write_buffer_size", "123");
    final List<String> names = new ArrayList<>(cfProps.stringPropertyNames());
    assertThat(names.indexOf("block_based_table_factory.index_type"))
        .isLessThan(names.indexOf("block_based_table_factory.partition_filters"));
    assertThat(names.indexOf("block_based_table_factory.partition_filters"))
        .isLessThan(names.indexOf("write_buffer_size"));
  }

  @Test
  public void parseSemicolonKeyValueStringKeepsFormatVersionAndCompactionKeys() {
    assertThat(
            RocksDbNativeOptionStrings.parseSemicolonKeyValueString(
                    "block_based_table_factory.format_version=5;")
                .getProperty("block_based_table_factory.format_version"))
        .isEqualTo("5");
    assertThat(
            RocksDbNativeOptionStrings.parseSemicolonKeyValueString(
                    "level_compaction_dynamic_level_bytes=false;")
                .getProperty("level_compaction_dynamic_level_bytes"))
        .isEqualTo("false");
    assertThat(
            RocksDbNativeOptionStrings.parseSemicolonKeyValueString("enable_blob_files=true;")
                .getProperty("enable_blob_files"))
        .isEqualTo("true");
    assertThat(
            RocksDbNativeOptionStrings.parseSemicolonKeyValueString(
                    "blob_garbage_collection_age_cutoff=0.25;")
                .getProperty("blob_garbage_collection_age_cutoff"))
        .isEqualTo("0.25");
  }

  @Test
  public void getColumnFamilyOptionsFromPropsAcceptsNonEmptyNativeProperties() {
    RocksDbUtil.loadNativeLibrary();
    final Properties cfProps = new Properties();
    cfProps.setProperty("ttl", "0");
    cfProps.setProperty("compression", "kLZ4Compression");
    try (ColumnFamilyOptions opts =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), cfProps)) {
      assertThat(opts).isNotNull();
    }
  }

  @Test
  public void getDBOptionsFromPropsAcceptsNativeString() {
    RocksDbUtil.loadNativeLibrary();
    final Properties dbProps = new Properties();
    dbProps.setProperty("max_file_opening_threads", "8");
    try (DBOptions opts = DBOptions.getDBOptionsFromProps(new ConfigOptions(), dbProps)) {
      assertThat(opts).isNotNull();
    }
  }

  @Test
  public void getColumnFamilyOptionsFromPropsAcceptsBonsaiTrieHotBlockTableConfiguration() {
    RocksDbUtil.loadNativeLibrary();
    final long cacheBytes = 32L * 1024 * 1024;
    final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps =
        new RocksDbNativeOptionStrings.InsertionOrderedProperties();
    cfProps.setProperty(
        "block_based_table_factory.format_version", "5");
    cfProps.setProperty("block_based_table_factory.index_type", "kTwoLevelIndexSearch");
    cfProps.setProperty("block_based_table_factory.filter_policy", "bloomfilter:10:false");
    cfProps.setProperty("block_based_table_factory.partition_filters", "true");
    cfProps.setProperty("block_based_table_factory.cache_index_and_filter_blocks", "true");
    cfProps.setProperty(
        "block_based_table_factory.cache_index_and_filter_blocks_with_high_priority", "true");
    cfProps.setProperty("block_based_table_factory.block_size", "32768");
    cfProps.setProperty("block_based_table_factory.block_cache", Long.toString(cacheBytes));
    try (ColumnFamilyOptions opts =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), cfProps)) {
      assertThat(opts).isNotNull();
    }
  }

  @Test
  public void getColumnFamilyOptionsFromPropsAcceptsNonHotCacheIndexInBlockCacheConfiguration() {
    RocksDbUtil.loadNativeLibrary();
    final long cacheBytes = 8 * 1024 * 1024;
    final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps =
        new RocksDbNativeOptionStrings.InsertionOrderedProperties();
    cfProps.setProperty("block_based_table_factory.format_version", "5");
    cfProps.setProperty("block_based_table_factory.filter_policy", "bloomfilter:10:false");
    cfProps.setProperty("block_based_table_factory.partition_filters", "false");
    cfProps.setProperty("block_based_table_factory.cache_index_and_filter_blocks", "true");
    cfProps.setProperty(
        "block_based_table_factory.cache_index_and_filter_blocks_with_high_priority", "true");
    cfProps.setProperty("block_based_table_factory.block_size", "32768");
    cfProps.setProperty("block_based_table_factory.block_cache", Long.toString(cacheBytes));
    try (ColumnFamilyOptions opts =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), cfProps)) {
      assertThat(opts).isNotNull();
    }
  }

  @Test
  public void
      getColumnFamilyOptionsFromPropsAcceptsBesuStyleBlockTableKeysWithBlockCacheCapacity() {
    RocksDbUtil.loadNativeLibrary();
    final long cacheBytes = 8 * 1024 * 1024;
    final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps =
        new RocksDbNativeOptionStrings.InsertionOrderedProperties();
    cfProps.setProperty("block_based_table_factory.index_type", "kTwoLevelIndexSearch");
    cfProps.setProperty("block_based_table_factory.format_version", "5");
    cfProps.setProperty("block_based_table_factory.filter_policy", "bloomfilter:10:false");
    cfProps.setProperty("block_based_table_factory.partition_filters", "true");
    cfProps.setProperty("block_based_table_factory.cache_index_and_filter_blocks", "false");
    cfProps.setProperty("block_based_table_factory.block_size", "32768");
    cfProps.setProperty("block_based_table_factory.block_cache", Long.toString(cacheBytes));
    cfProps.setProperty("block_based_table_factory.prepopulate_block_cache", "kFlushOnly");
    try (ColumnFamilyOptions opts =
        ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), cfProps)) {
      assertThat(opts).isNotNull();
    }
  }
}
