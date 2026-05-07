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
package org.hyperledger.besu.cli.subcommands.storage;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.bouncycastle.util.Arrays;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RocksDB subcommand helper methods. */
public class RocksDbHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDbHelper.class);

  /** Default Constructor. */
  RocksDbHelper() {}

  static void forEachColumnFamily(
      final String dbPath, final BiConsumer<RocksDB, ColumnFamilyHandle> task) {
    RocksDB.loadLibrary();
    // Open the RocksDB database with multiple column families
    List<byte[]> cfNames;
    try (final Options options = new Options().setCreateIfMissing(true)) {
      cfNames = RocksDB.listColumnFamilies(options, dbPath);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    for (byte[] cfName : cfNames) {
      cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
    }
    try (final RocksDB rocksdb = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles)) {
      for (ColumnFamilyHandle cfHandle : cfHandles) {
        task.accept(rocksdb, cfHandle);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      for (ColumnFamilyHandle cfHandle : cfHandles) {
        cfHandle.close();
      }
    }
  }

  static void forFilteredColumnFamily(
      final String dbPath,
      final BiConsumer<RocksDB, ColumnFamilyHandle> task,
      final List<String> columnFamilyNameFilter) {
    RocksDB.loadLibrary();
    // Open the RocksDB database with multiple column families
    List<byte[]> cfNames;
    try (final Options options = new Options().setCreateIfMissing(true)) {
      cfNames = RocksDB.listColumnFamilies(options, dbPath);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(cfNames.get(0))); // add default
    cfNames.stream()
        .filter(cfName -> columnFamilyNameFilter.contains(getNameById(cfName)))
        .forEach(
            cfName -> {
              cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
            });
    try (final RocksDB rocksdb = RocksDB.openReadOnly(dbPath, cfDescriptors, cfHandles)) {
      for (ColumnFamilyHandle cfHandle : cfHandles) {
        task.accept(rocksdb, cfHandle);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      for (ColumnFamilyHandle cfHandle : cfHandles) {
        cfHandle.close();
      }
    }
  }

  static void printStatsForColumnFamily(
      final RocksDB rocksdb, final ColumnFamilyHandle cfHandle, final PrintWriter out)
      throws RocksDBException {
    final String size = rocksdb.getProperty(cfHandle, "rocksdb.estimate-live-data-size");
    final String numberOfKeys = rocksdb.getProperty(cfHandle, "rocksdb.estimate-num-keys");
    final long sizeLong = Long.parseLong(size);
    final long numberOfKeysLong = Long.parseLong(numberOfKeys);
    if (!size.isBlank()
        && !numberOfKeys.isBlank()
        && isPopulatedColumnFamily(sizeLong, numberOfKeysLong)) {
      out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
      out.println("Column Family: " + getNameById(cfHandle.getName()));

      final String prefix = "rocksdb.";
      final String cfstats = "cfstats";
      final String cfstats_no_file_histogram = "cfstats-no-file-histogram";
      final String cf_file_histogram = "cf-file-histogram";
      final String cf_write_stall_stats = "cf-write-stall-stats";
      final String dbstats = "dbstats";
      final String db_write_stall_stats = "db-write-stall-stats";
      final String levelstats = "levelstats";
      final String block_cache_entry_stats = "block-cache-entry-stats";
      final String fast_block_cache_entry_stats = "fast-block-cache-entry-stats";
      final String num_immutable_mem_table = "num-immutable-mem-table";
      final String num_immutable_mem_table_flushed = "num-immutable-mem-table-flushed";
      final String mem_table_flush_pending = "mem-table-flush-pending";
      final String compaction_pending = "compaction-pending";
      final String background_errors = "background-errors";
      final String cur_size_active_mem_table = "cur-size-active-mem-table";
      final String cur_size_all_mem_tables = "cur-size-all-mem-tables";
      final String size_all_mem_tables = "size-all-mem-tables";
      final String num_entries_active_mem_table = "num-entries-active-mem-table";
      final String num_entries_imm_mem_tables = "num-entries-imm-mem-tables";
      final String num_deletes_active_mem_table = "num-deletes-active-mem-table";
      final String num_deletes_imm_mem_tables = "num-deletes-imm-mem-tables";
      final String estimate_num_keys = "estimate-num-keys";
      final String estimate_table_readers_mem = "estimate-table-readers-mem";
      final String is_file_deletions_enabled = "is-file-deletions-enabled";
      final String num_snapshots = "num-snapshots";
      final String oldest_snapshot_time = "oldest-snapshot-time";
      final String oldest_snapshot_sequence = "oldest-snapshot-sequence";
      final String num_live_versions = "num-live-versions";
      final String current_version_number = "current-super-version-number";
      final String estimate_live_data_size = "estimate-live-data-size";
      final String min_log_number_to_keep_str = "min-log-number-to-keep";
      final String min_obsolete_sst_number_to_keep_str = "min-obsolete-sst-number-to-keep";
      final String base_level_str = "base-level";
      final String total_sst_files_size = "total-sst-files-size";
      final String live_sst_files_size = "live-sst-files-size";
      final String obsolete_sst_files_size = "obsolete-sst-files-size";
      final String live_sst_files_size_at_temperature = "live-sst-files-size-at-temperature";
      final String estimate_pending_comp_bytes = "estimate-pending-compaction-bytes";
      final String aggregated_table_properties = "aggregated-table-properties";
      final String num_running_compactions = "num-running-compactions";
      final String num_running_flushes = "num-running-flushes";
      final String actual_delayed_write_rate = "actual-delayed-write-rate";
      final String is_write_stopped = "is-write-stopped";
      final String estimate_oldest_key_time = "estimate-oldest-key-time";
      final String block_cache_capacity = "block-cache-capacity";
      final String block_cache_usage = "block-cache-usage";
      final String block_cache_pinned_usage = "block-cache-pinned-usage";
      final String options_statistics = "options-statistics";
      final String num_blob_files = "num-blob-files";
      final String blob_stats = "blob-stats";
      final String total_blob_file_size = "total-blob-file-size";
      final String live_blob_file_size = "live-blob-file-size";
      final String live_blob_file_garbage_size = "live-blob-file-garbage-size";
      final String blob_cache_capacity = "blob-cache-capacity";
      final String blob_cache_usage = "blob-cache-usage";
      final String blob_cache_pinned_usage = "blob-cache-pinned-usage";
      Stream.of(
              cfstats,
              cfstats_no_file_histogram,
              cf_file_histogram,
              cf_write_stall_stats,
              dbstats,
              db_write_stall_stats,
              levelstats,
              block_cache_entry_stats,
              fast_block_cache_entry_stats,
              num_immutable_mem_table,
              num_immutable_mem_table_flushed,
              mem_table_flush_pending,
              compaction_pending,
              background_errors,
              cur_size_active_mem_table,
              cur_size_all_mem_tables,
              size_all_mem_tables,
              num_entries_active_mem_table,
              num_entries_imm_mem_tables,
              num_deletes_active_mem_table,
              num_deletes_imm_mem_tables,
              estimate_num_keys,
              estimate_table_readers_mem,
              is_file_deletions_enabled,
              num_snapshots,
              oldest_snapshot_time,
              oldest_snapshot_sequence,
              num_live_versions,
              current_version_number,
              estimate_live_data_size,
              min_log_number_to_keep_str,
              min_obsolete_sst_number_to_keep_str,
              base_level_str,
              total_sst_files_size,
              live_sst_files_size,
              obsolete_sst_files_size,
              live_sst_files_size_at_temperature,
              estimate_pending_comp_bytes,
              aggregated_table_properties,
              num_running_compactions,
              num_running_flushes,
              actual_delayed_write_rate,
              is_write_stopped,
              estimate_oldest_key_time,
              block_cache_capacity,
              block_cache_usage,
              block_cache_pinned_usage,
              options_statistics,
              num_blob_files,
              blob_stats,
              total_blob_file_size,
              live_blob_file_size,
              live_blob_file_garbage_size,
              blob_cache_capacity,
              blob_cache_usage,
              blob_cache_pinned_usage)
          .forEach(
              prop -> {
                try {
                  final String value = rocksdb.getProperty(cfHandle, prefix + prop);
                  if (!value.isBlank()) {
                    out.println(prop + ": " + value);
                  }
                } catch (RocksDBException e) {
                  LOG.debug("couldn't get property {}", prop);
                }
              });
      out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
    }
  }

  static ColumnFamilyUsage getAndPrintUsageForColumnFamily(
      final RocksDB rocksdb, final ColumnFamilyHandle cfHandle, final PrintWriter out)
      throws RocksDBException, NumberFormatException {
    return getAndPrintUsageForColumnFamily(rocksdb, cfHandle, out, SizeUnit.GIB);
  }

  static ColumnFamilyUsage getAndPrintUsageForColumnFamily(
      final RocksDB rocksdb,
      final ColumnFamilyHandle cfHandle,
      final PrintWriter out,
      final SizeUnit maxSizeUnit)
      throws RocksDBException, NumberFormatException {
    final String numberOfKeys = rocksdb.getProperty(cfHandle, "rocksdb.estimate-num-keys");
    if (!numberOfKeys.isBlank()) {
      try {
        final long numberOfKeysLong = Long.parseLong(numberOfKeys);
        final String totalSstFilesSize =
            rocksdb.getProperty(cfHandle, "rocksdb.total-sst-files-size");
        final long totalSstFilesSizeLong =
            !totalSstFilesSize.isBlank() ? Long.parseLong(totalSstFilesSize) : 0;

        final String totalBlobFilesSize =
            rocksdb.getProperty(cfHandle, "rocksdb.total-blob-file-size");
        final long totalBlobFilesSizeLong =
            !totalBlobFilesSize.isBlank() ? Long.parseLong(totalBlobFilesSize) : 0;

        final long totalFilesSize = totalSstFilesSizeLong + totalBlobFilesSizeLong;
        if (isPopulatedColumnFamily(0, numberOfKeysLong)) {
          printLine(
              out,
              getNameById(cfHandle.getName()),
              rocksdb.getProperty(cfHandle, "rocksdb.estimate-num-keys"),
              formatOutputSize(totalFilesSize, maxSizeUnit),
              formatOutputSize(totalSstFilesSizeLong, maxSizeUnit),
              formatOutputSize(totalBlobFilesSizeLong, maxSizeUnit));
        }
        return new ColumnFamilyUsage(
            getNameById(cfHandle.getName()),
            numberOfKeysLong,
            totalFilesSize,
            totalSstFilesSizeLong,
            totalBlobFilesSizeLong);
      } catch (NumberFormatException e) {
        LOG.error("Failed to parse string into long: " + e.getMessage());
      }
    }
    // return empty usage on error
    return new ColumnFamilyUsage(getNameById(cfHandle.getName()), 0, 0, 0, 0);
  }

  static void printTotals(final PrintWriter out, final List<ColumnFamilyUsage> columnFamilyUsages) {
    printTotals(out, columnFamilyUsages, SizeUnit.GIB);
  }

  static void printTotals(
      final PrintWriter out,
      final List<ColumnFamilyUsage> columnFamilyUsages,
      final SizeUnit maxSizeUnit) {
    final long totalKeys = columnFamilyUsages.stream().mapToLong(ColumnFamilyUsage::keys).sum();
    final long totalSize =
        columnFamilyUsages.stream().mapToLong(ColumnFamilyUsage::totalSize).sum();
    final long totalSsts =
        columnFamilyUsages.stream().mapToLong(ColumnFamilyUsage::sstFilesSize).sum();
    final long totalBlobs =
        columnFamilyUsages.stream().mapToLong(ColumnFamilyUsage::blobFilesSize).sum();
    printSeparator(out);
    printLine(
        out,
        "ESTIMATED TOTAL",
        String.valueOf(totalKeys),
        formatOutputSize(totalSize, maxSizeUnit),
        formatOutputSize(totalSsts, maxSizeUnit),
        formatOutputSize(totalBlobs, maxSizeUnit));
    printSeparator(out);
  }

  private static boolean isPopulatedColumnFamily(final long size, final long numberOfKeys) {
    return size != 0 || numberOfKeys != 0;
  }

  static String formatOutputSize(final long size) {
    return formatOutputSize(size, SizeUnit.GIB);
  }

  static String formatOutputSize(final long size, final SizeUnit maxUnit) {
    // Determine the appropriate unit, capped at maxUnit
    SizeUnit unit;
    if (size > SizeUnit.GIB.getDivisor() && maxUnit.ordinal() >= SizeUnit.GIB.ordinal()) {
      unit = SizeUnit.GIB;
    } else if (size > SizeUnit.MIB.getDivisor() && maxUnit.ordinal() >= SizeUnit.MIB.ordinal()) {
      unit = SizeUnit.MIB;
    } else if (size > SizeUnit.KIB.getDivisor() && maxUnit.ordinal() >= SizeUnit.KIB.ordinal()) {
      unit = SizeUnit.KIB;
    } else if (size > SizeUnit.GIB.getDivisor()) {
      // Size exceeds GiB but maxUnit is smaller, use maxUnit
      unit = maxUnit;
    } else if (size > SizeUnit.MIB.getDivisor()) {
      // Size exceeds MiB but maxUnit is smaller, use maxUnit
      unit = maxUnit;
    } else if (size > SizeUnit.KIB.getDivisor()) {
      // Size exceeds KiB but maxUnit is smaller, use maxUnit
      unit = maxUnit;
    } else {
      unit = SizeUnit.B;
    }
    return (size / unit.getDivisor()) + " " + unit.getSuffix();
  }

  private static String getNameById(final byte[] id) {
    for (KeyValueSegmentIdentifier segment : KeyValueSegmentIdentifier.values()) {
      if (Arrays.areEqual(segment.getId(), id)) {
        return segment.getName();
      }
    }
    return null; // id not found
  }

  static void printTableHeader(final PrintWriter out) {
    printSeparator(out);
    out.format(
        "| Column Family                  | Keys            | Total Size  | SST Files Size  | Blob Files Size  | \n");
    printSeparator(out);
  }

  private static void printSeparator(final PrintWriter out) {
    out.format(
        "|--------------------------------|-----------------|-------------|-----------------|------------------|\n");
  }

  static void printLine(
      final PrintWriter out,
      final String cfName,
      final String keys,
      final String totalFilesSize,
      final String sstFilesSize,
      final String blobFilesSize) {
    final String format = "| %-30s | %-15s | %-11s | %-15s | %-16s |\n";
    out.format(format, cfName, keys, totalFilesSize, sstFilesSize, blobFilesSize);
  }

  record ColumnFamilyUsage(
      String name, long keys, long totalSize, long sstFilesSize, long blobFilesSize) {}
}
