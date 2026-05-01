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

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.rocksdb.RocksDBStats;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Rocks db metrics factory. */
public class RocksDBMetricsFactory {

  /** The constant PUBLIC_ROCKS_DB_METRICS. */
  public static final RocksDBMetricsFactory PUBLIC_ROCKS_DB_METRICS =
      new RocksDBMetricsFactory(
          BesuMetricCategory.KVSTORE_ROCKSDB, BesuMetricCategory.KVSTORE_ROCKSDB_STATS);

  /** The constant PRIVATE_ROCKS_DB_METRICS. */
  public static final RocksDBMetricsFactory PRIVATE_ROCKS_DB_METRICS =
      new RocksDBMetricsFactory(
          BesuMetricCategory.KVSTORE_PRIVATE_ROCKSDB,
          BesuMetricCategory.KVSTORE_PRIVATE_ROCKSDB_STATS);

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricsFactory.class);

  private final MetricCategory rocksDbMetricCategory;
  private final MetricCategory statsDbMetricCategory;

  /**
   * Instantiates a new RocksDb metrics factory.
   *
   * @param rocksDbMetricCategory the rocks db metric category
   * @param statsDbMetricCategory the stats db metric category
   */
  public RocksDBMetricsFactory(
      final MetricCategory rocksDbMetricCategory, final MetricCategory statsDbMetricCategory) {
    this.rocksDbMetricCategory = rocksDbMetricCategory;
    this.statsDbMetricCategory = statsDbMetricCategory;
  }

  /**
   * Create RocksDb metrics.
   *
   * @param metricsSystem the metrics system
   * @param rocksDbConfiguration the rocks db configuration
   * @param db the db
   * @param stats the stats
   * @return the rocks db metrics
   */
  public RocksDBMetrics create(
      final MetricsSystem metricsSystem,
      final RocksDBConfiguration rocksDbConfiguration,
      final RocksDB db,
      final Statistics stats) {
    final OperationTimer readLatency =
        metricsSystem
            .createLabelledTimer(
                rocksDbMetricCategory,
                "read_latency_seconds",
                "Latency for read from RocksDB.",
                "database")
            .labels(rocksDbConfiguration.getLabel());
    final OperationTimer removeLatency =
        metricsSystem
            .createLabelledTimer(
                rocksDbMetricCategory,
                "remove_latency_seconds",
                "Latency of remove requests from RocksDB.",
                "database")
            .labels(rocksDbConfiguration.getLabel());
    final OperationTimer writeLatency =
        metricsSystem
            .createLabelledTimer(
                rocksDbMetricCategory,
                "write_latency_seconds",
                "Latency for write to RocksDB.",
                "database")
            .labels(rocksDbConfiguration.getLabel());
    final OperationTimer commitLatency =
        metricsSystem
            .createLabelledTimer(
                rocksDbMetricCategory,
                "commit_latency_seconds",
                "Latency for commits to RocksDB.",
                "database")
            .labels(rocksDbConfiguration.getLabel());

    RocksDBStats.registerRocksDBMetrics(stats, metricsSystem, statsDbMetricCategory);

    metricsSystem.createLongGauge(
        rocksDbMetricCategory,
        "rocks_db_table_readers_memory_bytes",
        "Estimated memory used for RocksDB index and filter blocks in bytes",
        () -> {
          try {
            return db.getLongProperty("rocksdb.estimate-table-readers-mem");
          } catch (final RocksDBException e) {
            LOG.debug("Failed to get RocksDB metric", e);
            return 0L;
          }
        });

    metricsSystem.createLongGauge(
        rocksDbMetricCategory,
        "rocks_db_files_size_bytes",
        "Estimated database size in bytes",
        () -> {
          try {
            return db.getLongProperty("rocksdb.live-sst-files-size");
          } catch (final RocksDBException e) {
            LOG.debug("Failed to get RocksDB metric", e);
            return 0L;
          }
        });

    final Counter rollbackCount =
        metricsSystem
            .createLabelledCounter(
                rocksDbMetricCategory,
                "rollback_count",
                "Number of RocksDB transactions rolled back.",
                "database")
            .labels(rocksDbConfiguration.getLabel());

    return new RocksDBMetrics(
        readLatency, removeLatency, writeLatency, commitLatency, rollbackCount);
  }
}
