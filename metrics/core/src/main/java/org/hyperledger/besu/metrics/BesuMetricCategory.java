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
package org.hyperledger.besu.metrics;

import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/** The enum Besu metric category. */
public enum BesuMetricCategory implements MetricCategory {
  /** Blockchain besu metric category. */
  BLOCKCHAIN("blockchain"),
  /** Ethereum besu metric category. */
  ETHEREUM("ethereum", false),
  /** Executors besu metric category. */
  EXECUTORS("executors"),
  /** Network besu metric category. */
  NETWORK("network"),
  /** Peers besu metric category. */
  PEERS("peers"),
  /** Permissioning besu metric category. */
  PERMISSIONING("permissioning"),
  /** Kvstore rocksdb besu metric category. */
  KVSTORE_ROCKSDB("rocksdb"),
  /** Kvstore private rocksdb besu metric category. */
  KVSTORE_PRIVATE_ROCKSDB("private_rocksdb"),
  /** Kvstore rocksdb stats besu metric category. */
  KVSTORE_ROCKSDB_STATS("rocksdb", false),
  /** Kvstore private rocksdb stats besu metric category. */
  KVSTORE_PRIVATE_ROCKSDB_STATS("private_rocksdb", false),
  /** Pruner besu metric category. */
  PRUNER("pruner"),
  /** Rpc besu metric category. */
  RPC("rpc"),
  /** Synchronizer besu metric category. */
  SYNCHRONIZER("synchronizer"),
  /** Transaction pool besu metric category. */
  TRANSACTION_POOL("transaction_pool"),
  /** Block processing besu metric category. */
  BLOCK_PROCESSING("block_processing"),
  /** Block access list besu metric category. */
  BAL("bal"),
  /** Bonsai cache besu metric category. */
  BONSAI_CACHE("bonsai_cache");

  private static final Optional<String> BESU_PREFIX = Optional.of("besu_");

  /** The constant DEFAULT_METRIC_CATEGORIES. */
  public static final Set<MetricCategory> DEFAULT_METRIC_CATEGORIES;

  static {
    // Why not KVSTORE_ROCKSDB and KVSTORE_ROCKSDB_STATS, KVSTORE_PRIVATE_ROCKSDB_STATS,
    // KVSTORE_PRIVATE_ROCKSDB_STATS?  They hurt performance under load.
    final EnumSet<BesuMetricCategory> besuCategories =
        EnumSet.complementOf(
            EnumSet.of(
                KVSTORE_ROCKSDB,
                KVSTORE_ROCKSDB_STATS,
                KVSTORE_PRIVATE_ROCKSDB,
                KVSTORE_PRIVATE_ROCKSDB_STATS));

    DEFAULT_METRIC_CATEGORIES =
        ImmutableSet.<MetricCategory>builder()
            .addAll(besuCategories)
            .addAll(EnumSet.allOf(StandardMetricCategory.class))
            .build();
  }

  private final String name;
  private final boolean besuSpecific;

  BesuMetricCategory(final String name) {
    this(name, true);
  }

  BesuMetricCategory(final String name, final boolean besuSpecific) {
    this.name = name;
    this.besuSpecific = besuSpecific;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<String> getApplicationPrefix() {
    return besuSpecific ? BESU_PREFIX : Optional.empty();
  }
}
