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
package org.hyperledger.besu.plugin.services.storage.rocksdb.configuration;

import java.util.Optional;

/** The RocksDb factory configuration. */
public class RocksDBFactoryConfiguration {

  private final int maxOpenFiles;
  private final int backgroundThreadCount;
  private final long cacheCapacity;
  private final boolean isHighSpec;
  private final boolean enableReadCacheForSnapshots;
  private final boolean isBlockchainGarbageCollectionEnabled;
  private final Optional<Double> blobGarbageCollectionAgeCutoff;
  private final Optional<Double> blobGarbageCollectionForceThreshold;
  private final Optional<String> additionalColumnFamilyOptions;
  private final Optional<String> additionalDatabaseOptions;

  /**
   * Instantiates a new RocksDb factory configuration.
   *
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled is garbage collection enabled for the BLOCKCHAIN
   *     column family
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   * @param additionalColumnFamilyOptions semicolon-separated RocksDB column-family options parsed
   *     natively (Nethermind-style); Besu then overlays programmatic CF settings in Java
   * @param additionalDatabaseOptions semicolon-separated {@code DBOptions} string for {@code
   *     DBOptions.getDBOptionsFromProps}; Besu still overlays create paths, max open files, stats,
   *     env threads, and WAL sizing
   */
  public RocksDBFactoryConfiguration(
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final boolean isHighSpec,
      final boolean enableReadCacheForSnapshots,
      final boolean isBlockchainGarbageCollectionEnabled,
      final Optional<Double> blobGarbageCollectionAgeCutoff,
      final Optional<Double> blobGarbageCollectionForceThreshold,
      final Optional<String> additionalColumnFamilyOptions,
      final Optional<String> additionalDatabaseOptions) {
    this.backgroundThreadCount = backgroundThreadCount;
    this.maxOpenFiles = maxOpenFiles;
    this.cacheCapacity = cacheCapacity;
    this.isHighSpec = isHighSpec;
    this.enableReadCacheForSnapshots = enableReadCacheForSnapshots;
    this.isBlockchainGarbageCollectionEnabled = isBlockchainGarbageCollectionEnabled;
    this.blobGarbageCollectionAgeCutoff = blobGarbageCollectionAgeCutoff;
    this.blobGarbageCollectionForceThreshold = blobGarbageCollectionForceThreshold;
    this.additionalColumnFamilyOptions = additionalColumnFamilyOptions;
    this.additionalDatabaseOptions = additionalDatabaseOptions;
  }

  /**
   * Gets max open files.
   *
   * @return the max open files
   */
  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  /**
   * Gets background thread count.
   *
   * @return the background thread count
   */
  public int getBackgroundThreadCount() {
    return backgroundThreadCount;
  }

  /**
   * Gets cache capacity.
   *
   * @return the cache capacity
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Is high spec.
   *
   * @return the boolean
   */
  public boolean isHighSpec() {
    return isHighSpec;
  }

  /**
   * Indicates whether read caching is enabled for snapshot access.
   *
   * @return {@code true} if read cache is enabled for snapshots; {@code false} otherwise.
   */
  public boolean isReadCacheEnabledForSnapshots() {
    return enableReadCacheForSnapshots;
  }

  /**
   * Is garbage collection enabled for the BLOCKCHAIN column family.
   *
   * @return the boolean
   */
  public boolean isBlockchainGarbageCollectionEnabled() {
    return isBlockchainGarbageCollectionEnabled;
  }

  /**
   * Gets blob garbage collection age cutoff.
   *
   * @return the blob garbage collection age cutoff, if set
   */
  public Optional<Double> getBlobGarbageCollectionAgeCutoff() {
    return blobGarbageCollectionAgeCutoff;
  }

  /**
   * Gets blob garbage collection force threshold.
   *
   * @return the blob garbage collection force threshold, if set
   */
  public Optional<Double> getBlobGarbageCollectionForceThreshold() {
    return blobGarbageCollectionForceThreshold;
  }

  /**
   * Additional column-family options as a semicolon-separated {@code key=value;} string, parsed
   * through RocksDB's native option parser where possible.
   *
   * @return the additional options, if any
   */
  public Optional<String> getAdditionalColumnFamilyOptions() {
    return additionalColumnFamilyOptions;
  }

  /**
   * Additional {@link org.rocksdb.DBOptions} as a semicolon-separated native string.
   *
   * @return the options string, if any
   */
  public Optional<String> getAdditionalDatabaseOptions() {
    return additionalDatabaseOptions;
  }
}
