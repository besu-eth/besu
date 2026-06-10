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

import java.lang.management.ManagementFactory;
import java.util.Optional;

import com.google.common.base.MoreObjects;
import com.sun.management.OperatingSystemMXBean;
import picocli.CommandLine;

/** The RocksDb cli options. */
public class RocksDBCLIOptions {

  /** The constant DEFAULT_MAX_OPEN_FILES. */
  public static final int DEFAULT_MAX_OPEN_FILES = 1024;

  /** Max open files for machines with at least 4 GiB available memory. */
  public static final int MAX_OPEN_FILES_4GB = 2048;

  /** Max open files for machines with at least 8 GiB available memory. */
  public static final int MAX_OPEN_FILES_8GB = 4096;

  /** Max open files for machines with at least 16 GiB available memory. */
  public static final int MAX_OPEN_FILES_16GB = 8192;

  /** Max open files for machines with at least 32 GiB available memory. */
  public static final int MAX_OPEN_FILES_32GB = 16384;

  private static final long GIB = 1024L * 1024L * 1024L;

  /** The constant DEFAULT_CACHE_CAPACITY. */
  public static final long DEFAULT_CACHE_CAPACITY = 134217728;

  /** The constant DEFAULT_BACKGROUND_THREAD_COUNT. */
  public static final int DEFAULT_BACKGROUND_THREAD_COUNT = 4;

  /** The constant DEFAULT_IS_HIGH_SPEC. */
  public static final boolean DEFAULT_IS_HIGH_SPEC = false;

  /** The default value indicating whether read caching is enabled for snapshot access. */
  public static final boolean DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS = false;

  /** The constant MAX_OPEN_FILES_FLAG. */
  public static final String MAX_OPEN_FILES_FLAG = "--Xplugin-rocksdb-max-open-files";

  /** The constant CACHE_CAPACITY_FLAG. */
  public static final String CACHE_CAPACITY_FLAG = "--Xplugin-rocksdb-cache-capacity";

  /** The constant BACKGROUND_THREAD_COUNT_FLAG. */
  public static final String BACKGROUND_THREAD_COUNT_FLAG =
      "--Xplugin-rocksdb-background-thread-count";

  /** The constant IS_HIGH_SPEC. */
  public static final String IS_HIGH_SPEC = "--Xplugin-rocksdb-high-spec-enabled";

  /** The constant ENABLE_READ_CACHE_FOR_SNAPSHOTS. */
  public static final String ENABLE_READ_CACHE_FOR_SNAPSHOTS =
      "--Xplugin-rocksdb-read-cache-snapshots-enabled";

  /** Key name for configuring blockchain_blob_garbage_collection_enabled */
  public static final String BLOB_BLOCKCHAIN_GARBAGE_COLLECTION_ENABLED =
      "--Xplugin-rocksdb-blockchain-blob-garbage-collection-enabled";

  /** Key name for configuring blob_garbage_collection_age_cutoff */
  public static final String BLOB_GARBAGE_COLLECTION_AGE_CUTOFF =
      "--Xplugin-rocksdb-blob-garbage-collection-age-cutoff";

  /** Key name for configuring blob_garbage_collection_force_threshold */
  public static final String BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD =
      "--Xplugin-rocksdb-blob-garbage-collection-force-threshold";

  /** The Max open files. */
  @CommandLine.Option(
      names = {MAX_OPEN_FILES_FLAG},
      hidden = true,
      paramLabel = "<INTEGER>",
      description =
          "Max number of files RocksDB will open. If unset, derives a value from available memory.")
  Optional<Integer> maxOpenFiles = Optional.empty();

  /** The Cache capacity. */
  @CommandLine.Option(
      names = {CACHE_CAPACITY_FLAG},
      hidden = true,
      defaultValue = "134217728",
      paramLabel = "<LONG>",
      description = "Cache capacity of RocksDB (default: ${DEFAULT-VALUE})")
  long cacheCapacity;

  /** The Background thread count. */
  @CommandLine.Option(
      names = {BACKGROUND_THREAD_COUNT_FLAG},
      hidden = true,
      defaultValue = "4",
      paramLabel = "<INTEGER>",
      description = "Number of RocksDB background threads (default: ${DEFAULT-VALUE})")
  int backgroundThreadCount;

  /** The Is high spec. */
  @CommandLine.Option(
      names = {IS_HIGH_SPEC},
      hidden = true,
      paramLabel = "<BOOLEAN>",
      description =
          "Use this flag to boost Besu performance if you have a 16 GiB RAM hardware or more (default: ${DEFAULT-VALUE})")
  boolean isHighSpec;

  /** Enables read caching during snapshot access. */
  @CommandLine.Option(
      names = {ENABLE_READ_CACHE_FOR_SNAPSHOTS},
      hidden = true,
      paramLabel = "<BOOLEAN>",
      description =
          "Enable read caching during snapshot access for better RPC performance (default: ${DEFAULT-VALUE}). May slow block processing.")
  boolean enableReadCacheForSnapshots;

  /** The Blob blockchain garbage collection enabled. */
  @CommandLine.Option(
      names = {BLOB_BLOCKCHAIN_GARBAGE_COLLECTION_ENABLED},
      hidden = true,
      paramLabel = "<BOOLEAN>",
      description =
          "Enable garbage collection for the BLOCKCHAIN column family (default: ${DEFAULT-VALUE})")
  boolean isBlockchainGarbageCollectionEnabled = false;

  /**
   * The Blob garbage collection age cutoff. The fraction of file age to be considered eligible for
   * GC; e.g. 0.25 = oldest 25% of files eligible; e.g. 1 = all files eligible When unspecified, use
   * RocksDB default
   */
  @CommandLine.Option(
      names = {BLOB_GARBAGE_COLLECTION_AGE_CUTOFF},
      hidden = true,
      paramLabel = "<DOUBLE>",
      description = "Blob garbage collection age cutoff (default: ${DEFAULT-VALUE})")
  Optional<Double> blobGarbageCollectionAgeCutoff = Optional.empty();

  /**
   * The Blob garbage collection force threshold. The fraction of garbage inside eligible blob files
   * to trigger GC; e.g. 1 = trigger when eligible file contains 100% garbage; e.g. 0 = trigger for
   * all eligible files; When unspecified, use Rocksdb default
   */
  @CommandLine.Option(
      names = {BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD},
      hidden = true,
      paramLabel = "<DOUBLE>",
      description = "Blob garbage collection force threshold (default: ${DEFAULT-VALUE})")
  Optional<Double> blobGarbageCollectionForceThreshold = Optional.empty();

  private RocksDBCLIOptions() {}

  /**
   * Create RocksDb cli options.
   *
   * @return the RocksDb cli options
   */
  public static RocksDBCLIOptions create() {
    return new RocksDBCLIOptions();
  }

  /**
   * RocksDb cli options from config.
   *
   * @param config the config
   * @return the RocksDb cli options
   */
  public static RocksDBCLIOptions fromConfig(final RocksDBConfiguration config) {
    final RocksDBCLIOptions options = create();
    options.maxOpenFiles = Optional.of(config.getMaxOpenFiles());
    options.cacheCapacity = config.getCacheCapacity();
    options.backgroundThreadCount = config.getBackgroundThreadCount();
    options.isHighSpec = config.isHighSpec();
    options.enableReadCacheForSnapshots = config.isReadCacheEnabledForSnapshots();
    options.isBlockchainGarbageCollectionEnabled = config.isBlockchainGarbageCollectionEnabled();
    options.blobGarbageCollectionAgeCutoff = config.getBlobGarbageCollectionAgeCutoff();
    options.blobGarbageCollectionForceThreshold = config.getBlobGarbageCollectionForceThreshold();
    return options;
  }

  /**
   * To domain object rocks db factory configuration.
   *
   * @return the rocks db factory configuration
   */
  public RocksDBFactoryConfiguration toDomainObject() {
    return new RocksDBFactoryConfiguration(
        maxOpenFiles.orElseGet(RocksDBCLIOptions::deriveMaxOpenFilesFromAvailableMemory),
        backgroundThreadCount,
        cacheCapacity,
        isHighSpec,
        enableReadCacheForSnapshots,
        isBlockchainGarbageCollectionEnabled,
        blobGarbageCollectionAgeCutoff,
        blobGarbageCollectionForceThreshold);
  }

  /**
   * Is high spec.
   *
   * @return the boolean
   */
  public boolean isHighSpec() {
    return isHighSpec;
  }

  public static int deriveMaxOpenFilesFromAvailableMemory() {
    final java.lang.management.OperatingSystemMXBean osBean =
        ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof OperatingSystemMXBean operatingSystemMXBean) {
      return calculateMaxOpenFiles(operatingSystemMXBean.getFreeMemorySize());
    }
    return DEFAULT_MAX_OPEN_FILES;
  }

  public static int calculateMaxOpenFiles(final long availableMemoryBytes) {
    if (availableMemoryBytes >= 32L * GIB) {
      return MAX_OPEN_FILES_32GB;
    }
    if (availableMemoryBytes >= 16L * GIB) {
      return MAX_OPEN_FILES_16GB;
    }
    if (availableMemoryBytes >= 8L * GIB) {
      return MAX_OPEN_FILES_8GB;
    }
    if (availableMemoryBytes >= 4L * GIB) {
      return MAX_OPEN_FILES_4GB;
    }
    return DEFAULT_MAX_OPEN_FILES;
  }

  /**
   * Gets blob db settings.
   *
   * @return the blob db settings
   */
  public BlobDBSettings getBlobDBSettings() {
    return new BlobDBSettings(
        isBlockchainGarbageCollectionEnabled,
        blobGarbageCollectionAgeCutoff,
        blobGarbageCollectionForceThreshold);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("maxOpenFiles", maxOpenFiles)
        .add("cacheCapacity", cacheCapacity)
        .add("backgroundThreadCount", backgroundThreadCount)
        .add("isHighSpec", isHighSpec)
        .add("enableReadCacheForSnapshots", enableReadCacheForSnapshots)
        .add("isBlockchainGarbageCollectionEnabled", isBlockchainGarbageCollectionEnabled)
        .add("blobGarbageCollectionAgeCutoff", blobGarbageCollectionAgeCutoff)
        .add("blobGarbageCollectionForceThreshold", blobGarbageCollectionForceThreshold)
        .toString();
  }

  /**
   * A container type BlobDBSettings
   *
   * @param isBlockchainGarbageCollectionEnabled the is blockchain garbage collection enabled
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   */
  public record BlobDBSettings(
      boolean isBlockchainGarbageCollectionEnabled,
      Optional<Double> blobGarbageCollectionAgeCutoff,
      Optional<Double> blobGarbageCollectionForceThreshold) {}
}
