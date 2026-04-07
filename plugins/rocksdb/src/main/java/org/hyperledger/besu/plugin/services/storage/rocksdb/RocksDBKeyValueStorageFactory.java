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

import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.BONSAI_WITH_VARIABLES;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.FOREST_WITH_RECEIPT_COMPACTION;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat.FOREST_WITH_VARIABLES;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.BLOB_BLOCKCHAIN_GARBAGE_COLLECTION_ENABLED;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.BLOB_GARBAGE_COLLECTION_AGE_CUTOFF;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageSegmentGroup;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.VersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.TransactionDBRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Rocks db key value storage factory creates segmented storage and uses a adapter to support
 * unsegmented keyvalue storage.
 */
public class RocksDBKeyValueStorageFactory implements KeyValueStorageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyValueStorageFactory.class);
  private static final EnumSet<BaseVersionedStorageFormat> SUPPORTED_VERSIONED_FORMATS =
      EnumSet.of(
          FOREST_WITH_RECEIPT_COMPACTION,
          BONSAI_WITH_RECEIPT_COMPACTION,
          BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION);
  private static final String NAME = "rocksdb";

  /** Subdirectory under the storage path for the dedicated world-state RocksDB (split layout). */
  public static final String STATE_DATABASE_DIR = "state";

  /** Subdirectory for blockchain, trie log, variables, sync segments, etc. (split layout). */
  public static final String MAIN_DATABASE_DIR = "main";

  /**
   * Multiply CLI {@code max_open_files} for the state DB (separate instance). Capped in {@link
   * #stateDatabaseMaxOpenFiles}.
   */
  private static final int STATE_DATABASE_MAX_OPEN_FILES_FACTOR = 4;

  private static final int STATE_DATABASE_MAX_OPEN_FILES_MIN = 4_096;
  private static final int STATE_DATABASE_MAX_OPEN_FILES_CAP = 32_768;

  private final RocksDBMetricsFactory rocksDBMetricsFactory;
  private DatabaseMetadata databaseMetadata;

  /** Legacy single-directory layout ({@code database/CURRENT}). */
  private RocksDBColumnarKeyValueStorage segmentedStorage;

  private final Map<KeyValueStorageSegmentGroup, RocksDBColumnarKeyValueStorage> segmentedStorages =
      new ConcurrentHashMap<>();
  private boolean metadataInitialized;
  private boolean legacyMonolithicDatabase;

  private final Supplier<RocksDBFactoryConfiguration> configuration;
  private final List<SegmentIdentifier> configuredSegments;
  private final List<SegmentIdentifier> ignorableSegments;

  /**
   * Instantiates a new RocksDb key value storage factory.
   *
   * @param configuration the configuration
   * @param configuredSegments the segments
   * @param ignorableSegments the ignorable segments
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> configuredSegments,
      final List<SegmentIdentifier> ignorableSegments,
      final RocksDBMetricsFactory rocksDBMetricsFactory) {
    this.configuration = configuration;
    this.configuredSegments = configuredSegments;
    this.ignorableSegments = ignorableSegments;
    this.rocksDBMetricsFactory = rocksDBMetricsFactory;
  }

  /**
   * Instantiates a new RocksDb key value storage factory.
   *
   * @param configuration the configuration
   * @param configuredSegments the segments
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> configuredSegments,
      final RocksDBMetricsFactory rocksDBMetricsFactory) {
    this(configuration, configuredSegments, List.of(), rocksDBMetricsFactory);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public KeyValueStorage create(
      final SegmentIdentifier segment,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    return new SegmentedKeyValueStorageAdapter(
        segment, create(List.of(segment), commonConfiguration, metricsSystem));
  }

  @Override
  public SegmentedKeyValueStorage create(
      final List<SegmentIdentifier> segments,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    init(commonConfiguration);

    // safety check to see that segments all exist within configured segments
    if (!configuredSegments.containsAll(segments)) {
      throw new StorageException(
          "Attempted to create storage for segments that are not configured: "
              + segments.stream()
                  .filter(segment -> !configuredSegments.contains(segment))
                  .map(SegmentIdentifier::toString)
                  .collect(Collectors.joining(", ")));
    }

    final KeyValueStorageSegmentGroup group =
        KeyValueStorageSegmentGroup.forSegment(segments.get(0));
    for (final SegmentIdentifier segment : segments) {
      if (KeyValueStorageSegmentGroup.forSegment(segment) != group) {
        throw new StorageException(
            "All segments in one create() call must belong to the same RocksDB instance; mixing "
                + group
                + " with "
                + KeyValueStorageSegmentGroup.forSegment(segment)
                + " is not supported.");
      }
    }

    if (legacyMonolithicDatabase) {
      if (segmentedStorage == null) {
        segmentedStorage =
            openColumnarStorage(
                segmentsForDatabaseFormat(),
                storagePath(commonConfiguration),
                commonConfiguration,
                metricsSystem,
                false);
      }
      return segmentedStorage;
    }

    final Path groupDir =
        group == KeyValueStorageSegmentGroup.STATE
            ? storagePath(commonConfiguration).resolve(STATE_DATABASE_DIR)
            : storagePath(commonConfiguration).resolve(MAIN_DATABASE_DIR);
    return segmentedStorages.computeIfAbsent(
        group,
        g ->
            openColumnarStorage(
                segmentsForGroup(g),
                groupDir,
                commonConfiguration,
                metricsSystem,
                g == KeyValueStorageSegmentGroup.STATE));
  }

  private List<SegmentIdentifier> segmentsForDatabaseFormat() {
    return configuredSegments.stream()
        .filter(
            segmentId ->
                segmentId.includeInDatabaseFormat(
                    databaseMetadata.getVersionedStorageFormat().getFormat()))
        .toList();
  }

  private List<SegmentIdentifier> segmentsForGroup(final KeyValueStorageSegmentGroup group) {
    return configuredSegments.stream()
        .filter(
            segmentId ->
                segmentId.includeInDatabaseFormat(
                    databaseMetadata.getVersionedStorageFormat().getFormat()))
        .filter(segmentId -> KeyValueStorageSegmentGroup.forSegment(segmentId) == group)
        .toList();
  }

  private RocksDBColumnarKeyValueStorage openColumnarStorage(
      final List<SegmentIdentifier> segmentsForFormat,
      final Path databaseDir,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem,
      final boolean stateDatabase) {
    if (segmentsForFormat.isEmpty()) {
      throw new StorageException("No column families configured for database at " + databaseDir);
    }
    try {
      Files.createDirectories(databaseDir);
    } catch (final IOException e) {
      throw new StorageException("Could not create database directory " + databaseDir, e);
    }
    final RocksDBConfiguration rocksConfig =
        buildRocksConfiguration(databaseDir, commonConfiguration, stateDatabase);
    if (stateDatabase) {
      LOG.atInfo()
          .setMessage(
              "Opening dedicated world-state RocksDB at {} with max_open_files={} (default Besu is 1024 unless overridden by {})")
          .addArgument(databaseDir)
          .addArgument(rocksConfig.getMaxOpenFiles())
          .addArgument("--Xplugin-rocksdb-max-open-files")
          .log();
    } else if (!legacyMonolithicDatabase) {
      LOG.atInfo()
          .setMessage("Opening main RocksDB at {} with max_open_files={}")
          .addArgument(databaseDir)
          .addArgument(rocksConfig::getMaxOpenFiles)
          .log();
    }
    return switch (databaseMetadata.getVersionedStorageFormat().getFormat()) {
      case FOREST -> {
        LOG.debug("FOREST mode detected, using TransactionDB.");
        yield new TransactionDBRocksDBColumnarKeyValueStorage(
            rocksConfig,
            segmentsForFormat,
            ignorableSegments,
            metricsSystem,
            rocksDBMetricsFactory);
      }
      case BONSAI, X_BONSAI_ARCHIVE -> {
        LOG.debug("BONSAI mode detected, Using OptimisticTransactionDB.");
        yield new OptimisticRocksDBColumnarKeyValueStorage(
            rocksConfig,
            segmentsForFormat,
            ignorableSegments,
            metricsSystem,
            rocksDBMetricsFactory);
      }
    };
  }

  private RocksDBConfiguration buildRocksConfiguration(
      final Path databaseDir,
      final BesuConfiguration commonConfiguration,
      final boolean stateDatabase) {
    final RocksDBFactoryConfiguration factoryConfig = configuration.get();
    final int effectiveMaxOpen =
        factoryConfig.getMaxOpenFiles() > 0
            ? factoryConfig.getMaxOpenFiles()
            : DEFAULT_MAX_OPEN_FILES;
    var configBuilder = RocksDBConfigurationBuilder.from(factoryConfig).databaseDir(databaseDir);
    if (factoryConfig.getMaxOpenFiles() <= 0) {
      configBuilder.maxOpenFiles(effectiveMaxOpen);
    }
    configBuilder.label(stateDatabase ? "state" : "main");
    if (stateDatabase) {
      configBuilder.maxOpenFiles(stateDatabaseMaxOpenFiles(effectiveMaxOpen));
    }
    applyHistoryExpiryOptions(configBuilder, commonConfiguration);
    return configBuilder.build();
  }

  private static int stateDatabaseMaxOpenFiles(final int configuredMaxOpenFiles) {
    final int base = configuredMaxOpenFiles > 0 ? configuredMaxOpenFiles : DEFAULT_MAX_OPEN_FILES;
    final int scaled =
        Math.max(STATE_DATABASE_MAX_OPEN_FILES_MIN, base * STATE_DATABASE_MAX_OPEN_FILES_FACTOR);
    return Math.min(STATE_DATABASE_MAX_OPEN_FILES_CAP, scaled);
  }

  private void applyHistoryExpiryOptions(
      final RocksDBConfigurationBuilder configBuilder,
      final BesuConfiguration commonConfiguration) {
    if (commonConfiguration.getDataStorageConfiguration().isHistoryExpiryPruneEnabled()) {
      configBuilder.isBlockchainGarbageCollectionEnabled(true);
      final RocksDBFactoryConfiguration factoryConfig = configuration.get();
      final double blobGarbageCollectionAgeCutoff =
          factoryConfig.getBlobGarbageCollectionAgeCutoff().orElse(0.5);
      final double blobGarbageCollectionForceThreshold =
          factoryConfig.getBlobGarbageCollectionForceThreshold().orElse(0.1);
      configBuilder.blobGarbageCollectionAgeCutoff(Optional.of(blobGarbageCollectionAgeCutoff));
      configBuilder.blobGarbageCollectionForceThreshold(
          Optional.of(blobGarbageCollectionForceThreshold));
    }
  }

  /**
   * Storage path.
   *
   * @param commonConfiguration the common configuration
   * @return the path
   */
  protected Path storagePath(final BesuConfiguration commonConfiguration) {
    return commonConfiguration.getStoragePath();
  }

  private void init(final BesuConfiguration commonConfiguration) {
    if (metadataInitialized) {
      return;
    }
    try {
      databaseMetadata = readDatabaseMetadata(commonConfiguration);
    } catch (final IOException e) {
      final String message =
          "Failed to retrieve the RocksDB database meta version: "
              + e.getMessage()
              + " could not be found. You may not have the appropriate permission to access the item.";
      throw new StorageException(message, e);
    }
    final Path storageRoot = storagePath(commonConfiguration);
    legacyMonolithicDatabase = Files.isRegularFile(storageRoot.resolve("CURRENT"));
    if (legacyMonolithicDatabase) {
      LOG.info(
          "Legacy monolithic RocksDB layout detected at {}; state is not split to a subdirectory. "
              + "For split layout (database/{}/ and database/{}/), start from an empty data directory.",
          storageRoot,
          MAIN_DATABASE_DIR,
          STATE_DATABASE_DIR);
    }
    if (commonConfiguration.getDataStorageConfiguration().isHistoryExpiryPruneEnabled()) {
      final RocksDBFactoryConfiguration factoryConfig = configuration.get();
      final double blobGarbageCollectionAgeCutoff =
          factoryConfig.getBlobGarbageCollectionAgeCutoff().orElse(0.5);
      final double blobGarbageCollectionForceThreshold =
          factoryConfig.getBlobGarbageCollectionForceThreshold().orElse(0.1);
      LOG.atInfo()
          .setMessage("History expiry prune is enabled so setting {}; {}={}; {}={}")
          .addArgument(BLOB_BLOCKCHAIN_GARBAGE_COLLECTION_ENABLED)
          .addArgument(BLOB_GARBAGE_COLLECTION_AGE_CUTOFF)
          .addArgument(blobGarbageCollectionAgeCutoff)
          .addArgument(BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD)
          .addArgument(blobGarbageCollectionForceThreshold)
          .log();
    }
    metadataInitialized = true;
  }

  private DatabaseMetadata readDatabaseMetadata(final BesuConfiguration commonConfiguration)
      throws IOException {
    final Path dataDir = commonConfiguration.getDataPath();
    final boolean dataDirExists = dataDir.toFile().exists();
    final boolean databaseExists = commonConfiguration.getStoragePath().toFile().exists();
    final boolean metadataExists = DatabaseMetadata.isPresent(dataDir);
    DatabaseMetadata metadata;
    if (databaseExists && !metadataExists) {
      throw new StorageException(
          "Database exists but metadata file not found, without it there is no safe way to open the database");
    }
    if (metadataExists) {
      metadata = DatabaseMetadata.lookUpFrom(dataDir);

      if (!metadata
          .getVersionedStorageFormat()
          .getFormat()
          .equals(commonConfiguration.getDataStorageConfiguration().getDatabaseFormat())) {
        handleFormatMismatch(commonConfiguration, dataDir, metadata);
      }

      final var runtimeVersion =
          BaseVersionedStorageFormat.defaultForNewDB(
              commonConfiguration.getDataStorageConfiguration());

      if (metadata.getVersionedStorageFormat().getVersion() > runtimeVersion.getVersion()) {
        final var maybeDowngradedMetadata =
            handleVersionDowngrade(dataDir, metadata, runtimeVersion);
        if (maybeDowngradedMetadata.isPresent()) {
          metadata = maybeDowngradedMetadata.get();
          metadata.writeToDirectory(dataDir);
        }
      }

      if (metadata.getVersionedStorageFormat().getVersion() < runtimeVersion.getVersion()) {
        final var maybeUpgradedMetadata = handleVersionUpgrade(dataDir, metadata, runtimeVersion);
        if (maybeUpgradedMetadata.isPresent()) {
          metadata = maybeUpgradedMetadata.get();
          metadata.writeToDirectory(dataDir);
        }
      }

      LOG.info("Existing database at {}. Metadata {}. Processing WAL...", dataDir, metadata);
    } else {

      metadata = DatabaseMetadata.defaultForNewDb(commonConfiguration);
      LOG.info(
          "No existing database at {}. Using default metadata for new db {}", dataDir, metadata);
      if (!dataDirExists) {
        Files.createDirectories(dataDir);
      }
      metadata.writeToDirectory(dataDir);
    }

    if (!isSupportedVersionedFormat(metadata.getVersionedStorageFormat())) {
      final String message = "Unsupported RocksDB metadata: " + metadata;
      LOG.error(message);
      throw new StorageException(message);
    }

    return metadata;
  }

  private static void handleFormatMismatch(
      final BesuConfiguration commonConfiguration,
      final Path dataDir,
      final DatabaseMetadata existingMetadata) {
    String error =
        String.format(
            "Database format mismatch: DB at %s is %s but config expects %s. "
                + "Please check your config.",
            dataDir,
            existingMetadata.getVersionedStorageFormat().getFormat().name(),
            commonConfiguration.getDataStorageConfiguration().getDatabaseFormat());

    throw new StorageException(error);
  }

  private Optional<DatabaseMetadata> handleVersionDowngrade(
      final Path dataDir,
      final DatabaseMetadata existingMetadata,
      final BaseVersionedStorageFormat runtimeVersion) {
    // here we put the code, or the messages, to perform an automated, or manual, downgrade of the
    // database, if supported, otherwise we just prevent Besu from starting since it will not
    // recognize the newer version.
    // In case we do an automated downgrade, then we also need to update the metadata on disk to
    // reflect the change to the runtime version, and return it.

    // Besu supports both formats of receipts so no downgrade is needed
    if (runtimeVersion == BONSAI_WITH_VARIABLES || runtimeVersion == FOREST_WITH_VARIABLES) {
      LOG.warn(
          "Database contains compacted receipts but receipt compaction is not enabled, new receipts  will "
              + "be not stored in the compacted format. If you want to remove compacted receipts from the "
              + "database it is necessary to resync Besu. Besu can support both compacted and non-compacted receipts.");
      return Optional.empty();
    }

    // for the moment there are supported automated downgrades, so we just fail.
    String error =
        String.format(
            "Database unsafe downgrade detect: DB at %s is %s with version %s but version %s is expected. "
                + "Please check your config and review release notes for supported downgrade procedures.",
            dataDir,
            existingMetadata.getVersionedStorageFormat().getFormat().name(),
            existingMetadata.getVersionedStorageFormat().getVersion(),
            runtimeVersion.getVersion());

    throw new StorageException(error);
  }

  private Optional<DatabaseMetadata> handleVersionUpgrade(
      final Path dataDir,
      final DatabaseMetadata existingMetadata,
      final BaseVersionedStorageFormat runtimeVersion) {
    // here we put the code, or the messages, to perform an automated, or manual, upgrade of the
    // database.
    // In case we do an automated upgrade, then we also need to update the metadata on disk to
    // reflect the change to the runtime version, and return it.

    // Besu supports both formats of receipts so no upgrade is needed other than updating metadata
    final VersionedStorageFormat existingVersionedStorageFormat =
        existingMetadata.getVersionedStorageFormat();
    if ((existingVersionedStorageFormat == BONSAI_WITH_VARIABLES
            && runtimeVersion == BONSAI_WITH_RECEIPT_COMPACTION)
        || (existingVersionedStorageFormat == FOREST_WITH_VARIABLES
            && runtimeVersion == FOREST_WITH_RECEIPT_COMPACTION)) {
      final DatabaseMetadata metadata = new DatabaseMetadata(runtimeVersion);
      try {
        metadata.writeToDirectory(dataDir);
        return Optional.of(metadata);
      } catch (IOException e) {
        throw new StorageException("Database upgrade to use receipt compaction failed", e);
      }
    }

    // for the moment there are no planned automated upgrades, so we just fail.
    String error =
        String.format(
            "Database unsafe downgrade detect: DB at %s is %s with version %s but version %s is expected. "
                + "Please check your config and review release notes for supported downgrade procedures.",
            dataDir,
            existingVersionedStorageFormat.getFormat().name(),
            existingVersionedStorageFormat.getVersion(),
            runtimeVersion.getVersion());

    throw new StorageException(error);
  }

  private boolean isSupportedVersionedFormat(final VersionedStorageFormat versionedStorageFormat) {
    return SUPPORTED_VERSIONED_FORMATS.stream()
        .anyMatch(
            vsf ->
                vsf.getFormat().equals(versionedStorageFormat.getFormat())
                    && vsf.getVersion() == versionedStorageFormat.getVersion());
  }

  /** Resets the segmentedStorage for Ephemery automatic restart. */
  public void reset() {
    segmentedStorage = null;
    segmentedStorages.clear();
    metadataInitialized = false;
  }

  @Override
  public void close() throws IOException {
    if (segmentedStorage != null) {
      segmentedStorage.close();
    }
    for (final RocksDBColumnarKeyValueStorage storage : segmentedStorages.values()) {
      storage.close();
    }
    segmentedStorages.clear();
  }

  @Override
  public boolean isSegmentIsolationSupported() {
    return true;
  }

  @Override
  public boolean isSnapshotIsolationSupported() {
    return true;
  }
}
