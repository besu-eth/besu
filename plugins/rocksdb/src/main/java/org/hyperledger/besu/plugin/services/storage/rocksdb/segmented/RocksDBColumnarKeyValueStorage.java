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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BLOCKCHAIN;

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbIterator;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbNativeOptionStrings;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbUtil;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Splitter;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.AbstractRocksIterator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.Status;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RocksDb columnar key value storage. */
public abstract class RocksDBColumnarKeyValueStorage implements SegmentedKeyValueStorage {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBColumnarKeyValueStorage.class);
  private static final int ROCKSDB_FORMAT_VERSION = 5;
  private static final long ROCKSDB_BLOCK_SIZE = 32768;

  /** RocksDb blockcache size when using the high spec option */
  protected static final long ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC = 1_073_741_824L;

  /** Max total size of all WAL file, after which a flush is triggered */
  protected static final long WAL_MAX_TOTAL_SIZE = 1_073_741_824L;

  /** Expected size of a single WAL file, to determine how many WAL files to keep around */
  protected static final long EXPECTED_WAL_FILE_SIZE = 67_108_864L;

  /** RocksDb number of log files to keep on disk */
  private static final long NUMBER_OF_LOG_FILES_TO_KEEP = 7;

  /** RocksDb Time to roll a log file (1 day = 3600 * 24 seconds) */
  private static final long TIME_TO_ROLL_LOG_FILE = 86_400L;

  /**
   * Keys set by {@link #mergeBesuNativeColumnFamilyOptionsBeforeParse}; user CLI cannot override.
   */
  private static final Set<String> BESU_MERGED_NATIVE_COLUMN_FAMILY_KEYS =
      Set.of(
          "block_based_table_factory.index_type",
          "block_based_table_factory.format_version",
          "block_based_table_factory.filter_policy",
          "block_based_table_factory.partition_filters",
          "block_based_table_factory.cache_index_and_filter_blocks",
          "block_based_table_factory.block_size",
          "block_based_table_factory.block_cache");

  static {
    RocksDbUtil.loadNativeLibrary();
  }

  /** atomic boolean to track if the storage is closed */
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  private final WriteOptions tryDeleteOptions =
      new WriteOptions().setNoSlowdown(true).setIgnoreMissingColumnFamilies(true);
  private final ReadOptions readOptions = new ReadOptions().setVerifyChecksums(false);
  private final MetricsSystem metricsSystem;
  private final RocksDBMetricsFactory rocksDBMetricsFactory;

  /** RocksDB DB configuration */
  protected final RocksDBConfiguration configuration;

  /** RocksDB DB options */
  protected DBOptions options;

  /** RocksDb transactionDB options */
  protected TransactionDBOptions txOptions;

  /** RocksDb statistics */
  protected final Statistics stats = new Statistics();

  /** RocksDB metrics */
  protected RocksDBMetrics metrics;

  /** Map of the columns handles by name */
  protected Map<SegmentIdentifier, RocksDbSegmentIdentifier> columnHandlesBySegmentIdentifier;

  /** Column descriptors */
  protected List<ColumnFamilyDescriptor> columnDescriptors;

  /** Column handles */
  protected List<ColumnFamilyHandle> columnHandles;

  /** Trimmed segments */
  protected List<SegmentIdentifier> trimmedSegments;

  /**
   * Instantiates a new Rocks db columnar key value storage.
   *
   * @param configuration the configuration
   * @param defaultSegments the segments
   * @param ignorableSegments the ignorable segments
   * @param metricsSystem the metrics system
   * @param rocksDBMetricsFactory the rocks db metrics factory
   * @throws StorageException the storage exception
   */
  public RocksDBColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> defaultSegments,
      final List<SegmentIdentifier> ignorableSegments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {

    this.configuration = configuration;
    this.metricsSystem = metricsSystem;
    this.rocksDBMetricsFactory = rocksDBMetricsFactory;

    try {
      trimmedSegments = new ArrayList<>(defaultSegments);
      final List<byte[]> existingColumnFamilies =
          RocksDB.listColumnFamilies(new Options(), configuration.getDatabaseDir().toString());
      // Only ignore if not existed currently
      ignorableSegments.stream()
          .filter(
              ignorableSegment ->
                  existingColumnFamilies.stream()
                      .noneMatch(existed -> Arrays.equals(existed, ignorableSegment.getId())))
          .forEach(trimmedSegments::remove);
      columnDescriptors =
          trimmedSegments.stream()
              .map(segment -> createColumnDescriptor(segment, configuration))
              .collect(Collectors.toList());

      setGlobalOptions(configuration, stats);

      txOptions = new TransactionDBOptions();
      columnHandles = new ArrayList<>(columnDescriptors.size());
    } catch (RocksDBException e) {
      throw parseRocksDBException(e, defaultSegments, ignorableSegments);
    }
  }

  /**
   * Create a Column Family Descriptor for a given segment It defines basically the different
   * options to apply to the corresponding Column Family
   *
   * <p>Additional CF options from configuration are parsed into a scratch {@link Properties}; Besu
   * then builds {@link RocksDbNativeOptionStrings.InsertionOrderedProperties} by applying its
   * block-table keys in a fixed order (so {@code index_type} precedes {@code partition_filters} in
   * the JNI option string), then copies remaining user keys. A single {@code
   * getColumnFamilyOptionsFromProps} call follows, which unlocks any column-family or {@code
   * block_based_table_factory.*} option the native RocksDB build accepts, even when rocksdbjni does
   * not expose it on Java option classes. Compaction and blob options are still set in Java where
   * needed. {@code level_compaction_dynamic_level_bytes} is taken from the latest on-disk {@code
   * OPTIONS-*} file when present for this column family (existing deployments); otherwise it
   * defaults to {@code true}.
   *
   * @param segment the segment identifier
   * @param configuration RocksDB configuration
   * @return a column family descriptor
   */
  private ColumnFamilyDescriptor createColumnDescriptor(
      final SegmentIdentifier segment, final RocksDBConfiguration configuration) {
    final boolean dynamicLevelCompaction =
        readLevelCompactionDynamicLevelBytesFromOptionsFile(segment, configuration);

    final Properties userCfProps =
        RocksDbNativeOptionStrings.parseSemicolonKeyValueString(
            configuration.getAdditionalColumnFamilyOptions().orElse(""));
    final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps =
        new RocksDbNativeOptionStrings.InsertionOrderedProperties();
    mergeBesuNativeColumnFamilyOptionsBeforeParse(cfProps, segment, configuration);
    final List<String> userKeys = new ArrayList<>(userCfProps.stringPropertyNames());
    Collections.sort(userKeys);
    for (final String key : userKeys) {
      if (!BESU_MERGED_NATIVE_COLUMN_FAMILY_KEYS.contains(key)) {
        cfProps.setProperty(key, userCfProps.getProperty(key));
      }
    }

    final ColumnFamilyOptions columnOpts = columnFamilyOptionsFromNativeProperties(cfProps);

    columnOpts.setLevelCompactionDynamicLevelBytes(dynamicLevelCompaction);
    if (segment.containsStaticData()) {
      configureBlobDBForSegment(segment, configuration, columnOpts);
    }
    return new ColumnFamilyDescriptor(segment.getId(), columnOpts);
  }

  /**
   * Writes Besu's block-table defaults onto {@code cfProps} in this call order so the JNI option
   * string has {@code index_type=kTwoLevelIndexSearch} before {@code partition_filters=true}
   * (partitioned filters need a two-level index when options are applied incrementally in native
   * code). Must run on an empty {@link RocksDbNativeOptionStrings.InsertionOrderedProperties}
   * before user keys are added.
   */
  private static void mergeBesuNativeColumnFamilyOptionsBeforeParse(
      final RocksDbNativeOptionStrings.InsertionOrderedProperties cfProps,
      final SegmentIdentifier segment,
      final RocksDBConfiguration configuration) {
    final long blockCacheBytes =
        configuration.isHighSpec() && segment.isEligibleToHighSpecFlag()
            ? ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC
            : configuration.getCacheCapacity();
    cfProps.setProperty(
        "block_based_table_factory.format_version", Integer.toString(ROCKSDB_FORMAT_VERSION));
    cfProps.setProperty("block_based_table_factory.filter_policy", "bloomfilter:10:false");
    cfProps.setProperty("block_based_table_factory.partition_filters", "false");
    cfProps.setProperty("block_based_table_factory.cache_index_and_filter_blocks", "false");
    cfProps.setProperty("block_based_table_factory.block_size", Long.toString(ROCKSDB_BLOCK_SIZE));
    cfProps.setProperty("block_based_table_factory.block_cache", Long.toString(blockCacheBytes));
  }

  /**
   * RocksDB stores column-family options in an {@code OPTIONS-*} file under the data directory. On
   * reopen, reading the on-disk value avoids overriding {@code
   * level_compaction_dynamic_level_bytes} and changing compaction behaviour for existing databases.
   * New databases have no file yet; we default to {@code true} (historical Besu default).
   */
  private boolean readLevelCompactionDynamicLevelBytesFromOptionsFile(
      final SegmentIdentifier segment, final RocksDBConfiguration configuration) {
    try {
      final ConfigOptions configOptions = new ConfigOptions();
      final DBOptions dbOptions = new DBOptions();
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

      final String latestOptionsFileName =
          OptionsUtil.getLatestOptionsFileName(
              configuration.getDatabaseDir().toString(), Env.getDefault());
      LOG.trace("Latest OPTIONS file detected: {}", latestOptionsFileName);

      final String optionsFilePath =
          configuration.getDatabaseDir().toString() + "/" + latestOptionsFileName;
      OptionsUtil.loadOptionsFromFile(configOptions, optionsFilePath, dbOptions, cfDescriptors);
      LOG.trace("RocksDB options loaded successfully from: {}", optionsFilePath);

      for (ColumnFamilyDescriptor descriptor : cfDescriptors) {
        if (Arrays.equals(descriptor.getName(), segment.getId())) {
          final boolean value = descriptor.getOptions().levelCompactionDynamicLevelBytes();
          LOG.trace("dynamicLevelBytes from existing DB options file: {}", value);
          return value;
        }
      }
    } catch (final RocksDBException ex) {
      // New database: no OPTIONS-* file to load yet.
    }
    return true;
  }

  /**
   * Always builds from {@code getColumnFamilyOptionsFromProps} (including when the properties map
   * is empty), then applies Besu's fixed {@code ttl=0} and LZ4 compression on top so they are never
   * omitted. On null or parse failure, starts from a bare {@link ColumnFamilyOptions}.
   */
  private static ColumnFamilyOptions columnFamilyOptionsFromNativeProperties(
      final Properties nativeCfProps) {
    ColumnFamilyOptions base;
    try {
      final ColumnFamilyOptions fromNative =
          ColumnFamilyOptions.getColumnFamilyOptionsFromProps(new ConfigOptions(), nativeCfProps);
      if (fromNative != null) {
        base = fromNative;
      } else {
        if (!nativeCfProps.isEmpty()) {
          LOG.warn(
              "RocksDB getColumnFamilyOptionsFromProps returned null; using bare ColumnFamilyOptions. Check {} for invalid native keys.",
              RocksDBCLIOptions.ADDITIONAL_COLUMN_FAMILY_OPTIONS);
        }
        base = new ColumnFamilyOptions();
      }
    } catch (final IllegalArgumentException ex) {
      LOG.warn(
          "Invalid RocksDB column-family options passed to getColumnFamilyOptionsFromProps; using bare ColumnFamilyOptions. {}",
          ex.getMessage());
      base = new ColumnFamilyOptions();
    }
    return base.setTtl(0).setCompressionType(CompressionType.LZ4_COMPRESSION);
  }

  private static void configureBlobDBForSegment(
      final SegmentIdentifier segment,
      final RocksDBConfiguration configuration,
      final ColumnFamilyOptions options) {
    options
        .setEnableBlobFiles(true)
        .setEnableBlobGarbageCollection(
            isStaticDataGarbageCollectionEnabled(segment, configuration))
        .setMinBlobSize(100)
        .setBlobCompressionType(CompressionType.LZ4_COMPRESSION);
    if (configuration.getBlobGarbageCollectionAgeCutoff().isPresent()) {
      // fraction of file age to be considered eligible for GC;
      // 0.25 = oldest 25% of files eligible;
      // 1 = all files eligible
      options.setBlobGarbageCollectionAgeCutoff(
          configuration.getBlobGarbageCollectionAgeCutoff().get());
    }
    if (configuration.getBlobGarbageCollectionForceThreshold().isPresent()) {
      // fraction of garbage in eligible blob files to trigger GC;
      // 1 = trigger when eligible file contains 100% garbage;
      // 0 = trigger for all eligible files;
      options.setBlobGarbageCollectionForceThreshold(
          configuration.getBlobGarbageCollectionForceThreshold().get());
    }
  }

  private static boolean isStaticDataGarbageCollectionEnabled(
      final SegmentIdentifier segment, final RocksDBConfiguration configuration) {
    if (BLOCKCHAIN.getName().equals(segment.getName())
        && configuration.isBlockchainGarbageCollectionEnabled()) {
      return true;
    } else {
      return segment.isStaticDataGarbageCollectionEnabled();
    }
  }

  /***
   * Set Global options (DBOptions)
   *
   * @param configuration RocksDB configuration
   * @param stats The statistics object
   */
  private void setGlobalOptions(final RocksDBConfiguration configuration, final Statistics stats) {
    final Properties dbProps =
        RocksDbNativeOptionStrings.parseDbOptionString(
            configuration.getAdditionalDatabaseOptions().orElse(""));
    final ConfigOptions cfgOpts = new ConfigOptions();
    DBOptions dbOpts;
    if (dbProps.isEmpty()) {
      dbOpts = new DBOptions();
    } else {
      try {
        dbOpts = DBOptions.getDBOptionsFromProps(cfgOpts, dbProps);
      } catch (final IllegalArgumentException ex) {
        LOG.warn(
            "Invalid RocksDB DB options passed to getDBOptionsFromProps; using programmatic DB defaults. {}",
            ex.getMessage());
        dbOpts = null;
      }
      if (dbOpts == null) {
        LOG.warn(
            "RocksDB getDBOptionsFromProps returned null; using programmatic DB defaults. Check {} for invalid native keys.",
            RocksDBCLIOptions.ADDITIONAL_DATABASE_OPTIONS);
        dbOpts = new DBOptions();
      }
    }
    options =
        dbOpts
            .setCreateIfMissing(true)
            .setMaxOpenFiles(configuration.getMaxOpenFiles())
            .setStatistics(stats)
            .setCreateMissingColumnFamilies(true)
            .setLogFileTimeToRoll(TIME_TO_ROLL_LOG_FILE)
            .setKeepLogFileNum(NUMBER_OF_LOG_FILES_TO_KEEP)
            .setEnv(Env.getDefault().setBackgroundThreads(configuration.getBackgroundThreadCount()))
            .setMaxTotalWalSize(WAL_MAX_TOTAL_SIZE)
            .setRecycleLogFileNum(WAL_MAX_TOTAL_SIZE / EXPECTED_WAL_FILE_SIZE);
  }

  /**
   * Parse RocksDBException and wrap in StorageException
   *
   * @param ex RocksDBException
   * @param defaultSegments segments requested to open
   * @param ignorableSegments segments which are ignorable if not present
   * @return StorageException wrapping the RocksDB Exception
   */
  protected static StorageException parseRocksDBException(
      final RocksDBException ex,
      final List<SegmentIdentifier> defaultSegments,
      final List<SegmentIdentifier> ignorableSegments) {
    String message = ex.getMessage();
    List<SegmentIdentifier> knownSegments =
        Streams.concat(defaultSegments.stream(), ignorableSegments.stream()).distinct().toList();

    // parse out unprintable segment names for a more useful exception:
    String columnExceptionMessagePrefix = "Column families not opened: ";
    if (message.contains(columnExceptionMessagePrefix)) {
      String substring = message.substring(message.indexOf(": ") + 2);

      List<String> unHandledSegments = new ArrayList<>();
      Splitter.on(", ")
          .splitToStream(substring)
          .forEach(
              part -> {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                unHandledSegments.add(
                    knownSegments.stream()
                        .filter(seg -> Arrays.equals(seg.getId(), bytes))
                        .findFirst()
                        .map(seg -> new SegmentRecord(seg.getName(), seg.getId()))
                        .orElse(new SegmentRecord(part, bytes))
                        .forDisplay());
              });

      return new StorageException(
          "RocksDBException: Unhandled column families: ["
              + unHandledSegments.stream().collect(Collectors.joining(", "))
              + "]");
    } else {
      return new StorageException(ex);
    }
  }

  void initMetrics() {
    metrics = rocksDBMetricsFactory.create(metricsSystem, configuration, getDB(), stats);
  }

  void initColumnHandles() throws RocksDBException {
    // will not include the DEFAULT columnHandle, we do not use it:
    columnHandlesBySegmentIdentifier =
        trimmedSegments.stream()
            .collect(
                Collectors.toMap(
                    segmentId -> segmentId,
                    segment -> {
                      var columnHandle =
                          columnHandles.stream()
                              .filter(
                                  ch -> {
                                    try {
                                      return Arrays.equals(ch.getName(), segment.getId());
                                    } catch (RocksDBException e) {
                                      throw new RuntimeException(e);
                                    }
                                  })
                              .findFirst()
                              .orElseThrow(
                                  () ->
                                      new RuntimeException(
                                          "Column handle not found for segment "
                                              + segment.getName()));
                      return new RocksDbSegmentIdentifier(getDB(), columnHandle);
                    }));
  }

  /**
   * Safe method to map segment identifier to column handle.
   *
   * @param segment segment identifier
   * @return column handle
   */
  protected ColumnFamilyHandle safeColumnHandle(final SegmentIdentifier segment) {
    RocksDbSegmentIdentifier safeRef = columnHandlesBySegmentIdentifier.get(segment);
    if (safeRef == null) {
      throw new RuntimeException("Column handle not found for segment " + segment.getName());
    }
    return safeRef.get();
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();

    try (final OperationTimer.TimingContext ignored = metrics.getReadLatency().startTimer()) {
      return Optional.ofNullable(getDB().get(safeColumnHandle(segment), readOptions, key));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {

    try (final RocksIterator rocksIterator =
        getDB().newIterator(safeColumnHandle(segmentIdentifier))) {
      rocksIterator.seekForPrev(key.toArrayUnsafe());
      return Optional.of(rocksIterator)
          .filter(AbstractRocksIterator::isValid)
          .map(it -> new NearestKeyValue(Bytes.of(it.key()), Optional.of(it.value())));
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(
      final SegmentIdentifier segmentIdentifier, final Bytes key) throws StorageException {

    try (final RocksIterator rocksIterator =
        getDB().newIterator(safeColumnHandle(segmentIdentifier))) {
      rocksIterator.seek(key.toArrayUnsafe());
      return Optional.of(rocksIterator)
          .filter(AbstractRocksIterator::isValid)
          .map(it -> new NearestKeyValue(Bytes.of(it.key()), Optional.of(it.value())));
    }
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    final RocksIterator rocksIterator = getDB().newIterator(safeColumnHandle(segmentIdentifier));
    rocksIterator.seekToFirst();
    return RocksDbIterator.create(rocksIterator).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    final RocksIterator rocksIterator = getDB().newIterator(safeColumnHandle(segmentIdentifier));
    rocksIterator.seek(startKey);
    return RocksDbIterator.create(rocksIterator).toStream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    final Bytes endKeyBytes = Bytes.wrap(endKey);
    final RocksIterator rocksIterator = getDB().newIterator(safeColumnHandle(segmentIdentifier));
    rocksIterator.seek(startKey);
    return RocksDbIterator.create(rocksIterator)
        .toStream()
        .takeWhile(e -> endKeyBytes.compareTo(Bytes.wrap(e.getKey())) >= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    final RocksIterator rocksIterator = getDB().newIterator(safeColumnHandle(segmentIdentifier));
    rocksIterator.seekToFirst();
    return RocksDbIterator.create(rocksIterator).toStreamKeys();
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    try {
      getDB().delete(safeColumnHandle(segmentIdentifier), tryDeleteOptions, key);
      return true;
    } catch (RocksDBException e) {
      if (e.getStatus().getCode() == Status.Code.Incomplete) {
        return false;
      } else {
        throw new StorageException(e);
      }
    }
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return stream(segmentIdentifier)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getKey)
        .collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return stream(segmentIdentifier)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    Optional.ofNullable(columnHandlesBySegmentIdentifier.get(segmentIdentifier))
        .ifPresent(RocksDbSegmentIdentifier::reset);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      txOptions.close();
      options.close();
      tryDeleteOptions.close();
      columnHandlesBySegmentIdentifier.values().stream()
          .map(RocksDbSegmentIdentifier::get)
          .forEach(ColumnFamilyHandle::close);
      getDB().close();
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDbKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }

  abstract RocksDB getDB();

  record SegmentRecord(String name, byte[] id) {
    public String forDisplay() {
      return String.format("'%s'(%s)", name, Bytes.of(id).toHexString());
    }
  }
}
