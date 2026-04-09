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
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbUtil;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
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
   * @param segment the segment identifier
   * @param configuration RocksDB configuration
   * @return a column family descriptor
   */
  private ColumnFamilyDescriptor createColumnDescriptor(
      final SegmentIdentifier segment, final RocksDBConfiguration configuration) {
    boolean dynamicLevelBytes = true;
    try {
      ConfigOptions configOptions = new ConfigOptions();
      DBOptions dbOptions = new DBOptions();
      List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

      String latestOptionsFileName =
          OptionsUtil.getLatestOptionsFileName(
              configuration.getDatabaseDir().toString(), Env.getDefault());
      LOG.trace("Latest OPTIONS file detected: " + latestOptionsFileName);

      String optionsFilePath =
          configuration.getDatabaseDir().toString() + "/" + latestOptionsFileName;
      OptionsUtil.loadOptionsFromFile(configOptions, optionsFilePath, dbOptions, cfDescriptors);

      LOG.trace("RocksDB options loaded successfully from: " + optionsFilePath);

      if (!cfDescriptors.isEmpty()) {
        Optional<ColumnFamilyOptions> matchedCfOptions = Optional.empty();
        for (ColumnFamilyDescriptor descriptor : cfDescriptors) {
          if (Arrays.equals(descriptor.getName(), segment.getId())) {
            matchedCfOptions = Optional.of(descriptor.getOptions());
            break;
          }
        }
        if (matchedCfOptions.isPresent()) {
          dynamicLevelBytes = matchedCfOptions.get().levelCompactionDynamicLevelBytes();
          LOG.trace("dynamicLevelBytes is set to an existing value : " + dynamicLevelBytes);
        }
      }
    } catch (RocksDBException ex) {
      // Options file is not found in the database
    }
    BlockBasedTableConfig basedTableConfig = createBlockBasedTableConfig(segment, configuration);

    final var options =
        new ColumnFamilyOptions()
            .setTtl(0)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setTableFormatConfig(basedTableConfig)
            .setLevelCompactionDynamicLevelBytes(dynamicLevelBytes);
    if (segment.containsStaticData()) {
      configureBlobDBForSegment(segment, configuration, options);
    }

    return new ColumnFamilyDescriptor(segment.getId(), options);
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
   * Create a Block Base Table configuration for each segment, depending on the configuration in place
   * and the segment itself
   *
   * @param segment The segment related to the column family
   * @param config RocksDB configuration
   * @return Block Base Table configuration
   */
  private BlockBasedTableConfig createBlockBasedTableConfig(
      final SegmentIdentifier segment, final RocksDBConfiguration config) {
    final LRUCache cache =
        new LRUCache(
            config.isHighSpec() && segment.isEligibleToHighSpecFlag()
                ? ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC
                : config.getCacheCapacity());
    return new BlockBasedTableConfig()
        .setFormatVersion(ROCKSDB_FORMAT_VERSION)
        .setBlockCache(cache)
        .setFilterPolicy(new BloomFilter(10, false))
        .setPartitionFilters(true)
        .setCacheIndexAndFilterBlocks(false)
        .setBlockSize(ROCKSDB_BLOCK_SIZE);
  }

  /***
   * Set Global options (DBOptions)
   *
   * @param configuration RocksDB configuration
   * @param stats The statistics object
   */
  private void setGlobalOptions(final RocksDBConfiguration configuration, final Statistics stats) {
    options = new DBOptions();
    options
        .setCreateIfMissing(true)
        .setMaxOpenFiles(configuration.getMaxOpenFiles())
        .setStatistics(stats)
        // Disable RocksDB's periodic DumpStats() to LOG (default is often 600s). Besu already
        // exposes Statistics via Prometheus; the native dump path has been a source of JNI SIGSEGVs
        // on some versions/platforms under load.
        .setStatsDumpPeriodSec(0)
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
