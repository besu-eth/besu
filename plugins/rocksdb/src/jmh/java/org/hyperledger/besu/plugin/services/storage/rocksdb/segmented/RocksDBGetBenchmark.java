/*
 * Copyright contributors to Besu.
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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.DEFAULT;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SortedWriter;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Times {@link RocksDBColumnarKeyValueStorage#get} for keys that are present and keys that are
 * absent, through the same column family options a Bonsai node uses.
 *
 * <p>Before RocksDB 10.9, a {@code get()} of an absent key threw and caught a C++ exception in the
 * JNI layer (facebook/rocksdb#14095), which made a miss cost more than a hit. Run this before and
 * after a RocksDB upgrade to see how a change in RocksDB affects Besu's reads.
 *
 * <p>The data is written with a {@link SortedWriter}, which ingests it as SST files, so reads go
 * through the block cache and Bloom filters as on a synced node, not through the memtable. The
 * absent keys are spread over the same range as the present ones, and the Bloom filter rejects most
 * of them.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RocksDBGetBenchmark {

  private static final int KEY_SIZE = 32;
  private static final int VALUE_SIZE = 32;
  private static final int LOOKUPS = 1 << 20;

  @Param({"1000000"})
  public int keyCount;

  // the database and the sorted files written beside it
  private Path root;
  private RocksDBColumnarKeyValueStorage storage;
  private byte[][] presentKeys;
  private byte[][] absentKeys;
  private int index;

  @Setup
  public void setUp() throws IOException {
    root = Files.createTempDirectory("rocksdb-get-benchmark");
    storage =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(root.resolve("database")).build(),
            List.of(DEFAULT, ACCOUNT_STORAGE_STORAGE),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    // even keys are stored, odd keys are not, so the absent keys are spread over the same range
    final byte[][] stored = new byte[keyCount][];
    for (int i = 0; i < keyCount; i++) {
      stored[i] = key(2L * i);
    }
    Arrays.sort(stored, Arrays::compareUnsigned);
    final Random random = new Random(42);
    final byte[] value = new byte[VALUE_SIZE];
    final SortedWriter writer = storage.startSortedWriter(ACCOUNT_STORAGE_STORAGE);
    for (final byte[] key : stored) {
      random.nextBytes(value);
      writer.put(key, value.clone());
    }
    writer.commit();

    presentKeys = new byte[LOOKUPS][];
    absentKeys = new byte[LOOKUPS][];
    for (int i = 0; i < LOOKUPS; i++) {
      final long k = random.nextInt(keyCount);
      presentKeys[i] = key(2 * k);
      absentKeys[i] = key(2 * k + 1);
    }
    if (get(presentKeys).isEmpty() || get(absentKeys).isPresent()) {
      throw new IllegalStateException("benchmark data was not written as expected");
    }
  }

  @TearDown
  public void tearDown() throws IOException {
    storage.close();
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
    }
  }

  @Benchmark
  public Optional<byte[]> getPresent() {
    return get(presentKeys);
  }

  @Benchmark
  public Optional<byte[]> getAbsent() {
    return get(absentKeys);
  }

  private Optional<byte[]> get(final byte[][] keys) {
    index = (index + 1) & (LOOKUPS - 1);
    return storage.get(ACCOUNT_STORAGE_STORAGE, keys[index]);
  }

  // spreads the keys over the key space like hashes, keeping n in the low bytes so keys are unique
  private static byte[] key(final long n) {
    final byte[] key = new byte[KEY_SIZE];
    final long spread = n * 0x9E3779B97F4A7C15L;
    for (int i = 0; i < Long.BYTES; i++) {
      key[i] = (byte) (spread >>> (8 * i));
      key[KEY_SIZE - 1 - i] = (byte) (n >>> (8 * i));
    }
    return key;
  }
}
