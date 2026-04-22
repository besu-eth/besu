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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_CACHE_CAPACITY;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage.NearestKeyValue;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NearestKeyValueStorageTest {

  @TempDir private static Path tempDir;

  private static RocksDBKeyValueStorageFactory rocksdbStorageFactory;
  private static BesuConfiguration commonConfiguration;

  @BeforeAll
  public static void setup() throws IOException {
    rocksdbStorageFactory =
        new RocksDBKeyValueStorageFactory(
            () ->
                new RocksDBFactoryConfiguration(
                    DEFAULT_MAX_OPEN_FILES,
                    DEFAULT_BACKGROUND_THREAD_COUNT,
                    DEFAULT_CACHE_CAPACITY,
                    DEFAULT_IS_HIGH_SPEC,
                    DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS,
                    false,
                    Optional.empty(),
                    Optional.empty()),
            Arrays.asList(KeyValueSegmentIdentifier.values()),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);

    Utils.createDatabaseMetadataV2(tempDir, DataStorageFormat.BONSAI, 2);

    mockCommonConfiguration(tempDir);
  }

  @Test
  public void testNearestRocksdbWithInMemoryKeyValueStorage() {
    final SegmentedKeyValueStorage rockdDBKeyValueStorage =
        getRocksDBKeyValueStorage(TRIE_BRANCH_STORAGE);
    final SegmentedKeyValueStorageTransaction rocksDbTransaction =
        rockdDBKeyValueStorage.startTransaction();

    final SegmentedKeyValueStorage inMemoryDBKeyValueStorage = getInMemoryDBKeyValueStorage();
    final SegmentedKeyValueStorageTransaction inMemoryDBTransaction =
        inMemoryDBKeyValueStorage.startTransaction();
    IntStream.range(1, 10)
        .forEach(
            i -> {
              final byte[] key = Bytes.fromHexString("0x000" + i).toArrayUnsafe();
              final byte[] value = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key, value);
              inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key, value);
              // different common prefix, and reversed order of bytes:
              final byte[] key2 = Bytes.fromHexString("0x010" + (10 - i)).toArrayUnsafe();
              final byte[] value2 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key2, value2);
              inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key2, value2);
              // different size:
              final byte[] key3 = Bytes.fromHexString("0x01011" + (10 - i)).toArrayUnsafe();
              final byte[] value3 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key3, value3);
              inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key3, value3);
              final byte[] key4 = Bytes.fromHexString("0x0" + (10 - i)).toArrayUnsafe();
              final byte[] value4 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key4, value4);
              inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key4, value4);
            });
    rocksDbTransaction.commit();
    inMemoryDBTransaction.commit();

    // compare rocksdb implementation with inmemory implementation
    rockdDBKeyValueStorage.stream(TRIE_BRANCH_STORAGE)
        .forEach(
            pair -> {
              final Bytes key = Bytes.of(pair.getKey());
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, key),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, key)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, key),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, key)))
                  .isTrue();

              final Bytes biggerKey = Bytes.concatenate(key, Bytes.of(0x01));
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, biggerKey),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, biggerKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, biggerKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, biggerKey)))
                  .isTrue();

              final Bytes smallerKey = key.slice(0, key.size() - 1);
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, smallerKey),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, smallerKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestAfter(
                              TRIE_BRANCH_STORAGE, smallerKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, smallerKey)))
                  .isTrue();

              final Bytes reversedKey = key.reverse();
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, reversedKey),
                          rockdDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, reversedKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          inMemoryDBKeyValueStorage.getNearestAfter(
                              TRIE_BRANCH_STORAGE, reversedKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, reversedKey)))
                  .isTrue();
            });
  }

  @Test
  public void testNearestRocksdbWithLayeredKeyValueStorage() {
    final SegmentedKeyValueStorage rockdDBKeyValueStorage =
        getRocksDBKeyValueStorage(TRIE_BRANCH_STORAGE);
    final SegmentedKeyValueStorageTransaction rocksDbTransaction =
        rockdDBKeyValueStorage.startTransaction();

    final SegmentedKeyValueStorage inMemoryDBKeyValueStorage = getInMemoryDBKeyValueStorage();
    final SegmentedKeyValueStorageTransaction inMemoryDBTransaction =
        inMemoryDBKeyValueStorage.startTransaction();

    final LayeredKeyValueStorage layeredDBKeyValueStorage =
        new LayeredKeyValueStorage(inMemoryDBKeyValueStorage);
    final SegmentedKeyValueStorageTransaction layeredDBTransaction =
        layeredDBKeyValueStorage.startTransaction();

    IntStream.range(1, 10)
        .forEach(
            i -> {
              final byte[] key = Bytes.fromHexString("0x000" + i).toArrayUnsafe();
              final byte[] value = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key, value);
              // as we have several layers I store sometimes in the child layer and sometimes in the
              // parent
              if (i % 2 == 0) {
                inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key, value);
              } else {
                layeredDBTransaction.put(TRIE_BRANCH_STORAGE, key, value);
              }
              // different common prefix, and reversed order of bytes:
              final byte[] key2 = Bytes.fromHexString("0x010" + (10 - i)).toArrayUnsafe();
              final byte[] value2 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key2, value2);
              // as we have several layers I store sometimes in the child layer and sometimes in the
              // parent
              if (i % 2 == 0) {
                inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key2, value2);
              } else {
                layeredDBTransaction.put(TRIE_BRANCH_STORAGE, key2, value2);
              }
              // different size:
              final byte[] key3 = Bytes.fromHexString("0x01011" + (10 - i)).toArrayUnsafe();
              final byte[] value3 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key3, value3);
              // as we have several layers I store sometimes in the child layer and sometimes in the
              // parent
              if (i % 2 == 0) {
                inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key3, value3);
              } else {
                layeredDBTransaction.put(TRIE_BRANCH_STORAGE, key3, value3);
              }
              final byte[] key4 = Bytes.fromHexString("0x0" + (10 - i)).toArrayUnsafe();
              final byte[] value4 = Bytes.fromHexString("0FFF").toArrayUnsafe();
              rocksDbTransaction.put(TRIE_BRANCH_STORAGE, key4, value4);
              // as we have several layers I store sometimes in the child layer and sometimes in the
              // parent
              if (i % 2 == 0) {
                inMemoryDBTransaction.put(TRIE_BRANCH_STORAGE, key4, value4);
              } else {
                layeredDBTransaction.put(TRIE_BRANCH_STORAGE, key4, value4);
              }
            });
    rocksDbTransaction.commit();
    inMemoryDBTransaction.commit();
    layeredDBTransaction.commit();

    // compare rocksdb implementation with inmemory implementation
    rockdDBKeyValueStorage.stream(TRIE_BRANCH_STORAGE)
        .forEach(
            pair -> {
              final Bytes key = Bytes.of(pair.getKey());
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, key),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, key)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, key),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, key)))
                  .isTrue();

              final Bytes biggerKey = Bytes.concatenate(key, Bytes.of(0x01));
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, biggerKey),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, biggerKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, biggerKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, biggerKey)))
                  .isTrue();

              final Bytes smallerKey = key.slice(0, key.size() - 1);
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, smallerKey),
                          rockdDBKeyValueStorage.getNearestBefore(TRIE_BRANCH_STORAGE, smallerKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, smallerKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, smallerKey)))
                  .isTrue();

              final Bytes reversedKey = key.reverse();
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, reversedKey),
                          rockdDBKeyValueStorage.getNearestBefore(
                              TRIE_BRANCH_STORAGE, reversedKey)))
                  .isTrue();
              assertThat(
                      isNearestKeyValueTheSame(
                          layeredDBKeyValueStorage.getNearestAfter(
                              TRIE_BRANCH_STORAGE, reversedKey),
                          rockdDBKeyValueStorage.getNearestAfter(TRIE_BRANCH_STORAGE, reversedKey)))
                  .isTrue();
            });
  }

  private SegmentedKeyValueStorage getRocksDBKeyValueStorage(final SegmentIdentifier segment) {
    return rocksdbStorageFactory.create(
        List.of(segment), commonConfiguration, new NoOpMetricsSystem());
  }

  private SegmentedKeyValueStorage getInMemoryDBKeyValueStorage() {
    return new SegmentedInMemoryKeyValueStorage();
  }

  private static void mockCommonConfiguration(final Path tempDataDir) {
    commonConfiguration = mock(BesuConfiguration.class);
    when(commonConfiguration.getStoragePath()).thenReturn(tempDataDir);
    when(commonConfiguration.getDataPath()).thenReturn(tempDataDir);
    DataStorageConfiguration dataStorageConfiguration = mock(DataStorageConfiguration.class);
    when(dataStorageConfiguration.getDataStorageFormat()).thenReturn(DataStorageFormat.BONSAI);
    lenient()
        .when(commonConfiguration.getDataStorageConfiguration())
        .thenReturn(dataStorageConfiguration);
  }

  private boolean isNearestKeyValueTheSame(
      final Optional<NearestKeyValue> v1, final Optional<NearestKeyValue> v2) {
    if (v1.isPresent() && v2.isPresent()) {
      final NearestKeyValue nearestKeyValue1 = v1.get();
      final NearestKeyValue nearestKeyValue2 = v2.get();
      if (nearestKeyValue1.key().equals(nearestKeyValue2.key())) {
        if (nearestKeyValue1.value().isPresent() && nearestKeyValue2.value().isPresent()) {
          return Arrays.equals(nearestKeyValue1.value().get(), nearestKeyValue2.value().get());
        }
      } else if (nearestKeyValue1.value().isEmpty() && nearestKeyValue2.value().isEmpty()) {
        return true;
      }
    } else if (v1.isEmpty() && v2.isEmpty()) {
      return true;
    }
    return false;
  }
}
