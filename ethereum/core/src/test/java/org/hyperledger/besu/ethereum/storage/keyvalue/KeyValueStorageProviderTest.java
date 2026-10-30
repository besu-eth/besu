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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KeyValueStorageProviderTest {

  @Test
  void closeClosesEveryStorageInstance() throws IOException {
    final SegmentedKeyValueStorage variables = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorage blockchain = mock(SegmentedKeyValueStorage.class);
    final KeyValueStorageProvider provider =
        new KeyValueStorageProvider(
            segments ->
                segments.contains(KeyValueSegmentIdentifier.VARIABLES) ? variables : blockchain,
            mock(KeyValueStorage.class),
            new NoOpMetricsSystem());
    provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.VARIABLES);
    provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.BLOCKCHAIN);

    provider.close();

    verify(variables).close();
    verify(blockchain).close();
  }

  @Test
  void closeContinuesAfterAStorageFailsToClose() throws IOException {
    final SegmentedKeyValueStorage failing = mock(SegmentedKeyValueStorage.class);
    final SegmentedKeyValueStorage other = mock(SegmentedKeyValueStorage.class);
    doThrow(new IOException("boom")).when(failing).close();
    final KeyValueStorageProvider provider =
        new KeyValueStorageProvider(
            segments -> segments.contains(KeyValueSegmentIdentifier.VARIABLES) ? failing : other,
            mock(KeyValueStorage.class),
            new NoOpMetricsSystem());
    provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.VARIABLES);
    provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.BLOCKCHAIN);

    provider.close();

    verify(failing).close();
    verify(other).close();
  }

  @Test
  void closeReleasesRealRocksDbSoItCanBeReopened(@TempDir final Path dataDir) throws IOException {
    final BesuConfiguration besuConfiguration = mock(BesuConfiguration.class);
    final DataStorageConfiguration dataStorageConfiguration = mock(DataStorageConfiguration.class);
    when(besuConfiguration.getDataPath()).thenReturn(dataDir);
    when(besuConfiguration.getStoragePath()).thenReturn(dataDir.resolve("database"));
    when(besuConfiguration.getDataStorageConfiguration()).thenReturn(dataStorageConfiguration);
    when(dataStorageConfiguration.getDatabaseFormat()).thenReturn(DataStorageFormat.BONSAI);
    final List<SegmentIdentifier> segments = List.of(KeyValueSegmentIdentifier.values());
    final RocksDBFactoryConfiguration rocksDbConfiguration =
        mock(RocksDBFactoryConfiguration.class);
    final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();

    final RocksDBKeyValueStorageFactory factory =
        new RocksDBKeyValueStorageFactory(
            () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    final AtomicReference<SegmentedKeyValueStorage> opened = new AtomicReference<>();
    final KeyValueStorageProvider provider =
        new KeyValueStorageProvider(
            requested -> {
              final SegmentedKeyValueStorage storage =
                  factory.create(requested, besuConfiguration, metricsSystem);
              opened.set(storage);
              return storage;
            },
            mock(KeyValueStorage.class),
            metricsSystem);
    provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.VARIABLES);

    provider.close();

    assertThat(opened.get().isClosed()).isTrue();
    // RocksDB holds an exclusive LOCK on the directory until it is closed, so reopening in the
    // same process only succeeds if the provider really closed the database.
    try (final SegmentedKeyValueStorage reopened =
        new RocksDBKeyValueStorageFactory(
                () -> rocksDbConfiguration, segments, RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)
            .create(segments, besuConfiguration, metricsSystem)) {
      assertThat(reopened.isClosed()).isFalse();
    }
  }
}
