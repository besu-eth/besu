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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.mainnet.slowblock.ReadMeteredSegmentedKeyValueStorage;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.LimitedInMemoryKeyValueStorage;

public class KeyValueStorageProviderBuilder {

  private static final long DEFAULT_WORLD_STATE_PRE_IMAGE_CACHE_SIZE = 5_000L;

  private KeyValueStorageFactory storageFactory;
  private BesuConfiguration commonConfiguration;
  private MetricsSystem metricsSystem;
  private boolean slowBlockTracingEnabled = false;

  public KeyValueStorageProviderBuilder withStorageFactory(
      final KeyValueStorageFactory storageFactory) {
    this.storageFactory = storageFactory;
    return this;
  }

  public KeyValueStorageProviderBuilder withCommonConfiguration(
      final BesuConfiguration commonConfiguration) {
    this.commonConfiguration = commonConfiguration;
    return this;
  }

  public KeyValueStorageProviderBuilder withMetricsSystem(final MetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;
    return this;
  }

  /**
   * Meters reads that cross the storage boundary, which slow-block metrics report as cache misses.
   * Storage is a process singleton with no per-block seam, so the decorator is installed once here
   * and attributes reads by thread; when tracing is off the storage is left undecorated.
   *
   * @param slowBlockTracingEnabled whether slow-block tracing is enabled
   * @return the builder
   */
  public KeyValueStorageProviderBuilder withSlowBlockTracingEnabled(
      final boolean slowBlockTracingEnabled) {
    this.slowBlockTracingEnabled = slowBlockTracingEnabled;
    return this;
  }

  public KeyValueStorageProvider build() {
    checkNotNull(storageFactory, "Cannot build a storage provider without a storage factory.");
    checkNotNull(
        commonConfiguration,
        "Cannot build a storage provider without the plugin common configuration.");
    checkNotNull(metricsSystem, "Cannot build a storage provider without a metrics system.");

    final KeyValueStorage worldStatePreImageStorage =
        new LimitedInMemoryKeyValueStorage(DEFAULT_WORLD_STATE_PRE_IMAGE_CACHE_SIZE);

    return new KeyValueStorageProvider(
        segments -> {
          final SegmentedKeyValueStorage storage =
              storageFactory.create(segments, commonConfiguration, metricsSystem);
          return slowBlockTracingEnabled
              ? new ReadMeteredSegmentedKeyValueStorage(storage)
              : storage;
        },
        worldStatePreImageStorage,
        (ObservableMetricsSystem) metricsSystem);
  }
}
