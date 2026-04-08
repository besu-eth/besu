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

import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.services.kvstore.LimitedInMemoryKeyValueStorage;

import java.util.function.Supplier;

public class KeyValueStorageProviderBuilder {

  private static final long DEFAULT_WORLD_STATE_PRE_IMAGE_CACHE_SIZE = 5_000L;

  private KeyValueStorageFactory storageFactory;
  private BesuConfiguration commonConfiguration;
  private MetricsSystem metricsSystem;

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

  public KeyValueStorageProvider build() {
    checkNotNull(storageFactory, "Cannot build a storage provider without a storage factory.");
    checkNotNull(
        commonConfiguration,
        "Cannot build a storage provider without the plugin common configuration.");
    checkNotNull(metricsSystem, "Cannot build a storage provider without a metrics system.");

    final Supplier<KeyValueStorage> preimageStorageSupplier =
        commonConfiguration.getDataStorageConfiguration().getHashPreImageStorageEnabled()
            ? () ->
                storageFactory.create(
                    KeyValueSegmentIdentifier.HASH_PREIMAGE_STORE,
                    commonConfiguration,
                    metricsSystem)
            : () -> new LimitedInMemoryKeyValueStorage(DEFAULT_WORLD_STATE_PRE_IMAGE_CACHE_SIZE);
    ;

    return new KeyValueStorageProvider(
        segments -> storageFactory.create(segments, commonConfiguration, metricsSystem),
        preimageStorageSupplier,
        (ObservableMetricsSystem) metricsSystem);
  }
}
