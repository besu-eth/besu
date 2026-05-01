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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

public class BonsaiFullFlatDbStrategy extends BonsaiFlatDbStrategy {

  protected final Counter getAccountNotFoundInFlatDatabasePersistedCounter;
  protected final Counter getAccountNotFoundInFlatDatabaseLayeredCounter;

  protected final Counter getStorageValueNotFoundInFlatDatabasePersistedCounter;
  protected final Counter getStorageValueNotFoundInFlatDatabaseLayeredCounter;

  public BonsaiFullFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);

    getAccountNotFoundInFlatDatabasePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_missing_flat_database_persisted",
            "Number of accounts not found in flat database from persisted storage (FCU)");

    getAccountNotFoundInFlatDatabaseLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_missing_flat_database_layered",
            "Number of accounts not found in flat database from layered storage (newPayload)");

    getStorageValueNotFoundInFlatDatabasePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_missing_flat_database_persisted",
            "Number of storage slots not found in flat database from persisted storage (FCU)");

    getStorageValueNotFoundInFlatDatabaseLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_missing_flat_database_layered",
            "Number of storage slots not found in flat database from layered storage (newPayload)");
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage,
      final boolean isLayered) {
    (isLayered ? getAccountLayeredCounter : getAccountPersistedCounter).inc();
    final Optional<Bytes> accountFound =
        storage.get(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
    if (accountFound.isPresent()) {
      (isLayered
              ? getAccountFoundInFlatDatabaseLayeredCounter
              : getAccountFoundInFlatDatabasePersistedCounter)
          .inc();
    } else {
      (isLayered
              ? getAccountNotFoundInFlatDatabaseLayeredCounter
              : getAccountNotFoundInFlatDatabasePersistedCounter)
          .inc();
    }
    return accountFound;
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage,
      final boolean isLayered) {
    (isLayered ? getStorageValueLayeredCounter : getStorageValuePersistedCounter).inc();
    final Optional<Bytes> storageFound =
        storage
            .get(
                ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes())
                    .toArrayUnsafe())
            .map(Bytes::wrap);
    if (storageFound.isPresent()) {
      (isLayered
              ? getStorageValueFlatDatabaseLayeredCounter
              : getStorageValueFlatDatabasePersistedCounter)
          .inc();
    } else {
      (isLayered
              ? getStorageValueNotFoundInFlatDatabaseLayeredCounter
              : getStorageValueNotFoundInFlatDatabasePersistedCounter)
          .inc();
    }

    return storageFound;
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    // NOOP
    // not need to reset anything in full mode
  }
}
