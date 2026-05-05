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
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/**
 * This class represents a strategy for reading data from a partial flat database. It extends the
 * FlatDbReaderStrategy and provides additional functionality for reading data from a merkle trie.
 * If data is missing in the flat database, this strategy falls back to the merkle trie to retrieve
 * the data. It adds a fallback mechanism for the `getAccount` and `getStorageValueByStorageSlotKey`
 * methods, which checks if the data is present in the flat database, and if not, queries the merkle
 * trie
 */
public class BonsaiPartialFlatDbStrategy extends BonsaiFlatDbStrategy {

  protected final Counter getAccountMerkleTriePersistedCounter;
  protected final Counter getAccountMerkleTrieLayeredCounter;
  protected final Counter getAccountMissingMerkleTriePersistedCounter;
  protected final Counter getAccountMissingMerkleTrieLayeredCounter;

  protected final Counter getStorageValueMerkleTriePersistedCounter;
  protected final Counter getStorageValueMerkleTrieLayeredCounter;
  protected final Counter getStorageValueMissingMerkleTriePersistedCounter;
  protected final Counter getStorageValueMissingMerkleTrieLayeredCounter;

  public BonsaiPartialFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);
    getAccountMerkleTriePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_merkle_trie_persisted",
            "Accounts found in merkle trie from persisted storage (FCU)");

    getAccountMerkleTrieLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_merkle_trie_layered",
            "Accounts found in merkle trie from layered storage (newPayload)");

    getAccountMissingMerkleTriePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_missing_merkle_trie_persisted",
            "Accounts not found anywhere from persisted storage (FCU)");

    getAccountMissingMerkleTrieLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_missing_merkle_trie_layered",
            "Accounts not found anywhere from layered storage (newPayload)");

    getStorageValueMerkleTriePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_merkle_trie_persisted",
            "Storage slots found in merkle trie from persisted storage (FCU)");

    getStorageValueMerkleTrieLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_merkle_trie_layered",
            "Storage slots found in merkle trie from layered storage (newPayload)");

    getStorageValueMissingMerkleTriePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_missing_merkle_trie_persisted",
            "Storage slots not found anywhere from persisted storage (FCU)");

    getStorageValueMissingMerkleTrieLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_missing_merkle_trie_layered",
            "Storage slots not found anywhere from layered storage (newPayload)");
  }

  @Override
  public Optional<Bytes> getFlatAccount(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final SegmentedKeyValueStorage storage,
      final boolean isLayered) {
    (isLayered ? getAccountLayeredCounter : getAccountPersistedCounter).inc();
    Optional<Bytes> response =
        storage.get(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
    if (response.isEmpty()) {
      final Optional<Bytes> worldStateRootHash = worldStateRootHashSupplier.get();
      if (worldStateRootHash.isPresent()) {
        response =
            new StoredMerklePatriciaTrie<>(
                    new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity()),
                    Bytes32.wrap(worldStateRootHash.get()))
                .get(accountHash.getBytes());
        if (response.isEmpty()) {
          (isLayered
                  ? getAccountMissingMerkleTrieLayeredCounter
                  : getAccountMissingMerkleTriePersistedCounter)
              .inc();
        } else {
          (isLayered ? getAccountMerkleTrieLayeredCounter : getAccountMerkleTriePersistedCounter)
              .inc();
        }
      }
    } else {
      (isLayered
              ? getAccountFoundInFlatDatabaseLayeredCounter
              : getAccountFoundInFlatDatabasePersistedCounter)
          .inc();
    }

    return response;
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
    Optional<Bytes> response =
        storage
            .get(
                ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes())
                    .toArrayUnsafe())
            .map(Bytes::wrap);
    if (response.isEmpty()) {
      final Optional<Hash> storageRoot = storageRootSupplier.get();
      final Optional<Bytes> worldStateRootHash = worldStateRootHashSupplier.get();
      if (storageRoot.isPresent() && worldStateRootHash.isPresent()) {
        response =
            new StoredMerklePatriciaTrie<>(
                    new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity()),
                    Bytes32.wrap(storageRoot.get().getBytes()))
                .get(storageSlotKey.getSlotHash().getBytes())
                .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));
        if (response.isEmpty())
          (isLayered
                  ? getStorageValueMissingMerkleTrieLayeredCounter
                  : getStorageValueMissingMerkleTriePersistedCounter)
              .inc();
        else
          (isLayered
                  ? getStorageValueMerkleTrieLayeredCounter
                  : getStorageValueMerkleTriePersistedCounter)
              .inc();
      }
    } else {
      (isLayered
              ? getStorageValueFlatDatabaseLayeredCounter
              : getStorageValueFlatDatabasePersistedCounter)
          .inc();
    }
    return response;
  }
}
