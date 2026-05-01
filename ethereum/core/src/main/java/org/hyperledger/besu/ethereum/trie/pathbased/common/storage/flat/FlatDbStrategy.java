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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/**
 * This class represents a FlatDbReaderStrategy, which is responsible for reading and writing data
 * from flat databases. It implements various methods for storing and retrieving account data, code
 * data, and storage data from the corresponding KeyValueStorage.
 */
public abstract class FlatDbStrategy {
  protected final MetricsSystem metricsSystem;
  protected final Counter getAccountPersistedCounter;
  protected final Counter getAccountLayeredCounter;
  protected final Counter getAccountFoundInFlatDatabasePersistedCounter;
  protected final Counter getAccountFoundInFlatDatabaseLayeredCounter;

  protected final Counter getStorageValuePersistedCounter;
  protected final Counter getStorageValueLayeredCounter;
  protected final Counter getStorageValueFlatDatabasePersistedCounter;
  protected final Counter getStorageValueFlatDatabaseLayeredCounter;
  protected final CodeStorageStrategy codeStorageStrategy;

  public FlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    this.metricsSystem = metricsSystem;
    this.codeStorageStrategy = codeStorageStrategy;

    getAccountPersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_persisted_total",
            "Total number of calls to getAccount from persisted storage (FCU)");

    getAccountLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_layered_total",
            "Total number of calls to getAccount from layered storage (newPayload)");

    getAccountFoundInFlatDatabasePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_flat_database_persisted",
            "Number of accounts found in flat database from persisted storage (FCU)");

    getAccountFoundInFlatDatabaseLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_flat_database_layered",
            "Number of accounts found in flat database from layered storage (newPayload)");

    getStorageValuePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_persisted_total",
            "Total number of calls to getStorageValueBySlotHash from persisted storage (FCU)");

    getStorageValueLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_layered_total",
            "Total number of calls to getStorageValueBySlotHash from layered storage (newPayload)");

    getStorageValueFlatDatabasePersistedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_flat_database_persisted",
            "Number of storage slots found in flat database from persisted storage (FCU)");

    getStorageValueFlatDatabaseLayeredCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_flat_database_layered",
            "Number of storage slots found in flat database from layered storage (newPayload)");
  }

  public boolean isCodeByCodeHash() {
    return codeStorageStrategy instanceof CodeHashCodeStorageStrategy;
  }

  /*
   * Retrieves the code data for the given code hash and account hash.
   */
  public Optional<Bytes> getFlatCode(
      final Hash codeHash, final Hash accountHash, final SegmentedKeyValueStorage storage) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return codeStorageStrategy.getFlatCode(codeHash, accountHash, storage);
    }
  }

  /*
   * Removes code for the given account hash.
   */
  public void removeFlatCode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash codeHash) {
    codeStorageStrategy.removeFlatCode(storage, transaction, accountHash, codeHash);
  }

  /*
   * Puts the code data for the given code hash and account hash.
   */
  public void putFlatCode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash codeHash,
      final Bytes code) {
    codeStorageStrategy.putFlatCode(storage, transaction, accountHash, codeHash, code);
  }

  /*
   * Puts the account data for the given account hash, using the world state root hash supplier and node loader.
   */
  public abstract void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue);

  public abstract void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash);

  /*
   * Puts the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */
  public abstract void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue);

  /*
   * Removes the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */
  public abstract void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash);

  public abstract void clearAll(final SegmentedKeyValueStorage storage);

  public abstract void resetOnResync(final SegmentedKeyValueStorage storage);

  public NavigableMap<Bytes32, Bytes> streamAccountFlatDatabase(
      final SegmentedKeyValueStorage storage,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final long max) {

    return toNavigableMap(accountsToPairStream(storage, startKeyHash, endKeyHash).limit(max));
  }

  public NavigableMap<Bytes32, Bytes> streamAccountFlatDatabase(
      final SegmentedKeyValueStorage storage,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {

    return toNavigableMap(accountsToPairStream(storage, startKeyHash).takeWhile(takeWhile));
  }

  /** streams RLP encoded storage values using a specified stream limit. */
  public NavigableMap<Bytes32, Bytes> streamStorageFlatDatabase(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final long max) {

    return toNavigableMap(
        storageToPairStream(storage, accountHash, startKeyHash, endKeyHash, RLP::encodeValue)
            .limit(max));
  }

  /** streams raw storage Bytes using a specified predicate filter and value mapper. */
  public NavigableMap<Bytes32, Bytes> streamStorageFlatDatabase(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {

    return toNavigableMap(
        storageToPairStream(storage, accountHash, startKeyHash, endKeyHash, RLP::encodeValue)
            .takeWhile(takeWhile));
  }

  /** streams raw storage Bytes using a specified predicate filter and value mapper. */
  public NavigableMap<Bytes32, Bytes> streamStorageFlatDatabase(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return toNavigableMap(
        storageToPairStream(storage, accountHash, startKeyHash, RLP::encodeValue)
            .takeWhile(takeWhile));
  }

  protected abstract Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper);

  protected abstract Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper);

  protected abstract Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash);

  protected abstract Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash);

  private NavigableMap<Bytes32, Bytes> toNavigableMap(
      final Stream<Pair<Bytes32, Bytes>> pairStream) {
    final TreeMap<Bytes32, Bytes> collected =
        pairStream.collect(
            Collectors.toMap(
                Pair::getFirst,
                Pair::getSecond,
                (v1, v2) -> v1,
                () -> new TreeMap<>(Comparator.comparing(Bytes::toHexString))));
    pairStream.close();
    return collected;
  }
}
