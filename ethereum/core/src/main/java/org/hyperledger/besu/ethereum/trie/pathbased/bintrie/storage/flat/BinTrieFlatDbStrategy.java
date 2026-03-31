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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Flat database strategy for BinTrie (FULL mode).
 *
 * <p>This strategy stores account and storage data in a flat key-value structure for efficient
 * access. Account data is keyed by address hash, and storage data is keyed by the concatenation of
 * account hash and slot hash.
 */
public class BinTrieFlatDbStrategy extends FlatDbStrategy implements BinTrieFlatDbReaderStrategy {

  protected final Counter getAccountNotFoundCounter;
  protected final Counter getStorageValueNotFoundCounter;

  public BinTrieFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);

    getAccountNotFoundCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bintrie_get_account_missing_flat_database",
            "Number of accounts not found in the BinTrie flat database");

    getStorageValueNotFoundCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bintrie_get_storagevalue_missing_flat_database",
            "Number of storage slots not found in the BinTrie flat database");
  }

  @Override
  public Optional<BinTrieAccount> getFlatAccount(
      final Address address,
      final PathBasedWorldView context,
      final SegmentedKeyValueStorage storage) {
    getAccountCounter.inc();

    final Optional<BinTrieAccount> account =
        storage
            .get(ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe())
            .map(Bytes::wrap)
            .map(encoded -> BinTrieAccount.fromRlp(context, address, encoded, true));

    if (account.isPresent()) {
      getAccountFoundInFlatDatabaseCounter.inc();
    } else {
      getAccountNotFoundCounter.inc();
    }
    return account;
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {
    getStorageValueCounter.inc();

    final Optional<Bytes> storageValue =
        storage
            .get(
                ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(
                        address.addressHash().getBytes(), storageSlotKey.getSlotHash().getBytes())
                    .toArrayUnsafe())
            .map(Bytes::wrap);

    if (storageValue.isPresent()) {
      getStorageValueFlatDatabaseCounter.inc();
    } else {
      getStorageValueNotFoundCounter.inc();
    }
    return storageValue;
  }

  @Override
  public void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue) {
    transaction.put(
        ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe(), accountValue.toArrayUnsafe());
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {
    transaction.remove(ACCOUNT_INFO_STATE, accountHash.getBytes().toArrayUnsafe());
  }

  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    transaction.put(
        ACCOUNT_STORAGE_STORAGE,
        Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe(),
        storageValue.toArrayUnsafe());
  }

  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    transaction.remove(
        ACCOUNT_STORAGE_STORAGE,
        Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe());
  }

  @Override
  public void clearAll(final SegmentedKeyValueStorage storage) {
    // NOOP - trie data is in BINTRIE_BRANCH_STORAGE
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    // NOOP
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    return Stream.empty();
  }
}
