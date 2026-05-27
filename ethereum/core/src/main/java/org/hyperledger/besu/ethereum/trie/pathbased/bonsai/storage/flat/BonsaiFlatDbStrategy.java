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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * This class represents a FlatDbReaderStrategy, which is responsible for reading and writing data
 * from flat databases. It implements various methods for storing and retrieving account data, code
 * data, and storage data from the corresponding KeyValueStorage.
 */
public abstract class BonsaiFlatDbStrategy extends FlatDbStrategy {

  public BonsaiFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);
  }

  /*
   * Retrieves the account data for the given account hash, using the world state root hash supplier and node loader.
   */
  public abstract Optional<Bytes> getFlatAccount(
      Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      NodeLoader nodeLoader,
      Hash accountHash,
      SegmentedKeyValueStorage storage);

  /*
   * Retrieves the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */

  public abstract Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      Supplier<Optional<Hash>> storageRootSupplier,
      NodeLoader nodeLoader,
      Hash accountHash,
      StorageSlotKey storageSlotKey,
      SegmentedKeyValueStorage storageStorage);

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
    storage.clear(ACCOUNT_INFO_STATE);
    storage.clear(ACCOUNT_STORAGE_STORAGE);
    storage.clear(CODE_STORAGE);
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    storage.clear(ACCOUNT_INFO_STATE);
    storage.clear(ACCOUNT_STORAGE_STORAGE);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {

    return storage
        .streamFromKey(
            ACCOUNT_STORAGE_STORAGE,
            Bytes.concatenate(accountHash.getBytes(), startKeyHash).toArrayUnsafe())
        .takeWhile(
            pair -> Bytes.wrap(pair.getKey()).slice(0, Bytes32.SIZE).equals(accountHash.getBytes()))
        .map(
            pair ->
                new Pair<>(
                    Bytes32.wrap(Bytes.wrap(pair.getKey()).slice(Bytes32.SIZE)),
                    valueMapper.apply(Bytes.wrap(pair.getValue()).trimLeadingZeros())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {

    return storage
        .streamFromKey(
            ACCOUNT_STORAGE_STORAGE,
            Bytes.concatenate(accountHash.getBytes(), startKeyHash).toArrayUnsafe(),
            Bytes.concatenate(accountHash.getBytes(), endKeyHash).toArrayUnsafe())
        .map(
            pair ->
                new Pair<>(
                    Bytes32.wrap(Bytes.wrap(pair.getKey()).slice(Bytes32.SIZE)),
                    valueMapper.apply(Bytes.wrap(pair.getValue()).trimLeadingZeros())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    return storage
        .streamFromKey(ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe(), endKeyHash.toArrayUnsafe())
        .map(pair -> new Pair<>(Bytes32.wrap(pair.getKey()), Bytes.wrap(pair.getValue())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    return storage
        .streamFromKey(ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe())
        .map(pair -> new Pair<>(Bytes32.wrap(pair.getKey()), Bytes.wrap(pair.getValue())));
  }
}
