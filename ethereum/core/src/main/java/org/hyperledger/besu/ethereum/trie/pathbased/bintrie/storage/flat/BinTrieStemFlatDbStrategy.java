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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINTRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequence;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequenceFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.factory.StoredNodeFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.hasher.StemHasher;
import org.hyperledger.besu.ethereum.stateless.bintrie.node.LeafNode;
import org.hyperledger.besu.ethereum.stateless.verkle.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Flat database strategy for BinTrie (STEM mode).
 *
 * <p>Unlike the FULL mode which stores account and storage data in separate flat structures, this
 * strategy accesses data directly from the trie via stems. This reduces data duplication by reading
 * directly from the binary trie branch storage.
 *
 * <p>Write operations are no-ops because data is written directly to the trie structure.
 */
public class BinTrieStemFlatDbStrategy extends FlatDbStrategy
    implements BinTrieFlatDbReaderStrategy {

  private static final TrieKeyFactory TRIE_KEY_FACTORY = new TrieKeyFactory(new StemHasher());
  private static final BytesBitSequenceFactory BIT_SEQ_FACTORY = new BytesBitSequenceFactory();
  private static final StoredNodeFactory<BytesBitSequence, Bytes> NODE_FACTORY =
      new StoredNodeFactory<>(
          (location, hash) -> Optional.empty(), BIT_SEQ_FACTORY, Function.identity());

  protected final Counter getAccountNotFoundCounter;
  protected final Counter getStorageValueNotFoundCounter;

  public BinTrieStemFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);

    getAccountNotFoundCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bintrie_stem_get_account_missing_flat_database",
            "Number of accounts not found in the BinTrie stem flat database");

    getStorageValueNotFoundCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bintrie_stem_get_storagevalue_missing_flat_database",
            "Number of storage slots not found in the BinTrie stem flat database");
  }

  @Override
  public Optional<BinTrieAccount> getFlatAccount(
      final Address address,
      final PathBasedWorldView context,
      final SegmentedKeyValueStorage storage) {
    getAccountCounter.inc();

    final Optional<BinTrieAccount> account =
        fetchLeafNodes(TRIE_KEY_FACTORY.getHeaderStem(address.getBytes()), storage)
            .flatMap(children -> decodeAccount(address, context, children));

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

    final BytesBitSequence stemId =
        TRIE_KEY_FACTORY.getStorageStem(
            address.getBytes(), storageSlotKey.getSlotKey().orElseThrow());

    final Optional<Bytes> value =
        fetchLeafNodes(stemId, storage)
            .flatMap(
                children -> {
                  final int idx =
                      TrieKeyUtils.getStorageKeySuffix(storageSlotKey.getSlotKey().orElseThrow())
                          .toInt();
                  return children.get(idx).value.map(Bytes32::wrap);
                });

    if (value.isPresent()) {
      getStorageValueFlatDatabaseCounter.inc();
    } else {
      getStorageValueNotFoundCounter.inc();
    }
    return value;
  }

  private Optional<List<LeafNode<BytesBitSequence, Bytes>>> fetchLeafNodes(
      final BytesBitSequence stemId, final SegmentedKeyValueStorage storage) {
    return storage
        .get(BINTRIE_BRANCH_STORAGE, stemId.encode())
        .map(Bytes::wrap)
        .map(bytes -> NODE_FACTORY.decodeStemNode(stemId, bytes).children);
  }

  private Optional<BinTrieAccount> decodeAccount(
      final Address address,
      final PathBasedWorldView context,
      final List<LeafNode<BytesBitSequence, Bytes>> children) {
    final Optional<Bytes> basicData = children.get(0).value;
    final Hash codeHash =
        children.get(1).value.map(Bytes32::wrap).map(Hash::wrap).orElse(Hash.EMPTY);

    return basicData
        .map(Bytes32::wrap)
        .map(
            encodedData ->
                BinTrieAccount.fromEncodedBasicData(
                    context, address, encodedData, codeHash, true, context.codeCache()));
  }

  @Override
  public void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue) {
    // NOOP - data is written directly to the trie
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {
    // NOOP - data is written directly to the trie
  }

  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    // NOOP - data is written directly to the trie
  }

  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    // NOOP - data is written directly to the trie
  }

  @Override
  public void clearAll(final SegmentedKeyValueStorage storage) {
    // NOOP - cannot clear flat db as we are using the trie directly
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
