/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.apache.tuweni.rlp.RLP.decodeValue;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.AccountHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A layered Bonsai world state storage that intercepts trie-node reads and collects them for
 * EIP-8025 witness generation.
 *
 * <p>Wraps a parent {@link BonsaiWorldStateKeyValueStorage} and overrides account- and storage-trie
 * read methods to record every node fetched into an internal set. It also replaces the flat-DB
 * strategy with one that always traverses the trie (bypassing any warm cache) so that no accessed
 * node is silently omitted from the witness.
 *
 * <p>Used exclusively by {@link BonsaiExecutionWitnessBuilder}; not intended for normal block
 * processing.
 */
public class BonsaiWorldStateWitnessStorage extends BonsaiWorldStateLayerStorage {

  private final Set<Bytes> trieNodes;
  private final BonsaiFlatDbStrategy witnessFlatDbStrategy;

  /**
   * Creates a new witness storage layered on top of {@code parent}.
   *
   * @param metricsSystem metrics system passed through to the internal flat-DB strategy
   * @param parent the underlying Bonsai storage that supplies the actual trie node bytes
   */
  public BonsaiWorldStateWitnessStorage(
      final MetricsSystem metricsSystem, final BonsaiWorldStateKeyValueStorage parent) {
    super(parent);
    this.witnessFlatDbStrategy = buildWitnessFlatDbStrategy(metricsSystem, parent);
    this.trieNodes = ConcurrentHashMap.newKeySet();
  }

  private BonsaiFlatDbStrategy buildWitnessFlatDbStrategy(
      final MetricsSystem metricsSystem, final BonsaiWorldStateKeyValueStorage parent) {

    final boolean isCodeByCodeHash = parent.getFlatDbStrategy().isCodeByCodeHash();

    return new BonsaiFlatDbStrategy(
        metricsSystem,
        isCodeByCodeHash
            ? new CodeHashCodeStorageStrategy()
            : new AccountHashCodeStorageStrategy()) {

      @Override
      public Optional<Bytes> getFlatAccount(
          final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
          final NodeLoader nodeLoader,
          final Hash accountHash,
          final SegmentedKeyValueStorage storage) {

        return worldStateRootHashSupplier
            .get()
            .flatMap(
                rootHash ->
                    new StoredMerklePatriciaTrie<>(
                            new StoredNodeFactory<>(
                                nodeLoader, Function.identity(), Function.identity()),
                            Bytes32.wrap(rootHash))
                        .get(accountHash.getBytes()));
      }

      @Override
      public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
          final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
          final Supplier<Optional<Hash>> storageRootSupplier,
          final NodeLoader nodeLoader,
          final Hash accountHash,
          final StorageSlotKey storageSlotKey,
          final SegmentedKeyValueStorage storage) {

        final Optional<Hash> storageRoot = storageRootSupplier.get();
        final Optional<Bytes> worldStateRootHash = worldStateRootHashSupplier.get();

        if (storageRoot.isEmpty() || worldStateRootHash.isEmpty()) {
          return Optional.empty();
        }

        return new StoredMerklePatriciaTrie<>(
                new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity()),
                Bytes32.wrap(storageRoot.get().getBytes()))
            .get(storageSlotKey.getSlotHash().getBytes())
            .map(bytes -> Bytes32.leftPad(decodeValue(bytes)));
      }
    };
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    final Optional<Bytes> accountStateTrieNode = super.getAccountStateTrieNode(location, nodeHash);
    accountStateTrieNode.ifPresent(trieNodes::add);
    return accountStateTrieNode;
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    final Optional<Bytes> accountStorageTrieNode =
        super.getAccountStorageTrieNode(accountHash, location, nodeHash);
    accountStorageTrieNode.ifPresent(trieNodes::add);
    return accountStorageTrieNode;
  }

  /**
   * Bypass the parent's flat-DB cache so the witness flat-DB strategy always traverses the trie and
   * intercepts every node via {@link #getAccountStateTrieNode}. Without this override, a warm entry
   * in the inherited {@code VersionedCacheManager} returns the cached flat-DB value directly,
   * skipping the trie traversal and leaving those nodes out of the witness.
   */
  @Override
  public Optional<Bytes> getAccount(final Hash accountHash) {
    return witnessFlatDbStrategy.getFlatAccount(
        this::getWorldStateRootHash,
        this::getAccountStateTrieNode,
        accountHash,
        composedWorldStateStorage);
  }

  /**
   * Bypass the parent's flat-DB cache so the witness flat-DB strategy always traverses the storage
   * trie and intercepts every node via {@link #getAccountStorageTrieNode}. Without this override, a
   * warm entry in the inherited {@code VersionedCacheManager} returns the cached flat-DB value
   * directly, skipping the trie traversal and leaving those nodes out of the witness.
   */
  @Override
  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey) {
    return witnessFlatDbStrategy.getFlatStorageValueByStorageSlotKey(
        this::getWorldStateRootHash,
        storageRootSupplier,
        (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
        accountHash,
        storageSlotKey,
        composedWorldStateStorage);
  }

  @Override
  public BonsaiFlatDbStrategy getFlatDbStrategy() {
    return witnessFlatDbStrategy;
  }

  /**
   * Returns the set of raw trie node bytes intercepted during this storage session.
   *
   * @return all trie nodes read through this storage, as raw {@link Bytes}
   */
  public Set<Bytes> getTrieNodes() {
    return trieNodes;
  }
}
