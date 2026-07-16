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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal.BonsaiBalWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

@SuppressWarnings("rawtypes")
public class BonsaiWorldState extends PathBasedWorldState {

  protected BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final CodeCache codeCache;
  private final EvmConfiguration evmConfiguration;

  public BonsaiWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedMerkleTrieLoader(),
        archive.getCachedWorldStorageManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig,
        codeCache);
  }

  public BonsaiWorldState(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.evmConfiguration = evmConfiguration;
    this.setAccumulator(
        new BonsaiWorldStateUpdateAccumulator(
            this,
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadAccount(
                    getWorldStateStorage(), worldStateRootHash, addr),
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadStorageSlot(
                    getWorldStateStorage(), addr, value),
            evmConfiguration,
            codeCache));
    this.codeCache = codeCache;
  }

  @Override
  public void applyBlockAccessListOverlay(final BlockAccessListOverlay blockAccessListOverlay) {
    setAccumulator(
        new BonsaiBalWorldStateUpdateAccumulator(
            this, evmConfiguration, codeCache, blockAccessListOverlay));
  }

  @Override
  public Optional<Bytes> getCode(@NotNull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage()
        .getAccount(address.addressHash())
        .map(bytes -> BonsaiAccount.fromRLP(accumulator, address, bytes, true, codeCache))
        .orElse(null);
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(storageKey))
        .orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage()
        .getStorageValueByStorageSlotKey(address.addressHash(), storageSlotKey)
        .map(UInt256::fromBytes);
  }

  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Supplier<Optional<Hash>> storageRootSupplier,
      final Address address,
      final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage()
        .getStorageValueByStorageSlotKey(storageRootSupplier, address.addressHash(), storageSlotKey)
        .map(UInt256::fromBytes);
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    final MerkleTrie<Bytes, Bytes> storageTrie = createStorageTrie(address.addressHash(), rootHash);
    return storageTrie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage = new BonsaiWorldStateLayerStorage(getWorldStateStorage());
    return this;
  }

  public void disableCacheMerkleTrieLoader() {
    this.bonsaiCachedMerkleTrieLoader = new NoopBonsaiCachedMerkleTrieLoader();
  }

  /** Default implementation hashes only; reference tests override to persist pre-images. */
  public Hash hashAndSavePreImage(final Bytes value) {
    return Hash.hash(value);
  }

  /** Account state trie rooted at the current world state root. */
  public MerkleTrie<Bytes, Bytes> createAccountStateTrie() {
    return createTrie(
        (location, hash) ->
            bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                getWorldStateStorage(), location, hash),
        Bytes32.wrap(worldStateRootHash.getBytes()));
  }

  /** Per-account storage trie rooted at {@code storageRoot}. */
  public MerkleTrie<Bytes, Bytes> createStorageTrie(
      final Hash accountHash, final Hash storageRoot) {
    return createTrie(
        (location, key) ->
            bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                getWorldStateStorage(), accountHash, location, key),
        Bytes32.wrap(storageRoot.getBytes()));
  }

  private MerkleTrie<Bytes, Bytes> createTrie(final NodeLoader nodeLoader, final Bytes32 rootHash) {
    if (worldStateConfig.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (worldStateConfig.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredMerklePatriciaTrie<>(
          nodeLoader, rootHash, Function.identity(), Function.identity());
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }
}
