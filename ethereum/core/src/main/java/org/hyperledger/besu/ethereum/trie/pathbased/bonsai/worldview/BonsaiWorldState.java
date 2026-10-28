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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia.PatriciaTrieFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal.BonsaiBalWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.PathBasedWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.CachingFrontierStorageRootTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.FrontierRootHashTracker;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.frontier.FrontierStorageRootTracker;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

@SuppressWarnings("rawtypes")
public class BonsaiWorldState extends PathBasedWorldState {

  protected BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final BonsaiCodeCache codeCache;
  private final EvmConfiguration evmConfiguration;
  private final FrontierRootHashTracker frontierRootHashTracker;

  public BonsaiWorldState(
      final BonsaiWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedMerkleTrieLoader(),
        archive.getWorldStateCacheManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig,
        codeCache);
  }

  public BonsaiWorldState(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final PathBasedWorldStateCacheManager worldStateCacheManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final BonsaiCodeCache codeCache) {
    super(worldStateKeyValueStorage, worldStateCacheManager, trieLogManager, worldStateConfig);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.evmConfiguration = evmConfiguration;
    final BonsaiWorldStateUpdateAccumulator acc =
        new BonsaiWorldStateUpdateAccumulator(
            this,
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadAccount(
                    getWorldStateStorage(), worldStateRootHash, addr),
            (addr, value) ->
                this.bonsaiCachedMerkleTrieLoader.preLoadStorageSlot(
                    getWorldStateStorage(), addr, value),
            evmConfiguration,
            codeCache);
    this.setAccumulator(acc);
    final FrontierStorageRootTracker frontierStorageRootTracker =
        worldStateConfig.isTrieDisabled()
            ? FrontierStorageRootTracker.NO_OP
            : new CachingFrontierStorageRootTracker(
                acc,
                (addressHash, baseRoot) ->
                    PatriciaTrieFactory.createFrontierTrie(
                        this,
                        (location, key) ->
                            bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                                getWorldStateStorage(), addressHash, location, key),
                        Bytes32.wrap(baseRoot.getBytes())));
    this.frontierRootHashTracker =
        new FrontierRootHashTracker(
            acc,
            rootHash ->
                PatriciaTrieFactory.createFrontierTrie(
                    this,
                    (location, hash) ->
                        bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                            getWorldStateStorage(), location, hash),
                    rootHash),
            frontierStorageRootTracker);
    // Keep frontier-derived caches aligned with accumulator resets.
    acc.setCommittedTransactionListener(frontierRootHashTracker);
    this.codeCache = codeCache;
  }

  @Override
  public void persist(final BlockHeader blockHeader, final StateRootCommitter committer) {
    frontierRootHashTracker.reset();
    super.persist(blockHeader, committer);
  }

  @Override
  public Hash frontierRootHash() {
    return frontierRootHashTracker.frontierRootHash(worldStateRootHash);
  }

  @Override
  public void applyBlockAccessListOverlay(final BlockAccessListOverlay blockAccessListOverlay) {
    setAccumulator(
        new BonsaiBalWorldStateUpdateAccumulator(
            this, evmConfiguration, codeCache, blockAccessListOverlay));
  }

  @Override
  public Optional<Code> getCode(@NotNull final Address address, final Hash codeHash) {
    final Optional<Code> code = getWorldStateStorage().getCode(codeHash, address.addressHash());
    if (code.isPresent()) {
      return code;
    }
    // a stored account always has its code, running empty code instead would fork
    if (isStoredCodeHash(address, codeHash)) {
      throw new MissingCodeException(address, codeHash);
    }
    return Optional.empty();
  }

  private boolean isStoredCodeHash(final Address address, final Hash codeHash) {
    final Account account = get(address);
    return account != null && account.getCodeHash().equals(codeHash);
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return worldStateKeyValueStorage;
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
    return PatriciaTrieFactory.getAllAccountStorage(this, address, rootHash);
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage = new BonsaiWorldStateLayerStorage(getWorldStateStorage());
    return this;
  }

  public BonsaiCachedMerkleTrieLoader getBonsaiCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  public void disableCacheMerkleTrieLoader() {
    this.bonsaiCachedMerkleTrieLoader = NoOpBonsaiCachedMerkleTrieLoader.INSTANCE;
  }

  public Hash hashAndSavePreImage(final Bytes value) {
    // by default do not save has preImages
    return Hash.hash(value);
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public BonsaiCodeCache codeCache() {
    return codeCache;
  }
}
