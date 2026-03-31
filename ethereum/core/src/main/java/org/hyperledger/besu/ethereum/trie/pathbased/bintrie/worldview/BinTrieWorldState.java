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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINTRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.hasher.StemHasher;
import org.hyperledger.besu.ethereum.trie.bintrie.BinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.LeafBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage.BinTrieUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * World state implementation using Binary Trie structure.
 *
 * <p>Unlike Bonsai which uses separate tries for accounts and storage, BinTrie uses a unified trie
 * with account and storage data embedded.
 */
public class BinTrieWorldState extends PathBasedWorldState {

  private static final Logger LOG = LoggerFactory.getLogger(BinTrieWorldState.class);

  private final CodeCache codeCache;
  private final TrieKeyFactory trieKeyFactory;

  public BinTrieWorldState(
      final BinTrieWorldStateProvider archive,
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedWorldStorageManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig,
        codeCache);
  }

  public BinTrieWorldState(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.setAccumulator(new BinTrieWorldStateUpdateAccumulator(this, evmConfiguration, codeCache));
    this.codeCache = codeCache;
    this.trieKeyFactory = new TrieKeyFactory(new StemHasher());
  }

  public BinTrieWorldState(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.codeCache = codeCache;
    this.trieKeyFactory = new TrieKeyFactory(new StemHasher());
  }

  @Override
  public BinTrieWorldStateKeyValueStorage getWorldStateStorage() {
    return (BinTrieWorldStateKeyValueStorage) worldStateKeyValueStorage;
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage()
        .getStorageValueByStorageSlotKey(address, storageSlotKey)
        .map(UInt256::fromBytes);
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(storageKey))
        .orElse(UInt256.ZERO);
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    return this;
  }

  @Override
  public Hash frontierRootHash() {
    return calculateRootHash(Optional.empty(), accumulator.copy());
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    return Collections.emptyMap();
  }

  @Override
  public void persist(final BlockHeader blockHeader) {
    LOG.atDebug()
        .setMessage("Persist world state for block {}")
        .addArgument(() -> blockHeader)
        .log();

    boolean success = false;
    final PathBasedWorldStateKeyValueStorage.Updater stateUpdater =
        worldStateKeyValueStorage.updater();
    Runnable saveTrieLog = () -> {};

    try {
      final Hash calculatedRootHash;
      if (blockHeader == null) {
        // No block header: calculate root from current state unless storage is frozen
        calculatedRootHash =
            calculateRootHash(
                isStorageFrozen ? Optional.empty() : Optional.of(stateUpdater), accumulator);
      } else if (!worldStateConfig.isTrieDisabled()) {
        // Normal case: calculate root using the block header
        calculatedRootHash = unsafeRootHashUpdate(blockHeader, stateUpdater);
      } else {
        // Trie is disabled: fallback to block's root hash, assuming trusted context
        calculatedRootHash = unsafeRootHashUpdate(blockHeader, stateUpdater);
      }

      if (blockHeader != null) {
        verifyWorldStateRoot(calculatedRootHash, blockHeader);
        final PathBasedWorldStateUpdateAccumulator<?> localCopy = accumulator.copy();
        saveTrieLog =
            () -> {
              trieLogManager.saveTrieLog(localCopy, calculatedRootHash, blockHeader, this);
              if (!isStorageFrozen) {
                cachedWorldStorageManager.addCachedLayer(blockHeader, calculatedRootHash, this);
              }
            };
        stateUpdater.saveWorldState(
            blockHeader.getBlockHash().getBytes(),
            Bytes32.wrap(calculatedRootHash.getBytes()),
            Bytes.EMPTY);
        worldStateBlockHash = blockHeader.getBlockHash();
      } else {
        stateUpdater.saveWorldState(
            Hash.ZERO.getBytes(), Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()), Bytes.EMPTY);
        worldStateBlockHash = Hash.ZERO;
      }
      worldStateRootHash = calculatedRootHash;
      success = true;
    } finally {
      if (success) {
        stateUpdater.commit();
        accumulator.reset();
        saveTrieLog.run();
      } else {
        stateUpdater.rollback();
        accumulator.reset();
      }
    }
  }

  @Override
  public Hash calculateRootHash(
      final Optional<PathBasedWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    return internalCalculateRootHash(
        maybeStateUpdater.map(BinTrieUpdater.class::cast),
        (BinTrieWorldStateUpdateAccumulator) worldStateUpdater);
  }

  protected Hash internalCalculateRootHash(
      final Optional<BinTrieUpdater> maybeUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final BinaryTrie stateTrie =
        createTrie((location, hash) -> worldStateKeyValueStorage.getStateTrieNode(location));

    for (final Address address : collectAddressesToPersist(worldStateUpdater)) {
      applyAddressUpdates(address, stateTrie, maybeUpdater, worldStateUpdater);
    }

    maybeUpdater.ifPresent(
        updater ->
            stateTrie.commit(
                (location, hash, value) -> {
                  final var tx = updater.getWorldStateTransaction();
                  if (value == null) {
                    tx.remove(BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe());
                  } else {
                    tx.put(BINTRIE_BRANCH_STORAGE, location.toArrayUnsafe(), value.toArrayUnsafe());
                  }
                }));

    return Hash.wrap(stateTrie.getRootHash());
  }

  private Set<Address> collectAddressesToPersist(
      final PathBasedWorldStateUpdateAccumulator<?> accumulator) {
    final Set<Address> addresses = new HashSet<>();
    addresses.addAll(accumulator.getAccountsToUpdate().keySet());
    addresses.addAll(accumulator.getCodeToUpdate().keySet());
    addresses.addAll(accumulator.getStorageToClear());
    addresses.addAll(accumulator.getStorageToUpdate().keySet());
    return addresses;
  }

  private void applyAddressUpdates(
      final Address address,
      final BinaryTrie stateTrie,
      final Optional<BinTrieUpdater> maybeUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final LeafBuilder leafBuilder = new LeafBuilder(trieKeyFactory);

    final boolean hasCodeUpdate =
        applyCodeUpdates(address, leafBuilder, maybeUpdater, worldStateUpdater);
    final boolean hasStorageUpdate =
        applyStorageUpdates(address, leafBuilder, maybeUpdater, worldStateUpdater);

    applyAccountUpdates(
        address, leafBuilder, hasStorageUpdate, hasCodeUpdate, maybeUpdater, worldStateUpdater);

    commitLeafBuilderToTrie(address, leafBuilder, stateTrie, worldStateUpdater);
  }

  private boolean applyCodeUpdates(
      final Address address,
      final LeafBuilder leafBuilder,
      final Optional<BinTrieUpdater> maybeUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final PathBasedValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);

    if (codeUpdate == null || codeUpdate.isUnchanged()) {
      return false;
    }

    final Bytes priorCode = codeUpdate.getPrior();
    final Bytes updatedCode = codeUpdate.getUpdated();

    if (isEmpty(priorCode) && isEmpty(updatedCode)) {
      return false;
    }

    final Hash addressHash = address.addressHash();

    if (updatedCode == null) {
      leafBuilder.addCodeRemoval(address, priorCode);
      maybeUpdater.ifPresent(u -> u.removeCode(addressHash, Hash.hash(priorCode)));
      return true;
    }

    leafBuilder.addCodeUpdate(address, updatedCode, Hash.hash(updatedCode));
    if (updatedCode.isEmpty()) {
      maybeUpdater.ifPresent(u -> u.removeCode(addressHash));
    } else {
      maybeUpdater.ifPresent(u -> u.putCode(addressHash, Hash.hash(updatedCode), updatedCode));
    }
    return true;
  }

  private boolean applyStorageUpdates(
      final Address address,
      final LeafBuilder leafBuilder,
      final Optional<BinTrieUpdater> maybeUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates =
        worldStateUpdater.getStorageToUpdate().get(address);

    if (storageUpdates == null || storageUpdates.isEmpty()) {
      return false;
    }

    boolean hasUpdates = false;
    final Hash addressHash = address.addressHash();

    for (final var entry : storageUpdates.entrySet()) {
      final PathBasedValue<UInt256> storageData = entry.getValue();
      if (storageData.isUnchanged()) {
        continue;
      }

      hasUpdates = true;
      final StorageSlotKey slotKey = entry.getKey();
      final Hash slotHash = slotKey.getSlotHash();
      final UInt256 newValue = storageData.getUpdated();

      if (newValue == null) {
        leafBuilder.addStorageRemoval(address, slotKey);
        maybeUpdater.ifPresent(u -> u.removeStorageValueBySlotHash(addressHash, slotHash));
      } else {
        leafBuilder.addStorageUpdate(address, slotKey, newValue);
        maybeUpdater.ifPresent(u -> u.putStorageValueBySlotHash(addressHash, slotHash, newValue));
      }
    }
    return hasUpdates;
  }

  private void applyAccountUpdates(
      final Address address,
      final LeafBuilder leafBuilder,
      final boolean hasStorageUpdate,
      final boolean hasCodeUpdate,
      final Optional<BinTrieUpdater> maybeUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final PathBasedValue<BinTrieAccount> accountUpdate =
        worldStateUpdater.getAccountsToUpdate().get(address);

    // If account is null but we have storage/code updates, load existing account data
    if (accountUpdate == null) {
      if (hasStorageUpdate || hasCodeUpdate) {
        loadExistingAccountDataToTrie(address, leafBuilder);
      }
      return;
    }

    // Skip if account is unchanged and no storage/code updates
    if (accountUpdate.isUnchanged() && !hasStorageUpdate && !hasCodeUpdate) {
      return;
    }

    final Hash addressHash = address.addressHash();

    if (accountUpdate.getUpdated() == null) {
      leafBuilder.addAccountRemoval(address);
      leafBuilder.addCodeHashRemoval(address);
      maybeUpdater.ifPresent(u -> u.removeAccountInfoState(addressHash));
      return;
    }

    final BinTrieAccount updatedAccount = accountUpdate.getUpdated();
    final BinTrieAccount priorAccount = accountUpdate.getPrior();

    // Handle coupled code/account updates like the original
    handleCoupledCodeAccountUpdates(
        address, leafBuilder, priorAccount, updatedAccount, worldStateUpdater);

    leafBuilder.addAccountUpdate(address, updatedAccount.getNonce(), updatedAccount.getBalance());
    maybeUpdater.ifPresent(
        u -> u.putAccountInfoState(addressHash, updatedAccount.serializeAccount()));
  }

  private void handleCoupledCodeAccountUpdates(
      final Address address,
      final LeafBuilder leafBuilder,
      final BinTrieAccount priorAccount,
      final BinTrieAccount updatedAccount,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {
    // Creating new account adds codeHash and codeSize
    if (priorAccount == null) {
      leafBuilder.addCodeHashUpdate(address, updatedAccount.getCodeHash());
      updatedAccount.getCodeSize().ifPresent(size -> leafBuilder.addCodeSizeUpdate(address, size));
      return;
    }
    // If account exists, only update code size if current code exists
    worldStateUpdater
        .getCode(address, updatedAccount.getCodeHash())
        .ifPresent(code -> leafBuilder.addCodeSizeUpdate(address, code.size()));
  }

  private void loadExistingAccountDataToTrie(final Address address, final LeafBuilder leafBuilder) {
    getWorldStateStorage()
        .getAccount(address, this)
        .ifPresent(
            account -> {
              leafBuilder.addCodeHashUpdate(address, account.getCodeHash());
              account.getCodeSize().ifPresent(size -> leafBuilder.addCodeSizeUpdate(address, size));
              leafBuilder.addAccountUpdate(address, account.getNonce(), account.getBalance());
            });
  }

  private void commitLeafBuilderToTrie(
      final Address address,
      final LeafBuilder leafBuilder,
      final BinaryTrie stateTrie,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    leafBuilder.getKeysForRemoval().forEach(stateTrie::remove);
    leafBuilder.getNonStorageUpdates().forEach(stateTrie::put);

    final var storageToUpdate = worldStateUpdater.getStorageToUpdate().get(address);
    if (storageToUpdate == null) {
      return;
    }

    for (final var entry : leafBuilder.getStorageUpdates().entrySet()) {
      final StorageSlotKey slotKey = entry.getKey();
      final var pair = entry.getValue();
      final PathBasedValue<UInt256> storageUpdate = storageToUpdate.get(slotKey);

      if (storageUpdate != null) {
        stateTrie
            .put(pair.getFirst(), pair.getSecond())
            .ifPresentOrElse(
                bytes -> storageUpdate.setPrior(UInt256.fromBytes(bytes)),
                () -> storageUpdate.setPrior(null));
      }
    }
  }

  private static boolean isEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }

  protected BinaryTrie createTrie(final org.hyperledger.besu.ethereum.trie.NodeLoader nodeLoader) {
    return new BinaryTrie(nodeLoader);
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage().getAccount(address, this).orElse(null);
  }

  @Override
  public Optional<Bytes> getCode(@NonNull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.wrap(Bytes32.ZERO);
  }
}
