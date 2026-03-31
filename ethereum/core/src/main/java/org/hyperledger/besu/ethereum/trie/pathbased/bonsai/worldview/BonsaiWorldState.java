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

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.NoopBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;
import org.apache.tuweni.units.bigints.UInt256;

@SuppressWarnings("rawtypes")
public class BonsaiWorldState extends PathBasedWorldState {

  protected BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final CodeCache codeCache;

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
  public Optional<Bytes> getCode(@NotNull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
  }

  @Override
  public Hash calculateRootHash(
      final Optional<PathBasedWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    return applyUpdatesAndComputeRoot(
        maybeStateUpdater.map(BonsaiWorldStateKeyValueStorage.Updater.class::cast),
        (BonsaiWorldStateUpdateAccumulator) worldStateUpdater);
  }

  /**
   * Applies all pending state changes and computes the new state root hash.
   *
   * <p>The update proceeds in four ordered phases:
   *
   * <ol>
   *   <li>Clear storage for self-destructed accounts
   *   <li>Update per-account storage tries (must precede account updates to produce storage roots)
   *   <li>Update contract code
   *   <li>Update account trie and compute the new root hash
   * </ol>
   */
  private Hash applyUpdatesAndComputeRoot(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {

    // Phase 1 — clear storage for accounts that were self-destructed
    clearStorage(maybeStateUpdater, worldStateUpdater);

    // Phase 2 — update every account's storage trie (must happen before account updates
    //           because the storage root is embedded in the account node)
    final boolean canParallelize = maybeStateUpdater.isEmpty();
    Stream<Map.Entry<Address, StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>>>
        storageStream = worldStateUpdater.getStorageToUpdate().entrySet().stream();
    if (canParallelize) {
      storageStream = storageStream.parallel();
    }
    storageStream.forEach(
        entry -> updateAccountStorageState(maybeStateUpdater, worldStateUpdater, entry));

    // Phase 3 — persist contract code changes (also computes code hashes)
    updateCode(maybeStateUpdater, worldStateUpdater);

    // Phase 4 — update the account trie, commit, and return the new root hash
    return commitAccountTrieAndComputeRoot(maybeStateUpdater, worldStateUpdater);
  }

  private Hash commitAccountTrieAndComputeRoot(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {
    final MerkleTrie<Bytes, Bytes> accountTrie =
        createTrie(
            (location, hash) ->
                bonsaiCachedMerkleTrieLoader.getAccountStateTrieNode(
                    getWorldStateStorage(), location, hash),
            Bytes32.wrap(worldStateRootHash.getBytes()));

    updateTheAccounts(maybeStateUpdater, worldStateUpdater, accountTrie);

    maybeStateUpdater.ifPresent(
        bonsaiUpdater -> accountTrie.commit(bonsaiUpdater::putAccountStateTrieNode));
    return Hash.wrap(accountTrie.getRootHash());
  }

  private void updateTheAccounts(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
      final MerkleTrie<Bytes, Bytes> accountTrie) {
    for (final Map.Entry<Address, PathBasedValue<BonsaiAccount>> accountUpdate :
        worldStateUpdater.getAccountsToUpdate().entrySet()) {
      final Bytes accountKey = accountUpdate.getKey().getBytes();
      final PathBasedValue<BonsaiAccount> bonsaiValue = accountUpdate.getValue();
      final BonsaiAccount updatedAccount = bonsaiValue.getUpdated();
      try {
        if (updatedAccount == null) {
          final Hash addressHash = hashAndSavePreImage(accountKey);
          accountTrie.remove(addressHash.getBytes());
          maybeStateUpdater.ifPresent(
              bonsaiUpdater -> bonsaiUpdater.removeAccountInfoState(addressHash));
        } else {
          final Hash addressHash = updatedAccount.getAddressHash();
          final Bytes accountValue = updatedAccount.serializeAccount();
          maybeStateUpdater.ifPresent(
              bonsaiUpdater ->
                  bonsaiUpdater.putAccountInfoState(hashAndSavePreImage(accountKey), accountValue));
          accountTrie.put(addressHash.getBytes(), accountValue);
        }
      } catch (MerkleTrieException e) {
        // need to throw to trigger the heal
        throw new MerkleTrieException(
            e.getMessage(), Optional.of(Address.wrap(accountKey)), e.getHash(), e.getLocation());
      }
    }
  }

  @VisibleForTesting
  protected void updateCode(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {
    maybeStateUpdater.ifPresent(
        bonsaiUpdater -> {
          for (final Map.Entry<Address, PathBasedValue<Bytes>> codeUpdate :
              worldStateUpdater.getCodeToUpdate().entrySet()) {
            final Bytes updatedCode = codeUpdate.getValue().getUpdated();
            final Hash accountHash = codeUpdate.getKey().addressHash();
            final Bytes priorCode = codeUpdate.getValue().getPrior();

            // code hasn't changed then do nothing
            if (Objects.equals(priorCode, updatedCode)
                || (codeIsEmpty(priorCode) && codeIsEmpty(updatedCode))) {
              continue;
            }

            if (codeIsEmpty(updatedCode)) {
              final Hash priorCodeHash = Hash.hash(priorCode);
              bonsaiUpdater.removeCode(accountHash, priorCodeHash);
            } else {
              final Hash codeHash = Hash.hash(codeUpdate.getValue().getUpdated());
              bonsaiUpdater.putCode(accountHash, codeHash, updatedCode);
            }
          }
        });
  }

  private boolean codeIsEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }

  private void updateAccountStorageState(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
      final Map.Entry<Address, StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>>
          storageAccountUpdate) {
    final Address updatedAddress = storageAccountUpdate.getKey();
    final Hash updatedAddressHash = updatedAddress.addressHash();
    if (worldStateUpdater.getAccountsToUpdate().containsKey(updatedAddress)) {
      final PathBasedValue<BonsaiAccount> accountValue =
          worldStateUpdater.getAccountsToUpdate().get(updatedAddress);
      final BonsaiAccount accountOriginal = accountValue.getPrior();
      final Hash storageRoot =
          (accountOriginal == null
                  || worldStateUpdater.getStorageToClear().contains(updatedAddress))
              ? Hash.EMPTY_TRIE_HASH
              : accountOriginal.getStorageRoot();
      final MerkleTrie<Bytes, Bytes> storageTrie =
          createTrie(
              (location, key) ->
                  bonsaiCachedMerkleTrieLoader.getAccountStorageTrieNode(
                      getWorldStateStorage(), updatedAddressHash, location, key),
              Bytes32.wrap(storageRoot.getBytes()));

      // for manicured tries and composting, collect branches here (not implemented)
      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageUpdate :
          storageAccountUpdate.getValue().entrySet()) {
        final Hash slotHash = storageUpdate.getKey().getSlotHash();
        final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
        try {

          if (!storageUpdate.getValue().isUnchanged()) {
            if (updatedStorage == null || updatedStorage.equals(UInt256.ZERO)) {
              maybeStateUpdater.ifPresent(
                  bonsaiUpdater ->
                      bonsaiUpdater.removeStorageValueBySlotHash(updatedAddressHash, slotHash));
              storageTrie.remove(slotHash.getBytes());
            } else {
              maybeStateUpdater.ifPresent(
                  bonsaiUpdater ->
                      bonsaiUpdater.putStorageValueBySlotHash(
                          updatedAddressHash, slotHash, updatedStorage));
              storageTrie.put(slotHash.getBytes(), encodeTrieValue(updatedStorage));
            }
          }
        } catch (MerkleTrieException e) {
          // need to throw to trigger the heal
          throw new MerkleTrieException(
              e.getMessage(), Optional.of(updatedAddress), e.getHash(), e.getLocation());
        }
      }

      final BonsaiAccount accountUpdated = accountValue.getUpdated();
      if (accountUpdated != null) {
        maybeStateUpdater.ifPresent(
            bonsaiUpdater ->
                storageTrie.commit(
                    (location, nodeHash, value) ->
                        writeStorageTrieNode(
                            bonsaiUpdater, updatedAddressHash, location, nodeHash, value)));
        // only use storage root of the trie when trie is enabled
        if (!worldStateConfig.isTrieDisabled()) {
          final Hash newStorageRoot = Hash.wrap(storageTrie.getRootHash());
          accountUpdated.setStorageRoot(newStorageRoot);
        }
      }
    }
    // for manicured tries and composting, trim and compost here
  }

  private void clearStorage(
      final Optional<BonsaiWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {
    for (final Address address : worldStateUpdater.getStorageToClear()) {
      // because we are clearing persisted values we need the account root as persisted
      final BonsaiAccount oldAccount =
          getWorldStateStorage()
              .getAccount(address.addressHash())
              .map(
                  bytes ->
                      BonsaiAccount.fromRLP(BonsaiWorldState.this, address, bytes, true, codeCache))
              .orElse(null);
      if (oldAccount == null) {
        // This is when an account is both created and deleted within the scope of the same
        // block. A not-uncommon DeFi bot pattern.
        continue;
      }
      final Hash addressHash = address.addressHash();
      final MerkleTrie<Bytes, Bytes> storageTrie =
          createTrie(
              (location, key) -> getStorageTrieNode(addressHash, location, key),
              Bytes32.wrap(oldAccount.getStorageRoot().getBytes()));
      try {
        StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageToDelete = null;
        Bytes32 nextKeyHash = Bytes32.ZERO;
        while (true) {
          final Map<Bytes32, Bytes> entriesToDelete = storageTrie.entriesFrom(nextKeyHash, 256);
          if (entriesToDelete.isEmpty()) {
            break;
          }
          if (storageToDelete == null) {
            storageToDelete =
                worldStateUpdater
                    .getStorageToUpdate()
                    .computeIfAbsent(
                        address,
                        add ->
                            new StorageConsumingMap<>(
                                address,
                                new ConcurrentHashMap<>(),
                                worldStateUpdater.getStoragePreloader()));
          }
          Bytes32 lastKeyHash = null;
          for (Map.Entry<Bytes32, Bytes> slot : entriesToDelete.entrySet()) {
            final StorageSlotKey storageSlotKey =
                new StorageSlotKey(Hash.wrap(slot.getKey()), Optional.empty());
            final UInt256 slotValue =
                UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(slot.getValue())));
            maybeStateUpdater.ifPresent(
                bonsaiUpdater ->
                    bonsaiUpdater.removeStorageValueBySlotHash(
                        address.addressHash(), storageSlotKey.getSlotHash()));
            storageToDelete
                .computeIfAbsent(storageSlotKey, key -> new PathBasedValue<>(slotValue, null, true))
                .setPrior(slotValue);
            lastKeyHash = slot.getKey();
          }
          entriesToDelete.keySet().forEach(storageTrie::remove);
          if (entriesToDelete.size() < 256) {
            break;
          }
          // entriesFrom() is inclusive, so continue from the next lexicographic key;
          // reusing lastKeyHash would work only because the previous batch was already removed.
          final Optional<Bytes32> maybeNextKeyHash = incrementBytes32(lastKeyHash);
          if (maybeNextKeyHash.isEmpty()) {
            break;
          }
          nextKeyHash = maybeNextKeyHash.get();
        }
      } catch (MerkleTrieException e) {
        // need to throw to trigger the heal
        throw new MerkleTrieException(
            e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
      }
    }
  }

  static Optional<Bytes32> incrementBytes32(final Bytes32 value) {
    final UInt256 incremented = UInt256.fromBytes(value).add(UInt256.ONE);
    return incremented.isZero() ? Optional.empty() : Optional.of(incremented);
  }

  @Override
  public Hash frontierRootHash() {
    return calculateRootHash(
        Optional.of(
            new BonsaiWorldStateKeyValueStorage.Updater(
                noOpSegmentedTx,
                noOpTx,
                worldStateKeyValueStorage.getFlatDbStrategy(),
                worldStateKeyValueStorage.getComposedWorldStateStorage())),
        accumulator.copy());
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage()
        .getAccount(address.addressHash())
        .map(bytes -> BonsaiAccount.fromRLP(accumulator, address, bytes, true, codeCache))
        .orElse(null);
  }

  protected Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage().getAccountStateTrieNode(location, nodeHash);
  }

  protected Optional<Bytes> getStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return getWorldStateStorage().getAccountStorageTrieNode(accountHash, location, nodeHash);
  }

  private void writeStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes value) {
    stateUpdater.putAccountStorageTrieNode(accountHash, location, nodeHash, value);
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
    final MerkleTrie<Bytes, Bytes> storageTrie =
        createTrie(
            (location, key) -> getStorageTrieNode(address.addressHash(), location, key),
            Bytes32.wrap(rootHash.getBytes()));
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

  protected Hash hashAndSavePreImage(final Bytes value) {
    // by default do not save has preImages
    return Hash.hash(value);
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
