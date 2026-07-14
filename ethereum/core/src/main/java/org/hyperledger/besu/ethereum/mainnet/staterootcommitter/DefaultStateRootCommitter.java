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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;
import org.apache.tuweni.units.bigints.UInt256;

/** Bonsai path-based root from accumulated block updates (no BAL background). */
public final class DefaultStateRootCommitter implements StateRootCommitter {

  public DefaultStateRootCommitter() {}

  @Override
  public StateRootComputation compute(
      final MutableWorldState mutableWorldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>) worldUpdater;
    final BonsaiWorldState bonsai = (BonsaiWorldState) mutableWorldState;
    final boolean storageFrozen = mutableWorldState.isStorageFrozen();
    final List<StateRootComputation.UpdaterWrite> writes = new ArrayList<>();
    final Hash root =
        new DefaultComputation(bonsai, (BonsaiWorldStateUpdateAccumulator) accumulator, storageFrozen)
            .executeInto(writes);
    if (blockHeader != null && bonsai.isTrieDisabled()) {
      return StateRootComputation.pathBased(blockHeader.getStateRoot(), writes);
    }
    return StateRootComputation.pathBased(root, writes);
  }

  private static final class DefaultComputation {

    private final BonsaiWorldState bonsai;
    private final BonsaiWorldStateUpdateAccumulator worldStateUpdater;
    private final boolean storageFrozen;

    /** Lock-free queue; storage futures and account staging may append concurrently. */
    private final ConcurrentLinkedQueue<StateRootComputation.UpdaterWrite> writes =
        new ConcurrentLinkedQueue<>();

    /**
     * Futures for storage-trie updates, keyed by address. Launched eagerly so storage I/O overlaps
     * with the sequential account trie staging loop.
     */
    private final Map<Address, CompletableFuture<Hash>> storageFutures = new ConcurrentHashMap<>();

    DefaultComputation(
        final BonsaiWorldState bonsai,
        final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
        final boolean storageFrozen) {
      this.bonsai = bonsai;
      this.worldStateUpdater = worldStateUpdater;
      this.storageFrozen = storageFrozen;
    }

    Hash executeInto(final List<StateRootComputation.UpdaterWrite> writeSink) {
      clearStorage();
      if (!storageFrozen) {
        collectCodeWrites();
      }

      final MerkleTrie<Bytes, Bytes> accountTrie = bonsai.createAccountStateTrie();

      // Step 1: launch storage trie updates concurrently for every touched account.
      for (final Map.Entry<Address, StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>>
          storageAccountUpdate : worldStateUpdater.getStorageToUpdate().entrySet()) {
        final Address address = storageAccountUpdate.getKey();
        if (worldStateUpdater.getAccountsToUpdate().containsKey(address)) {
          storageFutures.put(
              address,
              CompletableFuture.supplyAsync(
                  () -> updateStorageTrie(address, storageAccountUpdate.getValue())));
        }
      }

      // Step 2: stage account trie updates via putDeferred; join storage futures inside the callback.
      for (final Map.Entry<Address, PathBasedValue<BonsaiAccount>> accountUpdate :
          worldStateUpdater.getAccountsToUpdate().entrySet()) {
        final Address address = accountUpdate.getKey();
        final PathBasedValue<BonsaiAccount> accountValue = accountUpdate.getValue();
        final Hash addressHash = address.addressHash();
        try {
          accountTrie.putDeferred(
              addressHash.getBytes(),
              maybeRlp -> resolveAccount(address, addressHash, accountValue, maybeRlp));
        } catch (MerkleTrieException e) {
          throw new MerkleTrieException(
              e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
        }
      }

      if (!storageFrozen) {
        accountTrie.commit(
            (location, hash, value) ->
                writes.add(u -> u.putAccountStateTrieNode(location, hash, value)));
      }
      writeSink.addAll(writes);
      return Hash.wrap(accountTrie.getRootHash());
    }

    @SuppressWarnings("UnusedVariable")
    private Optional<Bytes> resolveAccount(
        final Address address,
        final Hash addressHash,
        final PathBasedValue<BonsaiAccount> accountValue,
        final Optional<Bytes> maybeRlp) {
      final BonsaiAccount updatedAccount = accountValue.getUpdated();
      final CompletableFuture<Hash> storageFuture = storageFutures.get(address);
      if (updatedAccount == null) {
        if (storageFuture != null) {
          storageFuture.join();
        }
        if (!storageFrozen) {
          writes.add(updater -> updater.removeAccountInfoState(addressHash));
        }
        // DeferredPutVisitor removes the account leaf when the merger returns empty.
        return Optional.empty();
      }

      if (storageFuture != null) {
        final Hash newStorageRoot = storageFuture.join();
        if (!bonsai.isTrieDisabled()) {
          updatedAccount.setStorageRoot(newStorageRoot);
        }
      }

      final Bytes accountValueBytes = updatedAccount.serializeAccount();
      if (!storageFrozen) {
        writes.add(updater -> updater.putAccountInfoState(addressHash, accountValueBytes));
      }
      return Optional.of(accountValueBytes);
    }

    private Hash updateStorageTrie(
        final Address updatedAddress,
        final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates) {
      final Hash updatedAddressHash = updatedAddress.addressHash();
      final BonsaiAccount accountOriginal =
          worldStateUpdater.getAccountsToUpdate().get(updatedAddress).getPrior();
      final Hash storageRoot =
          (accountOriginal == null
                  || worldStateUpdater.getStorageToClear().contains(updatedAddress))
              ? Hash.EMPTY_TRIE_HASH
              : accountOriginal.getStorageRoot();
      final MerkleTrie<Bytes, Bytes> storageTrie =
          bonsai.createTrie(
              (location, key) ->
                  bonsai
                      .getBonsaiCachedMerkleTrieLoader()
                      .getAccountStorageTrieNode(
                          bonsai.getWorldStateStorage(), updatedAddressHash, location, key),
              Bytes32.wrap(storageRoot.getBytes()));

      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageUpdate :
          storageUpdates.entrySet()) {
        final Hash slotHash = storageUpdate.getKey().getSlotHash();
        final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
        try {
          if (!storageUpdate.getValue().isUnchanged()) {
            if (updatedStorage == null || updatedStorage.equals(UInt256.ZERO)) {
              if (!storageFrozen) {
                writes.add(
                    updater -> updater.removeStorageValueBySlotHash(updatedAddressHash, slotHash));
              }
              storageTrie.remove(slotHash.getBytes());
            } else {
              if (!storageFrozen) {
                writes.add(
                    updater ->
                        updater.putStorageValueBySlotHash(
                            updatedAddressHash, slotHash, updatedStorage));
              }
              storageTrie.put(slotHash.getBytes(), encodeTrieValue(updatedStorage));
            }
          }
        } catch (MerkleTrieException e) {
          throw new MerkleTrieException(
              e.getMessage(), Optional.of(updatedAddress), e.getHash(), e.getLocation());
        }
      }

      final boolean accountDeleted =
          worldStateUpdater.getAccountsToUpdate().get(updatedAddress).getUpdated() == null;

      if (!storageFrozen && !accountDeleted) {
        storageTrie.commit(
            (location, nodeHash, value) ->
                writes.add(
                    u ->
                        u.putAccountStorageTrieNode(
                            updatedAddressHash, location, nodeHash, value)));
      }
      return accountDeleted ? Hash.EMPTY_TRIE_HASH : Hash.wrap(storageTrie.getRootHash());
    }

    private void clearStorage() {
      for (final Address address : worldStateUpdater.getStorageToClear()) {
        final BonsaiAccount oldAccount =
            bonsai
                .getWorldStateStorage()
                .getAccount(address.addressHash())
                .map(
                    bytes ->
                        BonsaiAccount.fromRLP(bonsai, address, bytes, true, bonsai.codeCache()))
                .orElse(null);
        if (oldAccount == null) {
          continue;
        }
        final Hash addressHash = address.addressHash();
        final MerkleTrie<Bytes, Bytes> storageTrie =
            bonsai.createTrie(
                (location, key) -> bonsai.getStorageTrieNode(addressHash, location, key),
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
            for (final Map.Entry<Bytes32, Bytes> slot : entriesToDelete.entrySet()) {
              final StorageSlotKey storageSlotKey =
                  new StorageSlotKey(Hash.wrap(slot.getKey()), Optional.empty());
              final UInt256 slotValue =
                  UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(slot.getValue())));
              writes.add(
                  updater ->
                      updater.removeStorageValueBySlotHash(
                          addressHash, storageSlotKey.getSlotHash()));
              storageToDelete
                  .computeIfAbsent(
                      storageSlotKey, key -> new PathBasedValue<>(slotValue, null, true))
                  .setPrior(slotValue);
              lastKeyHash = slot.getKey();
            }
            entriesToDelete.keySet().forEach(storageTrie::remove);
            if (entriesToDelete.size() < 256) {
              break;
            }
            final Optional<Bytes32> maybeNextKeyHash = incrementBytes32(lastKeyHash);
            if (maybeNextKeyHash.isEmpty()) {
              break;
            }
            nextKeyHash = maybeNextKeyHash.get();
          }
        } catch (MerkleTrieException e) {
          throw new MerkleTrieException(
              e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
        }
      }
    }

    private void collectCodeWrites() {
      for (final Map.Entry<Address, PathBasedValue<Bytes>> codeUpdate :
          worldStateUpdater.getCodeToUpdate().entrySet()) {
        final Bytes updatedCode = codeUpdate.getValue().getUpdated();
        final Hash accountHash = codeUpdate.getKey().addressHash();
        final Bytes priorCode = codeUpdate.getValue().getPrior();

        if (Objects.equals(priorCode, updatedCode)
            || (codeIsEmpty(priorCode) && codeIsEmpty(updatedCode))) {
          continue;
        }

        if (codeIsEmpty(updatedCode)) {
          final Hash priorCodeHash = Hash.hash(priorCode);
          writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
        } else {
          final Hash codeHash = Hash.hash(updatedCode);
          writes.add(updater -> updater.putCode(accountHash, codeHash, updatedCode));
        }
      }
    }

    private static boolean codeIsEmpty(final Bytes value) {
      return value == null || value.isEmpty();
    }

    private Optional<Bytes32> incrementBytes32(final Bytes32 value) {
      final UInt256 incremented = UInt256.fromBytes(value).add(UInt256.ONE);
      return incremented.isZero() ? Optional.empty() : Optional.of(incremented);
    }
  }
}
