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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;
import org.apache.tuweni.units.bigints.UInt256;

/** Bonsai path-based root from accumulated block updates (no BAL background). */
public final class DefaultStateRootCommitter implements StateRootCommitter {

    public DefaultStateRootCommitter() {
    }


  /**
   * Computes the state root from the given accumulator. When {@code accumulatorOverride} is
   * non-null it is used instead of the world state's live accumulator (e.g. dry-run {@code
   * rootHash()} on a copy).
   */
  @Override
  public StateRootComputation compute(
      final MutableWorldState mutableWorldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>) worldUpdater;
    final BonsaiWorldState bonsai = (BonsaiWorldState) mutableWorldState;
    final boolean storageFrozen = mutableWorldState.isStorageFrozen();
    final List<StateRootComputation.UpdaterWrite> writes =
        Collections.synchronizedList(new ArrayList<>());
    if (blockHeader != null && bonsai.isTrieDisabled()) {
      executeComputation(bonsai, writes, accumulator, storageFrozen);
      return StateRootComputation.pathBased(blockHeader.getStateRoot(), writes);
    }
    final Hash root = executeComputation(bonsai, writes, accumulator,storageFrozen);
    return StateRootComputation.pathBased(root, writes);
  }

  private Hash executeComputation(
          final BonsaiWorldState bonsai,
          final List<StateRootComputation.UpdaterWrite> writes,
          final PathBasedWorldStateUpdateAccumulator<?> accumulator, final boolean storageFrozen) {

    final BonsaiWorldStateUpdateAccumulator worldStateUpdater = (BonsaiWorldStateUpdateAccumulator) accumulator;

    clearStorage(writes, worldStateUpdater, bonsai);

    worldStateUpdater.getStorageToUpdate().entrySet().parallelStream()
        .forEach(entry -> updateAccountStorageState(writes, worldStateUpdater, entry, bonsai,storageFrozen));

    if(!storageFrozen){
      updateCode(writes, worldStateUpdater);
    }

    final MerkleTrie<Bytes, Bytes> accountTrie = bonsai.createAccountStateTrie();

    updateAccounts(writes, worldStateUpdater, accountTrie, bonsai, storageFrozen);
    if(!storageFrozen) {
      accountTrie.commit(
              (location, hash, value) ->
                      writes.add(u -> u.putAccountStateTrieNode(location, hash, value)));
    }
    return Hash.wrap(accountTrie.getRootHash());
  }

  private void updateAccounts(
          final List<StateRootComputation.UpdaterWrite> writes,
          final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
          final MerkleTrie<Bytes, Bytes> accountTrie,
          final BonsaiWorldState bonsai,
          final boolean storageFrozen) {
    for (final Map.Entry<Address, PathBasedValue<BonsaiAccount>> accountUpdate :
        worldStateUpdater.getAccountsToUpdate().entrySet()) {
      final Bytes accountKey = accountUpdate.getKey().getBytes();
      final PathBasedValue<BonsaiAccount> bonsaiValue = accountUpdate.getValue();
      final BonsaiAccount updatedAccount = bonsaiValue.getUpdated();
      try {
        if (updatedAccount == null) {
          final Hash addressHash = bonsai.hashAndSavePreImage(accountKey);
          accountTrie.remove(addressHash.getBytes());
          if(!storageFrozen) {
            writes.add(updater -> updater.removeAccountInfoState(addressHash));
          }
        } else {
          final Hash addressHash = updatedAccount.getAddressHash();
          final Bytes accountValue = updatedAccount.serializeAccount();
          final Hash preImageHash = bonsai.hashAndSavePreImage(accountKey);
          if(!storageFrozen) {
            writes.add(updater -> updater.putAccountInfoState(preImageHash, accountValue));
          }
          accountTrie.put(addressHash.getBytes(), accountValue);
        }
      } catch (MerkleTrieException e) {
        throw new MerkleTrieException(
            e.getMessage(), Optional.of(Address.wrap(accountKey)), e.getHash(), e.getLocation());
      }
    }
  }

  @VisibleForTesting
  public void updateCode(
      final List<StateRootComputation.UpdaterWrite> writes,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater) {
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

  private boolean codeIsEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }

  private void updateAccountStorageState(
          final List<StateRootComputation.UpdaterWrite> writes,
          final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
          final Map.Entry<Address, StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>>
          storageAccountUpdate,
          final BonsaiWorldState bonsai,
          final boolean storageFrozen) {
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
          bonsai.createTrie(
              (location, key) ->
                  bonsai
                      .getBonsaiCachedMerkleTrieLoader()
                      .getAccountStorageTrieNode(
                          bonsai.getWorldStateStorage(), updatedAddressHash, location, key),
              Bytes32.wrap(storageRoot.getBytes()));

      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageUpdate :
          storageAccountUpdate.getValue().entrySet()) {
        final Hash slotHash = storageUpdate.getKey().getSlotHash();
        final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
        try {
          if (!storageUpdate.getValue().isUnchanged()) {
            if (updatedStorage == null || updatedStorage.equals(UInt256.ZERO)) {
              if(!storageFrozen) {
                writes.add(
                        updater -> updater.removeStorageValueBySlotHash(updatedAddressHash, slotHash));
              }
              storageTrie.remove(slotHash.getBytes());
            } else {
              if(!storageFrozen) {
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

      final BonsaiAccount accountUpdated = accountValue.getUpdated();
      if (accountUpdated != null) {
        if(!storageFrozen) {
          storageTrie.commit(
                  (location, nodeHash, value) ->
                          writes.add(
                                  u ->
                                          u.putAccountStorageTrieNode(
                                                  updatedAddressHash, location, nodeHash, value)));
        }
        if (!bonsai.isTrieDisabled()) {
          accountUpdated.setStorageRoot(Hash.wrap(storageTrie.getRootHash()));
        }
      }
    }
  }

  private void clearStorage(
      final List<StateRootComputation.UpdaterWrite> writes,
      final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
      final BonsaiWorldState bonsai) {
    for (final Address address : worldStateUpdater.getStorageToClear()) {
      final BonsaiAccount oldAccount =
          bonsai
              .getWorldStateStorage()
              .getAccount(address.addressHash())
              .map(bytes -> BonsaiAccount.fromRLP(bonsai, address, bytes, true, bonsai.codeCache()))
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
                .computeIfAbsent(storageSlotKey, key -> new PathBasedValue<>(slotValue, null, true))
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

  @VisibleForTesting
  public Optional<Bytes32> incrementBytes32(final Bytes32 value) {
    final UInt256 incremented = UInt256.fromBytes(value).add(UInt256.ONE);
    return incremented.isZero() ? Optional.empty() : Optional.of(incremented);
  }
}
