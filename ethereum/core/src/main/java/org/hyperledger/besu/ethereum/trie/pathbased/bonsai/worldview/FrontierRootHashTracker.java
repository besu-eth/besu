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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;

import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class FrontierRootHashTracker {

  /** Creates a Merkle trie for the frontier account state from a given root hash. */
  @FunctionalInterface
  public interface AccountTrieFactory {
    MerkleTrie<Bytes, Bytes> create(Bytes32 rootHash);
  }

  /** Updates the storage root for an account that has pending storage changes. */
  @FunctionalInterface
  public interface StorageRootUpdater {
    void update(
        Address address,
        StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates);
  }

  private final BonsaiWorldStateUpdateAccumulator accumulator;
  private final AccountTrieFactory accountTrieFactory;
  private final StorageRootUpdater storageRootUpdater;

  private MerkleTrie<Bytes, Bytes> frontierTrie;
  private Hash frontierRootHashCache;

  public FrontierRootHashTracker(
      final BonsaiWorldStateUpdateAccumulator accumulator,
      final AccountTrieFactory accountTrieFactory,
      final StorageRootUpdater storageRootUpdater) {
    this.accumulator = accumulator;
    this.accountTrieFactory = accountTrieFactory;
    this.storageRootUpdater = storageRootUpdater;
  }

  /**
   * Computes the intermediate state root reflecting all transactions committed so far in the
   * current block. Called once per transaction on pre-Byzantium blocks to populate the receipt's
   * state root field.
   *
   * <p>The trie is created lazily on the first call and cached across subsequent calls within the
   * same block. Only accounts dirtied since the last call are applied, keeping per-call cost
   * proportional to the transaction's footprint rather than the entire block's accumulated state.
   *
   * <p>Must be called <em>after</em> {@code commit()} and {@code markTransactionBoundary()} so that
   * dirty addresses have been captured. Must be followed by {@link #reset()} (via {@code
   * persist()}) at the block boundary to discard the cached trie before the next block.
   *
   * @param baseRootHash the persisted state root from the end of the previous block
   * @return the state root hash incorporating all committed transactions
   */
  public Hash frontierRootHash(final Hash baseRootHash) {
    final Set<Address> dirty = accumulator.getFrontierDirtyAddresses();

    if (dirty.isEmpty()) {
      if (frontierRootHashCache == null) {
        frontierRootHashCache = baseRootHash;
      }
      return frontierRootHashCache;
    }

    if (frontierTrie == null) {
      frontierTrie = accountTrieFactory.create(Bytes32.wrap(baseRootHash.getBytes()));
    }

    try {
      for (final Address address : dirty) {
        final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates =
            accumulator.getStorageToUpdate().get(address);
        if (storageUpdates != null) {
          storageRootUpdater.update(address, storageUpdates);
        } else if (accumulator.getStorageToClear().contains(address)) {
          final PathBasedValue<BonsaiAccount> accountValue =
              accumulator.getAccountsToUpdate().get(address);
          if (accountValue != null && accountValue.getUpdated() != null) {
            accountValue.getUpdated().setStorageRoot(Hash.EMPTY_TRIE_HASH);
          }
        }

        // Every dirty address was added during commit(), which runs super.commit() first.
        // super.commit() always populates accountsToUpdate before we capture the address,
        // so a missing entry here means a bug in the dirty-tracking logic.
        final PathBasedValue<BonsaiAccount> accountValue =
            accumulator.getAccountsToUpdate().get(address);
        if (accountValue == null) {
          throw new IllegalStateException(
              "Dirty address " + address.toHexString() + " not found in accountsToUpdate");
        }
        final BonsaiAccount updated = accountValue.getUpdated();
        if (updated == null) {
          removeAccountFromTrie(frontierTrie, address);
        } else {
          updateAccountInTrie(frontierTrie, address, updated);
        }
      }

      frontierRootHashCache = Hash.wrap(frontierTrie.getRootHash());
      accumulator.clearFrontierDirtyAddresses(dirty);
      return frontierRootHashCache;
    } catch (final MerkleTrieException e) {
      // Discard the potentially-corrupted cached trie so the next call rebuilds from scratch.
      // Dirty addresses are intentionally NOT cleared — they will be reprocessed on retry.
      reset();
      throw e;
    }
  }

  public void reset() {
    frontierTrie = null;
    frontierRootHashCache = null;
  }

  private static void removeAccountFromTrie(
      final MerkleTrie<Bytes, Bytes> trie, final Address address) {
    try {
      trie.remove(address.addressHash().getBytes());
    } catch (final MerkleTrieException e) {
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  private static void updateAccountInTrie(
      final MerkleTrie<Bytes, Bytes> trie,
      final Address address,
      final BonsaiAccount updatedAccount) {
    try {
      trie.put(updatedAccount.getAddressHash().getBytes(), updatedAccount.serializeAccount());
    } catch (final MerkleTrieException e) {
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }
}
