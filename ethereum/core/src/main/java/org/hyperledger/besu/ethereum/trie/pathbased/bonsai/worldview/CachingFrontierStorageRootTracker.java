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

import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Caching {@link FrontierStorageRootTracker} that keeps an in-memory storage trie per dirty account
 * alive for the duration of a pre-Byzantium block.
 *
 * <p>{@link FrontierRootHashTracker#frontierRootHash(Hash)} calls {@link #update(Address,
 * StorageConsumingMap)} once per dirty account per transaction. The accumulator's storage-to-update
 * map grows cumulatively across transactions, so rebuilding each account's trie from its base root
 * on every call would be O(N^2) in slot writes per contract. This tracker preserves the trie across
 * calls and applies only the slot deltas introduced since the last call.
 *
 * <p>Lifecycle: pair every block of {@link #update} calls with a {@link #reset()} at the block
 * boundary to discard the cached tries before the next block.
 */
public class CachingFrontierStorageRootTracker implements FrontierStorageRootTracker {

  /** Builds a storage trie for a given account anchored at a base root. */
  @FunctionalInterface
  public interface StorageTrieFactory {
    MerkleTrie<Bytes, Bytes> create(Hash addressHash, Hash baseRoot);
  }

  private final BonsaiWorldStateUpdateAccumulator accumulator;
  private final StorageTrieFactory storageTrieFactory;

  private final Map<Address, CachedTrie> cache = new HashMap<>();

  public CachingFrontierStorageRootTracker(
      final BonsaiWorldStateUpdateAccumulator accumulator,
      final StorageTrieFactory storageTrieFactory) {
    this.accumulator = accumulator;
    this.storageTrieFactory = storageTrieFactory;
  }

  @Override
  public void update(
      final Address address,
      final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates) {
    final PathBasedValue<BonsaiAccount> accountValue =
        accumulator.getAccountsToUpdate().get(address);
    if (accountValue == null) {
      // FrontierRootHashTracker filters these out, but guard anyway: a missing entry here would
      // silently produce a wrong root.
      return;
    }
    final CachedTrie cached = getOrBuildCache(address, accountValue);

    try {
      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> slotUpdate :
          storageUpdates.entrySet()) {
        applySlot(cached, slotUpdate);
      }
    } catch (final MerkleTrieException e) {
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }

    final BonsaiAccount accountUpdated = accountValue.getUpdated();
    if (accountUpdated != null) {
      accountUpdated.setStorageRoot(Hash.wrap(cached.trie.getRootHash()));
    }
  }

  @Override
  public void reset() {
    cache.clear();
  }

  private CachedTrie getOrBuildCache(
      final Address address, final PathBasedValue<BonsaiAccount> accountValue) {
    final BonsaiAccount prior = accountValue.getPrior();
    final Hash baseRoot =
        (prior == null || accumulator.getStorageToClear().contains(address))
            ? Hash.EMPTY_TRIE_HASH
            : prior.getStorageRoot();
    final CachedTrie existing = cache.get(address);
    if (existing != null && existing.baseRoot.equals(baseRoot)) {
      return existing;
    }
    // Either no cache yet, or the account's base storage root changed since we cached (e.g.
    // SELFDESTRUCT mid-block). Build a fresh trie from the new base.
    final CachedTrie fresh =
        new CachedTrie(baseRoot, storageTrieFactory.create(address.addressHash(), baseRoot));
    cache.put(address, fresh);
    return fresh;
  }

  /**
   * Decides whether to apply a slot write to the cached trie.
   *
   * <p>Three cases, ordered because the cached trie already reflects writes from earlier tracker
   * calls in this block:
   *
   * <ol>
   *   <li>We previously wrote this slot and the value still matches: no-op.
   *   <li>We never wrote this slot and its net change since block start is {@code unchanged}: the
   *       base trie already has the right value, no walk needed.
   *   <li>Otherwise apply the current value. This covers reverts — a later tx restoring the
   *       original value after an earlier tx wrote it; an {@code isUnchanged}-only skip would leave
   *       the earlier write in the cache and produce wrong receipt roots.
   * </ol>
   */
  private static void applySlot(
      final CachedTrie cached,
      final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> slotUpdate) {
    final Bytes slotHashBytes = slotUpdate.getKey().getSlotHash().getBytes();
    final UInt256 updatedValue = slotUpdate.getValue().getUpdated();
    final Bytes newEncoded =
        (updatedValue == null || updatedValue.isZero()) ? null : encodeTrieValue(updatedValue);

    if (cached.lastAppliedValue.containsKey(slotHashBytes)) {
      if (Objects.equals(cached.lastAppliedValue.get(slotHashBytes), newEncoded)) {
        return;
      }
    } else if (slotUpdate.getValue().isUnchanged()) {
      return;
    }

    if (newEncoded == null) {
      cached.trie.remove(slotHashBytes);
    } else {
      cached.trie.put(slotHashBytes, newEncoded);
    }
    cached.lastAppliedValue.put(slotHashBytes, newEncoded);
  }

  private static final class CachedTrie {
    final Hash baseRoot;
    final MerkleTrie<Bytes, Bytes> trie;
    // slotHash bytes -> last value written to the trie (null marks a removed slot).
    final HashMap<Bytes, Bytes> lastAppliedValue = new HashMap<>();

    CachedTrie(final Hash baseRoot, final MerkleTrie<Bytes, Bytes> trie) {
      this.baseRoot = baseRoot;
      this.trie = trie;
    }
  }
}
