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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListIndex;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiBalAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.AbstractWorldUpdater;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * A {@link PathBasedWorldUpdater} backed directly by a {@link BlockAccessListIndex} rather than an
 * in-memory accumulator.
 *
 * <h2>Design contract</h2>
 *
 * <ul>
 *   <li><b>No shadow maps</b>: this class holds no {@code Map<Address, ...>} or {@code
 *       Set<Address>} that mirror mutations already tracked in the BAL. All such reads are
 *       delegated O(1) to the {@link BlockAccessListIndex}.
 *   <li><b>Lazy DB access</b>: a database read for an account is triggered only when the EVM
 *       actually touches that account, never eagerly for all BAL entries.
 *   <li><b>No-op commit</b>: in the parallel-worker context the EVM's mutations are captured by an
 *       {@code AccessLocationTracker} → {@code PartialBlockAccessView} and later applied to the
 *       main block accumulator. This updater therefore does not propagate anything on {@link
 *       #commit()}.
 *   <li><b>Trie-log delegation</b>: {@link #getAccountsToUpdate()}, {@link #getCodeToUpdate()} and
 *       {@link #getStorageToUpdate()} return empty maps because trie-log / root-hash generation is
 *       the responsibility of the main block accumulator, not the per-worker view.
 * </ul>
 */
@SuppressWarnings("unchecked")
public class BonsaiBalWorldStateUpdater extends AbstractWorldUpdater<PathBasedWorldView, BonsaiAccount>
    implements PathBasedWorldUpdater {

  /** O(1) index into the BlockAccessList – no state is copied from it. */
  private final BlockAccessListIndex balIndex;

  /**
   * The 1-based transaction position for this worker. Only BAL entries with {@code txIndex <
   * this.txIndex} are visible.
   */
  private final long txIndex;

  /** Kept locally so {@link #copy()} can create a fresh instance without reflection. */
  private final EvmConfiguration evmConfiguration;

  /**
   * Creates a new BAL-backed updater.
   *
   * @param balIndex O(1) index over the full {@link BlockAccessList}
   * @param txIndex 1-based position of the transaction being pre-executed; only BAL entries with
   *     {@code txIndex < this.txIndex} are applied
   * @param world the parent world view used for DB fallback reads
   * @param evmConfiguration EVM configuration (determines stacked vs journaled updater mode)
   */
  public BonsaiBalWorldStateUpdater(
      final BlockAccessListIndex balIndex,
      final long txIndex,
      final PathBasedWorldView world,
      final EvmConfiguration evmConfiguration) {
    super(world, evmConfiguration);
    this.balIndex = balIndex;
    this.txIndex = txIndex;
    this.evmConfiguration = evmConfiguration;
  }

  // ---------------------------------------------------------------------------
  // AbstractWorldUpdater — account loading
  // ---------------------------------------------------------------------------

  /**
   * Returns a {@link BonsaiBalAccount} for any address that has BAL-tracked mutations, so that
   * nonce/balance/code reads are served O(1) from the BAL without eagerly loading the DB account.
   * Falls back to a direct DB load for addresses not in the BAL (or in the BAL only for reads).
   */
  @Override
  protected BonsaiAccount getForMutation(final Address address) {
    final BlockAccessList.AccountChanges balChanges = balIndex.findAccount(address);
    if (balChanges != null && balChanges.hasAnyChange()) {
      return new BonsaiBalAccount(
          this, address, balIndex, txIndex, wrappedWorldView(), wrappedWorldView().codeCache());
    }
    final Account parentAccount = wrappedWorldView().get(address);
    return (parentAccount instanceof PathBasedAccount) ? (BonsaiAccount) parentAccount : null;
  }

  // ---------------------------------------------------------------------------
  // PathBasedWorldView — code
  // ---------------------------------------------------------------------------

  @Override
  public Optional<Bytes> getCode(final Address address, final Hash codeHash) {
    final BlockAccessList.AccountChanges balChanges = balIndex.findAccount(address);
    if (balChanges != null) {
      final BlockAccessList.CodeChange latest =
          findLatestBefore(balChanges.codeChanges(), BlockAccessList.CodeChange::txIndex);
      if (latest != null) {
        return Optional.ofNullable(latest.newCode()).filter(b -> !b.isEmpty());
      }
    }
    return wrappedWorldView().getCode(address, codeHash);
  }

  // ---------------------------------------------------------------------------
  // PathBasedWorldView — storage
  // ---------------------------------------------------------------------------

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 slotKey) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(slotKey))
        .orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    final List<BlockAccessList.StorageChange> slotChanges =
        balIndex.findSlotChanges(address, storageSlotKey);
    if (slotChanges != null) {
      final BlockAccessList.StorageChange latest =
          findLatestBefore(slotChanges, BlockAccessList.StorageChange::txIndex);
      if (latest != null) {
        return Optional.ofNullable(latest.newValue());
      }
    }
    return wrappedWorldView().getStorageValueByStorageSlotKey(address, storageSlotKey);
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    return wrappedWorldView().getAllAccountStorage(address, rootHash);
  }

  // ---------------------------------------------------------------------------
  // PathBasedWorldView — miscellaneous
  // ---------------------------------------------------------------------------

  @Override
  public boolean isModifyingHeadWorldState() {
    return false;
  }

  @Override
  public PathBasedWorldStateKeyValueStorage getWorldStateStorage() {
    return wrappedWorldView().getWorldStateStorage();
  }

  @Override
  public org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache codeCache() {
    return wrappedWorldView().codeCache();
  }

  // ---------------------------------------------------------------------------
  // WorldUpdater — lifecycle
  // ---------------------------------------------------------------------------

  @Override
  public void commit() {
    // intentional no-op — mutations are captured by AccessLocationTracker → PartialBlockAccessView
  }

  @Override
  public void revert() {
    reset();
  }

  @Override
  public Collection<? extends Account> getTouchedAccounts() {
    return getUpdatedAccounts();
  }

  @Override
  public Collection<Address> getDeletedAccountAddresses() {
    return getDeletedAccounts();
  }

  @Override
  public void markTransactionBoundary() {
    getUpdatedAccounts().clear();
    getDeletedAccounts().clear();
  }

  // ---------------------------------------------------------------------------
  // PathBasedWorldUpdater — storage-to-clear (self-destruct)
  // ---------------------------------------------------------------------------

  @Override
  public Set<Address> getStorageToClear() {
    return Collections.emptySet();
  }

  // ---------------------------------------------------------------------------
  // PathBasedWorldUpdater — lifecycle overrides
  // ---------------------------------------------------------------------------

  @Override
  public void reset() {
    super.reset();
  }

  @Override
  public PathBasedWorldUpdater copy() {
    return new BonsaiBalWorldStateUpdater(balIndex, txIndex, wrappedWorldView(), evmConfiguration);
  }

  // ---------------------------------------------------------------------------
  // TrieLogAccumulator — trie-log / root-hash generation (all empty)
  // ---------------------------------------------------------------------------

  @Override
  public Map<Address, ? extends TrieLog.LogTuple<? extends AccountValue>> getAccountsToUpdate() {
    return Collections.emptyMap();
  }

  @Override
  public Map<Address, ? extends TrieLog.LogTuple<Bytes>> getCodeToUpdate() {
    return Collections.emptyMap();
  }

  @Override
  public Map<Address, ? extends Map<StorageSlotKey, ? extends TrieLog.LogTuple<UInt256>>>
      getStorageToUpdate() {
    return Collections.emptyMap();
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /**
   * Finds the entry in {@code changes} with the largest {@code txIndex} that is strictly less than
   * {@link #txIndex}. The list must be in ascending {@code txIndex} order.
   */
  private <T> T findLatestBefore(
      final List<T> changes, final java.util.function.ToLongFunction<T> indexExtractor) {
    for (int i = changes.size() - 1; i >= 0; i--) {
      final T entry = changes.get(i);
      if (indexExtractor.applyAsLong(entry) < txIndex) {
        return entry;
      }
    }
    return null;
  }
}
