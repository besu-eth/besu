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
package org.hyperledger.besu.evm.worldstate;

import org.hyperledger.besu.collections.undo.UndoMap;
import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

/**
 * The Journaled updater.
 *
 * @param <W> the WorldView type parameter
 */
public class JournaledUpdater<W extends WorldView> implements WorldUpdater {

  final EvmConfiguration evmConfiguration;
  final WorldUpdater parentWorld;
  final AbstractWorldUpdater<W, ? extends MutableAccount> rootWorld;
  final UndoMap<Address, JournaledAccount> accounts;
  final UndoSet<Address> deleted;
  final long undoMark;

  /**
   * Instantiates a new Stacked updater.
   *
   * @param world the world
   * @param evmConfiguration the EVM Configuration parameters
   */
  @SuppressWarnings("unchecked")
  public JournaledUpdater(final WorldUpdater world, final EvmConfiguration evmConfiguration) {
    parentWorld = world;
    this.evmConfiguration = evmConfiguration;
    if (world instanceof JournaledUpdater<?>) {
      JournaledUpdater<W> journaledUpdater = (JournaledUpdater<W>) world;
      accounts = journaledUpdater.accounts;
      deleted = journaledUpdater.deleted;
      rootWorld = journaledUpdater.rootWorld;
    } else if (world instanceof AbstractWorldUpdater<?, ?>) {
      accounts = new UndoMap<>(new HashMap<>());
      deleted = UndoSet.of(new HashSet<>());
      rootWorld = (AbstractWorldUpdater<W, ? extends MutableAccount>) world;
    } else {
      throw new IllegalArgumentException(
          "WorldUpdater must be a JournaledWorldUpdater or an AbstractWorldUpdater");
    }
    undoMark = accounts.mark();
  }

  /**
   * Get an account suitable for mutation. Defer to parent if not tracked locally.
   *
   * @param address the account at the address, for mutaton.
   * @return the mutable account
   */
  protected MutableAccount getForMutation(final Address address) {
    final JournaledAccount wrappedTracker = accounts.get(address);
    if (wrappedTracker != null) {
      return wrappedTracker;
    }
    final MutableAccount account = rootWorld.getForMutation(address);
    return account == null ? null : new UpdateTrackingAccount<>(account);
  }

  @Override
  public Collection<? extends Account> getTouchedAccounts() {
    return new ArrayList<>(accounts.values());
  }

  @Override
  public Collection<Address> getDeletedAccountAddresses() {
    return new ArrayList<>(deleted);
  }

  /**
   * Remove all changes done by this layer. Rollback to the state prior to the updater's changes.
   */
  protected void reset() {
    accounts.values().forEach(a -> a.undo(undoMark));
    accounts.undo(undoMark);
    deleted.undo(undoMark);
  }

  @Override
  public void revert() {
    reset();
  }

  @Override
  public void commit() {
    if (!(parentWorld instanceof JournaledUpdater<?>)) {
      accounts.values().forEach(JournaledAccount::commit);
      deleted.forEach(parentWorld::deleteAccount);
    }
  }

  @Override
  public Optional<WorldUpdater> parentUpdater() {
    return Optional.of(parentWorld);
  }

  /** Mark transaction boundary. */
  @Override
  public void markTransactionBoundary() {
    accounts.values().forEach(JournaledAccount::markTransactionBoundary);
  }

  @Override
  public void observeBalance(final Address address) {
    parentWorld.observeBalance(address);
  }

  @Override
  public void observeSufficientBalance(final Address address, final Wei margin) {
    parentWorld.observeSufficientBalance(address, margin);
  }

  @Override
  public MutableAccount createAccount(final Address address, final long nonce, final Wei balance) {
    JournaledAccount journaledAccount =
        new JournaledAccount(rootWorld.createAccount(address, nonce, balance));
    accounts.put(address, journaledAccount);
    return new JournaledAccount(journaledAccount);
  }

  @Override
  public MutableAccount getAccount(final Address address) {
    // We may have updated it already, so check that first.
    final JournaledAccount existing = accounts.get(address);
    if (existing != null) {
      return existing;
    }
    if (deleted.contains(address)) {
      return null;
    }

    // Otherwise, get it from our wrapped view and create a new update tracker.
    final MutableAccount origin = rootWorld.getAccount(address);
    if (origin == null) {
      return null;
    } else {
      var newAccount = new JournaledAccount(origin);
      accounts.put(address, newAccount);
      return newAccount;
    }
  }

  @Override
  public void deleteAccount(final Address address) {
    deleted.add(address);
    var account = accounts.get(address);
    if (account != null) {
      account.setDeleted(true);
    }
  }

  @Override
  public Account get(final Address address) {
    final MutableAccount existing = accounts.get(address);
    if (existing != null) {
      return existing;
    }
    if (deleted.contains(address)) {
      return null;
    }
    return rootWorld.get(address);
  }

  @Override
  public WorldUpdater updater() {
    return new JournaledUpdater<W>(this, evmConfiguration);
  }
}
