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
package org.hyperledger.besu.evm.v2.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Table;
import org.apache.tuweni.bytes.Bytes32;
import org.jspecify.annotations.Nullable;

/**
 * The world updater of a transaction and its call frames on EVM v2.
 *
 * <p>Instead of buffering changes in an updater per call frame, all frames of a transaction share
 * one {@link TransactionJournalV2}, and each frame's updater is a mark in the journal's undo log.
 * Reverting a frame rolls the log back to its mark, committing a frame keeps its changes, and
 * committing the transaction's updater writes the changes to the updater it was created on.
 *
 * <p>A read does not end the updaters created on this one, so it sees their changes. A change
 * abandons them and discards their changes, as dropping a stacked updater would, and using an
 * abandoned updater again throws. Since only the running frame's updater can use the journal this
 * way, the transaction processor does not use it when the tracer may access the world state.
 */
public final class JournaledWorldUpdaterV2 implements WorldUpdater {

  private final TransactionJournalV2 journal;
  @Nullable private final JournaledWorldUpdaterV2 parentUpdater;
  final int mark;
  boolean open = true;
  boolean abandoned;

  /**
   * Creates the updater of a transaction on top of the given updater.
   *
   * @param parent the updater the transaction's changes are committed to
   */
  public JournaledWorldUpdaterV2(final WorldUpdater parent) {
    this(new TransactionJournalV2(parent), null);
  }

  private JournaledWorldUpdaterV2(
      final TransactionJournalV2 journal, @Nullable final JournaledWorldUpdaterV2 parentUpdater) {
    this.journal = journal;
    this.parentUpdater = parentUpdater;
    this.mark = journal.size();
    journal.open(this);
  }

  @Override
  @Nullable
  public Account get(final Address address) {
    final JournalAccountV2 account = journal.lookup(address);
    return account.exists ? account : null;
  }

  @Override
  @Nullable
  public MutableAccount getAccount(final Address address) {
    journal.activate(this);
    final JournalAccountV2 account = journal.lookup(address);
    if (!account.exists) {
      return null;
    }
    journal.touch(account, mark);
    return account;
  }

  @Override
  public MutableAccount createAccount(final Address address, final long nonce, final Wei balance) {
    journal.activate(this);
    final JournalAccountV2 account = journal.lookup(address);
    journal.create(account, nonce, balance);
    return account;
  }

  @Override
  public void deleteAccount(final Address address) {
    journal.activate(this);
    journal.delete(journal.lookup(address));
  }

  @Override
  public Collection<? extends Account> getTouchedAccounts() {
    return journal.touchedSince(mark);
  }

  @Override
  public Collection<Address> getDeletedAccountAddresses() {
    return journal.deletedSince(mark);
  }

  @Override
  public WorldUpdater updater() {
    journal.activate(this);
    return new JournaledWorldUpdaterV2(journal, this);
  }

  @Override
  public void revert() {
    checkNotAbandoned();
    if (!open) {
      return;
    }
    journal.activate(this);
    journal.undoTo(mark);
  }

  @Override
  public void commit() {
    checkNotAbandoned();
    if (!open) {
      return;
    }
    journal.close(this);
    if (parentUpdater == null) {
      journal.commitToParent();
    }
  }

  void checkNotAbandoned() {
    if (abandoned) {
      throw new IllegalStateException(
          "World updater used after a change through an updater below it discarded its changes");
    }
  }

  /**
   * Warms up a storage slot as per EIP-2929, for the access list of the transaction.
   *
   * @param address the account of the slot
   * @param slot the slot
   * @return true if the slot was already warm
   */
  public boolean warmUpStorage(final Address address, final Bytes32 slot) {
    journal.activate(this);
    return journal.warmUpStorage(address, slot);
  }

  /**
   * The storage slots the transaction has warmed up, for building an access list.
   *
   * @return the warm slots by account
   */
  public Table<Address, Bytes32, Boolean> getWarmedUpStorage() {
    return journal.warmedUpStorage();
  }

  @Override
  public Optional<WorldUpdater> parentUpdater() {
    return Optional.of(parentUpdater != null ? parentUpdater : journal.parent());
  }
}
