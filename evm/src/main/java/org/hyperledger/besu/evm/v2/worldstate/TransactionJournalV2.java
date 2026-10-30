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
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

/**
 * The accounts and storage a transaction on EVM v2 has looked up or changed, and an undo log of its
 * changes. Call frames revert by rolling the log back to the length it had when they started, so a
 * lookup costs one map probe however deep the call stack is.
 *
 * <p>Only the thread executing the transaction uses a journal.
 */
final class TransactionJournalV2 {

  /** The change an entry of the undo log records. */
  private enum UndoKind {
    // the undo restores the account's nonce
    NONCE_CHANGED,
    // the undo restores the account's balance
    BALANCE_CHANGED,
    // the undo restores the account's code
    CODE_CHANGED,
    // the undo restores the slot's value, its storage epoch, written flag and previous entry
    SLOT_CHANGED,
    // the undo restores the account's storage epoch and whether its storage was cleared
    STORAGE_CLEARED,
    // the undo restores the log index of the account's previous touch
    ACCOUNT_TOUCHED,
    // the undo restores the account as it was before it was created or deleted
    ACCOUNT_CREATED_OR_DELETED,
    // the undo makes the slot cold again
    SLOT_WARMED
  }

  /** A slot before a change, and the log index of the slot's previous change. */
  private record SlotState(@Nullable UInt256 current, int epoch, boolean dirty, int loggedAt) {}

  /** An account's storage before it was cleared. */
  private record StorageState(int epoch, boolean cleared) {}

  /** An account's state before it was created or deleted. */
  private record AccountState(
      boolean exists,
      boolean fresh,
      long nonce,
      Wei balance,
      @Nullable Code code,
      int storageEpoch,
      boolean storageCleared,
      int touchedAt,
      int deletedAt) {}

  private final WorldUpdater parent;
  private final HashMap<Address, JournalAccountV2> accounts = new HashMap<>();
  // SLOAD, SSTORE and SELFBALANCE look up their frame's account with the same Address instance on
  // every execution, and comparing the instance is cheaper than hashing and comparing the address
  @Nullable private Address lastAddress;
  @Nullable private JournalAccountV2 lastAccount;
  // access list slots of accounts not looked up yet, so that the access list alone does not load
  // an account from the parent, which would count as touched for the parallel collision detection
  private final HashMap<Address, Set<SlotKey>> accessListSlots = new HashMap<>();
  // never reused, so a slot value of a reverted epoch cannot pass for one of a later epoch
  private int lastStorageEpoch;

  // the undo log, one change per index; most transactions make only a few changes
  private UndoKind[] kinds = new UndoKind[16];
  private Object[] targets = new Object[16];
  private @Nullable Object[] oldObjects = new Object[16];
  private long[] oldLongs = new long[16];
  private int size;

  // updaters that have neither been committed nor reverted, innermost last
  private JournaledWorldUpdaterV2[] openUpdaters = new JournaledWorldUpdaterV2[8];
  private int openCount;

  TransactionJournalV2(final WorldUpdater parent) {
    this.parent = parent;
  }

  WorldUpdater parent() {
    return parent;
  }

  int size() {
    return size;
  }

  int nextStorageEpoch() {
    return ++lastStorageEpoch;
  }

  @SuppressWarnings("ReferenceEquality") // an address keeps its account for the whole transaction
  JournalAccountV2 lookup(final Address address) {
    final JournalAccountV2 last = lastAccount;
    if (address == lastAddress && last != null) {
      return last;
    }
    JournalAccountV2 account = accounts.get(address);
    if (account == null) {
      account = load(address);
    }
    lastAddress = address;
    lastAccount = account;
    return account;
  }

  private JournalAccountV2 load(final Address address) {
    final JournalAccountV2 account = new JournalAccountV2(this, address, parent.get(address));
    accounts.put(address, account);
    if (!accessListSlots.isEmpty()) {
      final Set<SlotKey> slots = accessListSlots.remove(address);
      if (slots != null) {
        slots.forEach(account::warmUpFromAccessList);
      }
    }
    return account;
  }

  /** Warms up a slot of the access list without loading the account, returning if it was warm. */
  boolean warmUpStorage(final Address address, final Bytes32 slot) {
    final UInt256 key = UInt256.fromBytes(slot);
    final JournalAccountV2 account = accounts.get(address);
    if (account != null) {
      return account.warmUpStorage(key);
    }
    return !accessListSlots
        .computeIfAbsent(address, unused -> new HashSet<>())
        .add(new SlotKey(key));
  }

  // updaters

  void open(final JournaledWorldUpdaterV2 updater) {
    if (openCount == openUpdaters.length) {
      openUpdaters = Arrays.copyOf(openUpdaters, openCount * 2);
    }
    openUpdaters[openCount++] = updater;
  }

  /**
   * Makes the updater the innermost open one before it changes the journal. An updater above it was
   * abandoned without a commit or revert, so its changes are discarded, as a stacked updater's
   * would be.
   */
  void activate(final JournaledWorldUpdaterV2 updater) {
    if (!updater.open) {
      updater.checkNotAbandoned();
      return;
    }
    while (openUpdaters[openCount - 1] != updater) {
      final JournaledWorldUpdaterV2 abandoned = openUpdaters[--openCount];
      openUpdaters[openCount] = null;
      abandoned.open = false;
      abandoned.abandoned = true;
      undoTo(abandoned.mark);
    }
  }

  void close(final JournaledWorldUpdaterV2 updater) {
    activate(updater);
    openUpdaters[--openCount] = null;
    updater.open = false;
  }

  // account lifecycle

  void touch(final JournalAccountV2 account, final int mark) {
    if (account.touchedAt >= mark) {
      return;
    }
    final int index = append(UndoKind.ACCOUNT_TOUCHED, account, null, account.touchedAt);
    account.touchedAt = index;
  }

  void create(final JournalAccountV2 account, final long nonce, final Wei balance) {
    final int index = append(UndoKind.ACCOUNT_CREATED_OR_DELETED, account, stateOf(account), 0);
    account.exists = true;
    account.fresh = true;
    account.nonce = nonce;
    account.balance = balance;
    account.code = Code.EMPTY_CODE;
    account.storageEpoch = nextStorageEpoch();
    account.storageCleared = false;
    account.touchedAt = index;
    account.deletedAt = -1;
  }

  void delete(final JournalAccountV2 account) {
    final int index = append(UndoKind.ACCOUNT_CREATED_OR_DELETED, account, stateOf(account), 0);
    account.exists = false;
    account.touchedAt = -1;
    account.deletedAt = index;
  }

  private static AccountState stateOf(final JournalAccountV2 account) {
    return new AccountState(
        account.exists,
        account.fresh,
        account.nonce,
        account.balance,
        account.code,
        account.storageEpoch,
        account.storageCleared,
        account.touchedAt,
        account.deletedAt);
  }

  // changes to an account

  void logNonce(final JournalAccountV2 account) {
    append(UndoKind.NONCE_CHANGED, account, null, account.nonce);
  }

  void logBalance(final JournalAccountV2 account) {
    append(UndoKind.BALANCE_CHANGED, account, account.balance, 0);
  }

  void logCode(final JournalAccountV2 account) {
    append(UndoKind.CODE_CHANGED, account, account.code, 0);
  }

  void logSlot(final JournalAccountV2.Slot slot) {
    // the slot's first entry since the innermost updater opened is the one its revert needs
    if (slot.loggedAt >= innermostMark()) {
      return;
    }
    final SlotState old = new SlotState(slot.current, slot.epoch, slot.dirty, slot.loggedAt);
    slot.loggedAt = append(UndoKind.SLOT_CHANGED, slot, old, 0);
  }

  private int innermostMark() {
    return openCount == 0 ? 0 : openUpdaters[openCount - 1].mark;
  }

  void logClearStorage(final JournalAccountV2 account) {
    final StorageState old = new StorageState(account.storageEpoch, account.storageCleared);
    append(UndoKind.STORAGE_CLEARED, account, old, 0);
  }

  void logWarmUp(final JournalAccountV2.Slot slot) {
    append(UndoKind.SLOT_WARMED, slot, null, 0);
  }

  private int append(
      final UndoKind kind,
      final Object target,
      @Nullable final Object oldObject,
      final long oldLong) {
    if (size == kinds.length) {
      final int capacity = size * 2;
      kinds = Arrays.copyOf(kinds, capacity);
      targets = Arrays.copyOf(targets, capacity);
      oldObjects = Arrays.copyOf(oldObjects, capacity);
      oldLongs = Arrays.copyOf(oldLongs, capacity);
    }
    kinds[size] = kind;
    targets[size] = target;
    oldObjects[size] = oldObject;
    oldLongs[size] = oldLong;
    return size++;
  }

  void undoTo(final int mark) {
    for (int i = size - 1; i >= mark; i--) {
      final Object old = oldObjects[i];
      switch (kinds[i]) {
        case NONCE_CHANGED -> ((JournalAccountV2) targets[i]).nonce = oldLongs[i];
        case BALANCE_CHANGED -> ((JournalAccountV2) targets[i]).balance = (Wei) old;
        case CODE_CHANGED -> ((JournalAccountV2) targets[i]).code = (Code) old;
        case SLOT_CHANGED -> {
          final JournalAccountV2.Slot slot = (JournalAccountV2.Slot) targets[i];
          final SlotState state = (SlotState) old;
          slot.current = state.current();
          slot.epoch = state.epoch();
          slot.dirty = state.dirty();
          slot.loggedAt = state.loggedAt();
        }
        case STORAGE_CLEARED -> {
          final JournalAccountV2 account = (JournalAccountV2) targets[i];
          final StorageState state = (StorageState) old;
          account.storageEpoch = state.epoch();
          account.storageCleared = state.cleared();
        }
        case SLOT_WARMED -> ((JournalAccountV2.Slot) targets[i]).warm = false;
        case ACCOUNT_TOUCHED -> ((JournalAccountV2) targets[i]).touchedAt = (int) oldLongs[i];
        case ACCOUNT_CREATED_OR_DELETED -> {
          final JournalAccountV2 account = (JournalAccountV2) targets[i];
          final AccountState state = (AccountState) old;
          account.exists = state.exists();
          account.fresh = state.fresh();
          account.nonce = state.nonce();
          account.balance = state.balance();
          account.code = state.code();
          account.storageEpoch = state.storageEpoch();
          account.storageCleared = state.storageCleared();
          account.touchedAt = state.touchedAt();
          account.deletedAt = state.deletedAt();
        }
      }
      targets[i] = null;
      oldObjects[i] = null;
    }
    size = mark;
  }

  // what changed since a mark

  /** Accounts touched since the mark that still exist, as a stacked updater would track them. */
  List<Account> touchedSince(final int mark) {
    final List<Account> touched = new ArrayList<>();
    for (int i = mark; i < size; i++) {
      // an account's latest touch is the one its touchedAt points to
      if ((kinds[i] == UndoKind.ACCOUNT_TOUCHED || kinds[i] == UndoKind.ACCOUNT_CREATED_OR_DELETED)
          && targets[i] instanceof JournalAccountV2 account
          && account.touchedAt == i
          && account.exists) {
        touched.add(account);
      }
    }
    return touched;
  }

  /** Accounts deleted since the mark and not created again. */
  List<Address> deletedSince(final int mark) {
    final List<Address> deleted = new ArrayList<>();
    for (int i = mark; i < size; i++) {
      if (kinds[i] == UndoKind.ACCOUNT_CREATED_OR_DELETED
          && targets[i] instanceof JournalAccountV2 account
          && account.deletedAt == i) {
        deleted.add(account.getAddress());
      }
    }
    return deleted;
  }

  /**
   * Writes the transaction's changes to the parent updater the way a stacked updater commits, so
   * the parent ends up with the same tracked accounts, deletions and storage writes.
   */
  void commitToParent() {
    for (final JournalAccountV2 account : accounts.values()) {
      if (account.deletedAt >= 0) {
        parent.deleteAccount(account.getAddress());
        continue;
      }
      if (account.touchedAt < 0) {
        continue;
      }
      final MutableAccount target;
      if (account.fresh) {
        target = parent.createAccount(account.getAddress(), account.nonce, account.balance);
      } else {
        target = parent.getAccount(account.getAddress());
        if (target == null) {
          throw new IllegalStateException(
              "Account " + account.getAddress() + " disappeared from the parent updater");
        }
      }
      // a parent that already tracks an empty account hands it back from createAccount unchanged
      target.setNonce(account.nonce);
      target.setBalance(account.balance);
      final Code code = account.code;
      if (code != null) {
        target.setCode(code.getBytes());
      }
      if (account.storageCleared) {
        target.clearStorage();
      }
      account.slots.forEach(
          (key, slot) -> {
            if (account.isWritten(slot)) {
              target.setStorageValue(key.value, slot.current);
            }
          });
    }
  }
}
