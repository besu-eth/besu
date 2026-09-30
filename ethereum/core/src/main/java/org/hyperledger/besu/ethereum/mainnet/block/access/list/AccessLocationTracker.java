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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView.AccountChangesBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView.PartialBlockAccessViewBuilder;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.Eip7928AccessList;
import org.hyperledger.besu.evm.worldstate.StackedUpdater;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class AccessLocationTracker implements Eip7928AccessList {

  private final long blockAccessIndex;
  private final boolean sharedIndex;
  private final Map<Address, AccountAccessList> touchedAccounts = new ConcurrentHashMap<>();

  /**
   * For a shared index only: accounts as they were when the index began, captured the first time
   * the index writes them. The system calls, and the withdrawals after the last transaction, share
   * one index and each builds a view, so every view diffs against these rather than against the
   * previous call's result.
   */
  private final Map<Address, IndexStartAccount> indexStartAccounts = new HashMap<>();

  public AccessLocationTracker(final long blockAccessIndex, final boolean sharedIndex) {
    this.blockAccessIndex = blockAccessIndex;
    this.sharedIndex = sharedIndex;
  }

  @Override
  public void clear() {
    touchedAccounts.clear();
    indexStartAccounts.clear();
  }

  @Override
  public void addTouchedAccount(final Address address) {
    touchedAccounts.computeIfAbsent(address, AccountAccessList::new);
  }

  @Override
  public void addSlotAccessForAccount(final Address address, final UInt256 slotKey) {
    touchedAccounts.computeIfAbsent(address, AccountAccessList::new).addSlotAccess(slotKey);
  }

  public static final class AccountAccessList {
    private final Address address;
    private final Set<UInt256> slots = ConcurrentHashMap.newKeySet();

    public AccountAccessList(final Address address) {
      this.address = address;
    }

    public void addSlotAccess(final UInt256 slotKey) {
      slots.add(slotKey);
    }

    public Address getAddress() {
      return address;
    }

    public Set<UInt256> getSlots() {
      return slots;
    }

    @Override
    public String toString() {
      return "AccountAccessList{" + "address=" + address + ", slots=" + slots + '}';
    }
  }

  public PartialBlockAccessView createPartialBlockAccessView(final WorldUpdater updater) {
    final StackedUpdater<?, ?> stackedUpdater = (StackedUpdater<?, ?>) updater;
    final PartialBlockAccessViewBuilder builder = new PartialBlockAccessViewBuilder();
    builder.withTxIndex(this.blockAccessIndex);

    final Collection<Address> deletedAddressesCol = stackedUpdater.getDeletedAccountAddresses();
    final Set<Address> deletedAddresses =
        deletedAddressesCol.isEmpty() ? Collections.emptySet() : new HashSet<>(deletedAddressesCol);

    final Collection<? extends UpdateTrackingAccount<?>> updatedAccountsCol =
        stackedUpdater.getUpdatedAccounts();
    final Set<Address> updatedAddresses;
    if (updatedAccountsCol.isEmpty()) {
      updatedAddresses = Collections.emptySet();
    } else {
      updatedAddresses = HashSet.newHashSet(updatedAccountsCol.size());
      for (final UpdateTrackingAccount<?> u : updatedAccountsCol) {
        updatedAddresses.add(u.getAddress());
      }
    }

    for (final Map.Entry<Address, AccountAccessList> entry : touchedAccounts.entrySet()) {
      final Address address = entry.getKey();
      final Set<UInt256> touchedSlots = entry.getValue().getSlots();
      final AccountChangesBuilder accountBuilder = builder.getOrCreateAccountBuilder(address);

      if (sharedIndex) {
        addChangesSinceIndexStart(
            accountBuilder,
            stackedUpdater,
            address,
            touchedSlots,
            deletedAddresses.contains(address),
            updatedAddresses.contains(address));
        continue;
      }

      final boolean isDeleted = deletedAddresses.contains(address);
      if (isDeleted || !updatedAddresses.contains(address)) {
        for (final UInt256 slot : touchedSlots) {
          accountBuilder.addStorageRead(new StorageSlotKey(slot));
        }
        if (isDeleted) {
          final Account originalAccount = findOriginalAccount(stackedUpdater, address);
          if (originalAccount != null && !originalAccount.getBalance().isZero()) {
            accountBuilder.withPostBalance(Wei.ZERO);
          }
        }
        continue;
      }

      final UpdateTrackingAccount<?> account =
          (UpdateTrackingAccount<?>) stackedUpdater.get(address);
      if (account == null) {
        for (final UInt256 slot : touchedSlots) {
          accountBuilder.addStorageRead(new StorageSlotKey(slot));
        }
        continue;
      }

      final Account wrappedAccount = account.getWrappedAccount();
      final Wei newBalance = account.getBalance();
      final long newNonce = account.getNonce();
      final Bytes newCode = account.getCode();

      if (wrappedAccount != null) {
        if (!newBalance.equals(wrappedAccount.getBalance())) {
          accountBuilder.withPostBalance(newBalance);
        }
        if (Long.compareUnsigned(newNonce, wrappedAccount.getNonce()) > 0) {
          accountBuilder.withNonceChange(newNonce);
        }
        if (!newCode.equals(wrappedAccount.getCode())) {
          accountBuilder.withNewCode(newCode);
        }
      } else {
        if (!newBalance.isZero()) {
          accountBuilder.withPostBalance(newBalance);
        }
        if (newNonce != 0L) {
          accountBuilder.withNonceChange(newNonce);
        }
        if (!newCode.isEmpty()) {
          accountBuilder.withNewCode(newCode);
        }
      }

      final Map<UInt256, UInt256> updatedStorage = account.getUpdatedStorage();
      for (final UInt256 touchedSlot : touchedSlots) {
        final StorageSlotKey slotKeyObj = new StorageSlotKey(touchedSlot);

        final UInt256 updatedValue = updatedStorage.get(touchedSlot);
        final boolean present = updatedValue != null || updatedStorage.containsKey(touchedSlot);

        if (!present) {
          accountBuilder.addStorageRead(slotKeyObj);
          continue;
        }

        final UInt256 originalValue = account.getOriginalStorageValue(touchedSlot);
        final boolean isSet = originalValue == null;
        final boolean isReset = updatedValue == null;
        final boolean isUpdate = originalValue != null && !originalValue.equals(updatedValue);
        if (isSet || isReset || isUpdate) {
          accountBuilder.addStorageChange(
              slotKeyObj, originalValue != null ? originalValue : UInt256.ZERO, updatedValue);
        } else {
          accountBuilder.addStorageRead(slotKeyObj);
        }
      }
    }
    return builder.build();
  }

  private void addChangesSinceIndexStart(
      final AccountChangesBuilder accountBuilder,
      final StackedUpdater<?, ?> stackedUpdater,
      final Address address,
      final Set<UInt256> touchedSlots,
      final boolean isDeleted,
      final boolean isUpdated) {
    final IndexStartAccount indexStart;
    final Account account;
    if (isDeleted) {
      indexStart =
          indexStartAccounts.computeIfAbsent(
              address, __ -> new IndexStartAccount(findOriginalAccount(stackedUpdater, address)));
      account = null;
    } else if (isUpdated || indexStartAccounts.containsKey(address)) {
      account = stackedUpdater.get(address);
      if (isUpdated && account instanceof UpdateTrackingAccount<?> updatedAccount) {
        captureIndexStart(updatedAccount, touchedSlots);
      }
      indexStart = indexStartAccounts.get(address);
    } else {
      addStorageReads(accountBuilder, touchedSlots);
      return;
    }

    if (indexStart == null) {
      addStorageReads(accountBuilder, touchedSlots);
      return;
    }
    if (account == null) {
      // Deleted at this index, by this view's updater or an earlier one.
      addStorageReads(accountBuilder, touchedSlots);
      if (!indexStart.balance.isZero()) {
        accountBuilder.withPostBalance(Wei.ZERO);
      }
      return;
    }

    final Wei newBalance = account.getBalance();
    final long newNonce = account.getNonce();
    final Bytes newCode = account.getCode();
    if (!newBalance.equals(indexStart.balance)) {
      accountBuilder.withPostBalance(newBalance);
    }
    if (Long.compareUnsigned(newNonce, indexStart.nonce) > 0) {
      accountBuilder.withNonceChange(newNonce);
    }
    if (!newCode.equals(indexStart.code)) {
      accountBuilder.withNewCode(newCode);
    }

    for (final UInt256 touchedSlot : touchedSlots) {
      final StorageSlotKey slotKeyObj = new StorageSlotKey(touchedSlot);
      final UInt256 originalValue = indexStart.storageValue(touchedSlot);
      if (originalValue == null) {
        accountBuilder.addStorageRead(slotKeyObj);
        continue;
      }
      final UInt256 updatedValue = account.getStorageValue(touchedSlot);
      if (originalValue.equals(updatedValue)) {
        accountBuilder.addStorageRead(slotKeyObj);
      } else {
        accountBuilder.addStorageChange(slotKeyObj, originalValue, updatedValue);
      }
    }
  }

  private void captureIndexStart(
      final UpdateTrackingAccount<?> account, final Set<UInt256> touchedSlots) {
    final IndexStartAccount indexStart =
        indexStartAccounts.computeIfAbsent(
            account.getAddress(), __ -> new IndexStartAccount(account.getWrappedAccount()));
    final Map<UInt256, UInt256> updatedStorage = account.getUpdatedStorage();
    for (final UInt256 slot : touchedSlots) {
      if (updatedStorage.containsKey(slot) && indexStart.storageValue(slot) == null) {
        indexStart.putStorageValue(slot, account.getOriginalStorageValue(slot));
      }
    }
  }

  private static void addStorageReads(
      final AccountChangesBuilder accountBuilder, final Set<UInt256> slots) {
    for (final UInt256 slot : slots) {
      accountBuilder.addStorageRead(new StorageSlotKey(slot));
    }
  }

  private static final class IndexStartAccount {
    private final Wei balance;
    private final long nonce;
    private final Bytes code;
    // Created on the first storage write, since withdrawal-only accounts never write storage.
    private Map<UInt256, UInt256> storage;

    private IndexStartAccount(final Account account) {
      this.balance = account != null ? account.getBalance() : Wei.ZERO;
      this.nonce = account != null ? account.getNonce() : 0L;
      this.code = account != null ? account.getCode() : Bytes.EMPTY;
    }

    private UInt256 storageValue(final UInt256 slot) {
      return storage != null ? storage.get(slot) : null;
    }

    private void putStorageValue(final UInt256 slot, final UInt256 value) {
      if (storage == null) {
        storage = new HashMap<>();
      }
      storage.put(slot, value);
    }
  }

  private Account findOriginalAccount(final WorldUpdater updater, final Address address) {
    WorldUpdater current = updater;
    while (current != null) {
      final Account account = current.get(address);
      if (account != null) {
        return account;
      }
      current = current.parentUpdater().orElse(null);
    }
    return null;
  }
}
