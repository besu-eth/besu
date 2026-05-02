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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class AccessLocationTracker implements Eip7928AccessList {

  private final long blockAccessIndex;
  private final Map<Address, AccountAccessList> touchedAccounts = new ConcurrentHashMap<>();

  public AccessLocationTracker(final long blockAccessIndex) {
    this.blockAccessIndex = blockAccessIndex;
  }

  @Override
  public void clear() {
    touchedAccounts.clear();
  }

  @Override
  public void addTouchedAccount(final Address address) {
    // computeIfAbsent avoids allocating an AccountAccessList when the entry already exists
    // (unlike putIfAbsent which always allocates) and performs a single map lookup.
    touchedAccounts.computeIfAbsent(address, AccountAccessList::new);
  }

  @Override
  public void addSlotAccessForAccount(final Address address, final UInt256 slotKey) {
    // Single map lookup instead of putIfAbsent + get, and no wasted AccountAccessList allocation
    // when the entry already exists.
    touchedAccounts.computeIfAbsent(address, AccountAccessList::new).addSlotAccess(slotKey);
  }

  public Collection<AccountAccessList> getTouchedAccounts() {
    return touchedAccounts.values();
  }

  public static final class AccountAccessList {
    private final Address address;
    // Backed by ConcurrentHashMap; equivalent concurrency to vertx ConcurrentHashSet, JDK-only,
    // and avoids pulling an extra dependency into this hot path.
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
        deletedAddressesCol.isEmpty()
            ? Collections.emptySet()
            : newSizedHashSet(deletedAddressesCol);

    final Collection<? extends UpdateTrackingAccount<?>> updatedAccountsCol =
        stackedUpdater.getUpdatedAccounts();
    final Set<Address> updatedAddresses;
    if (updatedAccountsCol.isEmpty()) {
      updatedAddresses = Collections.emptySet();
    } else {
      updatedAddresses = new HashSet<>(expectedCapacity(updatedAccountsCol.size()));
      for (final UpdateTrackingAccount<?> u : updatedAccountsCol) {
        updatedAddresses.add(u.getAddress());
      }
    }

    for (final Map.Entry<Address, AccountAccessList> entry : touchedAccounts.entrySet()) {
      final Address address = entry.getKey();
      final Set<UInt256> touchedSlots = entry.getValue().getSlots();
      final AccountChangesBuilder accountBuilder = builder.getOrCreateAccountBuilder(address);

      final boolean isDeleted = deletedAddresses.contains(address);
      // Fast read-only path: account was either deleted, or not updated in this updater.
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
          accountBuilder.addStorageChange(slotKeyObj, updatedValue);
        } else {
          accountBuilder.addStorageRead(slotKeyObj);
        }
      }
    }
    return builder.build();
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

  private static Set<Address> newSizedHashSet(final Collection<Address> src) {
    final HashSet<Address> set = new HashSet<>(expectedCapacity(src.size()));
    set.addAll(src);
    return set;
  }

  // Pre-size to avoid rehashing given HashMap's default 0.75 load factor.
  private static int expectedCapacity(final int expectedSize) {
    return (int) (expectedSize / 0.75f) + 1;
  }
}
