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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.impl.ConcurrentHashSet;
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
    touchedAccounts.putIfAbsent(address, new AccountAccessList(address));
  }

  @Override
  public void addSlotAccessForAccount(final Address address, final UInt256 slotKey) {
    addTouchedAccount(address);
    touchedAccounts.get(address).addSlotAccess(slotKey);
  }

  public Collection<AccountAccessList> getTouchedAccounts() {
    return touchedAccounts.values();
  }

  public static final class AccountAccessList {
    private final Address address;
    private final Set<UInt256> slots = new ConcurrentHashSet<>();

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
    StackedUpdater<?, ?> stackedUpdater = (StackedUpdater<?, ?>) updater;
    PartialBlockAccessViewBuilder builder = new PartialBlockAccessViewBuilder();
    builder.withTxIndex(this.blockAccessIndex);
    for (Map.Entry<Address, AccountAccessList> accountAccessListEntry :
        touchedAccounts.entrySet()) {
      final Address address = accountAccessListEntry.getKey();

      final AccountChangesBuilder accountBuilder = builder.getOrCreateAccountBuilder(address);

      if (stackedUpdater.getDeletedAccountAddresses().contains(address)
          || stackedUpdater.getUpdatedAccounts().stream()
              .map(UpdateTrackingAccount::getAddress)
              .filter(a -> a.equals(address))
              .findAny()
              .isEmpty()) {
        for (UInt256 slot : accountAccessListEntry.getValue().getSlots()) {
          final StorageSlotKey slotKeyObj = new StorageSlotKey(slot);
          accountBuilder.addStorageRead(slotKeyObj);
        }
        if (stackedUpdater.getDeletedAccountAddresses().contains(address)) {
          final Optional<Account> originalAccount = findOriginalAccount(stackedUpdater, address);
          if (originalAccount.isPresent() && !originalAccount.get().getBalance().isZero()) {
            accountBuilder.withPostBalance(Wei.ZERO);
          }
        }
        continue;
      }

      final UpdateTrackingAccount<?> account =
          (UpdateTrackingAccount<?>) stackedUpdater.get(address);

      if (account != null) {
        final Account wrappedAccount = account.getWrappedAccount();

        if (wrappedAccount != null) {
          Wei newBalance = account.getBalance();
          Wei originalBalance = wrappedAccount.getBalance();
          if (!newBalance.equals(originalBalance)) {
            accountBuilder.withPostBalance(newBalance);
          }

          long newNonce = account.getNonce();
          long originalNonce = wrappedAccount.getNonce();
          if (Long.compareUnsigned(newNonce, originalNonce) > 0) {
            accountBuilder.withNonceChange(newNonce);
          }

          Bytes newCode = account.getCode();
          Bytes originalCode = wrappedAccount.getCode();
          if (!newCode.equals(originalCode)) {
            accountBuilder.withNewCode(newCode);
          }
        } else {
          Wei newBalance = account.getBalance();
          if (!newBalance.isZero()) {
            accountBuilder.withPostBalance(newBalance);
          }

          final long newNonce = account.getNonce();
          if (newNonce != 0L) {
            accountBuilder.withNonceChange(newNonce);
          }

          Bytes newCode = account.getCode();
          if (!newCode.isEmpty()) {
            accountBuilder.withNewCode(newCode);
          }
        }

        final Map<UInt256, UInt256> updatedStorage = account.getUpdatedStorage();
        final Set<UInt256> txListTouchedSlots = accountAccessListEntry.getValue().getSlots();
        for (UInt256 touchedSlot : txListTouchedSlots) {
          StorageSlotKey slotKeyObj = new StorageSlotKey(touchedSlot);

          if (updatedStorage.containsKey(touchedSlot)) {
            final UInt256 originalValue = account.getOriginalStorageValue(touchedSlot);
            final UInt256 updatedValue = updatedStorage.get(touchedSlot);

            final boolean isSet = originalValue == null;
            final boolean isReset = updatedValue == null;
            final boolean isUpdate = originalValue != null && !originalValue.equals(updatedValue);
            final boolean isWrite = isSet || isReset || isUpdate;

            if (isWrite) {
              accountBuilder.addStorageChange(slotKeyObj, updatedValue);
            } else {
              accountBuilder.addStorageRead(slotKeyObj);
            }
          } else {
            accountBuilder.addStorageRead(slotKeyObj);
          }
        }
      } else {
        for (UInt256 slot : accountAccessListEntry.getValue().getSlots()) {
          final StorageSlotKey slotKeyObj = new StorageSlotKey(slot);
          accountBuilder.addStorageRead(slotKeyObj);
        }
      }
    }
    return builder.build();
  }

  private Optional<Account> findOriginalAccount(final WorldUpdater updater, final Address address) {
    final Account account = updater.get(address);
    if (account != null) {
      return Optional.of(account);
    }
    return updater
        .parentUpdater()
        .flatMap(parentUpdater -> findOriginalAccount(parentUpdater, address));
  }
}
