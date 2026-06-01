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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Address-indexed view of a {@link BlockAccessList}, built once per block. Does not resolve transaction
 * indices or materialize state — use {@link BlockAccessListOverlay} for per-transaction prior state.
 */
public final class BlockAccessListIndex {

  private final Map<Address, AccountEntry> accountEntries;

  private BlockAccessListIndex(final Map<Address, AccountEntry> accountEntries) {
    this.accountEntries = accountEntries;
  }

  public static BlockAccessListIndex of(final BlockAccessList blockAccessList) {
    final List<BlockAccessList.AccountChanges> accountChanges = blockAccessList.accountChanges();
    final Map<Address, AccountEntry> entries = HashMap.newHashMap(accountChanges.size());
    for (final BlockAccessList.AccountChanges changes : accountChanges) {
      entries.put(changes.address(), new AccountEntry(changes));
    }
    return new BlockAccessListIndex(entries);
  }

  public Optional<BlockAccessList.AccountChanges> getAccountChanges(final Address address) {
    final AccountEntry entry = accountEntries.get(address);
    return entry == null ? Optional.empty() : Optional.of(entry.accountChanges);
  }

  Optional<BlockAccessList.SlotChanges> getSlotChanges(
      final Address address, final StorageSlotKey storageSlotKey) {
    final AccountEntry entry = accountEntries.get(address);
    return entry == null ? Optional.empty() : entry.slotChanges(storageSlotKey);
  }

  private static final class AccountEntry {
    private final BlockAccessList.AccountChanges accountChanges;
    private volatile Map<StorageSlotKey, BlockAccessList.SlotChanges> storageBySlot;

    private AccountEntry(final BlockAccessList.AccountChanges accountChanges) {
      this.accountChanges = accountChanges;
    }

    Optional<BlockAccessList.SlotChanges> slotChanges(final StorageSlotKey storageSlotKey) {
      Map<StorageSlotKey, BlockAccessList.SlotChanges> slotIndex = storageBySlot;
      if (slotIndex == null) {
        synchronized (this) {
          slotIndex = storageBySlot;
          if (slotIndex == null) {
            final List<BlockAccessList.SlotChanges> storageChanges =
                accountChanges.storageChanges();
            if (storageChanges.isEmpty()) {
              slotIndex = Map.of();
            } else {
              final Map<StorageSlotKey, BlockAccessList.SlotChanges> built =
                  HashMap.newHashMap(storageChanges.size());
              for (final BlockAccessList.SlotChanges slotChange : storageChanges) {
                built.put(slotChange.slot(), slotChange);
              }
              slotIndex = built;
            }
            storageBySlot = slotIndex;
          }
        }
      }
      return Optional.ofNullable(slotIndex.get(storageSlotKey));
    }
  }
}
