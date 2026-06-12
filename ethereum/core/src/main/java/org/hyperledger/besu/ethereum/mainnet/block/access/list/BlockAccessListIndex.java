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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable O(1) lookup index over a {@link BlockAccessList}.
 *
 * <p>Building this index is O(|accounts| + |slots|). Once built, {@link #findAccount(Address)} and
 * {@link #findSlotChanges(Address, StorageSlotKey)} each complete in O(1).
 *
 * <p>This class intentionally does not duplicate the underlying {@link BlockAccessList}; it only
 * holds pointer maps into it.
 */
public final class BlockAccessListIndex {

  private final BlockAccessList bal;

  /** address → AccountChanges (O(1) account-level lookup). */
  private final Map<Address, BlockAccessList.AccountChanges> accountIndex;

  /** address → (slot → sorted StorageChange list) for O(1) slot-level write lookup. */
  private final Map<Address, Map<StorageSlotKey, List<BlockAccessList.StorageChange>>> slotIndex;

  /**
   * Builds index structures over {@code bal}. The {@link BlockAccessList} is not copied.
   *
   * @param bal the block access list to index
   */
  public BlockAccessListIndex(final BlockAccessList bal) {
    this.bal = bal;

    final int n = bal.accountChanges().size();
    final Map<Address, BlockAccessList.AccountChanges> accountMap = new HashMap<>(n * 2);
    final Map<Address, Map<StorageSlotKey, List<BlockAccessList.StorageChange>>> slotMap =
        new HashMap<>(n * 2);

    for (final BlockAccessList.AccountChanges ac : bal.accountChanges()) {
      accountMap.put(ac.address(), ac);

      if (!ac.storageChanges().isEmpty()) {
        final Map<StorageSlotKey, List<BlockAccessList.StorageChange>> perSlot =
            new HashMap<>(ac.storageChanges().size() * 2);
        for (final BlockAccessList.SlotChanges sc : ac.storageChanges()) {
          perSlot.put(sc.slot(), sc.changes());
        }
        slotMap.put(ac.address(), perSlot);
      }
    }

    this.accountIndex = Collections.unmodifiableMap(accountMap);
    this.slotIndex = Collections.unmodifiableMap(slotMap);
  }

  /**
   * Returns the underlying {@link BlockAccessList}.
   *
   * @return the block access list
   */
  public BlockAccessList getBlockAccessList() {
    return bal;
  }

  /**
   * O(1) lookup for all changes recorded for {@code address} in the BAL.
   *
   * @param address the account address to look up
   * @return the {@link BlockAccessList.AccountChanges} for {@code address}, or {@code null} if
   *     {@code address} is not present in the BAL
   */
  public BlockAccessList.AccountChanges findAccount(final Address address) {
    return accountIndex.get(address);
  }

  /**
   * O(1) lookup for all write-changes recorded for a specific storage slot.
   *
   * <p>The returned list is in ascending {@code txIndex} order (as guaranteed by the BAL builder).
   *
   * @param address the account address
   * @param slot the storage slot key
   * @return the list of {@link BlockAccessList.StorageChange} entries for {@code (address, slot)},
   *     or {@code null} if no write changes exist for that pair
   */
  public List<BlockAccessList.StorageChange> findSlotChanges(
      final Address address, final StorageSlotKey slot) {
    final Map<StorageSlotKey, List<BlockAccessList.StorageChange>> perSlot = slotIndex.get(address);
    return perSlot == null ? null : perSlot.get(slot);
  }
}
