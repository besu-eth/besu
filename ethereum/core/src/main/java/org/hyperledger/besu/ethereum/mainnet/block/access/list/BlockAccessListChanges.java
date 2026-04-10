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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListChanges {

  private BlockAccessListChanges() {}

  public static List<AccountFinalChanges> latestChanges(final BlockAccessList blockAccessList) {
    final List<AccountFinalChanges> accountFinalChanges = new ArrayList<>();

    for (final BlockAccessList.AccountChanges accountChanges : blockAccessList.accountChanges()) {
      if (!accountChanges.hasAnyChange()) {
        continue;
      }

      final List<StorageFinalChange> storageFinalChanges = new ArrayList<>();
      for (final BlockAccessList.SlotChanges slotChanges : accountChanges.storageChanges()) {
        final Optional<BlockAccessList.StorageChange> latestStorageChange =
            lastOf(slotChanges.changes());
        if (latestStorageChange.isPresent()) {
          storageFinalChanges.add(
              new StorageFinalChange(
                  slotChanges.slot(),
                  latestStorageChange.get().newValue() == null
                      ? UInt256.ZERO
                      : latestStorageChange.get().newValue()));
        }
      }

      accountFinalChanges.add(
          new AccountFinalChanges(
              accountChanges.address(),
              lastOf(accountChanges.balanceChanges())
                  .map(BlockAccessList.BalanceChange::postBalance),
              lastOf(accountChanges.nonceChanges()).map(BlockAccessList.NonceChange::newNonce),
              lastOf(accountChanges.codeChanges()).map(BlockAccessList.CodeChange::newCode),
              storageFinalChanges));
    }

    return accountFinalChanges;
  }

  public record AccountFinalChanges(
      Address address,
      Optional<Wei> balance,
      Optional<Long> nonce,
      Optional<Bytes> code,
      List<StorageFinalChange> storageChanges) {}

  public record StorageFinalChange(StorageSlotKey slot, UInt256 value) {}

  private static <T> Optional<T> lastOf(final List<T> list) {
    return list.isEmpty() ? Optional.empty() : Optional.of(list.getLast());
  }
}
