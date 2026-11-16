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

import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

/** Advances a world state from the final values a block access list records. */
public final class BlockAccessListStateApplier {

  private BlockAccessListStateApplier() {}

  /**
   * Writes the final value of every account and slot the list changes into the updater; an account
   * that ends empty is deleted (EIP-161/EIP-7523). Accounts the list only reads are untouched. The
   * updater is not committed.
   *
   * @param updater the updater to write to
   * @param blockAccessList the block access list whose final values are applied
   */
  public static void apply(final WorldUpdater updater, final BlockAccessList blockAccessList) {
    for (final BlockAccessListChanges.AccountFinalChanges changes :
        BlockAccessListChanges.latestChanges(blockAccessList)) {
      if (!changes.hasAnyChange()) {
        continue;
      }
      final MutableAccount account = updater.getOrCreate(changes.address());
      changes.nonce().ifPresent(account::setNonce);
      changes.balance().ifPresent(account::setBalance);
      changes.code().ifPresent(account::setCode);
      for (final BlockAccessListChanges.StorageFinalChange slot : changes.storageChanges()) {
        account.setStorageValue(slot.slot().getSlotKey().orElseThrow(), slot.value());
      }
      if (account.isEmpty()) {
        updater.deleteAccount(changes.address());
      }
    }
  }
}
