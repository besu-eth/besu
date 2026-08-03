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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

/**
 * Derives the write and unique-access counts from a finished {@link BlockAccessList}.
 *
 * <p>The block access list keeps per-transaction change entries, so writes are counted as write
 * events — a slot written by three transactions counts three times — which is the honest answer to
 * "how much writing did this block do". Reads lose their repetition on the way into the list, so
 * only the unique read counts come from here; totals come from the accumulator read observer.
 *
 * <p>Deletions are reported as net outcomes: a slot counts as deleted when its last written value
 * is zero, and an account when the last state the block wrote for it is empty.
 */
public final class SlowBlockBalDerivation {

  private SlowBlockBalDerivation() {}

  /**
   * Folds a block access list into the per-block metrics.
   *
   * @param bal the finished block access list
   * @param metrics the block aggregate to populate
   */
  public static void apply(final BlockAccessList bal, final SlowBlockMetrics metrics) {
    long uniqueStorageSlots = 0L;
    long writeAccounts = 0L;
    long writeStorageSlots = 0L;
    long writeCode = 0L;
    long writeCodeBytes = 0L;
    long accountsDeleted = 0L;
    long storageSlotsDeleted = 0L;

    for (final AccountChanges account : bal.accountChanges()) {
      // A slot is listed either as written or as read, never both, so these do not overlap.
      uniqueStorageSlots += account.storageChanges().size() + account.storageReads().size();

      writeAccounts +=
          account.balanceChanges().size()
              + account.nonceChanges().size()
              + account.codeChanges().size();
      writeCode += account.codeChanges().size();
      for (final CodeChange codeChange : account.codeChanges()) {
        writeCodeBytes += codeChange.newCode().size();
      }

      for (final SlotChanges slot : account.storageChanges()) {
        writeStorageSlots += slot.changes().size();
        final StorageChange lastChange = slot.changes().getLast();
        if (lastChange.newValue() == null || lastChange.newValue().isZero()) {
          storageSlotsDeleted++;
        }
      }

      if (isEmptiedByBlock(account)) {
        accountsDeleted++;
      }
    }

    metrics.setBlockAccessListDerived(
        bal.accountChanges().size(),
        uniqueStorageSlots,
        writeAccounts,
        writeStorageSlots,
        writeCode,
        writeCodeBytes,
        accountsDeleted,
        storageSlotsDeleted);
  }

  /**
   * Whether the last state this block wrote for the account is empty. A zero final balance is
   * necessary — an account cannot be empty with a balance — and any nonce or code the block wrote
   * must also have ended at its empty value.
   */
  private static boolean isEmptiedByBlock(final AccountChanges account) {
    if (account.balanceChanges().isEmpty()
        || !account.balanceChanges().getLast().postBalance().isZero()) {
      return false;
    }
    if (!account.nonceChanges().isEmpty() && account.nonceChanges().getLast().newNonce() != 0L) {
      return false;
    }
    return account.codeChanges().isEmpty() || account.codeChanges().getLast().newCode().isEmpty();
  }
}
