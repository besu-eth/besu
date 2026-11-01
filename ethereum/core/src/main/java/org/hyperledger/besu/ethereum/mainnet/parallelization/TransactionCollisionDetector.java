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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.Map;

import org.apache.tuweni.units.bigints.UInt256;

public class TransactionCollisionDetector {

  /**
   * Checks whether the block changed state that the transaction read or wrote while it executed
   * speculatively against the parent state.
   *
   * <p>The transaction collides when it touched the mining beneficiary, an account whose nonce,
   * balance or code the block changed, or a slot of an account whose slot the block changed or
   * whose storage the block cleared.
   *
   * @param transaction the transaction that executed speculatively
   * @param miningBeneficiary the mining beneficiary of the block
   * @param parallelizedTransactionContext the speculative result with the state it read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @return true if the speculative result must not be used
   */
  public boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    if (isAddressTouchedByTransaction(transaction, transactionAccumulator, miningBeneficiary)) {
      return true;
    }
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
      if (hasCollision(address, transactionAccumulator, blockAccumulator)) {
        return true;
      }
    }
    for (final Address address : transactionAccumulator.getDeletedAccountAddresses()) {
      if (hasCollision(address, transactionAccumulator, blockAccumulator)) {
        return true;
      }
    }
    return hasCollision(transaction.getSender(), transactionAccumulator, blockAccumulator)
        || transaction
            .getTo()
            .filter(to -> hasCollision(to, transactionAccumulator, blockAccumulator))
            .isPresent();
  }

  /**
   * Checks whether the transaction touched the address: its sender, its recipient, or an account
   * its execution read, wrote or deleted.
   *
   * @param transaction the transaction that executed speculatively
   * @param transactionAccumulator the state the transaction read and wrote
   * @param address the address to look for
   * @return true if the transaction touched the address
   */
  public boolean isAddressTouchedByTransaction(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final Address address) {
    return address.equals(transaction.getSender())
        || transaction.getTo().filter(address::equals).isPresent()
        || transactionAccumulator.getAccountsToUpdate().containsKey(address)
        || transactionAccumulator.getDeletedAccountAddresses().contains(address);
  }

  private boolean hasCollision(
      final Address address,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    final BonsaiValue<? extends BonsaiAccount> inBlock =
        blockAccumulator.getAccountsToUpdate().get(address);
    if (inBlock == null) {
      return false;
    }
    if (!areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
      return true;
    }
    final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> slots =
        transactionAccumulator.getStorageToUpdate().get(address);
    if (slots == null || slots.isEmpty()) {
      return false;
    }
    // re-creating an account drops the block's entries of the slots it cleared, so the
    // transaction's slots can't be compared with them
    if (blockAccumulator.getStorageToClear().contains(address)) {
      return true;
    }
    final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> blockSlots =
        blockAccumulator.getStorageToUpdate().get(address);
    if (blockSlots == null) {
      return false;
    }
    // the block may have changed many slots of a popular contract, the transaction few
    for (final StorageSlotKey slot : slots.keySet()) {
      final BonsaiValue<UInt256> inBlockSlot = blockSlots.get(slot);
      if (inBlockSlot != null && inBlockSlot.isChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compares the state of two accounts to check if their key properties are identical, excluding
   * any differences in their storage.
   *
   * <p>This method compares the following account properties: - Nonce - Balance - Code Hash
   *
   * <p>It returns true if these properties are equal for both accounts, and false otherwise. Note
   * that this comparison does not include the account's storage.
   *
   * @param prior The first account to compare (could be null).
   * @param next The second account to compare (could be null).
   * @return true if the account state properties are equal excluding storage, false otherwise.
   */
  private boolean areAccountDetailsEqualExcludingStorage(
      final BonsaiAccount prior, final BonsaiAccount next) {
    return (prior == null && next == null)
        || (prior != null
            && next != null
            && prior.getNonce() == next.getNonce()
            && prior.getBalance().equals(next.getBalance())
            && prior.getCodeHash().equals(next.getCodeHash()));
  }
}
