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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

public class TransactionCollisionDetector {

  /**
   * Checks if there is a conflict between the current block's state and the given transaction.
   *
   * <p>This method detects conflicts between the transaction and the block's state by checking if
   * the transaction modifies the same addresses and storage slots that are already modified by the
   * block. A conflict occurs in two cases: 1. If the transaction touches an address that is also
   * modified in the block, and the account details (excluding storage) are identical. In this case,
   * it checks if there is an overlap in the storage slots affected by both the transaction and the
   * block. 2. If the account details differ between the transaction and the block (excluding
   * storage), it immediately detects a conflict.
   *
   * <p>The method returns `true` if any such conflict is found, otherwise `false`.
   *
   * @param transaction The transaction to check for conflicts with the block's state.
   * @param parallelizedTransactionContext The context for the parallelized execution of the
   *     transaction.
   * @param blockAccumulator The accumulator containing the state updates of the current block.
   * @return true if there is a conflict between the transaction and the block's state, otherwise
   *     false.
   */
  public boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    return hasCollision(
        transaction, miningBeneficiary, parallelizedTransactionContext, blockAccumulator, null);
  }

  /**
   * Like {@link #hasCollision(Transaction, Address, ParallelizedTransactionContext,
   * PathBasedWorldStateUpdateAccumulator)}, except that an account of which earlier transactions
   * changed only the balance, while this transaction did not depend on that balance beyond it
   * sufficing for what the transaction spent and did not see the account empty, is no collision.
   * Such accounts are added to {@code creditedAccounts}: taking the result over must then carry the
   * transaction's balance change onto the balance the block has reached.
   *
   * @param transaction The transaction to check for conflicts with the block's state.
   * @param miningBeneficiary The beneficiary of the block's rewards.
   * @param parallelizedTransactionContext The context for the parallelized execution of the
   *     transaction.
   * @param blockAccumulator The accumulator containing the state updates of the current block.
   * @param creditedAccounts receives the accounts whose balance change must be carried over, or
   *     null to treat every balance change as a collision
   * @return true if there is a conflict between the transaction and the block's state
   */
  public boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final @Nullable List<Address> creditedAccounts) {
    final Set<Address> addressesTouchedByTransaction =
        getAddressesTouchedByTransaction(
            transaction, Optional.of(parallelizedTransactionContext.transactionAccumulator()));
    // every earlier transaction paid the beneficiary, so a transaction that touched it can only
    // keep its result by having paid it too, without depending on its balance
    if (addressesTouchedByTransaction.contains(miningBeneficiary) && creditedAccounts == null) {
      return true;
    }
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    for (final Address next : addressesTouchedByTransaction) {
      final BonsaiValue<? extends BonsaiAccount> inBlock =
          blockAccumulator.getAccountsToUpdate().get(next);
      if (inBlock == null) {
        continue;
      }
      if (!areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
        if (creditedAccounts == null
            || !isBalanceChangeCarried(transaction, transactionAccumulator, next, inBlock)) {
          return true;
        }
        creditedAccounts.add(next);
      }
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> slots =
          transactionAccumulator.getStorageToUpdate().get(next);
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> blockSlots =
          blockAccumulator.getStorageToUpdate().get(next);
      if (slots == null || blockSlots == null) {
        continue;
      }
      // the block may have changed many slots of a popular contract, the transaction few
      for (final StorageSlotKey slot : slots.keySet()) {
        final BonsaiValue<UInt256> inBlockSlot = blockSlots.get(slot);
        if (inBlockSlot != null && !inBlockSlot.isUnchanged()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Whether earlier transactions changed only the balance of an account whose balance the
   * transaction did not read, and spent from only while it sufficed by a margin the block's balance
   * keeps, so the transaction ends the same on the balance the block has reached. Emptiness depends
   * on the balance and decides call costs and state clearing, so an account empty on either side
   * does not qualify.
   */
  private static boolean isBalanceChangeCarried(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock) {
    final Set<Address> observedBalances = transactionAccumulator.getObservedBalances();
    // the sender's balance pays for the gas
    if (observedBalances == null
        || observedBalances.contains(address)
        || address.equals(transaction.getSender())) {
      return false;
    }
    final BonsaiValue<? extends BonsaiAccount> inTransaction =
        transactionAccumulator.getAccountsToUpdate().get(address);
    final BonsaiAccount blockPrior = inBlock.getPrior();
    final BonsaiAccount blockUpdated = inBlock.getUpdated();
    if (blockPrior == null
        || blockUpdated == null
        || inTransaction == null
        || inTransaction.getPrior() == null
        || inTransaction.getUpdated() == null
        || blockPrior.getNonce() != blockUpdated.getNonce()
        || !blockPrior.getCodeHash().equals(blockUpdated.getCodeHash())
        || blockPrior.isEmpty()
        || blockUpdated.isEmpty()
        || inTransaction.getUpdated().isEmpty()) {
      return false;
    }
    final Wei startingBalance = inTransaction.getPrior().getBalance();
    return transactionAccumulator
        .getBalanceMargin(address)
        // every spending found the balance sufficient by at least the margin
        .map(margin -> blockUpdated.getBalance().add(margin).compareTo(startingBalance) >= 0)
        // without spending, only a credit can be carried over
        .orElseGet(() -> inTransaction.getUpdated().getBalance().compareTo(startingBalance) >= 0);
  }

  /**
   * Retrieves the set of addresses that were touched by a transaction. This includes the sender and
   * recipient of the transaction, as well as any addresses that were read from or written to by the
   * transaction's execution.
   *
   * @param transaction The transaction to analyze.
   * @param accumulator An optional accumulator containing state changes made by the transaction.
   * @return A set of addresses touched by the transaction.
   */
  public Set<Address> getAddressesTouchedByTransaction(
      final Transaction transaction,
      final Optional<PathBasedWorldStateUpdateAccumulator<?>> accumulator) {
    HashSet<Address> addresses = new HashSet<>();
    addresses.add(transaction.getSender());
    if (transaction.getTo().isPresent()) {
      addresses.add(transaction.getTo().get());
    }
    accumulator.ifPresent(
        pathBasedWorldStateUpdateAccumulator -> {
          pathBasedWorldStateUpdateAccumulator
              .getAccountsToUpdate()
              .forEach(
                  (address, pathBasedValue) -> {
                    addresses.add(address);
                  });
          addresses.addAll(pathBasedWorldStateUpdateAccumulator.getDeletedAccountAddresses());
        });

    return addresses;
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
