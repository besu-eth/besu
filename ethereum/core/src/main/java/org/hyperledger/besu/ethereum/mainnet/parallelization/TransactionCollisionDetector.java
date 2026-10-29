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

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tuweni.units.bigints.UInt256;

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
    final Set<Address> addressesTouchedByTransaction =
        getAddressesTouchedByTransaction(
            transaction, Optional.of(parallelizedTransactionContext.transactionAccumulator()));
    if (addressesTouchedByTransaction.contains(miningBeneficiary)) {
      return true;
    }
    final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor =
        parallelizedTransactionContext.chainPredecessorAccumulator();
    if (chainPredecessor != null) {
      return hasCollisionWithChainedState(
          addressesTouchedByTransaction,
          parallelizedTransactionContext.transactionAccumulator(),
          chainPredecessor,
          blockAccumulator);
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
        return true;
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
   * A chained transaction ran on the state its predecessor left rather than on the parent state.
   * Its result holds if, for everything it touched, the block now has the value it started from:
   * the predecessor's where the predecessor touched it, the parent's otherwise.
   */
  private boolean hasCollisionWithChainedState(
      final Set<Address> addressesTouchedByTransaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    // a cleared storage is not visible slot by slot
    if (!transactionAccumulator.getStorageToClear().isEmpty()) {
      return true;
    }
    for (final Address address : addressesTouchedByTransaction) {
      if (blockAccumulator.getStorageToClear().contains(address)
          || !holdsStartingValue(
              blockAccumulator.getAccountsToUpdate().get(address),
              chainPredecessor.getAccountsToUpdate().get(address),
              TransactionCollisionDetector::areAccountDetailsEqualExcludingStorage)) {
        return true;
      }
    }
    for (final var slots : transactionAccumulator.getStorageToUpdate().entrySet()) {
      final Address address = slots.getKey();
      if (blockAccumulator.getStorageToClear().contains(address)) {
        return true;
      }
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> blockSlots =
          blockAccumulator.getStorageToUpdate().get(address);
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> predecessorSlots =
          chainPredecessor.getStorageToUpdate().get(address);
      for (final StorageSlotKey slot : slots.getValue().keySet()) {
        if (!holdsStartingValue(
            blockSlots == null ? null : blockSlots.get(slot),
            predecessorSlots == null ? null : predecessorSlots.get(slot),
            TransactionCollisionDetector::areSlotValuesEqual)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Whether the block's current value equals the one a chained transaction started from. An absent
   * entry means the value is still the parent's, which is also the prior of any present entry.
   */
  private static <T> boolean holdsStartingValue(
      final BonsaiValue<? extends T> inBlock,
      final BonsaiValue<? extends T> inPredecessor,
      final BiPredicate<T, T> equal) {
    if (inPredecessor == null) {
      return inBlock == null || equal.test(inBlock.getPrior(), inBlock.getUpdated());
    }
    if (inBlock == null) {
      return equal.test(inPredecessor.getPrior(), inPredecessor.getUpdated());
    }
    return equal.test(inBlock.getUpdated(), inPredecessor.getUpdated());
  }

  private static boolean areSlotValuesEqual(final UInt256 a, final UInt256 b) {
    // an absent slot is read as null and a cleared one written as zero
    return Objects.equals(a == null ? UInt256.ZERO : a, b == null ? UInt256.ZERO : b);
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
  private static boolean areAccountDetailsEqualExcludingStorage(
      final BonsaiAccount prior, final BonsaiAccount next) {
    return (prior == null && next == null)
        || (prior != null
            && next != null
            && prior.getNonce() == next.getNonce()
            && prior.getBalance().equals(next.getBalance())
            && prior.getCodeHash().equals(next.getCodeHash()));
  }
}
