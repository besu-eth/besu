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
import org.hyperledger.besu.ethereum.trie.pathbased.common.account.PathBasedAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
      final PathBasedWorldStateUpdateAccumulator<? extends PathBasedAccount> blockAccumulator) {
    final Set<Address> addressesTouchedByTransaction =
        getAddressesTouchedByTransaction(
            transaction, Optional.of(parallelizedTransactionContext.transactionAccumulator()));
    if (addressesTouchedByTransaction.contains(miningBeneficiary)) {
      return true;
    }
    for (final Address next : addressesTouchedByTransaction) {
      final Optional<AccountUpdateContext> maybeAddressTouchedByBlock =
          getAddressTouchedByBlock(next, Optional.of(blockAccumulator));
      if (maybeAddressTouchedByBlock.isPresent()) {
        if (maybeAddressTouchedByBlock.get().areAccountDetailsEqualExcludingStorage()) {
          final Set<StorageSlotKey> slotsTouchedByBlockAndByAddress =
              getSlotsTouchedByBlockAndByAddress(Optional.of(blockAccumulator), next);
          final Set<StorageSlotKey> slotsTouchedByTransactionAndByAddress =
              getSlotsTouchedByTransactionAndByAddress(
                  Optional.of(parallelizedTransactionContext.transactionAccumulator()), next);
          for (final StorageSlotKey touchedByTransactionAndByAddress :
              slotsTouchedByTransactionAndByAddress) {
            if (slotsTouchedByBlockAndByAddress.contains(touchedByTransactionAndByAddress)) {
              return true;
            }
          }
        } else {
          return true;
        }
      }
    }
    return false;
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
   * Retrieves the set of storage slot keys that have been touched by the given transaction for the
   * specified address, based on the provided world state update accumulator.
   *
   * <p>This method checks if the accumulator contains storage updates for the specified address. If
   * such updates are found, it adds the touched storage slot keys to the returned set. The method
   * does not distinguish between changes or unchanged slots; it simply collects all the storage
   * slot keys that have been touched by the transaction for the given address.
   *
   * @param accumulator An {@link Optional} containing the world state update accumulator, which
   *     holds the updates for storage slots.
   * @param address The address for which the touched storage slots are being retrieved.
   * @return A set of storage slot keys that have been touched by the transaction for the given
   *     address. If no updates are found, or the address has no associated updates, an empty set is
   *     returned.
   */
  private Set<StorageSlotKey> getSlotsTouchedByTransactionAndByAddress(
      final Optional<PathBasedWorldStateUpdateAccumulator<?>> accumulator, final Address address) {
    HashSet<StorageSlotKey> slots = new HashSet<>();
    accumulator.ifPresent(
        pathBasedWorldStateUpdateAccumulator -> {
          final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> map =
              pathBasedWorldStateUpdateAccumulator.getStorageToUpdate().get(address);
          if (map != null) {
            map.forEach(
                (storageSlotKey, slot) -> {
                  slots.add(storageSlotKey);
                });
          }
        });
    return slots;
  }

  /**
   * Retrieves the update context for the given address from the block's world state update
   * accumulator.
   *
   * <p>This method checks if the provided accumulator contains updates for the given address. If an
   * update is found, it compares the prior and updated states of the account to determine if the
   * key account details (excluding storage) are considered equal. It then returns an {@link
   * AccountUpdateContext} containing the address and the result of that comparison.
   *
   * <p>If no update is found for the address or the accumulator is absent, the method returns an
   * empty {@link Optional}.
   *
   * @param addressToFind The address for which the update context is being queried.
   * @param maybeBlockAccumulator An {@link Optional} containing the block's world state update
   *     accumulator, which holds the updates for the accounts in the block.
   * @return An {@link Optional} containing the {@link AccountUpdateContext} if the address is found
   *     in the block's updates, otherwise an empty {@link Optional}.
   */
  private Optional<AccountUpdateContext> getAddressTouchedByBlock(
      final Address addressToFind,
      final Optional<PathBasedWorldStateUpdateAccumulator<? extends PathBasedAccount>>
          maybeBlockAccumulator) {
    if (maybeBlockAccumulator.isPresent()) {
      final PathBasedWorldStateUpdateAccumulator<? extends PathBasedAccount> blockAccumulator =
          maybeBlockAccumulator.get();
      final PathBasedValue<? extends PathBasedAccount> pathBasedValue =
          blockAccumulator.getAccountsToUpdate().get(addressToFind);
      if (pathBasedValue != null) {
        return Optional.of(
            new AccountUpdateContext(
                addressToFind,
                areAccountDetailsEqualExcludingStorage(
                    pathBasedValue.getPrior(), pathBasedValue.getUpdated())));
      }
    }
    return Optional.empty();
  }

  /**
   * Retrieves the set of storage slot keys that have been updated in the block accumulator for the
   * specified address.
   *
   * <p>This method checks if the accumulator contains a storage map for the provided address. If
   * the address has associated storage updates, it iterates over the storage slots and add it to
   * the list only if the corresponding storage value has been modified (i.e., is not unchanged).
   *
   * @param accumulator An Optional containing the world state block update accumulator, which holds
   *     the storage updates.
   * @param address The address for which the storage slots are being queried.
   * @return A set of storage slot keys that have been updated for the given address. If no updates
   *     are found, or the address has no associated updates, an empty set is returned.
   */
  private Set<StorageSlotKey> getSlotsTouchedByBlockAndByAddress(
      final Optional<PathBasedWorldStateUpdateAccumulator<?>> accumulator, final Address address) {
    HashSet<StorageSlotKey> slots = new HashSet<>();
    accumulator.ifPresent(
        pathBasedWorldStateUpdateAccumulator -> {
          final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> map =
              pathBasedWorldStateUpdateAccumulator.getStorageToUpdate().get(address);
          if (map != null) {
            map.forEach(
                (storageSlotKey, slot) -> {
                  if (!slot.isUnchanged()) {
                    slots.add(storageSlotKey);
                  }
                });
          }
        });
    return slots;
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
      final PathBasedAccount prior, final PathBasedAccount next) {
    return (prior == null && next == null)
        || (prior != null
            && next != null
            && prior.getNonce() == next.getNonce()
            && prior.getBalance().equals(next.getBalance())
            && prior.getCodeHash().equals(next.getCodeHash()));
  }

  /**
   * Represents the context of an account update, including the account's address and whether the
   * key details of the account (excluding storage) are considered equal.
   *
   * <p>This record holds two main pieces of information: - `address`: The address of the account
   * being updated. - `areAccountDetailsEqualExcludingStorage`: A boolean value indicating whether
   * the account details, excluding the storage (nonce, balance, and code hash), are considered
   * equal when compared to a previous state.
   *
   * <p>This record is used to track changes to account states and determine if key properties are
   * unchanged, which helps in detecting whether further action is needed for the account update.
   */
  private record AccountUpdateContext(
      Address address, boolean areAccountDetailsEqualExcludingStorage) {

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AccountUpdateContext that = (AccountUpdateContext) o;
      return address.equals(that.address);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(address);
    }

    @Override
    public String toString() {
      return "AccountUpdateContext{"
          + "address="
          + address
          + ", areAccountDetailsEqualExcludingStorage="
          + areAccountDetailsEqualExcludingStorage
          + '}';
    }
  }
}
