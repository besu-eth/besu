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
import org.hyperledger.besu.evm.account.Account;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tuweni.units.bigints.UInt256;

public class TransactionCollisionDetector {

  /**
   * Checks whether the block changed state that the transaction read or wrote while it executed
   * speculatively against the parent state.
   *
   * <p>The transaction collides when it touched an account whose nonce, balance or code the block
   * changed, or a slot of an account whose slot the block changed or whose storage the block
   * cleared. A change the transaction did not depend on is carried over instead, as {@code
   * isBalanceChangeCarried} describes. A transaction chained on its sender's previous one executed
   * on the state that one left instead, and collides when it touched the mining beneficiary or when
   * the block does not hold that state for something it touched.
   *
   * @param transaction the transaction that executed speculatively, with a result that is not
   *     invalid
   * @param miningBeneficiary the mining beneficiary of the block
   * @param parallelizedTransactionContext the speculative result with the state it read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @param carriedAccounts receives the accounts whose change taking the result must carry over,
   *     only to be read when the method returns false
   * @return true if the speculative result must not be used
   */
  public boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final List<Address> carriedAccounts) {
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor =
        parallelizedTransactionContext.chainPredecessorAccumulator();
    if (chainPredecessor != null) {
      // earlier transactions paid the beneficiary their priority fees, and a chained result carries
      // no balance change over
      return isAddressTouchedByTransaction(transaction, transactionAccumulator, miningBeneficiary)
          || hasCollisionWithChainedState(
              transactionAccumulator, chainPredecessor, blockAccumulator);
    }
    // a run that touched the beneficiary only for its fee no longer holds it among its accounts,
    // so taking the result would drop the deletion of a beneficiary the block holds as empty. One
    // the block does not hold is created and deleted again, and a non-empty one stays as it is.
    if (transactionAccumulator.getDeletedAccountAddresses().contains(miningBeneficiary)) {
      final Account beneficiary = blockAccumulator.get(miningBeneficiary);
      if (beneficiary != null && beneficiary.isEmpty()) {
        return true;
      }
    }
    // an executed transaction loaded its sender and recipient, and the commit added the accounts
    // it deleted
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
      if (hasCollision(
          transaction, address, transactionAccumulator, blockAccumulator, carriedAccounts)) {
        return true;
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
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    // a cleared storage is not visible slot by slot
    if (!transactionAccumulator.getStorageToClear().isEmpty()) {
      return true;
    }
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
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
      return inBlock == null || holdsItsPrior(inBlock, equal);
    }
    if (inBlock == null) {
      return holdsItsPrior(inPredecessor, equal);
    }
    return equal.test(inBlock.getUpdated(), inPredecessor.getUpdated());
  }

  /** A value only read is unchanged without loading it, as its prior and update share a loader. */
  private static <T> boolean holdsItsPrior(
      final BonsaiValue<? extends T> value, final BiPredicate<T, T> equal) {
    return !value.isChanged() || equal.test(value.getPrior(), value.getUpdated());
  }

  private static boolean areSlotValuesEqual(final UInt256 a, final UInt256 b) {
    // an absent slot is read as null and a cleared one written as zero
    return Objects.equals(a == null ? UInt256.ZERO : a, b == null ? UInt256.ZERO : b);
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
      final Transaction transaction,
      final Address address,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final List<Address> carriedAccounts) {
    final BonsaiValue<? extends BonsaiAccount> inBlock =
        blockAccumulator.getAccountsToUpdate().get(address);
    if (inBlock == null) {
      return false;
    }
    if (!areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
      if (!isBalanceChangeCarried(transaction, transactionAccumulator, address, inBlock)) {
        return true;
      }
      carriedAccounts.add(address);
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
   * Whether earlier transactions changed only the balance of an account whose balance the
   * transaction did not read, and spent from only while it sufficed by a margin the block's balance
   * keeps, so the transaction ends the same on the balance the block has reached. The sender's
   * balance qualifies when the block only raised it. A nonce the block changed qualifies for an
   * account without code when the transaction only depended on it being non-zero. Emptiness depends
   * on the balance and decides call costs and state clearing, so an account empty on either side
   * does not qualify.
   */
  private static boolean isBalanceChangeCarried(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock) {
    final Set<Address> observedBalances = transactionAccumulator.getObservedBalances();
    if (observedBalances == null || observedBalances.contains(address)) {
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
        || !blockPrior.getCodeHash().equals(blockUpdated.getCodeHash())
        || blockPrior.isEmpty()
        || blockUpdated.isEmpty()
        || inTransaction.getUpdated().isEmpty()
        || (blockPrior.getNonce() != blockUpdated.getNonce()
            && !isNonceChangeCarried(transaction, inTransaction, address, inBlock))) {
      return false;
    }
    if (address.equals(transaction.getSender())) {
      // the upfront cost and every spending the run observed sufficed on the parent's balance, so
      // they suffice on a higher one
      return blockUpdated.getBalance().compareTo(blockPrior.getBalance()) >= 0;
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
   * Whether the transaction ends the same with the nonce the block has reached. The exact nonce is
   * read for the sender, for an EIP-7702 authority, and to create a contract from the account. A
   * creation in a frame that reverts leaves the nonce unchanged although the created address
   * depended on it, so an account with code, which can create, does not qualify. Emptiness and the
   * collision check of a created address only ask whether the nonce is zero, so it must be non-zero
   * on both sides.
   */
  private static boolean isNonceChangeCarried(
      final Transaction transaction,
      final BonsaiValue<? extends BonsaiAccount> inTransaction,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock) {
    return inBlock.getPrior().getNonce() != 0
        && inBlock.getUpdated().getNonce() != 0
        && !inBlock.getUpdated().hasCode()
        && !address.equals(transaction.getSender())
        && inTransaction.getPrior().getNonce() == inTransaction.getUpdated().getNonce()
        && transaction
            .getCodeDelegationList()
            .map(
                delegations ->
                    delegations.stream()
                        .noneMatch(
                            delegation ->
                                delegation.authorizer().filter(address::equals).isPresent()))
            .orElse(true);
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
