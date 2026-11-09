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
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;

public class TransactionCollisionDetector {

  /**
   * Checks whether the block changed state that the transaction read or wrote while it executed
   * speculatively against the parent state.
   *
   * <p>The transaction collides when it touched the mining beneficiary, an account whose nonce,
   * balance or code the block changed, or a slot of an account whose slot the block changed or
   * whose storage the block cleared. A transaction chained on its sender's previous one executed on
   * the state that one left instead, and collides when the block does not hold that state for
   * something it touched.
   *
   * @param transaction the transaction that executed speculatively, with a result that is not
   *     invalid
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
    return hasCollision(
        transaction, miningBeneficiary, parallelizedTransactionContext, blockAccumulator, null);
  }

  /**
   * Like {@link #hasCollision(Transaction, Address, ParallelizedTransactionContext,
   * PathBasedWorldStateUpdateAccumulator)}, except that an account of which earlier transactions
   * changed only the balance is no collision when the transaction did not depend on that balance
   * beyond it sufficing for what the transaction spent from it, and saw the account empty on no
   * side. Such accounts are added to {@code balancesCarried}: taking the result over must then
   * carry the transaction's balance change onto the balance the block has reached.
   *
   * @param transaction the transaction that executed speculatively, with a result that is not
   *     invalid
   * @param miningBeneficiary the mining beneficiary of the block
   * @param parallelizedTransactionContext the speculative result with the state it read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @param balancesCarried receives the accounts whose balance change must be carried over, or null
   *     to treat every balance change as a collision
   * @return true if the speculative result must not be used
   */
  public boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final @Nullable List<Address> balancesCarried) {
    return hasCollision(
        transaction,
        miningBeneficiary,
        parallelizedTransactionContext,
        blockAccumulator,
        balancesCarried,
        false);
  }

  /**
   * Like {@link #hasCollision(Transaction, Address, ParallelizedTransactionContext,
   * PathBasedWorldStateUpdateAccumulator, List)} for a transaction that ran on the parent state
   * although earlier transactions of its sender come before it in the block. The changes those made
   * to the sender's account are no collision here; the caller checks them with {@link
   * #isSenderChangeCarried}.
   *
   * @param transaction the transaction that executed speculatively, with a result that is not
   *     invalid
   * @param miningBeneficiary the mining beneficiary of the block
   * @param parallelizedTransactionContext the speculative result with the state it read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @param balancesCarried receives the accounts whose balance change must be carried over
   * @return true if the speculative result must not be used
   */
  public boolean hasCollisionExceptSender(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final List<Address> balancesCarried) {
    return hasCollision(
        transaction,
        miningBeneficiary,
        parallelizedTransactionContext,
        blockAccumulator,
        balancesCarried,
        true);
  }

  private boolean hasCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final @Nullable List<Address> balancesCarried,
      final boolean senderChecked) {
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor =
        parallelizedTransactionContext.chainPredecessorAccumulator();
    // every earlier transaction paid the beneficiary, so a transaction that touched it can only
    // keep its result by having paid it too, without depending on its balance
    if (isAddressTouchedByTransaction(transaction, transactionAccumulator, miningBeneficiary)
        && (balancesCarried == null
            || chainPredecessor != null
            || transactionAccumulator.getDeletedAccountAddresses().contains(miningBeneficiary)
            || !transactionAccumulator.getAccountsToUpdate().containsKey(miningBeneficiary))) {
      return true;
    }
    if (chainPredecessor != null) {
      return hasCollisionWithChainedState(
          transactionAccumulator, chainPredecessor, blockAccumulator);
    }
    // an executed transaction loaded its sender and recipient, and the commit added the accounts
    // it deleted
    final Address checkedSender = senderChecked ? transaction.getSender() : null;
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
      if (hasCollision(
          transaction,
          address,
          transactionAccumulator,
          blockAccumulator,
          balancesCarried,
          checkedSender)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the changes earlier transactions of the block made to the sender's account leave the
   * result of a transaction that ran on the parent state valid, so that taking it over only has to
   * carry its nonce and balance change onto the account the block has reached. The transaction must
   * not have read the sender's balance, and the sender has no code on any side, so that its
   * execution did not depend on the account. Whether the block's account still passes the checks of
   * the sender is left to the caller.
   *
   * @param transaction the transaction that executed speculatively on the parent state
   * @param transactionAccumulator the state the transaction read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @return true if the sender's account is no reason to execute the transaction again
   */
  public static boolean isSenderChangeCarried(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    final Address sender = transaction.getSender();
    final Set<Address> observedBalances = transactionAccumulator.getObservedBalances();
    if (observedBalances == null
        || observedBalances.contains(sender)
        || transactionAccumulator.getBalanceMargin(sender).isPresent()
        || transactionAccumulator.getDeletedAccountAddresses().contains(sender)
        || transactionAccumulator.getStorageToClear().contains(sender)
        || transactionAccumulator.getCodeToUpdate().containsKey(sender)) {
      return false;
    }
    final BonsaiValue<? extends BonsaiAccount> inTransaction =
        transactionAccumulator.getAccountsToUpdate().get(sender);
    final BonsaiValue<? extends BonsaiAccount> inBlock =
        blockAccumulator.getAccountsToUpdate().get(sender);
    if (inTransaction == null
        || inBlock == null
        || inTransaction.getPrior() == null
        || inTransaction.getUpdated() == null
        || inBlock.getPrior() == null
        || inBlock.getUpdated() == null) {
      return false;
    }
    final BonsaiAccount transactionPrior = inTransaction.getPrior();
    final BonsaiAccount transactionUpdated = inTransaction.getUpdated();
    final BonsaiAccount blockPrior = inBlock.getPrior();
    final BonsaiAccount blockUpdated = inBlock.getUpdated();
    // both ran from the parent's account, which only the sender's own transactions changed
    return transactionPrior.getNonce() == blockPrior.getNonce()
        && transactionPrior.getBalance().equals(blockPrior.getBalance())
        && transactionUpdated.getNonce() == transactionPrior.getNonce() + 1
        && blockUpdated.getNonce() == transaction.getNonce()
        && Hash.EMPTY.equals(transactionPrior.getCodeHash())
        && Hash.EMPTY.equals(transactionUpdated.getCodeHash())
        && Hash.EMPTY.equals(blockPrior.getCodeHash())
        && Hash.EMPTY.equals(blockUpdated.getCodeHash());
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
      final @Nullable List<Address> balancesCarried,
      final @Nullable Address checkedSender) {
    final BonsaiValue<? extends BonsaiAccount> inBlock =
        blockAccumulator.getAccountsToUpdate().get(address);
    if (inBlock == null) {
      return false;
    }
    if (!address.equals(checkedSender)
        && !areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
      if (balancesCarried == null
          || !isBalanceChangeCarried(transaction, transactionAccumulator, address, inBlock)) {
        return true;
      }
      balancesCarried.add(address);
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
   * keeps, so the transaction ends the same on the balance the block has reached. Emptiness depends
   * on the balance and decides call costs and state clearing, so an account empty on any side does
   * not qualify, nor does one that a debit could empty.
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
        || inTransaction.getPrior().isEmpty()
        || inTransaction.getUpdated().isEmpty()) {
      return false;
    }
    final Wei startingBalance = inTransaction.getPrior().getBalance();
    final Optional<Wei> margin = transactionAccumulator.getBalanceMargin(address);
    if (margin.isEmpty()) {
      // without spending, only a credit can be carried over
      return inTransaction.getUpdated().getBalance().compareTo(startingBalance) >= 0;
    }
    // every spending found the balance sufficient by at least the margin; on the block's balance
    // a spending can leave none, which must not empty the account
    return blockUpdated.getBalance().add(margin.get()).compareTo(startingBalance) >= 0
        && (blockUpdated.getNonce() > 0 || !blockUpdated.getCodeHash().equals(Hash.EMPTY));
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

  /**
   * Replay benchmarks: why {@link #hasCollision(Transaction, Address,
   * ParallelizedTransactionContext, PathBasedWorldStateUpdateAccumulator, List)} found a collision,
   * as a kind and the address it concerns, or null if it found none.
   */
  public @Nullable String collisionReason(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final boolean carryBalances) {
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor =
        parallelizedTransactionContext.chainPredecessorAccumulator();
    if (isAddressTouchedByTransaction(transaction, transactionAccumulator, miningBeneficiary)
        && (!carryBalances
            || chainPredecessor != null
            || transactionAccumulator.getDeletedAccountAddresses().contains(miningBeneficiary)
            || !transactionAccumulator.getAccountsToUpdate().containsKey(miningBeneficiary))) {
      return "coinbase " + miningBeneficiary;
    }
    if (chainPredecessor != null) {
      return hasCollisionWithChainedState(
              transactionAccumulator, chainPredecessor, blockAccumulator)
          ? "chained " + transaction.getSender()
          : null;
    }
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
      final BonsaiValue<? extends BonsaiAccount> inBlock =
          blockAccumulator.getAccountsToUpdate().get(address);
      if (inBlock == null) {
        continue;
      }
      if (!areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
        final String kind =
            accountChangeKind(transaction, transactionAccumulator, address, inBlock, carryBalances);
        if (kind != null) {
          return kind + " " + address;
        }
      }
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> slots =
          transactionAccumulator.getStorageToUpdate().get(address);
      if (slots == null || slots.isEmpty()) {
        continue;
      }
      if (blockAccumulator.getStorageToClear().contains(address)) {
        return "cleared " + address;
      }
      final Map<StorageSlotKey, ? extends BonsaiValue<UInt256>> blockSlots =
          blockAccumulator.getStorageToUpdate().get(address);
      if (blockSlots == null) {
        continue;
      }
      for (final StorageSlotKey slot : slots.keySet()) {
        final BonsaiValue<UInt256> inBlockSlot = blockSlots.get(slot);
        if (inBlockSlot != null && inBlockSlot.isChanged()) {
          return "slot " + address;
        }
      }
    }
    return null;
  }

  private static @Nullable String accountChangeKind(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final boolean carryBalances) {
    final BonsaiAccount prior = inBlock.getPrior();
    final BonsaiAccount updated = inBlock.getUpdated();
    if (prior == null || updated == null) {
      return "created-or-deleted";
    }
    if (prior.getNonce() != updated.getNonce()
        || !prior.getCodeHash().equals(updated.getCodeHash())) {
      return address.equals(transaction.getSender()) ? "sender-nonce" : "nonce-or-code";
    }
    if (!carryBalances) {
      return address.equals(transaction.getSender()) ? "sender-balance" : "balance";
    }
    if (isBalanceChangeCarried(transaction, transactionAccumulator, address, inBlock)) {
      return null;
    }
    if (address.equals(transaction.getSender())) {
      return "sender-balance";
    }
    final Set<Address> observed = transactionAccumulator.getObservedBalances();
    if (observed != null && observed.contains(address)) {
      return "balance-read";
    }
    if (transactionAccumulator.getBalanceMargin(address).isPresent()) {
      return "balance-margin";
    }
    return "balance-other";
  }
}
