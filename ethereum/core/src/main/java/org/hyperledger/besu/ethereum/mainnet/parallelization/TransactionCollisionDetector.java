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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BalanceObservations;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.Account;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;

import org.apache.tuweni.units.bigints.UInt256;

public class TransactionCollisionDetector {

  private static final Optional<Map<Address, CarriedAccount>> COLLISION = Optional.empty();

  /**
   * The balance and nonce the account ends with when the result is taken.
   *
   * @param balance the block's balance plus the transaction's change of it
   * @param nonce the nonce of the block or of the transaction, whichever changed it
   */
  public record CarriedAccount(Wei balance, long nonce) {}

  /**
   * Decides whether block processing can take the transaction's speculative result, and what it
   * then sets the accounts it carries over to.
   *
   * <p>A run alone collides when an earlier transaction of the block:
   *
   * <ul>
   *   <li>changed the nonce, balance or code of an account the run touched, unless {@code
   *       isAccountChangeCarried} carries the change over;
   *   <li>changed a slot the run touched;
   *   <li>cleared the storage of an account whose slots the run touched.
   * </ul>
   *
   * <p>A chained run collides when it touched the mining beneficiary beyond its fee, or when the
   * block does not hold, for something it touched, the value the chain started from. It carries
   * nothing over.
   *
   * <p>Both collide when the run deleted the mining beneficiary and the block holds it as an
   * existing empty account.
   *
   * @param transaction the transaction that executed speculatively, with a result that is not
   *     invalid
   * @param miningBeneficiary the mining beneficiary of the block
   * @param parallelizedTransactionContext the speculative result with the state it read and wrote
   * @param blockAccumulator the state changes of the earlier transactions of the block
   * @return what taking the result sets each carried account to, or empty if the speculative result
   *     must not be used
   */
  public Optional<Map<Address, CarriedAccount>> carryOverIfNoCollision(
      final Transaction transaction,
      final Address miningBeneficiary,
      final ParallelizedTransactionContext parallelizedTransactionContext,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator =
        parallelizedTransactionContext.transactionAccumulator();
    final PathBasedWorldStateUpdateAccumulator<?> chainPredecessor =
        parallelizedTransactionContext.chainPredecessorAccumulator();

    if (wouldDropBeneficiaryDeletion(transactionAccumulator, blockAccumulator, miningBeneficiary)) {
      return COLLISION;
    }

    if (chainPredecessor != null) {
      if (isTouchedBeyondItsFee(transaction, transactionAccumulator, miningBeneficiary)
          || hasCollisionWithChainedState(
              transactionAccumulator, chainPredecessor, blockAccumulator)) {
        return COLLISION;
      }
      return Optional.of(Map.of());
    }

    return carryOverRunAlone(transaction, transactionAccumulator, blockAccumulator);
  }

  /**
   * Whether taking the result would drop the deletion of the mining beneficiary. A run that touched
   * the beneficiary only for its fee no longer holds it among its accounts, so a deletion of it is
   * left only among the deleted ones. That matters only when the block holds the beneficiary as an
   * existing empty account: one the block does not hold is created and deleted again, and a
   * non-empty one stays as it is.
   */
  private static boolean wouldDropBeneficiaryDeletion(
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator,
      final Address miningBeneficiary) {
    if (!transactionAccumulator.getDeletedAccountAddresses().contains(miningBeneficiary)) {
      return false;
    }

    final Account beneficiary = blockAccumulator.get(miningBeneficiary);
    return beneficiary != null && beneficiary.isEmpty();
  }

  private static Optional<Map<Address, CarriedAccount>> carryOverRunAlone(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
    final Optional<BalanceObservations> observations =
        transactionAccumulator.getBalanceObservations();
    final Map<Address, CarriedAccount> carriedAccounts = new HashMap<>();

    // an executed transaction loaded its sender and recipient, and the commit added the accounts
    // it deleted
    for (final Address address : transactionAccumulator.getAccountsToUpdate().keySet()) {
      final BonsaiValue<? extends BonsaiAccount> inBlock =
          blockAccumulator.getAccountsToUpdate().get(address);
      if (inBlock == null) {
        continue;
      }

      if (!areAccountDetailsEqualExcludingStorage(inBlock.getPrior(), inBlock.getUpdated())) {
        final BonsaiValue<? extends BonsaiAccount> inTransaction =
            transactionAccumulator.getAccountsToUpdate().get(address);
        // a run that recorded no balance observations carries nothing over
        if (observations.isEmpty()
            || !isAccountChangeCarried(
                transaction, address, inBlock, inTransaction, observations.get())) {
          return COLLISION;
        }
        carriedAccounts.put(address, carry(inBlock, inTransaction));
      }

      if (hasStorageCollision(address, transactionAccumulator, blockAccumulator)) {
        return COLLISION;
      }
    }

    return Optional.of(carriedAccounts);
  }

  /** Whether the block changed a slot of the account the run touched, or cleared its storage. */
  private static boolean hasStorageCollision(
      final Address address,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final PathBasedWorldStateUpdateAccumulator<? extends BonsaiAccount> blockAccumulator) {
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

  /**
   * Whether the transaction touched the beneficiary other than through its fee: as its sender or
   * recipient, or as an account its run holds. A beneficiary touched only for the fee was removed
   * from the run's accounts, and a deletion of it left behind is checked separately.
   */
  private static boolean isTouchedBeyondItsFee(
      final Transaction transaction,
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final Address beneficiary) {
    return beneficiary.equals(transaction.getSender())
        || transaction.getTo().filter(beneficiary::equals).isPresent()
        || transactionAccumulator.getAccountsToUpdate().containsKey(beneficiary);
  }

  /**
   * Whether the run's change of an account the block changed can be carried over to the state the
   * block has reached. It can when:
   *
   * <ul>
   *   <li>the run did not read the account's balance;
   *   <li>the account is not empty in the parent state, at the transaction's turn or after the run,
   *       as emptiness decides call costs and state clearing;
   *   <li>the block did not change the account's code;
   *   <li>if the block changed the nonce, the run only depended on it being non-zero, as {@code
   *       isNonceChangeCarried} describes;
   *   <li>everything the run spent from the balance still finds enough on the block's balance.
   * </ul>
   */
  private static boolean isAccountChangeCarried(
      final Transaction transaction,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final BonsaiValue<? extends BonsaiAccount> inTransaction,
      final BalanceObservations observations) {
    if (observations.isObserved(address)) {
      return false;
    }

    if (!staysNonEmpty(inBlock, inTransaction)
        || codeChanged(inBlock)
        || (nonceChanged(inBlock)
            && !isNonceChangeCarried(transaction, address, inBlock, inTransaction))) {
      return false;
    }

    return spendingStillSuffices(transaction, address, inBlock, inTransaction, observations);
  }

  // not empty in the parent state, at the transaction's turn and after the run
  private static boolean staysNonEmpty(
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final BonsaiValue<? extends BonsaiAccount> inTransaction) {
    return isNonEmpty(inBlock.getPrior())
        && isNonEmpty(inBlock.getUpdated())
        && isNonEmpty(inTransaction.getPrior())
        && isNonEmpty(inTransaction.getUpdated());
  }

  private static boolean isNonEmpty(final Account account) {
    return account != null && !account.isEmpty();
  }

  private static boolean codeChanged(final BonsaiValue<? extends BonsaiAccount> inBlock) {
    return !inBlock.getPrior().getCodeHash().equals(inBlock.getUpdated().getCodeHash());
  }

  private static boolean nonceChanged(final BonsaiValue<? extends BonsaiAccount> inBlock) {
    return inBlock.getPrior().getNonce() != inBlock.getUpdated().getNonce();
  }

  /**
   * Whether the run's result holds with the nonce the block has reached. The run must leave the
   * nonce as it was, since the block's nonce stays. The exact nonce is read for the sender, for an
   * EIP-7702 authority, and to create a contract from the account. A creation in a frame that
   * reverts leaves the nonce unchanged although the created address depended on it, so an account
   * with code, which can create, does not qualify. Emptiness and the collision check of a created
   * address only ask whether the nonce is zero, so it must be non-zero on both sides.
   */
  private static boolean isNonceChangeCarried(
      final Transaction transaction,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final BonsaiValue<? extends BonsaiAccount> inTransaction) {
    return inBlock.getPrior().getNonce() != 0
        && inBlock.getUpdated().getNonce() != 0
        && !inBlock.getUpdated().hasCode()
        && inTransaction.getPrior().getNonce() == inTransaction.getUpdated().getNonce()
        && !isSenderOrAuthority(transaction, address);
  }

  private static boolean isSenderOrAuthority(final Transaction transaction, final Address address) {
    return address.equals(transaction.getSender())
        || transaction.getCodeDelegationList().stream()
            .flatMap(List::stream)
            .anyMatch(delegation -> delegation.authorizer().filter(address::equals).isPresent());
  }

  /**
   * Whether everything the run spent from the account's balance still finds enough on the balance
   * the block has reached. The sender's upfront cost and every spending the run observed sufficed
   * on the parent's balance, so they suffice when the block only raised it. Any other account
   * qualifies when the run only added to its balance, or when the block's balance plus the smallest
   * margin still covers the balance the run started from.
   */
  private static boolean spendingStillSuffices(
      final Transaction transaction,
      final Address address,
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final BonsaiValue<? extends BonsaiAccount> inTransaction,
      final BalanceObservations observations) {
    final Wei blockBalance = inBlock.getUpdated().getBalance();
    if (address.equals(transaction.getSender())) {
      return blockBalance.compareTo(inBlock.getPrior().getBalance()) >= 0;
    }

    final Wei start = inTransaction.getPrior().getBalance();
    final Optional<Wei> margin = observations.margin(address);
    if (margin.isEmpty()) {
      return inTransaction.getUpdated().getBalance().compareTo(start) >= 0;
    }

    return blockBalance.add(margin.get()).compareTo(start) >= 0;
  }

  private static CarriedAccount carry(
      final BonsaiValue<? extends BonsaiAccount> inBlock,
      final BonsaiValue<? extends BonsaiAccount> inTransaction) {
    // the carry-over rules keep the balance from going negative
    final Wei balance =
        inBlock
            .getUpdated()
            .getBalance()
            .add(inTransaction.getUpdated().getBalance())
            .subtract(inTransaction.getPrior().getBalance());
    // the transaction's nonce where it changed it, the block's otherwise
    final long nonce =
        inTransaction.getUpdated().getNonce() != inTransaction.getPrior().getNonce()
            ? inTransaction.getUpdated().getNonce()
            : inBlock.getUpdated().getNonce();
    return new CarriedAccount(balance, nonce);
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
