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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

public interface PendingTransactions {

  void reset();

  /**
   * Whether the pool relies on the transaction pool to limit the pending transactions of accounts
   * that are delegated, or are going to be (EIP-7702), see {@link DelegationLimits}.
   *
   * <p>A pool that admits transactions from unrelated, untrusted senders, and relies on the
   * fundability of the transactions queued behind the first one of a sender, must return true,
   * since a delegated account can have its balance moved by a third party without consuming its
   * nonce. A pool that does not, as a gas-free FIFO one, keeps the default, and is not affected by
   * the limits in any way.
   *
   * @return true if the delegation limits must be enforced for this pool
   */
  default boolean requiresDelegationLimits() {
    return false;
  }

  /**
   * Removes pending transactions that have become invalid, because they could not be executed, or
   * would not be allowed in the pool anymore. It is only called for pools that require the
   * delegation limits.
   *
   * @param invalidatedTransactions the transactions to remove, if a sender has more than one, they
   *     are ordered by nonce descending, so no gaps are created
   */
  default void removeInvalidated(final Collection<PendingTransaction> invalidatedTransactions) {
    throw new UnsupportedOperationException(
        "Removing invalidated transactions is not supported by this pool");
  }

  void evictOldTransactions();

  List<Transaction> getLocalTransactions();

  List<Transaction> getPriorityTransactions();

  TransactionAddedResult addTransaction(
      PendingTransaction transaction, Optional<Account> maybeSenderAccount);

  void selectTransactions(PendingTransactionsSelector selector);

  long maxSize();

  int size();

  boolean containsTransaction(Transaction transaction);

  Optional<Transaction> getTransactionByHash(Hash transactionHash);

  Collection<PendingTransaction> getPendingTransactions();

  /**
   * Returns all pending transactions for the given sender, sorted by nonce in ascending order.
   *
   * @param sender the sender address
   * @return transactions for the sender sorted by nonce ascending, or an empty list if none exist
   */
  SenderPendingTransactionsData getPendingTransactionsFor(Address sender);

  Map<Address, SenderPendingTransactionsData> getPendingTransactionsBySender();

  OptionalLong getNextNonceForSender(Address sender);

  void manageBlockAdded(
      BlockHeader blockHeader,
      List<Transaction> confirmedTransactions,
      final List<Transaction> reorgTransactions,
      FeeMarket feeMarket);

  String toTraceLog();

  String logStats();

  Status getStatus();

  Optional<Transaction> restoreBlob(Transaction transaction);

  @FunctionalInterface
  interface PendingTransactionsSelector {
    Map<PendingTransaction, TransactionSelectionResult> evaluatePendingTransactions(
        List<PendingTransaction> candidatePendingTransactions);
  }

  record Status(long pendingCount, long queuedCount) {}
}
