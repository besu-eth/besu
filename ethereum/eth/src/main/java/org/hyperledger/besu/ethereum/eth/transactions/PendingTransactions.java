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

  long subscribePendingTransactions(PendingTransactionAddedListener listener);

  void unsubscribePendingTransactions(long id);

  long subscribeDroppedTransactions(PendingTransactionDroppedListener listener);

  void unsubscribeDroppedTransactions(long id);

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
