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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** This is a no-op implementation, that is used when the txpool is disabled */
public class DisabledPendingTransactions implements PendingTransactions {

  @Override
  public void reset() {}

  @Override
  public void evictOldTransactions() {}

  @Override
  public List<Transaction> getLocalTransactions() {
    return List.of();
  }

  @Override
  public List<Transaction> getPriorityTransactions() {
    return List.of();
  }

  @Override
  public TransactionAddedResult addTransaction(
      final PendingTransaction transaction, final Optional<Account> maybeSenderAccount) {
    return TransactionAddedResult.DISABLED;
  }

  @Override
  public void selectTransactions(final PendingTransactionsSelector selector) {}

  @Override
  public long maxSize() {
    return 0;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean containsTransaction(final Transaction transaction) {
    return false;
  }

  @Override
  public Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    return Optional.empty();
  }

  @Override
  public Collection<PendingTransaction> getPendingTransactions() {
    return List.of();
  }

  @Override
  public SenderPendingTransactionsData getPendingTransactionsFor(final Address sender) {
    return SenderPendingTransactionsData.empty(sender);
  }

  @Override
  public Map<Address, SenderPendingTransactionsData> getPendingTransactionsBySender() {
    return Map.of();
  }

  @Override
  public long subscribePendingTransactions(final PendingTransactionAddedListener listener) {
    return 0;
  }

  @Override
  public void unsubscribePendingTransactions(final long id) {}

  @Override
  public long subscribeDroppedTransactions(final PendingTransactionDroppedListener listener) {
    return 0;
  }

  @Override
  public void unsubscribeDroppedTransactions(final long id) {}

  @Override
  public OptionalLong getNextNonceForSender(final Address sender) {
    return OptionalLong.empty();
  }

  @Override
  public void manageBlockAdded(
      final BlockHeader blockHeader,
      final List<Transaction> confirmedTransactions,
      final List<Transaction> reorgTransactions,
      final FeeMarket feeMarket) {}

  @Override
  public String toTraceLog() {
    return "Disabled";
  }

  @Override
  public String logStats() {
    return "Disabled";
  }

  @Override
  public Status getStatus() {
    return new Status(0, 0);
  }

  @Override
  public Optional<Transaction> restoreBlob(final Transaction transaction) {
    return Optional.empty();
  }
}
