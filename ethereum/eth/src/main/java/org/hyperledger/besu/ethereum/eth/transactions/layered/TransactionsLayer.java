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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;

public interface TransactionsLayer {

  String name();

  void reset();

  Optional<PendingTransaction> getByHash(Hash transactionHash);

  boolean contains(Transaction transaction);

  /**
   * Try to add a pending transaction to this layer. The {@code addReason} is used to discriminate
   * between a new tx that is added to the pool, or a tx that is already in the pool, but is moving
   * internally between layers, for example, due to a promotion or demotion. The distinction is
   * needed since we only need to send a notification for a new tx, and not when it is only an
   * internal move.
   *
   * @param pendingTransaction the tx to try to add to this layer
   * @param gap the nonce gap between the current sender nonce and the tx
   * @param addReason define if it is a new tx or an internal move
   * @return the result of the add operation
   */
  TransactionAddedResult add(PendingTransaction pendingTransaction, int gap, AddReason addReason);

  /**
   * Remove the pending tx from the pool
   *
   * @param pendingTransaction the pending tx
   * @param reason the reason it is removed from the pool
   */
  void remove(PendingTransaction pendingTransaction, PoolRemovalReason reason);

  /**
   * Penalize a pending transaction. Penalization could be applied to notify the txpool that this
   * pending tx has some temporary issues that prevent it from being included in a block, and so it
   * should be de-prioritized in some ways, so it will be re-evaluated only after non penalized
   * pending txs. For example: if during the evaluation for block inclusion, the pending tx is
   * excluded because the sender has not enough balance to send it, this could be a transient issue
   * since later the sender could receive some funds, but in any case we penalize the pending tx, so
   * it is pushed down in the order of prioritized pending txs.
   *
   * @param penalizedTransaction the tx to penalize
   * @param selectionResult the reason of the penalization
   */
  void penalize(
      PendingTransaction penalizedTransaction, final TransactionSelectionResult selectionResult);

  void blockAdded(
      FeeMarket feeMarket,
      BlockHeader blockHeader,
      final Map<Address, Long> maxConfirmedNonceBySender);

  List<PendingTransaction> getAll();

  /**
   * Returns all pending transactions across all layers, grouped by sender and sorted by nonce in
   * ascending order per sender.
   *
   * <p>The nonce ordering relies on each layer's internal {@code NavigableMap<Long,
   * PendingTransaction>} being nonce-sorted, and on the invariant that a layer always holds
   * lower-nonce transactions than its next layer (e.g. ready transactions before sparse ones).
   *
   * @return a map from sender address to the list of that sender's pending transactions, sorted by
   *     nonce ascending
   */
  Map<Address, List<PendingTransaction>> getAllBySender();

  /**
   * Returns all pending transactions for the given sender across all layers, sorted by nonce in
   * ascending order.
   *
   * @param sender the sender address
   * @return transactions for the sender sorted by nonce ascending, or an empty list if none exist
   */
  List<PendingTransaction> getAllFor(Address sender);

  List<Transaction> getAllLocal();

  List<Transaction> getAllPriority();

  int count();

  OptionalLong getNextNonceFor(Address sender);

  /**
   * Get the sender nonce has seen by this and the following layers
   *
   * @param sender the sender for which retrieve the txpool
   * @return either the sender nonce or empty if the sender is unknown to this or the following
   *     layers
   */
  OptionalLong getCurrentNonceFor(Address sender);

  List<PendingTransaction> promote(
      Predicate<PendingTransaction> promotionFilter,
      final long freeSpace,
      final int freeSlots,
      final int[] remainingPromotionsPerType);

  long subscribeToAdded(PendingTransactionAddedListener listener);

  void unsubscribeFromAdded(long id);

  long subscribeToDropped(PendingTransactionDroppedListener listener);

  void unsubscribeFromDropped(long id);

  PendingTransaction promoteFor(Address sender, long nonce, final int[] remainingPromotionsPerType);

  void notifyAdded(PendingTransaction pendingTransaction);

  long getCumulativeUsedSpace();

  String logStats();

  PendingTransactions.Status getStatus();

  String logSender(Address sender);
}
