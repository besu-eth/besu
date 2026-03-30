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

import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logMinGasPrice;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logMinPriorityFeePerGas;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Holds the current set of executable pending transactions, that are candidate for inclusion on
 * next block. The pending transactions are kept sorted by paid fee descending.
 */
public abstract class AbstractPrioritizedTransactions extends AbstractSequentialTransactionsLayer {
  protected final TreeSet<PendingTransaction> orderByFee;
  private final MiningConfiguration miningConfiguration;

  public AbstractPrioritizedTransactions(
      final TransactionPoolConfiguration poolConfig,
      final EthScheduler ethScheduler,
      final TransactionsLayer prioritizedTransactions,
      final TransactionPoolMetrics metrics,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester,
      final BlobCache blobCache,
      final MiningConfiguration miningConfiguration) {
    super(
        poolConfig,
        ethScheduler,
        prioritizedTransactions,
        transactionReplacementTester,
        metrics,
        blobCache);
    this.orderByFee = new TreeSet<>(this::compareByFee);
    this.miningConfiguration = miningConfiguration;
    getAndLogMinPriorityFeePerGas();
    getAndLogMinTransactionGasPrice();
  }

  @Override
  public void reset() {
    super.reset();
    orderByFee.clear();
  }

  @Override
  public String name() {
    return "prioritized";
  }

  @Override
  protected TransactionAddedResult canAdd(
      final PendingTransaction pendingTransaction, final int gap) {
    final var senderTxs = txsBySender.get(pendingTransaction.getSender());

    if (hasExpectedNonce(senderTxs, pendingTransaction, gap) && hasPriority(pendingTransaction)) {

      return TransactionAddedResult.ADDED;
    }

    return TransactionAddedResult.TRY_NEXT_LAYER;
  }

  @Override
  protected void internalAdd(
      final NavigableMap<Long, PendingTransaction> senderTxs, final PendingTransaction addedTx) {
    orderByFee.add(addedTx);
  }

  @Override
  protected void internalReplaced(final PendingTransaction replacedTx) {
    orderByFee.remove(replacedTx);
  }

  private boolean hasPriority(final PendingTransaction pendingTransaction) {
    // check if there is space for that tx type
    final var txType = pendingTransaction.getTransaction().getType();
    if (txCountByType[txType.ordinal()]
        >= poolConfig
            .getMaxPrioritizedTransactionsByType()
            .getOrDefault(txType, Integer.MAX_VALUE)) {
      return false;
    }

    // if it does not pass the promotion filter, then has not priority
    if (!promotionFilter(pendingTransaction)) {
      return false;
    }

    // if there is space add it, otherwise check if it has more value than the last one
    if (orderByFee.size() < poolConfig.getMaxPrioritizedTransactions()) {
      return true;
    }
    return compareByFee(pendingTransaction, orderByFee.first()) > 0;
  }

  @Override
  protected int maxTransactionsNumber() {
    return poolConfig.getMaxPrioritizedTransactions();
  }

  @Override
  protected PendingTransaction getEvictable() {
    return orderByFee.first();
  }

  protected abstract int compareByFee(final PendingTransaction pt1, final PendingTransaction pt2);

  @Override
  protected void internalRemove(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction removedTx,
      final LayeredRemovalReason removalReason) {
    orderByFee.remove(removedTx);
  }

  @Override
  protected void internalPenalize(final PendingTransaction penalizedTx) {
    orderByFee.remove(penalizedTx);
    penalizedTx.decrementScore();
    orderByFee.add(penalizedTx);
  }

  @Override
  public List<PendingTransaction> promote(
      final Predicate<PendingTransaction> promotionFilter,
      final long freeSpace,
      final int freeSlots,
      final int[] remainingPromotionsPerType) {
    return List.of();
  }

  /**
   * Here the max number of txs of a specific type that can be promoted, is defined by the
   * configuration, so we return the difference between the configured max and the current count of
   * txs for each type
   *
   * @return an array containing the max amount of txs that can be promoted for each type
   */
  @Override
  protected int[] getRemainingPromotionsPerType() {
    final var allTypes = TransactionType.values();
    final var remainingPromotionsPerType = new int[allTypes.length];
    for (int i = 0; i < allTypes.length; i++) {
      remainingPromotionsPerType[i] =
          poolConfig
                  .getMaxPrioritizedTransactionsByType()
                  .getOrDefault(allTypes[i], Integer.MAX_VALUE)
              - txCountByType[i];
    }
    return remainingPromotionsPerType;
  }

  /**
   * Return the full content of this layer, organized as a list of sender pending txs. For each
   * sender the collection pending txs is ordered by nonce asc.
   *
   * <p>Returned sender list order detail: first the sender of the most profitable tx.
   *
   * @return a list of sender pending txs
   */
  @Override
  public List<List<PendingTransaction>> getBySender() {
    final var sendersToAdd = new HashSet<>(txsBySender.keySet());
    return orderByFee.descendingSet().stream()
        .map(PendingTransaction::getSender)
        .filter(sendersToAdd::remove)
        .map(sender -> List.copyOf(txsBySender.get(sender).values()))
        .toList();
  }

  /**
   * Returns pending txs by sender and ordered by score desc. In case a sender has pending txs with
   * different scores, then in nonce sequence, every time there is a score decrease, his pending txs
   * will be put in a new entry with that score. For example if a sender has 3 pending txs (where
   * the first number is the nonce and the score is between parenthesis): 0(127), 1(126), 2(127),
   * then for he there will be 2 entries:
   *
   * <ul>
   *   <li>0(127)
   *   <li>1(126), 2(127)
   * </ul>
   *
   * @return pending txs by sender and ordered by score desc
   */
  public List<PendingTransaction> getByScore() {
    final var sendersToAdd = new HashSet<>(txsBySender.keySet());
    final var candidatesByScore =
        orderByFee.descendingSet().stream()
            .map(PendingTransaction::getSender)
            .filter(sendersToAdd::remove)
            .flatMap(sender -> splitByScore(txsBySender.get(sender)).entrySet().stream())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> {
                      a.addAll(b);
                      return a;
                    },
                    TreeMap::new))
            .descendingMap();

    final List<PendingTransaction> result = new ArrayList<>(pendingTransactions.size());
    candidatesByScore.values().forEach(result::addAll);
    return result;
  }

  private Map<Byte, List<PendingTransaction>> splitByScore(
      final NavigableMap<Long, PendingTransaction> txsBySender) {
    final var splitByScore = new HashMap<Byte, List<PendingTransaction>>();
    byte currScore = txsBySender.firstEntry().getValue().getScore();
    for (final var entry : txsBySender.entrySet()) {
      if (entry.getValue().getScore() < currScore) {
        // score decreased, we need to start a new split
        currScore = entry.getValue().getScore();
      }
      splitByScore.computeIfAbsent(currScore, k -> new ArrayList<>()).add(entry.getValue());
    }
    return splitByScore;
  }

  @Override
  protected long cacheFreeSpace() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected void internalConsistencyCheck(
      final Map<Address, NavigableMap<Long, PendingTransaction>> prevLayerTxsBySender) {
    super.internalConsistencyCheck(prevLayerTxsBySender);

    final var controlOrderByFee = new TreeSet<>(this::compareByFee);
    controlOrderByFee.addAll(pendingTransactions.values());

    final var itControl = controlOrderByFee.iterator();
    final var itCurrent = orderByFee.iterator();

    while (itControl.hasNext()) {
      assert itControl.next().equals(itCurrent.next())
          : "orderByFee does not match pendingTransactions";
    }

    assert itCurrent.hasNext() == false : "orderByFee has more elements that pendingTransactions";
  }

  protected Wei getAndLogMinTransactionGasPrice() {
    final var currMinTransactionGasPrice = miningConfiguration.getMinTransactionGasPrice();
    logMinGasPrice(currMinTransactionGasPrice);
    return currMinTransactionGasPrice;
  }

  protected Wei getAndLogMinPriorityFeePerGas() {
    final var currMinPriorityFeePerGas = miningConfiguration.getMinPriorityFeePerGas();
    logMinPriorityFeePerGas(currMinPriorityFeePerGas);
    return currMinPriorityFeePerGas;
  }
}
