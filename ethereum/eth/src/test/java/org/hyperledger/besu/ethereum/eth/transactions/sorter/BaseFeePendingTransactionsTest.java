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
package org.hyperledger.besu.ethereum.eth.transactions.sorter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.TX_EVALUATION_TOO_LONG;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

public class BaseFeePendingTransactionsTest extends AbstractPendingTransactionsTestBase {

  @Override
  AbstractPendingTransactionsSorter getPendingTransactions(
      final TransactionPoolConfiguration poolConfig, final Optional<Clock> clock) {
    return new BaseFeePendingTransactionsSorter(
        poolConfig,
        clock.orElse(TestClock.system(ZoneId.systemDefault())),
        metricsSystem,
        AbstractPendingTransactionsTestBase::mockBlockHeader,
        transactionEventNotifier.newEmitter(),
        new DeterministicEthScheduler());
  }

  private static final Random randomizeTxType = new Random();
  private static final Wei BASE_FEE = Wei.of(10L);

  @Override
  protected Transaction createTransaction(final long transactionNumber) {
    var tx = new TransactionTestFixture().value(Wei.of(transactionNumber)).nonce(transactionNumber);
    if (randomizeTxType.nextBoolean()) {
      tx.type(TransactionType.EIP1559)
          .maxFeePerGas(Optional.of(Wei.of(5000L)))
          .maxPriorityFeePerGas(Optional.of(Wei.of(50L)));
    }
    return tx.createTransaction(KEYS1);
  }

  @Test
  public void shouldSelectByScoreThenPriorityAcrossStaticAndDynamicRanges() {
    // with a base fee, a legacy tx is in the dynamic range, while a 1559 one, that can pay its
    // whole max priority fee, is in the static range
    ((BaseFeePendingTransactionsSorter) transactions).updateBaseFee(BASE_FEE);
    final Transaction priorityDynamicTx = transactionWithNonceSenderAndGasPrice(0, KEYS1, 20L);
    final Transaction staticTx = eip1559Transaction(KEYS2);

    transactions.addTransaction(
        PendingTransaction.newPendingTransaction(staticTx, false, false, MAX_SCORE),
        Optional.empty());
    transactions.addTransaction(
        PendingTransaction.newPendingTransaction(priorityDynamicTx, true, true, MAX_SCORE),
        Optional.empty());

    // the priority tx is selected first, even if it pays less, and it takes too long
    final List<Transaction> firstSelection = new ArrayList<>();
    transactions.selectTransactions(
        pendingTxs -> {
          pendingTxs.forEach(pendingTx -> firstSelection.add(pendingTx.getTransaction()));
          return Map.of(
              pendingTxs.get(0), CompletableFuture.completedFuture(TX_EVALUATION_TOO_LONG));
        });
    assertThat(firstSelection).containsExactly(priorityDynamicTx, staticTx);

    // once penalized, having priority does not keep it ahead of the not penalized tx
    final List<Transaction> secondSelection = new ArrayList<>();
    transactions.selectTransactions(
        pendingTxs -> {
          pendingTxs.forEach(pendingTx -> secondSelection.add(pendingTx.getTransaction()));
          return Map.of();
        });
    assertThat(secondSelection).containsExactly(staticTx, priorityDynamicTx);
  }

  @Test
  public void shouldNotEvictPriorityTransactionPayingLessThanNonPriorityOnesInAnotherRange() {
    // a priority tx in the dynamic range, that pays less than all the txs in the static range
    ((BaseFeePendingTransactionsSorter) transactions).updateBaseFee(BASE_FEE);
    final Transaction priorityDynamicTx = transactionWithNonceSenderAndGasPrice(0, KEYS1, 20L);
    transactions.addTransaction(
        PendingTransaction.newPendingTransaction(priorityDynamicTx, true, true, MAX_SCORE),
        Optional.empty());

    final List<Transaction> staticTxs = new ArrayList<>();
    for (int i = 1; i < MAX_TRANSACTIONS; i++) {
      final Transaction staticTx = eip1559Transaction(SIGNATURE_ALGORITHM.generateKeyPair());
      staticTxs.add(staticTx);
      transactions.addTransaction(
          PendingTransaction.newPendingTransaction(staticTx, false, false, MAX_SCORE),
          Optional.empty());
    }
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);

    // the pool is full, so adding a tx evicts the least priority one, that is the newest of the
    // non-priority txs, and not the priority one
    final Transaction evictedOnAdd = eip1559Transaction(SIGNATURE_ALGORITHM.generateKeyPair());
    transactions.addTransaction(
        PendingTransaction.newPendingTransaction(evictedOnAdd, false, false, MAX_SCORE),
        Optional.empty());

    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);
    assertTransactionPending(priorityDynamicTx);
    staticTxs.forEach(this::assertTransactionPending);
    assertTransactionNotPending(evictedOnAdd);
  }

  private Transaction eip1559Transaction(final KeyPair keyPair) {
    return new TransactionTestFixture()
        .nonce(0)
        .type(TransactionType.EIP1559)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(50L)))
        .createTransaction(keyPair);
  }
}
