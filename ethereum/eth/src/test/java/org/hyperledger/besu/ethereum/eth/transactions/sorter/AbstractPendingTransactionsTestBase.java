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
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ADDED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ALREADY_KNOWN;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT;
import static org.hyperledger.besu.ethereum.eth.transactions.sorter.SequencedRemovalReason.EVICTED;
import static org.hyperledger.besu.ethereum.eth.transactions.sorter.SequencedRemovalReason.INVALID;
import static org.hyperledger.besu.ethereum.eth.transactions.sorter.SequencedRemovalReason.REPLACED;
import static org.hyperledger.besu.ethereum.eth.transactions.sorter.SequencedRemovalReason.TIMED_EVICTION;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public abstract class AbstractPendingTransactionsTestBase {

  protected static final int MAX_TRANSACTIONS = 5;
  protected static final int MAX_TRANSACTIONS_LARGE_POOL = 15;
  private static final float LIMITED_TRANSACTIONS_BY_SENDER_PERCENTAGE = 0.8f;
  protected static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  protected static final KeyPair KEYS1 = SIGNATURE_ALGORITHM.generateKeyPair();
  protected static final KeyPair KEYS2 = SIGNATURE_ALGORITHM.generateKeyPair();
  protected static final String ADDED_COUNTER = "transactions_added_total";
  protected static final String REMOVED_COUNTER = "transactions_removed_total";
  protected static final String REMOTE = "remote";
  protected static final String LOCAL = "local";
  protected static final String DROPPED = "dropped";

  protected final TestClock clock = new TestClock();
  protected final StubMetricsSystem metricsSystem = new StubMetricsSystem();
  protected AbstractPendingTransactionsSorter transactions =
      getPendingTransactions(
          ImmutableTransactionPoolConfiguration.builder()
              .txPoolMaxSize(MAX_TRANSACTIONS)
              .txPoolLimitByAccountPercentage(Fraction.fromFloat(1.0f))
              .build(),
          Optional.empty());
  private final TransactionPoolConfiguration senderLimitedConfig =
      ImmutableTransactionPoolConfiguration.builder()
          .txPoolMaxSize(MAX_TRANSACTIONS)
          .txPoolLimitByAccountPercentage(
              Fraction.fromFloat(LIMITED_TRANSACTIONS_BY_SENDER_PERCENTAGE))
          .build();
  protected PendingTransactions senderLimitedTransactions =
      getPendingTransactions(senderLimitedConfig, Optional.empty());
  protected AbstractPendingTransactionsSorter transactionsLarge =
      getPendingTransactions(
          ImmutableTransactionPoolConfiguration.builder()
              .txPoolMaxSize(MAX_TRANSACTIONS_LARGE_POOL)
              .txPoolLimitByAccountPercentage(Fraction.fromFloat(1.0f))
              .build(),
          Optional.empty());

  protected final Transaction transaction1 = createTransaction(2);
  protected final Transaction transaction2 = createTransaction(1);
  protected final Transaction transaction3 = createTransaction(3);

  protected final Transaction zeroGasPriceTX1Sdr1 = createZeroGasPriceTransactionSender1(1);
  protected final Transaction zeroGasPriceTX2Sdr1 = createZeroGasPriceTransactionSender1(2);
  protected final Transaction zeroGasPriceTX3Sdr1 = createZeroGasPriceTransactionSender1(3);
  protected final Transaction zeroGasPriceTX1Sdr2 = createZeroGasPriceTransactionSender2(1);
  protected final Transaction zeroGasPriceTX2Sdr2 = createZeroGasPriceTransactionSender2(2);
  protected final Transaction zeroGasPriceTX3Sdr2 = createZeroGasPriceTransactionSender2(3);

  protected final PendingTransactionAddedListener listener =
      mock(PendingTransactionAddedListener.class);
  protected final PendingTransactionDroppedListener droppedListener =
      mock(PendingTransactionDroppedListener.class);
  protected static final Address SENDER1 = Util.publicKeyToAddress(KEYS1.getPublicKey());
  protected static final Address SENDER2 = Util.publicKeyToAddress(KEYS2.getPublicKey());

  abstract AbstractPendingTransactionsSorter getPendingTransactions(
      final TransactionPoolConfiguration poolConfig, Optional<Clock> clock);

  @Test
  public void shouldReturnExclusivelyLocalTransactionsWhenAppropriate() {

    final Transaction localTransaction0 = createTransaction(0);
    transactions.addTransaction(createLocalPendingTransaction(localTransaction0), Optional.empty());
    assertThat(transactions.size()).isEqualTo(1);

    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(transactions.size()).isEqualTo(2);

    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());
    assertThat(transactions.size()).isEqualTo(3);

    final List<Transaction> localTransactions = transactions.getLocalTransactions();
    assertThat(localTransactions.size()).isEqualTo(1);
  }

  @Test
  public void shouldAddATransaction() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(1);

    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());
    assertThat(transactions.size()).isEqualTo(2);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(2);
  }

  @Test
  public void shouldReturnEmptyOptionalWhenNoTransactionWithGivenHashExists() {
    assertThat(transactions.getTransactionByHash(Hash.EMPTY_TRIE_HASH)).isEmpty();
  }

  @Test
  public void shouldGetTransactionByHash() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    assertTransactionPending(transaction1);
  }

  @Test
  public void shouldDropOldestTransactionWhenLimitExceeded() {
    final Transaction oldestTransaction =
        transactionWithNonceSenderAndGasPrice(0, SIGNATURE_ALGORITHM.generateKeyPair(), 10L);
    final Account oldestSender = mock(Account.class);
    when(oldestSender.getNonce()).thenReturn(0L);
    senderLimitedTransactions.addTransaction(
        createRemotePendingTransaction(oldestTransaction), Optional.of(oldestSender));
    for (int i = 1; i < MAX_TRANSACTIONS; i++) {
      final Account sender = mock(Account.class);
      when(sender.getNonce()).thenReturn((long) i);
      senderLimitedTransactions.addTransaction(
          createRemotePendingTransaction(
              transactionWithNonceSenderAndGasPrice(i, SIGNATURE_ALGORITHM.generateKeyPair(), 10L)),
          Optional.of(sender));
    }
    assertThat(senderLimitedTransactions.size()).isEqualTo(MAX_TRANSACTIONS);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isZero();

    final Account lastSender = mock(Account.class);
    when(lastSender.getNonce()).thenReturn(6L);
    senderLimitedTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(MAX_TRANSACTIONS + 1)),
        Optional.of(lastSender));
    assertThat(senderLimitedTransactions.size()).isEqualTo(MAX_TRANSACTIONS);
    assertTransactionNotPending(oldestTransaction);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(1);
  }

  @Test
  public void shouldDropTransactionWithATooFarNonce() {
    Transaction furthestFutureTransaction = null;
    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
      furthestFutureTransaction = transactionWithNonceSenderAndGasPrice(i, KEYS1, 10L);
      senderLimitedTransactions.addTransaction(
          createRemotePendingTransaction(furthestFutureTransaction), Optional.empty());
    }
    assertThat(senderLimitedTransactions.size())
        .isEqualTo(senderLimitedConfig.getTxPoolMaxFutureTransactionByAccount());
    assertThat(senderLimitedConfig.getTxPoolMaxFutureTransactionByAccount()).isEqualTo(4);
    assertThat(senderLimitedTransactions.getTransactionByHash(furthestFutureTransaction.getHash()))
        .isEmpty();
  }

  @Test
  public void shouldHandleMaximumTransactionLimitCorrectlyWhenSameTransactionAddedMultipleTimes() {
    transactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0)), Optional.empty());

    for (int i = 1; i < MAX_TRANSACTIONS; i++) {
      transactions.addTransaction(
          createRemotePendingTransaction(createTransaction(i)), Optional.empty());
    }
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);

    transactions.addTransaction(
        createRemotePendingTransaction(createTransaction(MAX_TRANSACTIONS + 1)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(createTransaction(MAX_TRANSACTIONS + 2)), Optional.empty());
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);
  }

  @Test
  public void shouldPrioritizeLocalTransaction() {
    final Transaction localTransaction = createTransaction(0);
    transactions.addTransaction(createLocalPendingTransaction(localTransaction), Optional.empty());

    for (int i = 1; i <= MAX_TRANSACTIONS; i++) {
      transactions.addTransaction(
          createRemotePendingTransaction(createTransaction(i)), Optional.empty());
    }
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);
    assertTransactionPending(localTransaction);
  }

  @Test
  public void shouldStartDroppingLocalTransactionsWhenPoolIsFullOfLocalTransactions() {
    Transaction lastLocalTransactionForSender = null;

    for (int i = 0; i <= MAX_TRANSACTIONS; i++) {
      lastLocalTransactionForSender = createTransaction(i);
      transactions.addTransaction(
          createLocalPendingTransaction(lastLocalTransactionForSender), Optional.empty());
    }
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);
    assertTransactionNotPending(lastLocalTransactionForSender);
  }

  @Test
  public void shouldNotifyListenerWhenRemoteTransactionAdded() {
    transactions.subscribePendingTransactions(listener);

    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());

    verify(listener).onTransactionAdded(transaction1);
  }

  @Test
  public void shouldNotNotifyListenerAfterUnsubscribe() {
    final long id = transactions.subscribePendingTransactions(listener);

    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());

    verify(listener).onTransactionAdded(transaction1);

    transactions.unsubscribePendingTransactions(id);

    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());

    verifyNoMoreInteractions(listener);
  }

  @Test
  public void shouldNotifyListenerWhenLocalTransactionAdded() {
    transactions.subscribePendingTransactions(listener);

    transactions.addTransaction(createLocalPendingTransaction(transaction1), Optional.empty());

    verify(listener).onTransactionAdded(transaction1);
  }

  @Test
  public void shouldNotifyDroppedListenerWhenRemoteTransactionDropped() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());

    transactions.subscribeDroppedTransactions(droppedListener);

    transactions.removeTransaction(transaction1, TIMED_EVICTION);

    verify(droppedListener).onTransactionDropped(transaction1, TIMED_EVICTION);
  }

  @Test
  public void shouldNotNotifyDroppedListenerAfterUnsubscribe() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());

    final long id = transactions.subscribeDroppedTransactions(droppedListener);

    transactions.removeTransaction(transaction1, EVICTED);

    verify(droppedListener).onTransactionDropped(transaction1, EVICTED);

    transactions.unsubscribeDroppedTransactions(id);

    transactions.removeTransaction(transaction2, EVICTED);

    verifyNoMoreInteractions(droppedListener);
  }

  @Test
  public void shouldNotifyDroppedListenerWhenLocalTransactionDropped() {
    transactions.addTransaction(createLocalPendingTransaction(transaction1), Optional.empty());

    transactions.subscribeDroppedTransactions(droppedListener);

    transactions.removeTransaction(transaction1, REPLACED);

    verify(droppedListener).onTransactionDropped(transaction1, REPLACED);
  }

  @Test
  public void shouldNotNotifyDroppedListenerWhenTransactionAddedToBlock() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());

    transactions.subscribeDroppedTransactions(droppedListener);

    transactions.transactionAddedToBlock(transaction1);

    verifyNoInteractions(droppedListener);
  }

  @Test
  public void selectTransactionsInCorrectOrder() {
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX2Sdr1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX3Sdr1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX1Sdr2), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX1Sdr1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX2Sdr2), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactionsLarge.addTransaction(
                createRemotePendingTransaction(zeroGasPriceTX3Sdr2), Optional.empty()))
        .isEqualTo(ADDED);

    transactionsLarge.selectTransactions(
        pendingTxs -> {
          // The order should be:
          // sender 2, nonce 1
          // sender 1, nonce 1
          // sender 1, nonce 2
          // sender 1, nonce 3
          // sender 2, nonce 2
          // sender 2, nonce 3
          assertThat(pendingTxs.get(0).getTransaction()).isEqualTo(zeroGasPriceTX1Sdr2);
          assertThat(pendingTxs.get(1).getTransaction()).isEqualTo(zeroGasPriceTX1Sdr1);
          assertThat(pendingTxs.get(2).getTransaction()).isEqualTo(zeroGasPriceTX2Sdr1);
          assertThat(pendingTxs.get(3).getTransaction()).isEqualTo(zeroGasPriceTX3Sdr1);
          assertThat(pendingTxs.get(4).getTransaction()).isEqualTo(zeroGasPriceTX2Sdr2);
          assertThat(pendingTxs.get(5).getTransaction()).isEqualTo(zeroGasPriceTX3Sdr2);

          return Collections.emptyMap();
        });
  }

  @Test
  public void shouldNotSelectReplacedTransaction() {
    final Transaction transaction1 = transactionWithNonceSenderAndGasPrice(1, KEYS1, 1);
    final Transaction transaction2 = transactionWithNonceSenderAndGasPrice(1, KEYS1, 2);

    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());

    transactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs)
              .map(PendingTransaction::getTransaction)
              .containsExactly(transaction2);
          return Map.of(pendingTxs.get(0), SELECTED);
        });
  }

  @Test
  public void invalidTransactionIsDeletedFromPendingTransactions() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());

    transactions.selectTransactions(
        pendingTxs ->
            pendingTxs.stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        pt ->
                            TransactionSelectionResult.invalid(
                                TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE.name()))));

    assertThat(transactions.size()).isZero();
  }

  @Test
  public void shouldReturnEmptyOptionalAsMaximumNonceWhenNoTransactionsPresent() {
    assertThat(transactions.getNextNonceForSender(SENDER1)).isEmpty();
  }

  @Test
  public void shouldReturnEmptyOptionalAsMaximumNonceWhenLastTransactionForSenderRemoved() {
    final Transaction transaction = transactionWithNonceAndSender(1, KEYS1);
    transactions.addTransaction(createRemotePendingTransaction(transaction), Optional.empty());
    transactions.removeTransaction(transaction, INVALID);
    assertThat(transactions.getNextNonceForSender(SENDER1)).isEmpty();
  }

  @Test
  public void shouldReplaceTransactionWithSameSenderAndNonce() {
    final Transaction transaction1 = transactionWithNonceSenderAndGasPrice(1, KEYS1, 1);
    final Transaction transaction1b = transactionWithNonceSenderAndGasPrice(1, KEYS1, 2);
    final Transaction transaction2 = transactionWithNonceSenderAndGasPrice(2, KEYS1, 1);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction2), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1b), Optional.empty()))
        .isEqualTo(ADDED);

    assertTransactionNotPending(transaction1);
    assertTransactionPending(transaction1b);
    assertTransactionPending(transaction2);
    assertThat(transactions.size()).isEqualTo(2);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(3);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(1);
  }

  @Test
  public void shouldReplaceTransactionWithSameSenderAndNonce_multipleReplacements() {
    final int replacedTxCount = 5;
    final List<Transaction> replacedTransactions = new ArrayList<>();
    for (int i = 0; i < replacedTxCount; i++) {
      final Transaction duplicateTx = transactionWithNonceSenderAndGasPrice(1, KEYS1, i + 1);
      replacedTransactions.add(duplicateTx);
      transactions.addTransaction(createRemotePendingTransaction(duplicateTx), Optional.empty());
    }
    final Transaction finalReplacingTx = transactionWithNonceSenderAndGasPrice(1, KEYS1, 100);
    final Transaction independentTx = transactionWithNonceSenderAndGasPrice(2, KEYS1, 1);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(independentTx), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(finalReplacingTx), Optional.empty()))
        .isEqualTo(ADDED);

    // All tx's except the last duplicate should be removed
    replacedTransactions.forEach(this::assertTransactionNotPending);
    assertTransactionPending(finalReplacingTx);
    // Tx with distinct nonce should be maintained
    assertTransactionPending(independentTx);

    assertThat(transactions.size()).isEqualTo(2);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(replacedTxCount + 2);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED))
        .isEqualTo(replacedTxCount);
  }

  @Test
  public void
      shouldReplaceTransactionWithSameSenderAndNonce_multipleReplacementsAddedLocallyAndRemotely() {
    final int replacedTxCount = 11;
    final List<Transaction> replacedTransactions = new ArrayList<>();
    int remoteDuplicateCount = 0;
    for (int i = 0; i < replacedTxCount; i++) {
      final Transaction duplicateTx =
          transactionWithNonceSenderAndGasPrice(1, KEYS1, (i * 110 / 100) + 1);
      replacedTransactions.add(duplicateTx);
      if (i % 2 == 0) {
        transactions.addTransaction(createRemotePendingTransaction(duplicateTx), Optional.empty());
        remoteDuplicateCount++;
      } else {
        transactions.addTransaction(createLocalPendingTransaction(duplicateTx), Optional.empty());
      }
    }
    final Transaction finalReplacingTx = transactionWithNonceSenderAndGasPrice(1, KEYS1, 100);
    final Transaction independentTx = transactionWithNonceSenderAndGasPrice(2, KEYS1, 1);
    assertThat(
            transactions.addTransaction(
                createLocalPendingTransaction(finalReplacingTx), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(independentTx), Optional.empty()))
        .isEqualTo(ADDED);

    // All tx's except the last duplicate should be removed
    replacedTransactions.forEach(this::assertTransactionNotPending);
    assertTransactionPending(finalReplacingTx);
    // Tx with distinct nonce should be maintained
    assertTransactionPending(independentTx);

    final int localDuplicateCount = replacedTxCount - remoteDuplicateCount;
    assertThat(transactions.size()).isEqualTo(2);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE))
        .isEqualTo(remoteDuplicateCount + 1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, LOCAL))
        .isEqualTo(localDuplicateCount + 1);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED))
        .isEqualTo(remoteDuplicateCount);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, LOCAL, DROPPED))
        .isEqualTo(localDuplicateCount);
  }

  @Test
  public void shouldReplaceOnlyTransactionFromSenderWhenItHasTheSameNonce() {
    final Transaction transaction1 = transactionWithNonceSenderAndGasPrice(1, KEYS1, 1);
    final Transaction transaction1b = transactionWithNonceSenderAndGasPrice(1, KEYS1, 2);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1b), Optional.empty()))
        .isEqualTo(ADDED);

    assertTransactionNotPending(transaction1);
    assertTransactionPending(transaction1b);
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(2);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(1);
  }

  @Test
  public void shouldNotReplaceTransactionWithSameSenderAndNonceWhenGasPriceIsLower() {
    final Transaction transaction1 = transactionWithNonceSenderAndGasPrice(1, KEYS1, 2);
    final Transaction transaction1b = transactionWithNonceSenderAndGasPrice(1, KEYS1, 1);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ADDED);

    transactions.subscribePendingTransactions(listener);
    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1b), Optional.empty()))
        .isEqualTo(REJECTED_UNDERPRICED_REPLACEMENT);

    assertTransactionNotPending(transaction1b);
    assertTransactionPending(transaction1);
    assertThat(transactions.size()).isEqualTo(1);
    verifyNoInteractions(listener);
  }

  @Test
  public void shouldTrackMaximumNonceForEachSender() {
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(0, KEYS1)), Optional.empty());
    assertMaximumNonceForSender(SENDER1, 1);

    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(1, KEYS1)), Optional.empty());
    assertMaximumNonceForSender(SENDER1, 2);

    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(2, KEYS1)), Optional.empty());
    assertMaximumNonceForSender(SENDER1, 3);

    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(4, KEYS2)), Optional.empty());
    assertMaximumNonceForSender(SENDER2, 5);
    assertMaximumNonceForSender(SENDER1, 3);
  }

  @Test
  public void shouldNotIncreasePriorityOfTransactionsBecauseOfNonceOrder() {
    final Transaction transaction1 = transactionWithNonceAndSender(0, KEYS1);
    final Transaction transaction2 = transactionWithNonceAndSender(1, KEYS1);
    final Transaction transaction3 = transactionWithNonceAndSender(2, KEYS1);
    final Transaction transaction4 = transactionWithNonceAndSender(4, KEYS2);

    transactions.addTransaction(createLocalPendingTransaction(transaction3), Optional.empty());
    transactions.addTransaction(createLocalPendingTransaction(transaction4), Optional.empty());
    transactions.addTransaction(createLocalPendingTransaction(transaction2), Optional.empty());
    transactions.addTransaction(createLocalPendingTransaction(transaction1), Optional.empty());

    transactions.selectTransactions(
        pendingTxs -> {
          // Ignoring nonces, the order would be 3, 4, 2, 1 but we have to delay 3 and 2 until after
          // 1.
          assertThat(pendingTxs)
              .map(PendingTransaction::getTransaction)
              .containsExactly(transaction4, transaction1, transaction2, transaction3);

          return pendingTxs.stream().collect(Collectors.toMap(Function.identity(), pt -> SELECTED));
        });
  }

  @Test
  public void shouldReturnZeroStatusWhenPoolIsEmpty() {
    final PendingTransactions.Status status = transactions.getStatus();
    assertThat(status.pendingCount()).isZero();
    assertThat(status.queuedCount()).isZero();
  }

  @Test
  public void shouldCountAllTransactionsAsPendingWhenNonceIsSequential() {
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(0, KEYS1)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(1, KEYS1)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(2, KEYS1)), Optional.empty());

    final PendingTransactions.Status status = transactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(3);
    assertThat(status.queuedCount()).isZero();
  }

  @Test
  public void shouldCountTransactionsBeyondNonceGapAsQueued() {
    // nonce 0 is pending, nonce 2 is queued (gap at 1)
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(0, KEYS1)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(2, KEYS1)), Optional.empty());

    final PendingTransactions.Status status = transactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(1);
    assertThat(status.queuedCount()).isEqualTo(1);
  }

  @Test
  public void shouldAggregatePendingAndQueuedAcrossMultipleSenders() {
    // SENDER1: nonces 0, 1 — sequential, all pending
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(0, KEYS1)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(1, KEYS1)), Optional.empty());
    // SENDER2: nonces 0, 2 — gap at 1: 1 pending, 1 queued
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(0, KEYS2)), Optional.empty());
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(2, KEYS2)), Optional.empty());

    final PendingTransactions.Status status = transactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(3);
    assertThat(status.queuedCount()).isEqualTo(1);
  }

  @Test
  public void shouldUseAccountNonceToComputePendingCountWhenGapIsPresent() {
    // account nonce is 2, txs at 2, 3, 5 — gap at 4: 2 pending (nonces 2,3), 1 queued (nonce 5)
    final Account sender = mock(Account.class);
    when(sender.getNonce()).thenReturn(2L);
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(2, KEYS1)),
        Optional.of(sender));
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(3, KEYS1)),
        Optional.of(sender));
    transactions.addTransaction(
        createRemotePendingTransaction(transactionWithNonceAndSender(5, KEYS1)),
        Optional.of(sender));

    final PendingTransactions.Status status = transactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(2);
    assertThat(status.queuedCount()).isEqualTo(1);
  }

  protected void assertMaximumNonceForSender(final Address sender1, final int i) {
    assertThat(transactions.getNextNonceForSender(sender1)).isEqualTo(OptionalLong.of(i));
  }

  protected Transaction transactionWithNonceAndSender(final int nonce, final KeyPair keyPair) {
    return new TransactionTestFixture().nonce(nonce).createTransaction(keyPair);
  }

  protected Transaction transactionWithNonceSenderAndGasPrice(
      final int nonce, final KeyPair keyPair, final long gasPrice) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasPrice(Wei.of(gasPrice))
        .createTransaction(keyPair);
  }

  protected void assertTransactionPending(final Transaction t) {
    assertThat(transactions.getTransactionByHash(t.getHash())).contains(t);
  }

  protected void assertTransactionNotPending(final Transaction t) {
    assertThat(transactions.getTransactionByHash(t.getHash())).isEmpty();
  }

  protected Transaction createTransaction(final long transactionNumber) {
    return new TransactionTestFixture()
        .value(Wei.of(transactionNumber))
        .nonce(transactionNumber)
        .gasPrice(Wei.of(0))
        .createTransaction(KEYS1);
  }

  protected Transaction createZeroGasPriceTransactionSender1(final long transactionNumber) {
    return new TransactionTestFixture()
        .value(Wei.of(transactionNumber))
        .nonce(transactionNumber)
        .gasPrice(Wei.of(0))
        .maxFeePerGas(Optional.of(Wei.of(0)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(0)))
        .createTransaction(KEYS1);
  }

  protected Transaction createZeroGasPriceTransactionSender2(final long transactionNumber) {
    return new TransactionTestFixture()
        .value(Wei.of(transactionNumber))
        .nonce(transactionNumber)
        .gasPrice(Wei.of(0))
        .maxFeePerGas(Optional.of(Wei.of(0)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(0)))
        .createTransaction(KEYS2);
  }

  private PendingTransaction createRemotePendingTransaction(
      final Transaction transaction, final long addedAt) {
    return PendingTransaction.newPendingTransaction(transaction, false, false, MAX_SCORE, addedAt);
  }

  private PendingTransaction createRemotePendingTransaction(final Transaction transaction) {
    return PendingTransaction.newPendingTransaction(transaction, false, false, MAX_SCORE);
  }

  private PendingTransaction createLocalPendingTransaction(final Transaction transaction) {
    return PendingTransaction.newPendingTransaction(transaction, true, true, MAX_SCORE);
  }

  @Test
  public void shouldEvictMultipleOldTransactions() {
    final int maxTransactionRetentionHours = 1;
    final PendingTransactions transactions =
        getPendingTransactions(
            ImmutableTransactionPoolConfiguration.builder()
                .pendingTxRetentionPeriod(maxTransactionRetentionHours)
                .txPoolMaxSize(MAX_TRANSACTIONS)
                .txPoolLimitByAccountPercentage(Fraction.fromFloat(1.0f))
                .build(),
            Optional.of(clock));

    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(transactions.size()).isEqualTo(1);
    transactions.addTransaction(createRemotePendingTransaction(transaction2), Optional.empty());
    assertThat(transactions.size()).isEqualTo(2);

    clock.step(2L, ChronoUnit.HOURS);
    transactions.evictOldTransactions();
    assertThat(transactions.size()).isEqualTo(0);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(2);
  }

  @Test
  public void shouldEvictSingleOldTransaction() {
    final PendingTransactions evictSingleTransactions =
        getPendingTransactions(
            ImmutableTransactionPoolConfiguration.builder()
                .pendingTxRetentionPeriod(1)
                .txPoolMaxSize(MAX_TRANSACTIONS)
                .txPoolLimitByAccountPercentage(Fraction.fromFloat(1.0f))
                .build(),
            Optional.of(clock));
    evictSingleTransactions.addTransaction(
        createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(evictSingleTransactions.size()).isEqualTo(1);
    clock.step(2L, ChronoUnit.HOURS);
    evictSingleTransactions.evictOldTransactions();
    assertThat(evictSingleTransactions.size()).isEqualTo(0);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(1);
  }

  @Test
  public void shouldEvictExclusivelyOldTransactions() {
    final PendingTransactions twoHourEvictionTransactionPool =
        getPendingTransactions(
            ImmutableTransactionPoolConfiguration.builder()
                .pendingTxRetentionPeriod(2)
                .txPoolMaxSize(MAX_TRANSACTIONS)
                .txPoolLimitByAccountPercentage(Fraction.fromFloat(1.0f))
                .build(),
            Optional.of(clock));

    twoHourEvictionTransactionPool.subscribeDroppedTransactions(droppedListener);

    twoHourEvictionTransactionPool.addTransaction(
        createRemotePendingTransaction(transaction1, clock.millis()), Optional.empty());
    assertThat(twoHourEvictionTransactionPool.size()).isEqualTo(1);
    clock.step(3L, ChronoUnit.HOURS);
    twoHourEvictionTransactionPool.addTransaction(
        createRemotePendingTransaction(transaction2, clock.millis()), Optional.empty());
    assertThat(twoHourEvictionTransactionPool.size()).isEqualTo(2);
    twoHourEvictionTransactionPool.evictOldTransactions();
    assertThat(twoHourEvictionTransactionPool.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(REMOVED_COUNTER, REMOTE, DROPPED)).isEqualTo(1);
    verify(droppedListener).onTransactionDropped(transaction1, TIMED_EVICTION);
  }

  @Test
  public void shouldNotIncrementAddedCounterWhenRemoteTransactionAlreadyPresent() {
    transactions.addTransaction(createLocalPendingTransaction(transaction1), Optional.empty());
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, LOCAL)).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(0);

    assertThat(
            transactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ALREADY_KNOWN);
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, LOCAL)).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(0);
  }

  @Test
  public void shouldNotIncrementAddedCounterWhenLocalTransactionAlreadyPresent() {
    transactions.addTransaction(createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, LOCAL)).isEqualTo(0);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(1);

    assertThat(
            transactions.addTransaction(
                createLocalPendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ALREADY_KNOWN);
    assertThat(transactions.size()).isEqualTo(1);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, LOCAL)).isEqualTo(0);
    assertThat(metricsSystem.getCounterValue(ADDED_COUNTER, REMOTE)).isEqualTo(1);
  }

  @Test
  public void assertThatCorrectNonceIsReturned() {
    assertThat(transactions.getNextNonceForSender(transaction1.getSender())).isEmpty();
    addLocalTransactions(1, 2, 4, 5);
    assertThat(transactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(3);
    addLocalTransactions(3);
    assertThat(transactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(6);
    addLocalTransactions(6, 10);

    // assert that transactions are pruned by account from latest future nonces first
    assertThat(transactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(6);
  }

  @Test
  public void assertThatCorrectNonceIsReturnedForSenderLimitedPool() {
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction1.getSender())).isEmpty();
    addLocalTransactions(senderLimitedTransactions, 1, 2, 4, 5);
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(3);
    addLocalTransactions(senderLimitedTransactions, 3);

    // assert we have dropped previously added tx 5, and next nonce is now 5
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(5);
    addLocalTransactions(senderLimitedTransactions, 6, 10);

    // assert that we drop future nonces first:
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(5);
  }

  @Test
  public void assertThatCorrectNonceIsReturnedLargeGap() {
    assertThat(transactions.getNextNonceForSender(transaction1.getSender())).isEmpty();
    addLocalTransactions(1, 2, Long.MAX_VALUE);
    assertThat(transactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(3);
    addLocalTransactions(3);
  }

  @Test
  public void assertThatCorrectNonceIsReturnedWithRepeatedTXes() {
    assertThat(transactions.getNextNonceForSender(transaction1.getSender())).isEmpty();
    addLocalTransactions(1, 2, 4, 4, 4, 4, 4, 4, 4, 4);
    assertThat(transactions.getNextNonceForSender(transaction1.getSender()))
        .isPresent()
        .hasValue(3);
    addLocalTransactions(3);
  }

  protected void addLocalTransactions(final long... nonces) {
    addLocalTransactions(transactions, nonces);
  }

  protected void addLocalTransactions(final PendingTransactions sorter, final long... nonces) {
    for (final long nonce : nonces) {
      final Account sender = mock(Account.class);
      when(sender.getNonce()).thenReturn(1L);
      sorter.addTransaction(
          createLocalPendingTransaction(createTransaction(nonce)), Optional.of(sender));
    }
  }

  protected static BlockHeader mockBlockHeader() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.empty());
    return blockHeader;
  }

  @Test
  public void shouldPrioritizeGasPriceThenTimeAddedToPool() {
    // Make sure the 100 gas price TX isn't dropped
    transactions.subscribeDroppedTransactions(
        (transaction, reason) ->
            assertThat(transaction.getGasPrice().get().toLong()).isLessThan(100));

    // Fill the pool with transactions from random senders
    final List<Transaction> lowGasPriceTransactions =
        IntStream.range(0, MAX_TRANSACTIONS)
            .mapToObj(
                i -> {
                  final Account randomSender = mock(Account.class);
                  final Transaction lowPriceTx =
                      transactionWithNonceSenderAndGasPrice(
                          0, SIGNATURE_ALGORITHM.generateKeyPair(), 10);
                  transactions.addTransaction(
                      createRemotePendingTransaction(lowPriceTx), Optional.of(randomSender));
                  return lowPriceTx;
                })
            .collect(Collectors.toUnmodifiableList());

    // This should kick the highest-sequence tx with the low gas price out, namely the most-recent
    // one we added
    final Account highPriceSender = mock(Account.class);
    final Transaction highGasPriceTransaction =
        transactionWithNonceSenderAndGasPrice(0, KEYS1, 100);
    transactions.addTransaction(
        createRemotePendingTransaction(highGasPriceTransaction), Optional.of(highPriceSender));
    assertThat(transactions.size()).isEqualTo(MAX_TRANSACTIONS);

    assertTransactionPending(highGasPriceTransaction);
    assertTransactionNotPending(lowGasPriceTransactions.get(lowGasPriceTransactions.size() - 1));
    lowGasPriceTransactions.stream()
        .limit(lowGasPriceTransactions.size() - 1)
        .forEach(this::assertTransactionPending);
  }
}
