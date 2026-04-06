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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.hyperledger.besu.datatypes.TransactionType.BLOB;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ADDED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ALREADY_KNOWN;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.AddReason.MOVE;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.AddReason.NEW;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.DROPPED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.INVALIDATED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.REPLACED;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.GAS_PRICE_BELOW_CURRENT_BASE_FEE;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolReplacementHandler;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.SequencedMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LayeredPendingTransactionsTest extends BaseTransactionPoolTest {

  protected static final int MAX_TRANSACTIONS = 5;
  protected static final int MAX_PRIORITIZED_BLOB_TRANSACTIONS = MAX_TRANSACTIONS + 1;
  protected static final int MAX_CAPACITY_BYTES = 150_000;
  protected static final Wei DEFAULT_BASE_FEE = Wei.of(100);
  protected static final int LIMITED_TRANSACTIONS_BY_SENDER = 4;
  protected static final String REMOTE = "remote";
  protected static final String LOCAL = "local";
  protected static final String NO_PRIORITY = "no";
  protected final PendingTransactionAddedListener listener =
      mock(PendingTransactionAddedListener.class);
  protected final PendingTransactionDroppedListener droppedListener =
      mock(PendingTransactionDroppedListener.class);

  private final TransactionPoolConfiguration poolConf =
      ImmutableTransactionPoolConfiguration.builder()
          .maxPrioritizedTransactions(MAX_TRANSACTIONS)
          .maxPrioritizedTransactionsByType(Map.of(BLOB, MAX_PRIORITIZED_BLOB_TRANSACTIONS))
          .maxFutureBySender(MAX_TRANSACTIONS)
          .build();

  private final TransactionPoolConfiguration senderLimitedConfig =
      ImmutableTransactionPoolConfiguration.builder()
          .maxPrioritizedTransactions(MAX_TRANSACTIONS)
          .maxPrioritizedTransactionsByType(Map.of(BLOB, MAX_PRIORITIZED_BLOB_TRANSACTIONS))
          .maxFutureBySender(LIMITED_TRANSACTIONS_BY_SENDER)
          .build();

  private final TransactionPoolConfiguration smallPoolConfig =
      ImmutableTransactionPoolConfiguration.builder()
          .maxPrioritizedTransactions(MAX_TRANSACTIONS)
          .maxPrioritizedTransactionsByType(Map.of(BLOB, MAX_PRIORITIZED_BLOB_TRANSACTIONS))
          .maxFutureBySender(LIMITED_TRANSACTIONS_BY_SENDER)
          .pendingTransactionsLayerMaxCapacityBytes(MAX_CAPACITY_BYTES)
          .build();

  private LayeredPendingTransactions senderLimitedTransactions;
  private LayeredPendingTransactions pendingTransactions;
  private LayeredPendingTransactions smallPendingTransactions;
  private CreatedLayers senderLimitedLayers;
  private CreatedLayers layers;
  private CreatedLayers smallLayers;
  private TransactionPoolMetrics txPoolMetrics;

  private static BlockHeader mockBlockHeader() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(DEFAULT_BASE_FEE));
    return blockHeader;
  }

  private CreatedLayers createLayers(final TransactionPoolConfiguration poolConfig) {

    final BiFunction<PendingTransaction, PendingTransaction, Boolean> transactionReplacementTester =
        (t1, t2) ->
            new TransactionPoolReplacementHandler(
                    poolConf.getPriceBump(), poolConfig.getBlobPriceBump())
                .shouldReplace(t1, t2, mockBlockHeader());

    final EvictCollectorLayer evictCollector = new EvictCollectorLayer(txPoolMetrics);

    final SparseTransactions sparseTransactions =
        new SparseTransactions(
            poolConfig,
            ethScheduler,
            evictCollector,
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache());

    final ReadyTransactions readyTransactions =
        new ReadyTransactions(
            poolConfig,
            ethScheduler,
            sparseTransactions,
            txPoolMetrics,
            transactionReplacementTester,
            new BlobCache());

    final BaseFeePrioritizedTransactions prioritizedTransactions =
        new BaseFeePrioritizedTransactions(
            poolConfig,
            LayeredPendingTransactionsTest::mockBlockHeader,
            ethScheduler,
            readyTransactions,
            txPoolMetrics,
            transactionReplacementTester,
            FeeMarket.london(0L),
            new BlobCache(),
            MiningConfiguration.newDefault().setMinTransactionGasPrice(DEFAULT_MIN_GAS_PRICE),
            senderBalanceChecker);
    return new CreatedLayers(
        prioritizedTransactions, readyTransactions, sparseTransactions, evictCollector);
  }

  @BeforeEach
  public void setup() {

    txPoolMetrics = new TransactionPoolMetrics(metricsSystem);

    layers = createLayers(poolConf);
    senderLimitedLayers = createLayers(senderLimitedConfig);
    smallLayers = createLayers(smallPoolConfig);

    pendingTransactions =
        new LayeredPendingTransactions(poolConf, layers.prioritizedTransactions, ethScheduler);

    senderLimitedTransactions =
        new LayeredPendingTransactions(
            senderLimitedConfig, senderLimitedLayers.prioritizedTransactions, ethScheduler);

    smallPendingTransactions =
        new LayeredPendingTransactions(
            smallPoolConfig, smallLayers.prioritizedTransactions, ethScheduler);
  }

  @Test
  public void returnExclusivelyLocalTransactionsWhenAppropriate() {
    final Transaction localTransaction0 = createTransaction(0, KEYS2);
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(localTransaction0), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(1);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(2);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(3);

    final List<Transaction> localTransactions = pendingTransactions.getLocalTransactions();
    assertThat(localTransactions.size()).isEqualTo(1);
  }

  @Test
  public void addRemoteTransactions() {
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(1);

    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(1);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction1), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(2);

    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(2);
  }

  @Test
  public void getNotPresentTransaction() {
    assertThat(pendingTransactions.getTransactionByHash(Hash.EMPTY_TRIE_HASH)).isEmpty();
  }

  @Test
  public void getTransactionByHash() {
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());
    assertTransactionPending(pendingTransactions, transaction0);
  }

  @Test
  public void evictTransactionsWhenSizeLimitExceeded() {
    final List<Transaction> firstTxs = new ArrayList<>(MAX_TRANSACTIONS);

    smallPendingTransactions.subscribeDroppedTransactions(droppedListener);

    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
      final Account sender = mock(Account.class);
      when(sender.getNonce()).thenReturn((long) i);
      final var tx =
          createTransactionOfSize(
              i,
              DEFAULT_BASE_FEE.add(i),
              (int) smallPoolConfig.getPendingTransactionsLayerMaxCapacityBytes() + 1,
              SIGNATURE_ALGORITHM.generateKeyPair());
      smallPendingTransactions.addTransaction(
          createRemotePendingTransaction(tx), Optional.of(sender));
      firstTxs.add(tx);
      assertTransactionPending(smallPendingTransactions, tx);
    }

    assertThat(smallPendingTransactions.size()).isEqualTo(MAX_TRANSACTIONS);

    final Transaction lastBigTx =
        createTransactionOfSize(
            0,
            DEFAULT_MIN_GAS_PRICE.multiply(1000),
            (int) smallPoolConfig.getPendingTransactionsLayerMaxCapacityBytes(),
            SIGNATURE_ALGORITHM.generateKeyPair());
    final Account lastSender = mock(Account.class);
    when(lastSender.getNonce()).thenReturn(0L);
    smallPendingTransactions.addTransaction(
        createRemotePendingTransaction(lastBigTx), Optional.of(lastSender));
    assertTransactionPending(smallPendingTransactions, lastBigTx);

    assertTransactionNotPending(smallPendingTransactions, firstTxs.get(0));
    assertThat(
            getRemovedCount(
                REMOTE, NO_PRIORITY, DROPPED.label(), smallLayers.evictedCollector.name()))
        .isEqualTo(1);
    // before get evicted definitively, the tx moves to the lower layers, where it does not fix,
    // until is discarded
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, MOVE, smallLayers.readyTransactions.name()))
        .isEqualTo(1);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, MOVE, smallLayers.sparseTransactions.name()))
        .isEqualTo(1);
    assertThat(smallLayers.evictedCollector.getEvictedTransactions())
        .map(PendingTransaction::getTransaction)
        .contains(firstTxs.get(0));
    verify(droppedListener).onTransactionDropped(firstTxs.get(0), DROPPED);
  }

  @Test
  public void txsMovingToNextLayerWhenFirstIsFull() {
    final List<Transaction> txs = new ArrayList<>(MAX_TRANSACTIONS + 1);

    pendingTransactions.subscribeDroppedTransactions(droppedListener);

    for (int i = 0; i < MAX_TRANSACTIONS + 1; i++) {
      final Account sender = mock(Account.class);
      when(sender.getNonce()).thenReturn((long) i);
      final var tx =
          createTransaction(i, DEFAULT_BASE_FEE.add(i), SIGNATURE_ALGORITHM.generateKeyPair());
      pendingTransactions.addTransaction(createRemotePendingTransaction(tx), Optional.of(sender));
      txs.add(tx);
      assertTransactionPending(pendingTransactions, tx);
    }

    assertThat(pendingTransactions.size()).isEqualTo(MAX_TRANSACTIONS + 1);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(MAX_TRANSACTIONS + 1);

    // one tx moved to the ready layer since the prioritized was full
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, MOVE, layers.readyTransactions.name()))
        .isEqualTo(1);

    // first tx is the lowest value one so it is the first to be moved to ready
    assertThat(layers.readyTransactions.contains(txs.get(0))).isTrue();
    verifyNoInteractions(droppedListener);
  }

  @Test
  public void addTransactionForMultipleSenders() {
    final var transactionSenderA = createTransaction(0, KEYS1);
    final var transactionSenderB = createTransaction(0, KEYS2);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transactionSenderA), Optional.empty()))
        .isEqualTo(ADDED);
    assertTransactionPending(pendingTransactions, transactionSenderA);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transactionSenderB), Optional.empty()))
        .isEqualTo(ADDED);
    assertTransactionPending(pendingTransactions, transactionSenderB);
  }

  @Test
  public void dropIfTransactionTooFarInFutureForTheSender() {
    final var futureTransaction =
        createTransaction(poolConf.getTxPoolMaxFutureTransactionByAccount() + 1);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(futureTransaction), Optional.empty()))
        .isEqualTo(NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER);
    assertTransactionNotPending(pendingTransactions, futureTransaction);
  }

  @Test
  public void dropAlreadyConfirmedTransaction() {
    final Account sender = mock(Account.class);
    when(sender.getNonce()).thenReturn(5L);

    final Transaction oldTransaction = createTransaction(2);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(oldTransaction), Optional.of(sender)))
        .isEqualTo(ALREADY_KNOWN);
    assertThat(pendingTransactions.size()).isEqualTo(0);
    assertTransactionNotPending(pendingTransactions, oldTransaction);
  }

  @Test
  public void notifyListenerWhenRemoteTransactionAdded() {
    pendingTransactions.subscribePendingTransactions(listener);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());

    verify(listener).onTransactionAdded(transaction0);
  }

  @Test
  public void notifyListenerWhenLocalTransactionAdded() {
    pendingTransactions.subscribePendingTransactions(listener);

    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transaction0), Optional.empty());

    verify(listener).onTransactionAdded(transaction0);
  }

  @Test
  public void notNotifyListenerAfterUnsubscribe() {
    final long id = pendingTransactions.subscribePendingTransactions(listener);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());

    verify(listener).onTransactionAdded(transaction0);

    pendingTransactions.unsubscribePendingTransactions(id);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction1), Optional.empty());

    verifyNoMoreInteractions(listener);
  }

  @Test
  public void notSelectReplacedTransaction() {
    final Transaction transaction1 = createTransaction(0, KEYS1);
    final Transaction transaction1b = createTransactionReplacement(transaction1, KEYS1);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction1), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction1b), Optional.empty());

    pendingTransactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs)
              .map(PendingTransaction::getTransaction)
              .containsExactly(transaction1b);
          return Map.of(pendingTxs.get(0), SELECTED);
        });
  }

  @Test
  public void selectTransactionsFromSameSenderInNonceOrder() {
    final Transaction transaction0 = createTransaction(0, KEYS1);
    final Transaction transaction1 = createTransaction(1, KEYS1);
    final Transaction transaction2 = createTransaction(2, KEYS1);

    // add out of order
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transaction2), Optional.empty());
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transaction1), Optional.empty());
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transaction0), Optional.empty());

    pendingTransactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs)
              .map(PendingTransaction::getTransaction)
              .containsExactly(transaction0, transaction1, transaction2);
          return pendingTxs.stream().collect(Collectors.toMap(Function.identity(), pt -> SELECTED));
        });
  }

  @Test
  public void notForceNonceOrderWhenSendersDiffer() {
    final Account sender2 = mock(Account.class);
    when(sender2.getNonce()).thenReturn(1L);

    final Transaction transactionSender1 =
        createTransaction(0, DEFAULT_MIN_GAS_PRICE.multiply(2), KEYS1);
    final Transaction transactionSender2 =
        createTransaction(1, DEFAULT_MIN_GAS_PRICE.multiply(4), KEYS2);

    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transactionSender1), Optional.empty());
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transactionSender2), Optional.of(sender2));

    pendingTransactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs)
              .map(PendingTransaction::getTransaction)
              .containsExactly(transactionSender2, transactionSender1);

          return pendingTxs.stream().collect(Collectors.toMap(Function.identity(), pt -> SELECTED));
        });
  }

  @Test
  public void invalidTransactionIsDeletedFromPendingTransactions() {
    final var pendingTx0 = createRemotePendingTransaction(transaction0);
    pendingTransactions.addTransaction(pendingTx0, Optional.empty());

    final var droppedTxCollector = new DroppedTransactionCollector();
    pendingTransactions.subscribeDroppedTransactions(droppedTxCollector);

    pendingTransactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs).containsExactly(pendingTx0);
          return Map.of(
              pendingTx0, TransactionSelectionResult.invalid(UPFRONT_COST_EXCEEDS_BALANCE.name()));
        });

    // assert that first tx is removed from the pool
    assertThat(droppedTxCollector.droppedTransactions)
        .containsExactly(entry(transaction0, INVALIDATED));
    assertThat(pendingTransactions.getPendingTransactions()).isEmpty();
  }

  @Test
  public void temporarilyInvalidTransactionIsKeptInPendingTransactions() {
    final var pendingTx0 = createRemotePendingTransaction(transaction0);
    pendingTransactions.addTransaction(pendingTx0, Optional.empty());

    pendingTransactions.selectTransactions(
        pendingTxs -> {
          assertThat(pendingTxs).containsExactly(pendingTx0);
          return Map.of(
              pendingTx0,
              TransactionSelectionResult.invalidPenalized(GAS_PRICE_BELOW_CURRENT_BASE_FEE.name()));
        });

    assertThat(pendingTransactions.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsExactly(transaction0);
  }

  @Test
  public void returnEmptyOptionalAsMaximumNonceWhenNoTransactionsPresent() {
    assertThat(pendingTransactions.getNextNonceForSender(SENDER1)).isEmpty();
  }

  @Test
  public void replaceTransactionWithSameSenderAndNonce() {
    final Transaction transaction1 = createTransaction(0, DEFAULT_MIN_GAS_PRICE.multiply(4), KEYS1);
    final Transaction transaction1b = createTransactionReplacement(transaction1, KEYS1);
    final Transaction transaction2 = createTransaction(1, DEFAULT_MIN_GAS_PRICE.multiply(2), KEYS1);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction2), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            pendingTransactions
                .addTransaction(createRemotePendingTransaction(transaction1b), Optional.empty())
                .isReplacement())
        .isTrue();

    assertTransactionNotPending(pendingTransactions, transaction1);
    assertTransactionPending(pendingTransactions, transaction1b);
    assertTransactionPending(pendingTransactions, transaction2);
    assertThat(pendingTransactions.size()).isEqualTo(2);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(3);
    assertThat(
            getRemovedCount(
                REMOTE, NO_PRIORITY, REPLACED.label(), layers.prioritizedTransactions.name()))
        .isEqualTo(1);
  }

  @Test
  public void replaceTransactionWithSameSenderAndNonce_multipleReplacements() {
    final int replacedTxCount = 5;
    final List<Transaction> replacedTransactions = new ArrayList<>(replacedTxCount);
    Transaction duplicateTx = createTransaction(0, DEFAULT_BASE_FEE.add(Wei.of(50)), KEYS1);
    for (int i = 0; i < replacedTxCount; i++) {
      replacedTransactions.add(duplicateTx);
      pendingTransactions.addTransaction(
          createRemotePendingTransaction(duplicateTx), Optional.empty());
      duplicateTx = createTransactionReplacement(duplicateTx, KEYS1);
    }

    final Transaction independentTx = createTransaction(1, DEFAULT_BASE_FEE.add(Wei.ONE), KEYS1);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(independentTx), Optional.empty()))
        .isEqualTo(ADDED);
    assertThat(
            pendingTransactions
                .addTransaction(createRemotePendingTransaction(duplicateTx), Optional.empty())
                .isReplacement())
        .isTrue();

    // All txs except the last duplicate should be removed
    replacedTransactions.forEach(tx -> assertTransactionNotPending(pendingTransactions, tx));
    assertTransactionPending(pendingTransactions, duplicateTx);
    // Tx with distinct nonce should be maintained
    assertTransactionPending(pendingTransactions, independentTx);

    assertThat(pendingTransactions.size()).isEqualTo(2);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(replacedTxCount + 2);
    assertThat(
            getRemovedCount(
                REMOTE, NO_PRIORITY, REPLACED.label(), layers.prioritizedTransactions.name()))
        .isEqualTo(replacedTxCount);
  }

  @Test
  public void
      replaceTransactionWithSameSenderAndNonce_multipleReplacementsAddedLocallyAndRemotely() {
    final int replacedTxCount = 5;
    final List<Transaction> replacedTransactions = new ArrayList<>(replacedTxCount);
    int remoteDuplicateCount = 0;
    Transaction replacingTx = createTransaction(0, KEYS1);
    for (int i = 0; i < replacedTxCount; i++) {
      replacedTransactions.add(replacingTx);
      if (i % 2 == 0) {
        pendingTransactions.addTransaction(
            createRemotePendingTransaction(replacingTx), Optional.empty());
        remoteDuplicateCount++;
      } else {
        pendingTransactions.addTransaction(
            createLocalPendingTransaction(replacingTx), Optional.empty());
      }
      replacingTx = createTransactionReplacement(replacingTx, KEYS1);
    }

    final Transaction independentTx = createTransaction(1);
    assertThat(
            pendingTransactions
                .addTransaction(createLocalPendingTransaction(replacingTx), Optional.empty())
                .isReplacement())
        .isTrue();
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(independentTx), Optional.empty()))
        .isEqualTo(ADDED);

    // All txs except the last duplicate should be removed
    replacedTransactions.forEach(tx -> assertTransactionNotPending(pendingTransactions, tx));
    assertTransactionPending(pendingTransactions, replacingTx);

    // Tx with distinct nonce should be maintained
    assertTransactionPending(pendingTransactions, independentTx);

    final int localDuplicateCount = replacedTxCount - remoteDuplicateCount;
    assertThat(pendingTransactions.size()).isEqualTo(2);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(remoteDuplicateCount + 1);
    assertThat(getAddedCount(LOCAL, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(localDuplicateCount + 1);
    assertThat(
            getRemovedCount(
                REMOTE, NO_PRIORITY, REPLACED.label(), layers.prioritizedTransactions.name()))
        .isEqualTo(remoteDuplicateCount);
    assertThat(
            getRemovedCount(
                LOCAL, NO_PRIORITY, REPLACED.label(), layers.prioritizedTransactions.name()))
        .isEqualTo(localDuplicateCount);
  }

  @Test
  public void notReplaceTransactionWithSameSenderAndNonceWhenGasPriceIsLower() {
    final Transaction transaction1 = createTransaction(0, DEFAULT_MIN_GAS_PRICE.add(1));
    final Transaction transaction1b = createTransaction(0, DEFAULT_MIN_GAS_PRICE);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction1), Optional.empty()))
        .isEqualTo(ADDED);

    pendingTransactions.subscribePendingTransactions(listener);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction1b), Optional.empty()))
        .isEqualTo(REJECTED_UNDERPRICED_REPLACEMENT);

    assertTransactionNotPending(pendingTransactions, transaction1b);
    assertTransactionPending(pendingTransactions, transaction1);
    assertThat(pendingTransactions.size()).isEqualTo(1);
    verifyNoInteractions(listener);
  }

  @Test
  public void trackNextNonceForEachSender() {
    // first sender consecutive txs: 0->1->2
    final Account firstSender = mock(Account.class);
    when(firstSender.getNonce()).thenReturn(0L);
    when(firstSender.getAddress()).thenReturn(SENDER1);
    assertNoNextNonceForSender(pendingTransactions, SENDER1);
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS1)), Optional.of(firstSender));
    assertNextNonceForSender(pendingTransactions, SENDER1, 1);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(1, KEYS1)), Optional.of(firstSender));
    assertNextNonceForSender(pendingTransactions, SENDER1, 2);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS1)), Optional.of(firstSender));
    assertNextNonceForSender(pendingTransactions, SENDER1, 3);

    // second sender not in orders: 3->0->2->1
    final Account secondSender = mock(Account.class);
    when(secondSender.getNonce()).thenReturn(0L);
    when(secondSender.getAddress()).thenReturn(SENDER2);
    assertNoNextNonceForSender(pendingTransactions, SENDER2);
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(3, KEYS2)), Optional.of(secondSender));
    assertNoNextNonceForSender(pendingTransactions, SENDER2);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS2)), Optional.of(secondSender));
    assertNextNonceForSender(pendingTransactions, SENDER2, 1);

    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS2)), Optional.of(secondSender));
    assertNextNonceForSender(pendingTransactions, SENDER2, 1);

    // tx 1 will fill the nonce gap and all txs will be ready
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(1, KEYS2)), Optional.of(secondSender));
    assertNextNonceForSender(pendingTransactions, SENDER2, 4);
  }

  @Test
  public void correctNonceIsReturned() {
    final Account sender = mock(Account.class);
    when(sender.getNonce()).thenReturn(1L);
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender())).isEmpty();
    // since tx 3 is missing, 4 is sparse,
    // note that 0 is already known since sender nonce is 1
    addLocalTransactions(pendingTransactions, sender, 0, 1, 2, 4);
    assertThat(pendingTransactions.size()).isEqualTo(3);
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(3);

    // tx 3 arrives and is added, while 4 is moved to ready
    addLocalTransactions(pendingTransactions, sender, 3);
    assertThat(pendingTransactions.size()).isEqualTo(4);
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(5);

    // when 5 is added, the pool is full, and so 6 and 7 are dropped since too far in future
    addLocalTransactions(pendingTransactions, sender, 5, 6, 7);
    assertThat(pendingTransactions.size()).isEqualTo(5);

    // assert that transactions are pruned by account from the latest future nonce first
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(6);
  }

  @Test
  public void correctNonceIsReturnedForSenderLimitedPool() {
    final Account sender = mock(Account.class);
    when(sender.getNonce()).thenReturn(1L);

    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction2.getSender())).isEmpty();
    // since tx 3 is missing, 4 is sparse,
    // note that 0 is already known since sender nonce is 1
    addLocalTransactions(senderLimitedTransactions, sender, 0, 1, 2, 4);
    assertThat(senderLimitedTransactions.size()).isEqualTo(3);
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(3);

    // tx 3 arrives and is added, while 4 is moved to ready
    addLocalTransactions(senderLimitedTransactions, sender, 3);
    assertThat(senderLimitedTransactions.size()).isEqualTo(4);
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(5);

    // for sender max 4 txs are allowed, so 5, 6 and 7 are dropped since too far in future
    addLocalTransactions(senderLimitedTransactions, sender, 5, 6, 7);
    assertThat(senderLimitedTransactions.size()).isEqualTo(4);

    // assert that we drop txs with future nonce first
    assertThat(senderLimitedTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(5);
  }

  @Test
  public void correctNonceIsReturnedWithRepeatedTransactions() {
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender())).isEmpty();
    final Account sender = mock(Account.class);
    addLocalTransactions(pendingTransactions, sender, 0, 1, 2, 1, 0, 4);
    assertThat(pendingTransactions.getNextNonceForSender(transaction2.getSender()))
        .isPresent()
        .hasValue(3);
    addLocalTransactions(pendingTransactions, sender, 3);
  }

  @Test
  public void shouldNotIncrementAddedCounterWhenRemoteTransactionAlreadyPresent() {
    pendingTransactions.addTransaction(
        createLocalPendingTransaction(transaction0), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(1);
    assertThat(getAddedCount(LOCAL, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(1);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isZero();

    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction0), Optional.empty()))
        .isEqualTo(ALREADY_KNOWN);
    assertThat(pendingTransactions.size()).isEqualTo(1);
    assertThat(getAddedCount(LOCAL, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(1);
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isZero();
  }

  @Test
  public void shouldNotIncrementAddedCounterWhenLocalTransactionAlreadyPresent() {
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(transaction0), Optional.empty());
    assertThat(pendingTransactions.size()).isEqualTo(1);
    assertThat(getAddedCount(LOCAL, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isZero();
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(1);

    assertThat(
            pendingTransactions.addTransaction(
                createLocalPendingTransaction(transaction0), Optional.empty()))
        .isEqualTo(ALREADY_KNOWN);
    assertThat(pendingTransactions.size()).isEqualTo(1);
    assertThat(getAddedCount(LOCAL, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isZero();
    assertThat(getAddedCount(REMOTE, NO_PRIORITY, NEW, layers.prioritizedTransactions.name()))
        .isEqualTo(1);
  }

  @Test
  public void doNothingIfTransactionAlreadyPending() {
    final var addedTxs = populateCache(1, 0);
    assertThat(
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(addedTxs[0].transaction),
                Optional.of(addedTxs[0].account)))
        .isEqualTo(ALREADY_KNOWN);
    assertTransactionPending(pendingTransactions, addedTxs[0].transaction);
  }

  @Test
  public void returnsCorrectNextNonceWhenAddedTransactionsHaveGaps() {
    final var addedTxs = populateCache(3, 0, 1);
    assertThat(pendingTransactions.getNextNonceForSender(addedTxs[0].transaction.getSender()))
        .isPresent()
        .hasValue(1);
  }

  @Test
  public void shouldReturnZeroStatusWhenPoolIsEmpty() {
    final PendingTransactions.Status status = pendingTransactions.getStatus();
    assertThat(status.pendingCount()).isZero();
    assertThat(status.queuedCount()).isZero();
  }

  @Test
  public void shouldCountAllTransactionsAsPendingWhenNoncesAreSequential() {
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS1)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(1, KEYS1)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS1)), Optional.empty());

    final PendingTransactions.Status status = pendingTransactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(3);
    assertThat(status.queuedCount()).isZero();
  }

  @Test
  public void shouldCountTransactionsBeyondNonceGapAsQueued() {
    // nonce 0 lands in prioritized/ready (pending), nonce 2 lands in sparse with gap=1 (queued)
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS1)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS1)), Optional.empty());

    final PendingTransactions.Status status = pendingTransactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(1);
    assertThat(status.queuedCount()).isEqualTo(1);
  }

  @Test
  public void shouldAggregatePendingAndQueuedAcrossMultipleSenders() {
    // SENDER1: nonces 0, 1 — sequential, all pending
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS1)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(1, KEYS1)), Optional.empty());
    // SENDER2: nonces 0, 2 — gap at 1: 1 pending, 1 queued
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS2)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS2)), Optional.empty());

    final PendingTransactions.Status status = pendingTransactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(3);
    assertThat(status.queuedCount()).isEqualTo(1);
  }

  @Test
  public void shouldMoveTxFromQueuedToPendingWhenGapIsFilled() {
    // start with a gap: nonce 0 pending, nonce 2 queued
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(0, KEYS1)), Optional.empty());
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(2, KEYS1)), Optional.empty());

    assertThat(pendingTransactions.getStatus().pendingCount()).isEqualTo(1);
    assertThat(pendingTransactions.getStatus().queuedCount()).isEqualTo(1);

    // fill the gap: nonce 1 arrives, nonce 2 is promoted from sparse to ready/prioritized
    pendingTransactions.addTransaction(
        createRemotePendingTransaction(createTransaction(1, KEYS1)), Optional.empty());

    final PendingTransactions.Status status = pendingTransactions.getStatus();
    assertThat(status.pendingCount()).isEqualTo(3);
    assertThat(status.queuedCount()).isZero();
  }

  private TransactionAndAccount[] populateCache(final int numTxs, final long startingNonce) {
    return populateCache(numTxs, KEYS1, startingNonce, OptionalLong.empty());
  }

  private TransactionAndAccount[] populateCache(
      final int numTxs, final long startingNonce, final long missingNonce) {
    return populateCache(numTxs, KEYS1, startingNonce, OptionalLong.of(missingNonce));
  }

  private TransactionAndAccount[] populateCache(
      final int numTxs,
      final KeyPair keys,
      final long startingNonce,
      final OptionalLong maybeGapNonce) {
    final List<TransactionAndAccount> addedTransactions = new ArrayList<>(numTxs);
    for (int i = 0; i < numTxs; i++) {
      final long nonce = startingNonce + i;
      if (maybeGapNonce.isEmpty() || maybeGapNonce.getAsLong() != nonce) {
        final var transaction = createTransaction(nonce, keys);
        final Account sender = mock(Account.class);
        when(sender.getNonce()).thenReturn(startingNonce);
        final var res =
            pendingTransactions.addTransaction(
                createRemotePendingTransaction(transaction), Optional.of(sender));
        assertTransactionPending(pendingTransactions, transaction);
        assertThat(res).isEqualTo(ADDED);
        addedTransactions.add(new TransactionAndAccount(transaction, sender));
      }
    }
    return addedTransactions.toArray(TransactionAndAccount[]::new);
  }

  record TransactionAndAccount(Transaction transaction, Account account) {}

  record CreatedLayers(
      AbstractPrioritizedTransactions prioritizedTransactions,
      ReadyTransactions readyTransactions,
      SparseTransactions sparseTransactions,
      EvictCollectorLayer evictedCollector) {}

  static class DroppedTransactionCollector implements PendingTransactionDroppedListener {
    final SequencedMap<Transaction, RemovalReason> droppedTransactions = new LinkedHashMap<>();

    @Override
    public void onTransactionDropped(final Transaction transaction, final RemovalReason reason) {
      droppedTransactions.put(transaction, reason);
    }
  }
}
