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
import static org.hyperledger.besu.datatypes.TransactionType.EIP1559;
import static org.hyperledger.besu.datatypes.TransactionType.FRONTIER;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ADDED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.DROPPED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.BlobsWithCommitments;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class BaseFeePrioritizedTransactionsTest extends AbstractPrioritizedTransactionsTestBase {
  private static final FeeMarket EIP1559_FEE_MARKET = FeeMarket.london(0L);
  private static final Wei DEFAULT_BASE_FEE = DEFAULT_MIN_GAS_PRICE.subtract(2);
  private static final Random randomizeTxType = new Random();

  @Override
  AbstractPrioritizedTransactions getSorter(
      final TransactionPoolConfiguration poolConfig,
      final TransactionsLayer nextLayer,
      final TransactionPoolMetrics txPoolMetrics,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester,
      final MiningConfiguration miningConfiguration) {

    return new BaseFeePrioritizedTransactions(
        poolConfig,
        this::mockBlockHeader,
        ethScheduler,
        nextLayer,
        txPoolMetrics,
        transactionReplacementTester,
        EIP1559_FEE_MARKET,
        new BlobCache(),
        miningConfiguration,
        senderBalanceChecker);
  }

  @Override
  protected BlockHeader mockBlockHeader() {
    return mockBlockHeader(DEFAULT_BASE_FEE);
  }

  private BlockHeader mockBlockHeader(final Wei baseFee) {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(baseFee));
    return blockHeader;
  }

  @Override
  protected Transaction createTransaction(
      final long nonce, final Wei maxGasPrice, final KeyPair keys) {

    return createTransaction(
        randomizeTxType.nextBoolean() ? EIP1559 : FRONTIER, nonce, maxGasPrice, keys);
  }

  protected Transaction createTransaction(
      final TransactionType type, final long nonce, final Wei maxGasPrice, final KeyPair keys) {

    var tx = new TransactionTestFixture().value(Wei.of(nonce)).nonce(nonce).type(type);
    if (type.supports1559FeeMarket()) {
      tx.maxFeePerGas(Optional.of(maxGasPrice))
          .maxPriorityFeePerGas(Optional.of(maxGasPrice.divide(10)));
    } else {
      tx.gasPrice(maxGasPrice);
    }
    return tx.createTransaction(keys);
  }

  @Override
  protected Transaction createTransactionReplacement(
      final Transaction originalTransaction, final KeyPair keys) {
    return createTransaction(
        originalTransaction.getType(),
        originalTransaction.getNonce(),
        originalTransaction.getMaxGasPrice().multiply(2),
        originalTransaction.getMaxGasPrice().multiply(2).divide(10),
        originalTransaction.getPayload().size(),
        originalTransaction.getBlobCount(),
        originalTransaction
            .getBlobsWithCommitments()
            .map(BlobsWithCommitments::getBlobType)
            .orElse(BlobType.KZG_PROOF),
        originalTransaction.getCodeDelegationList().orElse(null),
        keys);
  }

  @Test
  public void shouldPrioritizeEffectivePriorityFeeThenTimeAddedToPoolOnMixedTypes() {
    final var nextBlockBaseFee = Optional.of(DEFAULT_MIN_GAS_PRICE.subtract(1));

    final PendingTransaction highGasPriceTransaction =
        createRemotePendingTransaction(
            createTransaction(0, DEFAULT_MIN_GAS_PRICE.multiply(2), KEYS1));

    final List<PendingTransaction> lowValueTxs =
        IntStream.range(0, MAX_TRANSACTIONS)
            .mapToObj(
                i ->
                    new PendingTransaction.Remote(
                        createTransaction(
                            0,
                            DEFAULT_MIN_GAS_PRICE.add(1),
                            SIGNATURE_ALGORITHM.generateKeyPair())))
            .collect(Collectors.toUnmodifiableList());

    final var lowestPriorityFee =
        lowValueTxs.stream()
            .sorted(
                Comparator.comparing(
                    pt -> pt.getTransaction().getEffectivePriorityFeePerGas(nextBlockBaseFee)))
            .findFirst()
            .get()
            .getTransaction()
            .getEffectivePriorityFeePerGas(nextBlockBaseFee);

    final var firstLowValueTx =
        lowValueTxs.stream()
            .filter(
                pt ->
                    pt.getTransaction()
                        .getEffectivePriorityFeePerGas(nextBlockBaseFee)
                        .equals(lowestPriorityFee))
            .findFirst()
            .get();

    shouldPrioritizeValueThenTimeAddedToPool(
        lowValueTxs.iterator(), highGasPriceTransaction, firstLowValueTx);
  }

  @Test
  public void txBelowCurrentMineableMinPriorityFeeIsNotPrioritized() {
    miningConfiguration.setMinPriorityFeePerGas(Wei.of(5));
    final PendingTransaction lowPriorityFeeTx =
        createRemotePendingTransaction(
            createTransaction(0, DEFAULT_MIN_GAS_PRICE.subtract(1), KEYS1));
    assertThat(prioritizeTransaction(lowPriorityFeeTx)).isEqualTo(DROPPED);
    assertEvicted(lowPriorityFeeTx);
    assertTransactionNotPrioritized(lowPriorityFeeTx);
  }

  @Test
  public void txWithPriorityBelowCurrentMineableMinPriorityFeeIsPrioritized() {
    miningConfiguration.setMinPriorityFeePerGas(Wei.of(5));
    final PendingTransaction lowGasPriceTx =
        createRemotePendingTransaction(
            createTransaction(0, DEFAULT_MIN_GAS_PRICE.subtract(1), KEYS1), true);
    assertThat(prioritizeTransaction(lowGasPriceTx)).isEqualTo(ADDED);
    assertTransactionPrioritized(lowGasPriceTx);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"EIP1559", "BLOB"})
  public void txWithEffectiveGasPriceBelowCurrentMineableMinGasPriceIsNotPrioritized(
      final TransactionType type) {
    final PendingTransaction lowGasPriceTx =
        createRemotePendingTransaction(
            createTransaction(
                type, 0, DEFAULT_MIN_GAS_PRICE, Wei.ONE, 0, 1, BlobType.KZG_PROOF, null, KEYS1));
    assertThat(prioritizeTransaction(lowGasPriceTx)).isEqualTo(DROPPED);
    assertEvicted(lowGasPriceTx);
    assertTransactionNotPrioritized(lowGasPriceTx);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"EIP1559", "FRONTIER"})
  public void shouldPrioritizePriorityFeeThenTimeAddedToPoolSameTypeTxs(
      final TransactionType transactionType) {
    final PendingTransaction highGasPriceTransaction =
        createRemotePendingTransaction(
            createTransaction(0, DEFAULT_MIN_GAS_PRICE.multiply(200), KEYS1));

    final var lowValueTxs =
        IntStream.range(0, MAX_TRANSACTIONS)
            .mapToObj(
                i ->
                    createRemotePendingTransaction(
                        createTransaction(
                            transactionType,
                            0,
                            DEFAULT_MIN_GAS_PRICE.add(1).multiply(20),
                            0,
                            null,
                            SIGNATURE_ALGORITHM.generateKeyPair())))
            .collect(Collectors.toUnmodifiableList());

    shouldPrioritizeValueThenTimeAddedToPool(
        lowValueTxs.iterator(), highGasPriceTransaction, lowValueTxs.get(0));
  }

  @Test
  public void maxNumberOfTxsForTypeIsEnforced() {
    final var limitedType = MAX_TRANSACTIONS_BY_TYPE.entrySet().iterator().next();
    final var maxNumber = limitedType.getValue();
    final var addedTxs = new ArrayList<Transaction>(maxNumber);
    for (int i = 0; i < maxNumber; i++) {
      final var tx =
          createTransaction(
              limitedType.getKey(),
              0,
              DEFAULT_MIN_GAS_PRICE,
              DEFAULT_MIN_GAS_PRICE.divide(10),
              0,
              1,
              BlobType.KZG_PROOF,
              null,
              SIGNATURE_ALGORITHM.generateKeyPair());
      addedTxs.add(tx);
      assertThat(prioritizeTransaction(tx)).isEqualTo(ADDED);
    }

    final var overflowTx =
        createTransaction(
            limitedType.getKey(),
            0,
            DEFAULT_MIN_GAS_PRICE,
            DEFAULT_MIN_GAS_PRICE.divide(10),
            0,
            1,
            BlobType.KZG_PROOF,
            null,
            SIGNATURE_ALGORITHM.generateKeyPair());
    assertThat(prioritizeTransaction(overflowTx)).isEqualTo(DROPPED);

    addedTxs.forEach(this::assertTransactionPrioritized);
    assertTransactionNotPrioritized(overflowTx);
  }

  @Test
  public void maxNumberOfTxsForTypeWithReplacement() {
    final var limitedType = MAX_TRANSACTIONS_BY_TYPE.entrySet().iterator().next();
    final var maxNumber = limitedType.getValue();
    final var addedTxs = new ArrayList<Transaction>(maxNumber);
    for (int i = 0; i < maxNumber; i++) {
      final var tx =
          createTransaction(
              limitedType.getKey(),
              i,
              DEFAULT_MIN_GAS_PRICE,
              DEFAULT_MIN_GAS_PRICE.divide(10),
              0,
              1,
              BlobType.KZG_PROOF,
              null,
              KEYS1);
      addedTxs.add(tx);
      assertThat(prioritizeTransaction(tx)).isEqualTo(ADDED);
    }

    final var replacedTx = addedTxs.get(0);
    final var replacementTx = createTransactionReplacement(replacedTx, KEYS1);
    final var txAddResult = prioritizeTransaction(replacementTx);

    assertThat(txAddResult.isReplacement()).isTrue();
    assertThat(txAddResult.maybeReplacedTransaction())
        .map(PendingTransaction::getTransaction)
        .contains(replacedTx);

    addedTxs.remove(replacedTx);
    addedTxs.forEach(this::assertTransactionPrioritized);
    assertTransactionNotPrioritized(replacedTx);
    assertTransactionPrioritized(replacementTx);
  }
}
