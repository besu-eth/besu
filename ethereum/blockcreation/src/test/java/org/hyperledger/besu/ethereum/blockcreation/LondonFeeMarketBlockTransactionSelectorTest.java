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
package org.hyperledger.besu.ethereum.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER1;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER2;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.UPFRONT_COST_EXCEEDS_BALANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockTransactionSelector;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolReplacementHandler;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseFeePrioritizedTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.EndLayer;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.ReadyTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderBalanceChecker;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SparseTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class LondonFeeMarketBlockTransactionSelectorTest
    extends AbstractBlockTransactionSelectorTest {

  @Override
  protected GenesisConfig getGenesisConfig() {
    return GenesisConfig.fromResource("/block-transaction-selector/london-genesis.json");
  }

  @Override
  protected ProtocolSchedule createProtocolSchedule() {
    return new ProtocolScheduleBuilder(
            genesisConfig.getConfigOptions(),
            Optional.of(CHAIN_ID),
            ProtocolSpecAdapters.create(0, Function.identity()),
            false,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem())
        .createProtocolSchedule();
  }

  @Override
  protected TransactionPool createTransactionPool() {
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolMaxSize(5)
            .txPoolLimitByAccountPercentage(Fraction.fromFloat(1f))
            .minGasPrice(Wei.ONE)
            .build();
    final PendingTransactions pendingTransactions =
        new BaseFeePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());
    transactionPool.setEnabled();
    return transactionPool;
  }

  @Test
  public void eip1559TransactionCurrentGasPriceLessThanMinimumIsSkippedAndKeptInThePool() {
    final ProcessableBlockHeader blockHeader = createBlock(301_000, Wei.ONE);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.of(6), DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    // tx is willing to pay max 7 wei for gas, but current network condition (baseFee == 1)
    // result in it paying 2 wei, that is below the minimum accepted by the node, so it is skipped
    final Transaction tx = createEIP1559Transaction(1, Wei.of(7L), Wei.ONE, 100_000);
    final var addResults = transactionPool.addRemoteTransactions(List.of(tx));
    assertThat(addResults).extractingByKey(tx.getHash()).isEqualTo(ValidationResult.valid());

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).isEmpty();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(tx, TransactionSelectionResult.CURRENT_TX_PRICE_BELOW_MIN));
    assertThat(transactionPool.count()).isEqualTo(1);
  }

  @Test
  public void eip1559TransactionCurrentGasPriceGreaterThanMinimumIsSelected() {
    final ProcessableBlockHeader blockHeader = createBlock(301_000, Wei.of(5));

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.of(6), DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    // tx is willing to pay max 7 wei for gas, and current network condition (baseFee == 5)
    // result in it paying the max, that is >= the minimum accepted by the node, so it is selected
    final Transaction tx = createEIP1559Transaction(1, Wei.of(7), Wei.ONE, 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));

    ensureTransactionIsValid(tx);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(tx);
    assertThat(results.getNotSelectedTransactions()).isEmpty();
  }

  @Test
  public void eip1559PriorityTransactionCurrentGasPriceLessThanMinimumIsSelected() {
    final ProcessableBlockHeader blockHeader = createBlock(301_000, Wei.ONE);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.of(6), DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    // tx is willing to pay max 7 wei for gas, but current network condition (baseFee == 1)
    // result in it paying 2 wei, that is below the minimum accepted by the node, but since it is
    // a local sender it is accepted anyway
    final Transaction tx = createEIP1559Transaction(1, Wei.of(7L), Wei.ONE, 100_000);
    final var addResult = transactionPool.addTransactionViaApi(tx);
    assertThat(addResult.isValid()).isTrue();

    ensureTransactionIsValid(tx);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(tx);
    assertThat(results.getNotSelectedTransactions()).isEmpty();
  }

  @Test
  public void transactionFromSameSenderWithMixedTypes() {
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);

    final Transaction txFrontier1 = createTransaction(0, Wei.of(7L), 100_000);
    final Transaction txLondon1 = createEIP1559Transaction(1, Wei.ONE, Wei.ONE, 100_000);
    final Transaction txFrontier2 = createTransaction(2, Wei.of(7L), 100_000);
    final Transaction txLondon2 = createEIP1559Transaction(3, Wei.ONE, Wei.ONE, 100_000);

    ensureTransactionIsValid(txFrontier1);
    ensureTransactionIsValid(txLondon1);
    ensureTransactionIsValid(txFrontier2);
    ensureTransactionIsValid(txLondon2);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(List.of(txFrontier1, txLondon1, txFrontier2, txLondon2));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions())
        .containsExactly(txFrontier1, txLondon1, txFrontier2, txLondon2);
    assertThat(results.getNotSelectedTransactions()).isEmpty();
  }

  // --- noLateFunding tests: sequenced pool (BaseFeePendingTransactionsSorter) ---

  @Test
  public void sequencedPool_upfrontCostExceedsBalance_noLateFundingFalse_txRemainsInPool() {
    transactionPool = createSequencedPoolWithNoLateFunding(false);
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    final Transaction tx = createTransaction(0, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));
    ensureTransactionIsInvalid(tx, UPFRONT_COST_EXCEEDS_BALANCE);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(tx.getHash())).isPresent();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(tx, TransactionSelectionResult.invalidPenalized(UPFRONT_COST_EXCEEDS_BALANCE.name())));
  }

  @Test
  public void sequencedPool_upfrontCostExceedsBalance_noLateFundingTrue_txRemovedFromPool() {
    transactionPool = createSequencedPoolWithNoLateFunding(true);
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    final Transaction tx = createTransaction(0, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));
    ensureTransactionIsInvalid(tx, UPFRONT_COST_EXCEEDS_BALANCE);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(tx.getHash())).isNotPresent();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(tx, TransactionSelectionResult.invalid(UPFRONT_COST_EXCEEDS_BALANCE.name())));
  }

  // --- noLateFunding tests: layered pool (LayeredPendingTransactions) ---

  @Test
  public void layeredPool_upfrontCostExceedsBalance_noLateFundingFalse_txRemainsInPool() {
    transactionPool = createLayeredPoolWithNoLateFunding(false);
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    final Transaction tx = createTransaction(0, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));
    ensureTransactionIsInvalid(tx, UPFRONT_COST_EXCEEDS_BALANCE);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(tx.getHash())).isPresent();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(tx, TransactionSelectionResult.invalidPenalized(UPFRONT_COST_EXCEEDS_BALANCE.name())));
  }

  @Test
  public void layeredPool_upfrontCostExceedsBalance_noLateFundingTrue_txRemovedFromPool() {
    // The layered pool schedules removal via ethScheduler.scheduleServiceTask(); mock it to run
    // the removal task inline so the assertion can observe the post-removal pool state.
    when(ethScheduler.scheduleServiceTask(any(Runnable.class)))
        .thenAnswer(
            inv -> {
              inv.getArgument(0, Runnable.class).run();
              return null;
            });

    transactionPool = createLayeredPoolWithNoLateFunding(true);
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    final Transaction tx = createTransaction(0, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));
    ensureTransactionIsInvalid(tx, UPFRONT_COST_EXCEEDS_BALANCE);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(tx.getHash())).isNotPresent();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(tx, TransactionSelectionResult.invalid(UPFRONT_COST_EXCEEDS_BALANCE.name())));
  }

  private TransactionPool createSequencedPoolWithNoLateFunding(final boolean noLateFunding) {
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolMaxSize(5)
            .txPoolLimitByAccountPercentage(Fraction.fromFloat(1f))
            .minGasPrice(Wei.ONE)
            .noLateFunding(noLateFunding)
            .build();
    final PendingTransactions pending =
        new BaseFeePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metricsSystem,
            blockchain::getChainHeadHeader);
    final TransactionPool pool =
        new TransactionPool(
            () -> pending,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());
    pool.setEnabled();
    return pool;
  }

  private TransactionPool createLayeredPoolWithNoLateFunding(final boolean noLateFunding) {
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolMaxSize(5)
            .txPoolLimitByAccountPercentage(Fraction.fromFloat(1f))
            .minGasPrice(Wei.ONE)
            .noLateFunding(noLateFunding)
            .build();
    final TransactionPoolMetrics metrics = new TransactionPoolMetrics(metricsSystem);
    final BiFunction<PendingTransaction, PendingTransaction, Boolean> replacementTester =
        (t1, t2) ->
            new TransactionPoolReplacementHandler(poolConf.getPriceBump(), poolConf.getBlobPriceBump())
                .shouldReplace(t1, t2, blockchain.getChainHeadHeader());
    final EndLayer evictCollector = new EndLayer(metrics);
    final SparseTransactions sparse =
        new SparseTransactions(poolConf, ethScheduler, evictCollector, metrics, replacementTester, new BlobCache());
    final ReadyTransactions ready =
        new ReadyTransactions(poolConf, ethScheduler, sparse, metrics, replacementTester, new BlobCache());
    final BaseFeePrioritizedTransactions prioritized =
        new BaseFeePrioritizedTransactions(
            poolConf,
            blockchain::getChainHeadHeader,
            ethScheduler,
            ready,
            metrics,
            replacementTester,
            FeeMarket.london(0L),
            new BlobCache(),
            MiningConfiguration.newDefault().setMinTransactionGasPrice(Wei.ONE),
            new SenderBalanceChecker.NoOpChecker());
    final PendingTransactions layered =
        new LayeredPendingTransactions(poolConf, prioritized, ethScheduler);
    final TransactionPool pool =
        new TransactionPool(
            () -> layered,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            metrics,
            poolConf,
            new BlobCache());
    pool.setEnabled();
    return pool;
  }

  @Test
  @Override
  public void shouldNotSelectTransactionsWithPriorityFeeLessThanConfig() {
    ProcessableBlockHeader blockHeader = createBlock(5_000_000, Wei.ONE);
    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder().from(defaultTestMiningConfiguration).build();
    miningConfiguration.setMinPriorityFeePerGas(Wei.of(7));

    final Transaction txSelected1 =
        createEIP1559Transaction(0, Wei.of(8), Wei.of(8), 100_000, SENDER1);
    ensureTransactionIsValid(txSelected1);

    // transaction txNotSelected1 should not be selected
    final Transaction txNotSelected1 =
        createEIP1559Transaction(1, Wei.of(7), Wei.of(7), 100_000, SENDER1);
    ensureTransactionIsValid(txNotSelected1);

    // transaction txSelected2 should be selected
    final Transaction txSelected2 =
        createEIP1559Transaction(0, Wei.of(8), Wei.of(8), 100_000, SENDER2);
    ensureTransactionIsValid(txSelected2);

    // transaction txNotSelected2 should not be selected
    final Transaction txNotSelected2 =
        createEIP1559Transaction(1, Wei.of(8), Wei.of(6), 100_000, SENDER2);
    ensureTransactionIsValid(txNotSelected2);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            miningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(
        List.of(txSelected1, txNotSelected1, txSelected2, txNotSelected2));

    assertThat(transactionPool.getPendingTransactions().size()).isEqualTo(4);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsOnly(txSelected1, txSelected2);
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                txNotSelected1, TransactionSelectionResult.PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN),
            entry(
                txNotSelected2, TransactionSelectionResult.PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN));
  }
}
