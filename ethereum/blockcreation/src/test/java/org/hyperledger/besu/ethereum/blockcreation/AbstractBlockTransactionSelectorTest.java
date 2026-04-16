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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER1;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER2;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER3;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER5;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.EXECUTION_INTERRUPTED;
import static org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason.NONCE_TOO_LOW;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_SELECTION_TIMEOUT;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_SELECTION_TIMEOUT_INVALID_TX;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.TX_EVALUATION_TOO_LONG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockTransactionSelector;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.difficulty.fixed.FixedDifficultyProtocolSchedule;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.plugin.services.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.services.TransactionSelectionServiceImpl;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class AbstractBlockTransactionSelectorTest {
  protected static final BigInteger CHAIN_ID = BigInteger.valueOf(42L);

  protected final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  protected GenesisConfig genesisConfig;
  protected MutableBlockchain blockchain;
  protected TransactionPool transactionPool;
  protected MutableWorldState worldState;
  protected ProtocolSchedule protocolSchedule;
  protected TransactionSelectionService transactionSelectionService;
  protected MiningConfiguration defaultTestMiningConfiguration;

  @Mock protected EthScheduler ethScheduler;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  protected ProtocolContext protocolContext;

  @Mock protected MainnetTransactionProcessor transactionProcessor;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  protected EthContext ethContext;

  @BeforeEach
  public void setup() {
    genesisConfig = getGenesisConfig();
    protocolSchedule = createProtocolSchedule();
    transactionSelectionService = new TransactionSelectionServiceImpl();
    defaultTestMiningConfiguration =
        createMiningParameters(
            transactionSelectionService, Wei.ZERO, DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME);

    final Block genesisBlock =
        GenesisState.fromConfig(genesisConfig, protocolSchedule, new CodeCache()).getBlock();

    blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                new InMemoryKeyValueStorage(),
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0);

    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    worldState = InMemoryKeyValueStorageProvider.createInMemoryWorldState();
    final var worldStateUpdater = worldState.updater();
    Arrays.stream(Sender.values())
        .map(Sender::address)
        .forEach(address -> worldStateUpdater.createAccount(address, 0, Wei.of(1_000_000_000L)));
    worldStateUpdater.commit();

    when(protocolContext.getWorldStateArchive().getWorldState(any(WorldStateQueryParams.class)))
        .thenReturn(Optional.of(worldState));
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);
    when(ethScheduler.scheduleBlockCreationTask(anyLong(), any(Runnable.class)))
        .thenAnswer(invocation -> CompletableFuture.runAsync(invocation.getArgument(1)));
    when(ethScheduler.scheduleFutureTask(any(Runnable.class), any(Duration.class)))
        .thenAnswer(
            invocation -> {
              final Duration delay = invocation.getArgument(1);
              CompletableFuture.delayedExecutor(delay.toMillis(), MILLISECONDS)
                  .execute(invocation.getArgument(0));
              return null;
            });
  }

  protected abstract GenesisConfig getGenesisConfig();

  protected abstract ProtocolSchedule createProtocolSchedule();

  protected abstract TransactionPool createTransactionPool();

  protected ProcessableBlockHeader createBlock(final long gasLimit) {
    return createBlock(gasLimit, Wei.ONE);
  }

  protected ProcessableBlockHeader createBlock(final long gasLimit, final Wei baseFee) {
    return BlockHeaderBuilder.create()
        .parentHash(Hash.EMPTY)
        .coinbase(Address.fromHexString(String.format("%020x", 1)))
        .difficulty(Difficulty.ONE)
        .number(1)
        .gasLimit(gasLimit)
        .timestamp(Instant.now().toEpochMilli())
        .baseFee(baseFee)
        .buildProcessableBlockHeader();
  }

  @Test
  public void emptyPendingTransactionsResultsInEmptyVettingResult() {
    final ProtocolSchedule protocolSchedule =
        FixedDifficultyProtocolSchedule.create(
            GenesisConfig.fromResource("/dev.json").getConfigOptions(),
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final MainnetTransactionProcessor mainnetTransactionProcessor =
        protocolSchedule.getByBlockHeader(blockHeader(0)).getTransactionProcessor();

    // The block should fit 5 transactions only
    final ProcessableBlockHeader blockHeader = createBlock(500_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            mainnetTransactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).isEmpty();
    assertThat(results.getNotSelectedTransactions()).isEmpty();
    assertThat(results.getReceipts()).isEmpty();
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(0);
  }

  @Test
  public void validPendingTransactionIsIncludedInTheBlock() {
    final ProcessableBlockHeader blockHeader = createBlock(500_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final Transaction transaction = createTransaction(1, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(transaction));

    ensureTransactionIsValid(transaction, 0, 5);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(transaction);
    assertThat(results.getNotSelectedTransactions()).isEmpty();
    assertThat(results.getReceipts().size()).isEqualTo(1);
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(99995L);
  }

  @Test
  public void validPendingTransactionIsNotIncludedIfSelectionCancelled() {
    final ProcessableBlockHeader blockHeader = createBlock(500_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final Transaction transaction = createTransaction(1, Wei.of(7L), 100_000);
    transactionPool.addRemoteTransactions(List.of(transaction));

    ensureTransactionIsValid(transaction, 0, 5);

    selector.cancel();
    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).isEmpty();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(transaction, TransactionSelectionResult.SELECTION_CANCELLED));
    assertThat(results.getReceipts().size()).isEqualTo(0);
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(0L);
  }

  @Test
  public void invalidTransactionsAreSkippedButBlockStillFills() {
    // The block should fit 4 transactions only
    final ProcessableBlockHeader blockHeader = createBlock(400_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final List<Transaction> transactionsToInject = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      final Transaction tx = createTransaction(i, Wei.of(7), 100_000, Sender.values()[i]);
      transactionsToInject.add(tx);
      if (i == 1) {
        ensureTransactionIsInvalid(tx, TransactionInvalidReason.NONCE_TOO_LOW);
      } else {
        ensureTransactionIsValid(tx);
      }
    }
    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    final Transaction invalidTx = transactionsToInject.get(1);

    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                invalidTx,
                TransactionSelectionResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW.name())));
    assertThat(results.getSelectedTransactions().size()).isEqualTo(4);
    assertThat(results.getSelectedTransactions().contains(invalidTx)).isFalse();
    assertThat(results.getReceipts().size()).isEqualTo(4);
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(400_000);
  }

  @Test
  public void subsetOfPendingTransactionsIncludedWhenBlockGasLimitHit() {
    final ProcessableBlockHeader blockHeader = createBlock(301_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final List<Transaction> transactionsToInject = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      final Transaction tx = createTransaction(i, Wei.of(7), 100_000);
      transactionsToInject.add(tx);
      ensureTransactionIsValid(tx);
    }
    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions().size()).isEqualTo(3);

    assertThat(results.getSelectedTransactions().containsAll(transactionsToInject.subList(0, 3)))
        .isTrue();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(transactionsToInject.get(3), TransactionSelectionResult.BLOCK_FULL));
    assertThat(results.getReceipts().size()).isEqualTo(3);
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(300_000);

    // Ensure receipts have the correct cumulative gas
    assertThat(results.getReceipts().get(0).getCumulativeGasUsed()).isEqualTo(100_000);
    assertThat(results.getReceipts().get(1).getCumulativeGasUsed()).isEqualTo(200_000);
    assertThat(results.getReceipts().get(2).getCumulativeGasUsed()).isEqualTo(300_000);
  }

  @Test
  public void transactionTooLargeForBlockDoesNotPreventMoreBeingAddedIfBlockOccupancyNotReached() {
    final ProcessableBlockHeader blockHeader = createBlock(300_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    // Add 3 transactions to the Pending Transactions, 79% of block, 100% of block and 10% of block
    // should end up selecting the first and third only.
    // NOTE - PendingTransactions outputs these in nonce order
    final Transaction[] txs =
        new Transaction[] {
          createTransaction(0, Wei.of(10), (long) (blockHeader.getGasLimit() * 0.79), SENDER1),
          createTransaction(0, Wei.of(10), blockHeader.getGasLimit(), SENDER2),
          createTransaction(0, Wei.of(10), (long) (blockHeader.getGasLimit() * 0.1), SENDER3)
        };

    for (final Transaction tx : txs) {
      ensureTransactionIsValid(tx);
    }
    transactionPool.addRemoteTransactions(Arrays.stream(txs).toList());

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(txs[0], txs[2]);
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(txs[1], TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS));
  }

  @Test
  public void ifATransactionIsNotSelectedFollowingOnesFromTheSameSenderAreSkipped() {
    final ProcessableBlockHeader blockHeader = createBlock(300_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    // Add 3 transactions from the same sender to the Pending Transactions
    // first is selected
    // second id not selected
    // third is skipped, not processed, since cannot be selected due to the nonce gap
    final Transaction[] txs =
        new Transaction[] {
          createTransaction(0, Wei.of(10), (long) (blockHeader.getGasLimit() * 0.5), SENDER1),
          createTransaction(1, Wei.of(10), (long) (blockHeader.getGasLimit() * 0.6), SENDER1),
          createTransaction(2, Wei.of(10), (long) (blockHeader.getGasLimit() * 0.1), SENDER1)
        };

    for (Transaction tx : txs) {
      ensureTransactionIsValid(tx);
    }
    transactionPool.addRemoteTransactions(Arrays.stream(txs).toList());

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(txs[0]);
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(txs[1], TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS),
            entry(txs[2], TransactionSelectionResult.SENDER_WITH_PREVIOUS_TX_NOT_SELECTED));
  }

  @Test
  public void transactionSelectionStopsWhenBlockIsFull() {
    final ProcessableBlockHeader blockHeader = createBlock(3_000_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final long minTxGasCost = getGasCalculator().getMinimumTransactionCost();

    // Add 4 transactions to the Pending Transactions
    // 0) 90% of block (selected)
    // 1) 90% of block (skipped since too large)
    // 2) enough gas to only leave space for a transaction with the min gas cost (selected)
    // 3) min gas cost (selected and 100% block gas used)
    // 4) min gas cost (not selected since block is full)
    // NOTE - PendingTransactions outputs these in nonce order

    final long gasLimit0s1 = (long) (blockHeader.getGasLimit() * 0.9);
    final long gasLimit1s1 = (long) (blockHeader.getGasLimit() * 0.9);
    final long gasLimit0s2 = blockHeader.getGasLimit() - gasLimit0s1 - minTxGasCost;
    final long gasLimit1s2 = minTxGasCost;
    final long gasLimit2s2 = minTxGasCost;

    final List<Transaction> transactionsToInject = Lists.newArrayList();
    transactionsToInject.add(createTransaction(0, Wei.of(7), gasLimit0s1, SENDER1));
    transactionsToInject.add(createTransaction(1, Wei.of(7), gasLimit1s1, SENDER1));
    transactionsToInject.add(createTransaction(0, Wei.of(7), gasLimit0s2, SENDER2));
    transactionsToInject.add(createTransaction(1, Wei.of(7), gasLimit1s2, SENDER2));
    transactionsToInject.add(createTransaction(2, Wei.of(7), gasLimit2s2, SENDER2));

    for (final Transaction tx : transactionsToInject) {
      ensureTransactionIsValid(tx);
    }
    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions())
        .containsExactly(
            transactionsToInject.get(0), transactionsToInject.get(2), transactionsToInject.get(3));
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                transactionsToInject.get(1),
                TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS),
            entry(transactionsToInject.get(4), TransactionSelectionResult.BLOCK_FULL));
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(blockHeader.getGasLimit());
  }

  @Test
  public void transactionSelectionStopsWhenRemainingGasIsNotEnoughForAnyMoreTransaction() {
    final ProcessableBlockHeader blockHeader = createBlock(3_000_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final long minTxGasCost = getGasCalculator().getMinimumTransactionCost();

    // Add 4 transactions to the Pending Transactions
    // 0) 90% of block (selected)
    // 1) 90% of block (skipped since too large)
    // 2) do not fill the block, but leaves less gas than the min for a tx (selected)
    // 3) min gas cost (skipped since not enough gas remaining)
    // NOTE - PendingTransactions outputs these in nonce order

    final long gasLimit0s1 = (long) (blockHeader.getGasLimit() * 0.9);
    final long gasLimit1s1 = (long) (blockHeader.getGasLimit() * 0.9);
    final long gasLimit0s2 = blockHeader.getGasLimit() - gasLimit0s1 - (minTxGasCost - 1);
    final long gasLimit1s2 = minTxGasCost;

    final List<Transaction> transactionsToInject = new ArrayList<>(4);
    transactionsToInject.add(createTransaction(0, Wei.of(10), gasLimit0s1, SENDER1));
    transactionsToInject.add(createTransaction(1, Wei.of(10), gasLimit1s1, SENDER1));
    transactionsToInject.add(createTransaction(0, Wei.of(10), gasLimit0s2, SENDER2));
    transactionsToInject.add(createTransaction(1, Wei.of(10), gasLimit1s2, SENDER2));

    for (final Transaction tx : transactionsToInject) {
      ensureTransactionIsValid(tx);
    }
    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions())
        .containsExactly(transactionsToInject.get(0), transactionsToInject.get(2));
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                transactionsToInject.get(1),
                TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS),
            entry(transactionsToInject.get(3), TransactionSelectionResult.BLOCK_FULL));
    assertThat(blockHeader.getGasLimit() - results.getCumulativeRegularGasUsed())
        .isLessThan(minTxGasCost);
  }

  @Test
  public void shouldDiscardTransactionsThatFailValidation() {
    final ProcessableBlockHeader blockHeader = createBlock(300_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final Transaction validTransaction = createTransaction(0, Wei.of(10), 21_000);

    ensureTransactionIsValid(validTransaction, 21_000, 0);
    final Transaction invalidTransaction = createTransaction(3, Wei.of(10), 21_000);
    ensureTransactionIsInvalid(invalidTransaction, TransactionInvalidReason.NONCE_TOO_LOW);

    transactionPool.addRemoteTransactions(List.of(validTransaction, invalidTransaction));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(validTransaction.getHash())).isPresent();
    assertThat(transactionPool.getTransactionByHash(invalidTransaction.getHash())).isNotPresent();
    assertThat(results.getSelectedTransactions()).containsExactly(validTransaction);
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                invalidTransaction,
                TransactionSelectionResult.invalid(TransactionInvalidReason.NONCE_TOO_LOW.name())));
  }

  @Test
  public void transactionSelectionPluginShouldWork_PreProcessing() {
    final ProcessableBlockHeader blockHeader = createBlock(300_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);

    final Transaction selected = createTransaction(0, Wei.of(10), 21_000, SENDER1);
    ensureTransactionIsValid(selected, 21_000, 0);

    final Transaction notSelectedTransient = createTransaction(1, Wei.of(10), 21_000, SENDER1);
    ensureTransactionIsValid(notSelectedTransient, 21_000, 0);

    final Transaction notSelectedInvalid = createTransaction(2, Wei.of(10), 21_000, SENDER2);
    ensureTransactionIsValid(notSelectedInvalid, 21_000, 0);

    final PluginTransactionSelector pluginTransactionSelector =
        new PluginTransactionSelector() {
          @Override
          public TransactionSelectionResult evaluateTransactionPreProcessing(
              final TransactionEvaluationContext evaluationContext) {
            if (evaluationContext
                .getPendingTransaction()
                .getTransaction()
                .equals(notSelectedTransient))
              return PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID_TRANSIENT;
            if (evaluationContext
                .getPendingTransaction()
                .getTransaction()
                .equals(notSelectedInvalid))
              return PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID;
            return SELECTED;
          }

          @Override
          public TransactionSelectionResult evaluateTransactionPostProcessing(
              final TransactionEvaluationContext evaluationContext,
              final org.hyperledger.besu.plugin.data.TransactionProcessingResult processingResult) {
            return SELECTED;
          }
        };

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        new PluginTransactionSelectorFactory() {
          @Override
          public PluginTransactionSelector create(
              final org.hyperledger.besu.plugin.data.ProcessableBlockHeader pendingBlockHeader,
              final SelectorsStateManager selectorsStateManager) {
            return pluginTransactionSelector;
          }
        };

    final var colletorPluginTransactionSelector = new CollectorPluginTransactionSelector();

    final PluginTransactionSelectorFactory collectorTransactionSelectorFactory =
        new PluginTransactionSelectorFactory() {
          @Override
          public PluginTransactionSelector create(
              final org.hyperledger.besu.plugin.data.ProcessableBlockHeader pendingBlockHeader,
              final SelectorsStateManager selectorsStateManager) {
            return colletorPluginTransactionSelector;
          }
        };

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);
    // registering the collector factory as second factory, mean that txs that do not pass the first
    // selector should not be processed by this one and thus not collected
    transactionSelectionService.registerPluginTransactionSelectorFactory(
        collectorTransactionSelectorFactory);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(
        List.of(selected, notSelectedTransient, notSelectedInvalid));

    final TransactionSelectionResults transactionSelectionResults =
        selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(notSelectedTransient.getHash())).isPresent();
    assertThat(transactionPool.getTransactionByHash(notSelectedInvalid.getHash())).isNotPresent();
    assertThat(transactionSelectionResults.getSelectedTransactions()).containsOnly(selected);
    assertThat(transactionSelectionResults.getNotSelectedTransactions())
        .containsOnly(
            entry(
                notSelectedTransient,
                PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID_TRANSIENT),
            entry(notSelectedInvalid, PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID));

    assertThat(colletorPluginTransactionSelector.getSeenPreProcessing())
        .containsExactly(selected.getHash());
    assertThat(colletorPluginTransactionSelector.getSeenPostProcessing())
        .containsExactly(selected.getHash());
  }

  @Test
  public void transactionSelectionPluginShouldWork_PostProcessing() {
    final ProcessableBlockHeader blockHeader = createBlock(300_000);

    long maxGasUsedByTransaction = 21_000;

    final Transaction selected = createTransaction(0, Wei.of(10), 21_000);
    ensureTransactionIsValid(selected, maxGasUsedByTransaction, 0);

    // Add + 1 to gasUsedByTransaction so it will fail in the post processing selection
    final Transaction notSelected = createTransaction(1, Wei.of(10), 30_000);
    ensureTransactionIsValid(notSelected, maxGasUsedByTransaction + 1, 0);

    final PluginTransactionSelector pluginTransactionSelector =
        new PluginTransactionSelector() {
          @Override
          public TransactionSelectionResult evaluateTransactionPreProcessing(
              final TransactionEvaluationContext evaluationContext) {
            return SELECTED;
          }

          @Override
          public TransactionSelectionResult evaluateTransactionPostProcessing(
              final TransactionEvaluationContext evaluationContext,
              final org.hyperledger.besu.plugin.data.TransactionProcessingResult processingResult) {
            // the transaction with max gas +1 should fail
            if (processingResult.getEstimateGasUsedByTransaction() > maxGasUsedByTransaction) {
              return PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID_TRANSIENT;
            }
            return SELECTED;
          }
        };

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        new PluginTransactionSelectorFactory() {
          @Override
          public PluginTransactionSelector create(
              final org.hyperledger.besu.plugin.data.ProcessableBlockHeader pendingBlockHeader,
              final SelectorsStateManager selectorsStateManager) {
            return pluginTransactionSelector;
          }
        };

    final var colletorPluginTransactionSelector = new CollectorPluginTransactionSelector();

    final PluginTransactionSelectorFactory collectorTransactionSelectorFactory =
        new PluginTransactionSelectorFactory() {
          @Override
          public PluginTransactionSelector create(
              final org.hyperledger.besu.plugin.data.ProcessableBlockHeader pendingBlockHeader,
              final SelectorsStateManager selectorsStateManager) {
            return colletorPluginTransactionSelector;
          }
        };

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);
    transactionSelectionService.registerPluginTransactionSelectorFactory(
        collectorTransactionSelectorFactory);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(List.of(selected, notSelected));

    final TransactionSelectionResults transactionSelectionResults =
        selector.buildTransactionListForBlock();

    assertThat(transactionSelectionResults.getSelectedTransactions()).contains(selected);
    assertThat(transactionSelectionResults.getNotSelectedTransactions())
        .containsOnly(
            entry(notSelected, PluginTransactionSelectionResult.GENERIC_PLUGIN_INVALID_TRANSIENT));

    // notSelected is seen preprocessing since it is marked invalid only in the postprocessing
    // evaluation
    assertThat(colletorPluginTransactionSelector.getSeenPreProcessing())
        .containsExactly(selected.getHash(), notSelected.getHash());
    assertThat(colletorPluginTransactionSelector.getSeenPostProcessing())
        .containsExactly(selected.getHash());
  }

  @Test
  public void transactionSelectionPluginsShouldBeNotifiedWhenTransactionSelectionCompletes() {
    record Mocks(
        PluginTransactionSelectorFactory pluginTransactionSelectorFactory,
        PluginTransactionSelector pluginTransactionSelector) {}

    final Supplier<Mocks> transactionSelectorFactoryGenerator =
        () -> {
          final PluginTransactionSelectorFactory transactionSelectorFactory =
              mock(PluginTransactionSelectorFactory.class);
          final PluginTransactionSelector transactionSelector =
              mock(PluginTransactionSelector.class);
          when(transactionSelector.evaluateTransactionPreProcessing(any())).thenReturn(SELECTED);
          when(transactionSelector.evaluateTransactionPostProcessing(any(), any()))
              .thenReturn(SELECTED);
          when(transactionSelectorFactory.create(any(), any())).thenReturn(transactionSelector);
          return new Mocks(transactionSelectorFactory, transactionSelector);
        };

    final var transactionSelectorFactories =
        List.of(
            transactionSelectorFactoryGenerator.get(), transactionSelectorFactoryGenerator.get());

    transactionSelectorFactories.stream()
        .map(Mocks::pluginTransactionSelectorFactory)
        .forEach(transactionSelectionService::registerPluginTransactionSelectorFactory);

    final Transaction transaction = createTransaction(0, Wei.of(10), 21_000);
    ensureTransactionIsValid(transaction, 21_000, 0);

    final TransactionInvalidReason invalidReason =
        TransactionInvalidReason.PLUGIN_TX_POOL_VALIDATOR;
    final Transaction invalidTransaction = createTransaction(1, Wei.of(10), 21_000);
    ensureTransactionIsInvalid(
        invalidTransaction, TransactionInvalidReason.PLUGIN_TX_POOL_VALIDATOR);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            createBlock(300_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(List.of(transaction, invalidTransaction));

    selector.buildTransactionListForBlock();

    ArgumentCaptor<TransactionEvaluationContext> argumentCaptor =
        ArgumentCaptor.forClass(TransactionEvaluationContext.class);

    // selected transaction must be notified to the selector
    transactionSelectorFactories.stream()
        .map(Mocks::pluginTransactionSelector)
        .forEach(
            pluginTransactionSelector -> {
              verify(pluginTransactionSelector)
                  .onTransactionSelected(
                      argumentCaptor.capture(), any(TransactionProcessingResult.class));
              PendingTransaction selected = argumentCaptor.getValue().getPendingTransaction();
              assertThat(selected.getTransaction()).isEqualTo(transaction);
            });

    // unselected transaction must be notified to the selector with correct reason
    transactionSelectorFactories.stream()
        .map(Mocks::pluginTransactionSelector)
        .forEach(
            pluginTransactionSelector -> {
              verify(pluginTransactionSelector)
                  .onTransactionNotSelected(
                      argumentCaptor.capture(),
                      eq(TransactionSelectionResult.invalid(invalidReason.toString())));
              PendingTransaction rejectedTransaction =
                  argumentCaptor.getValue().getPendingTransaction();
              assertThat(rejectedTransaction.getTransaction()).isEqualTo(invalidTransaction);
            });
  }

  @Test
  public void transactionWithIncorrectNonceRemainsInPoolAndNotSelected() {
    final ProcessableBlockHeader blockHeader = createBlock(5_000_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final Transaction futureTransaction = createTransaction(4, Wei.of(10), 100_000);

    transactionPool.addRemoteTransactions(List.of(futureTransaction));
    ensureTransactionIsInvalid(futureTransaction, TransactionInvalidReason.NONCE_TOO_HIGH);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(transactionPool.getTransactionByHash(futureTransaction.getHash())).isPresent();
    assertThat(results.getSelectedTransactions()).isEmpty();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(
            entry(
                futureTransaction,
                TransactionSelectionResult.invalidPenalized(
                    TransactionInvalidReason.NONCE_TOO_HIGH.name())));
  }

  @Test
  public void increaseOfMinGasPriceAtRuntimeExcludeTxFromBeingSelected() {
    final Transaction transaction = createTransaction(0, Wei.of(7L), 100_000);
    final ProcessableBlockHeader blockHeader = createBlock(500_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder().from(defaultTestMiningConfiguration).build();

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            miningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(List.of(transaction));

    ensureTransactionIsValid(transaction, 0, 5);

    // raise the minGasPrice at runtime from 1 wei to 10 wei
    miningConfiguration.setMinTransactionGasPrice(Wei.of(10));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    // now the tx gasPrice is below the new minGasPrice, it is not selected but stays in the pool
    assertThat(results.getSelectedTransactions()).isEmpty();
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(transaction, TransactionSelectionResult.CURRENT_TX_PRICE_BELOW_MIN));
    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction);
  }

  @Test
  public void decreaseOfMinGasPriceAtRuntimeIncludeTxThatWasPreviouslyNotSelected() {
    final Transaction transaction = createTransaction(0, Wei.of(7L), 100_000);
    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder().from(defaultTestMiningConfiguration).build();
    final ProcessableBlockHeader blockHeader = createBlock(500_000);

    final Address miningBeneficiary = AddressHelpers.ofValue(1);

    final BlockTransactionSelector selector1 =
        createBlockSelectorAndSetupTxPool(
            miningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);
    transactionPool.addRemoteTransactions(List.of(transaction));

    ensureTransactionIsValid(transaction, 0, 5);

    // raise the minGasPrice at runtime from 1 wei to 10 wei
    miningConfiguration.setMinTransactionGasPrice(Wei.of(10));

    final TransactionSelectionResults results1 = selector1.buildTransactionListForBlock();

    // now the tx gasPrice is below the new minGasPrice, it is not selected but stays in the pool
    assertThat(results1.getSelectedTransactions()).isEmpty();
    assertThat(results1.getNotSelectedTransactions())
        .containsOnly(entry(transaction, TransactionSelectionResult.CURRENT_TX_PRICE_BELOW_MIN));
    assertThat(transactionPool.getPendingTransactions())
        .map(PendingTransaction::getTransaction)
        .containsOnly(transaction);

    // decrease the minGasPrice at runtime from 10 wei to 5 wei
    miningConfiguration.setMinTransactionGasPrice(Wei.of(5));

    final BlockTransactionSelector selector2 =
        createBlockSelector(
            miningConfiguration,
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    final TransactionSelectionResults results2 = selector2.buildTransactionListForBlock();

    // now the tx gasPrice is above the new minGasPrice and it is selected
    assertThat(results2.getSelectedTransactions()).contains(transaction);
    assertThat(results2.getNotSelectedTransactions()).isEmpty();
  }

  @Test
  public void shouldNotSelectTransactionsWithPriorityFeeLessThanConfig() {
    ProcessableBlockHeader blockHeader = createBlock(5_000_000, Wei.ONE);
    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder().from(defaultTestMiningConfiguration).build();
    miningConfiguration.setMinPriorityFeePerGas(Wei.of(7));
    final Transaction txSelected = createTransaction(1, Wei.of(8), 100_000);
    ensureTransactionIsValid(txSelected);
    // transaction txNotSelected should not be selected
    final Transaction txNotSelected = createTransaction(2, Wei.of(7), 100_000);
    ensureTransactionIsValid(txNotSelected);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            miningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(List.of(txSelected, txNotSelected));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsOnly(txSelected);
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(txNotSelected, PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN));
  }

  @ParameterizedTest
  @MethodSource("subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver")
  public void subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver(
      final boolean isPoa,
      final boolean preProcessingTooLate,
      final boolean processingTooLate,
      final boolean postProcessingTooLate) {

    internalBlockSelectionTimeoutSimulation(
        isPoa,
        preProcessingTooLate,
        processingTooLate,
        postProcessingTooLate,
        500,
        BLOCK_SELECTION_TIMEOUT,
        false);
  }

  @ParameterizedTest
  @MethodSource("subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver")
  public void pendingTransactionsThatTakesTooLongToEvaluateIsPenalized(
      final boolean isPoa,
      final boolean preProcessingTooLate,
      final boolean processingTooLate,
      final boolean postProcessingTooLate) {

    internalBlockSelectionTimeoutSimulation(
        isPoa,
        preProcessingTooLate,
        processingTooLate,
        postProcessingTooLate,
        900,
        TX_EVALUATION_TOO_LONG,
        false);
  }

  @Test
  public void shouldHandleTimeoutBeforeAnyTransactionIsEvaluated() {
    // set a very short max time for tx selection to force the timeout before evaluation of the tx
    final int txsSelectionMaxTime = 1;
    final AtomicReference<TransactionSelectionResults> selectionResults = new AtomicReference<>();

    // delay the start of the selection after we get the selection results
    // since it means the timeout happened
    when(ethScheduler.scheduleBlockCreationTask(anyLong(), any(Runnable.class)))
        .thenAnswer(
            invocation ->
                CompletableFuture.runAsync(
                    () -> {
                      await()
                          .atMost(Duration.ofSeconds(2))
                          .until(() -> selectionResults.get() != null);
                      invocation.getArgument(1);
                    }));

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, PositiveNumber.fromInt(txsSelectionMaxTime)),
            transactionProcessor,
            createBlock(301_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);
    final var tx = createTransaction(0, Wei.of(7), 100_000);
    ensureTransactionIsValid(tx);
    transactionPool.addRemoteTransactions(List.of(tx));

    final var results = selector.buildTransactionListForBlock();
    selectionResults.set(results);
    assertThat(results.getSelectedTransactions()).isEmpty();
  }

  @Test
  public void txEvaluationContextIsCancelledReturnsTrueOnTimeout() {
    final AtomicBoolean tecIsCancelled = new AtomicBoolean(false);
    final int txsSelectionMaxTime = 200;
    final int txProcessingTime = txsSelectionMaxTime * 2;

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        mock(PluginTransactionSelectorFactory.class);
    when(transactionSelectorFactory.create(any(), any()))
        .thenReturn(
            new PluginTransactionSelector() {
              @Override
              public TransactionSelectionResult evaluateTransactionPreProcessing(
                  final TransactionEvaluationContext evaluationContext) {
                try {
                  // pretend the tx is taking a long time to process
                  // so the timeout happens mid-processing
                  Thread.sleep(txProcessingTime);
                } catch (InterruptedException e) {
                  // ignore
                }
                return SELECTED;
              }

              @Override
              public TransactionSelectionResult evaluateTransactionPostProcessing(
                  final TransactionEvaluationContext evaluationContext,
                  final org.hyperledger.besu.plugin.data.TransactionProcessingResult
                      processingResult) {
                tecIsCancelled.set(evaluationContext.isCancelled());
                return SELECTED;
              }
            });

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, PositiveNumber.fromInt(txsSelectionMaxTime)),
            transactionProcessor,
            createBlock(301_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);

    final var tx = createTransaction(0, Wei.of(7), 100_000);
    ensureTransactionIsValid(tx);
    transactionPool.addRemoteTransactions(List.of(tx));

    final var results = selector.buildTransactionListForBlock();
    // since there was a timeout we expected not selected txs
    assertThat(results.getSelectedTransactions()).isEmpty();
    // the processing txs will check for cancellation asynchronously from the
    // results, so we wait a reasonable max amount of time for it to eventually
    // check that the selection is cancelled
    await().atMost(Duration.ofMillis(txProcessingTime * 2)).until(tecIsCancelled::get);
  }

  @Test
  public void txEvaluationContextIsCancelledReturnsTrueOnCancellation() {
    final AtomicBoolean tecIsCancelled = new AtomicBoolean(false);
    final AtomicReference<BlockTransactionSelector> selector = new AtomicReference<>();
    final PluginTransactionSelectorFactory transactionSelectorFactory =
        mock(PluginTransactionSelectorFactory.class);
    when(transactionSelectorFactory.create(any(), any()))
        .thenReturn(
            new PluginTransactionSelector() {
              @Override
              public TransactionSelectionResult evaluateTransactionPreProcessing(
                  final TransactionEvaluationContext evaluationContext) {
                // cancel selection during the evaluation of the tx
                selector.get().cancel();
                return SELECTED;
              }

              @Override
              public TransactionSelectionResult evaluateTransactionPostProcessing(
                  final TransactionEvaluationContext evaluationContext,
                  final org.hyperledger.besu.plugin.data.TransactionProcessingResult
                      processingResult) {
                tecIsCancelled.set(evaluationContext.isCancelled());
                return SELECTED;
              }
            });

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);

    selector.set(
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, PositiveNumber.fromInt(1000)),
            transactionProcessor,
            createBlock(301_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService));

    final var tx = createTransaction(0, Wei.of(7), 100_000);
    ensureTransactionIsValid(tx);
    transactionPool.addRemoteTransactions(List.of(tx));

    selector.get().buildTransactionListForBlock();
    await().atMost(Duration.ofMillis(500)).until(tecIsCancelled::get);
  }

  /**
   * When an external thread calls cancel() while the plugin selection task is genuinely running
   * (sleeping), FutureTask.cancel(true) transitions the task to CANCELLED/INTERRUPTED state and
   * get() throws CancellationException. The old code re-threw that exception; the new code catches
   * it, logs at DEBUG, and returns gracefully.
   */
  @Test
  public void externalCancellationDuringPluginSelectionIsHandledGracefully() {
    final CountDownLatch pluginTaskStarted = new CountDownLatch(1);
    final AtomicReference<BlockTransactionSelector> selectorRef = new AtomicReference<>();

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        mock(PluginTransactionSelectorFactory.class);
    when(transactionSelectorFactory.create(any(), any()))
        .thenReturn(
            new PluginTransactionSelector() {
              @Override
              public TransactionSelectionResult evaluateTransactionPreProcessing(
                  final TransactionEvaluationContext evaluationContext) {
                pluginTaskStarted.countDown();
                try {
                  // simulate slow plugin work; will be interrupted by the external cancel()
                  Thread.sleep(5_000);
                } catch (InterruptedException e) {
                  // expected when the task is externally cancelled
                }
                return SELECTED;
              }

              @Override
              public TransactionSelectionResult evaluateTransactionPostProcessing(
                  final TransactionEvaluationContext evaluationContext,
                  final org.hyperledger.besu.plugin.data.TransactionProcessingResult
                      processingResult) {
                return SELECTED;
              }
            });

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);

    selectorRef.set(
        createBlockSelectorAndSetupTxPool(
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, PositiveNumber.fromInt(10_000)),
            transactionProcessor,
            createBlock(301_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService));

    final var tx = createTransaction(0, Wei.of(7), 100_000);
    ensureTransactionIsValid(tx);
    transactionPool.addRemoteTransactions(List.of(tx));

    // Cancel from an external thread once the plugin task is running
    final CompletableFuture<Void> cancellationTask =
        CompletableFuture.runAsync(
            () -> {
              try {
                if (!pluginTaskStarted.await(5, TimeUnit.SECONDS)) {
                  return;
                }
                selectorRef.get().cancel();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // must complete without throwing; the old code propagated CancellationException out of
    // pluginTimeLimitedSelection, which would bubble up through buildTransactionListForBlock()
    final var results = selectorRef.get().buildTransactionListForBlock();
    assertThat(results).isNotNull();
    cancellationTask.orTimeout(5, TimeUnit.SECONDS).join();
  }

  /**
   * When no plugin factory is registered the plugin phase is a no-op, so processTransaction is only
   * called during internal selection. This test verifies that the CountDownLatch added in
   * internalTimeLimitedSelection causes buildTransactionListForBlock() to wait for the selection
   * thread to fully finish before returning, even when cancel() is called mid-processing and the
   * thread performs additional work after the interrupt.
   *
   * <p>Coordination flow:
   *
   * <ol>
   *   <li>Selection thread calls cancel() on itself and blocks on {@code cleanupCanFinish}.
   *   <li>It signals {@code cleanupStarted} so the test thread knows the latch has not yet counted
   *       down and that {@code buildTransactionListForBlock()} is therefore still blocked.
   *   <li>Test thread asserts the build future is still running, then releases {@code
   *       cleanupCanFinish}.
   *   <li>Selection thread unblocks, sets {@code selectionThreadFinished}, and counts down the
   *       internal latch — allowing buildTransactionListForBlock() to return.
   * </ol>
   */
  @Test
  public void internalSelectionLatchEnsuresBuildTransactionListWaitsForSelectionThreadToFinish()
      throws InterruptedException {
    final AtomicBoolean selectionThreadFinished = new AtomicBoolean(false);
    final AtomicReference<BlockTransactionSelector> selectorRef = new AtomicReference<>();
    final CountDownLatch cleanupStarted = new CountDownLatch(1);
    final CountDownLatch cleanupCanFinish = new CountDownLatch(1);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            // no plugin factory registered: plugin phase does nothing
            createMiningParameters(
                transactionSelectionService, Wei.ZERO, PositiveNumber.fromInt(5_000)),
            transactionProcessor,
            createBlock(500_000),
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService);
    selectorRef.set(selector);

    final var tx = createTransaction(0, Wei.of(7), 100_000);
    transactionPool.addRemoteTransactions(List.of(tx));

    // On processing: cancel the selector (from within the selection thread), then block on
    // cleanupCanFinish to simulate post-interrupt cleanup work, then mark as finished.
    final Answer<TransactionProcessingResult> slowCancelAnswer =
        invocation -> {
          selectorRef.get().cancel(); // triggers FutureTask.cancel(true) → interrupts this thread
          try {
            Thread.sleep(Long.MAX_VALUE); // immediately interrupted by cancel(true)
          } catch (InterruptedException e) {
            // interrupt received: signal that we are in cleanup and block until released
            cleanupStarted.countDown();
            try {
              cleanupCanFinish.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          }
          selectionThreadFinished.set(true);
          return TransactionProcessingResult.invalid(
              ValidationResult.invalid(EXECUTION_INTERRUPTED));
        };

    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any()))
        .thenAnswer(slowCancelAnswer);
    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any(), any()))
        .thenAnswer(slowCancelAnswer);

    // Run on a background thread so the test thread can drive cleanup timing.
    final CompletableFuture<Void> buildFuture =
        CompletableFuture.runAsync(selector::buildTransactionListForBlock);

    // Wait until the selection thread has started cleanup; at this point internalSelectionDone
    // has NOT yet counted down, so buildFuture must still be blocked.
    assertThat(cleanupStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(buildFuture.isDone()).isFalse();

    // Release the selection thread; it will set selectionThreadFinished and count down the latch,
    // which unblocks buildTransactionListForBlock().
    cleanupCanFinish.countDown();
    buildFuture.orTimeout(5, TimeUnit.SECONDS).join();

    // Without the CountDownLatch, buildTransactionListForBlock() would have returned before the
    // selection thread reached selectionThreadFinished.set(true).
    assertThat(selectionThreadFinished.get()).isTrue();
  }

  private void internalBlockSelectionTimeoutSimulation(
      final boolean isPoa,
      final boolean preProcessingTooLate,
      final boolean processingTooLate,
      final boolean postProcessingTooLate,
      final long longProcessingTxTime,
      final TransactionSelectionResult longProcessingTxResult,
      final boolean isLongProcessingTxDropped) {

    final long fastProcessingTxTime = 200;

    final Supplier<Answer<TransactionSelectionResult>> inTime = () -> invocation -> SELECTED;

    final BiFunction<Transaction, Long, Answer<TransactionSelectionResult>> tooLate =
        (p, t) ->
            invocation -> {
              final org.hyperledger.besu.ethereum.blockcreation.txselection
                      .TransactionEvaluationContext
                  ctx = invocation.getArgument(0);
              if (ctx.getTransaction().equals(p)) {
                try {
                  Thread.sleep(t);
                } catch (final InterruptedException e) {
                  return TransactionSelectionResult.invalidPenalized(EXECUTION_INTERRUPTED.name());
                }
              } else {
                try {
                  Thread.sleep(fastProcessingTxTime);
                } catch (final InterruptedException e) {
                  return TransactionSelectionResult.invalidPenalized(EXECUTION_INTERRUPTED.name());
                }
              }
              return SELECTED;
            };

    final ProcessableBlockHeader blockHeader = createBlock(301_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final int poaGenesisBlockPeriod = 1;
    final int blockTxsSelectionMaxTime = 750;

    final List<Transaction> transactionsToInject = new ArrayList<>(3);
    for (int i = 0; i < 2; i++) {
      final Transaction tx = createTransaction(i, Wei.of(7), 100_000);
      transactionsToInject.add(tx);
      ensureTransactionIsValid(tx, 0, 0, processingTooLate ? fastProcessingTxTime : 0);
    }

    final Transaction lateTx = createTransaction(2, Wei.of(7), 100_000);
    transactionsToInject.add(lateTx);
    ensureTransactionIsValid(lateTx, 0, 0, processingTooLate ? longProcessingTxTime : 0);

    PluginTransactionSelector transactionSelector = mock(PluginTransactionSelector.class);
    when(transactionSelector.evaluateTransactionPreProcessing(any()))
        .thenAnswer(
            preProcessingTooLate ? tooLate.apply(lateTx, longProcessingTxTime) : inTime.get());

    when(transactionSelector.evaluateTransactionPostProcessing(any(), any()))
        .thenAnswer(
            postProcessingTooLate ? tooLate.apply(lateTx, longProcessingTxTime) : inTime.get());

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        mock(PluginTransactionSelectorFactory.class);
    when(transactionSelectorFactory.create(any(), any())).thenReturn(transactionSelector);

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            isPoa
                ? createMiningParameters(
                    transactionSelectionService,
                    Wei.ZERO,
                    poaGenesisBlockPeriod,
                    PositiveNumber.fromInt(75))
                : createMiningParameters(
                    transactionSelectionService,
                    Wei.ZERO,
                    PositiveNumber.fromInt(blockTxsSelectionMaxTime)),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    // third tx is not selected, even if it could fit in the block,
    // since the selection time was over
    assertThat(results.getSelectedTransactions().size()).isEqualTo(2);

    assertThat(results.getSelectedTransactions().containsAll(transactionsToInject.subList(0, 2)))
        .isTrue();

    assertThat(results.getReceipts().size()).isEqualTo(2);
    assertThat(results.getCumulativeRegularGasUsed()).isEqualTo(200_000);

    // Ensure receipts have the correct cumulative gas
    assertThat(results.getReceipts().get(0).getCumulativeGasUsed()).isEqualTo(100_000);
    assertThat(results.getReceipts().get(1).getCumulativeGasUsed()).isEqualTo(200_000);

    // given enough time we can check the not selected tx
    await().until(() -> !results.getNotSelectedTransactions().isEmpty());
    assertThat(results.getNotSelectedTransactions())
        .containsOnly(entry(lateTx, longProcessingTxResult));
    assertThat(transactionPool.getTransactionByHash(lateTx.getHash()).isEmpty())
        .isEqualTo(isLongProcessingTxDropped);
  }

  @Disabled("flaky #7150")
  @ParameterizedTest
  @MethodSource("subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver")
  public void subsetOfInvalidPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver(
      final boolean isPoa,
      final boolean preProcessingTooLate,
      final boolean processingTooLate,
      final boolean postProcessingTooLate) {

    internalBlockSelectionTimeoutSimulationInvalidTxs(
        isPoa,
        preProcessingTooLate,
        processingTooLate,
        postProcessingTooLate,
        500,
        BLOCK_SELECTION_TIMEOUT_INVALID_TX,
        true,
        NONCE_TOO_LOW);
  }

  @ParameterizedTest
  @MethodSource("subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver")
  public void
      evaluationOfInvalidPendingTransactionThatTakesTooLongToEvaluateIsInterruptedAndPenalized(
          final boolean isPoa,
          final boolean preProcessingTooLate,
          final boolean processingTooLate,
          final boolean postProcessingTooLate) {

    internalBlockSelectionTimeoutSimulationInvalidTxs(
        isPoa,
        preProcessingTooLate,
        processingTooLate,
        postProcessingTooLate,
        900,
        TX_EVALUATION_TOO_LONG,
        false,
        NONCE_TOO_LOW);
  }

  private void internalBlockSelectionTimeoutSimulationInvalidTxs(
      final boolean isPoa,
      final boolean preProcessingTooLate,
      final boolean processingTooLate,
      final boolean postProcessingTooLate,
      final long longProcessingTxTime,
      final TransactionSelectionResult longProcessingTxResult,
      final boolean isLongProcessingTxDropped,
      final TransactionInvalidReason txInvalidReason) {

    final int txCount = 3;
    final long fastProcessingTxTime = 200;
    final var invalidSelectionResult = TransactionSelectionResult.invalid(txInvalidReason.name());

    final Supplier<Answer<TransactionSelectionResult>> inTime = () -> invocation -> SELECTED;

    final BiFunction<Transaction, Long, Answer<TransactionSelectionResult>> tooLate =
        (p, t) ->
            invocation -> {
              final org.hyperledger.besu.ethereum.blockcreation.txselection
                      .TransactionEvaluationContext
                  ctx = invocation.getArgument(0);
              if (ctx.getTransaction().equals(p)) {
                try {
                  Thread.sleep(t);
                } catch (final InterruptedException e) {
                  return TransactionSelectionResult.invalidPenalized(EXECUTION_INTERRUPTED.name());
                }
              } else {
                try {
                  Thread.sleep(fastProcessingTxTime);
                } catch (final InterruptedException e) {
                  return TransactionSelectionResult.invalidPenalized(EXECUTION_INTERRUPTED.name());
                }
              }
              return invalidSelectionResult;
            };

    final ProcessableBlockHeader blockHeader = createBlock(301_000);
    final Address miningBeneficiary = AddressHelpers.ofValue(1);
    final int poaGenesisBlockPeriod = 1;
    final int blockTxsSelectionMaxTime = 750;

    final List<Transaction> transactionsToInject = new ArrayList<>(txCount);
    for (int i = 0; i < txCount - 1; i++) {
      final Transaction tx = createTransaction(0, Wei.of(7), 100_000, Sender.values()[i]);
      transactionsToInject.add(tx);
      if (processingTooLate) {
        ensureTransactionIsInvalid(tx, txInvalidReason, fastProcessingTxTime);
      } else {
        ensureTransactionIsValid(tx);
      }
    }

    final Transaction lateTx = createTransaction(0, Wei.of(7), 100_000, SENDER5);
    transactionsToInject.add(lateTx);
    if (processingTooLate) {
      ensureTransactionIsInvalid(lateTx, txInvalidReason, longProcessingTxTime);
    } else {
      ensureTransactionIsValid(lateTx);
    }

    PluginTransactionSelector transactionSelector = mock(PluginTransactionSelector.class);
    when(transactionSelector.evaluateTransactionPreProcessing(any()))
        .thenAnswer(
            preProcessingTooLate ? tooLate.apply(lateTx, longProcessingTxTime) : inTime.get());

    when(transactionSelector.evaluateTransactionPostProcessing(any(), any()))
        .thenAnswer(
            postProcessingTooLate ? tooLate.apply(lateTx, longProcessingTxTime) : inTime.get());

    final PluginTransactionSelectorFactory transactionSelectorFactory =
        mock(PluginTransactionSelectorFactory.class);
    when(transactionSelectorFactory.create(any(), any())).thenReturn(transactionSelector);

    transactionSelectionService.registerPluginTransactionSelectorFactory(
        transactionSelectorFactory);

    final BlockTransactionSelector selector =
        createBlockSelectorAndSetupTxPool(
            isPoa
                ? createMiningParameters(
                    transactionSelectionService,
                    Wei.ZERO,
                    poaGenesisBlockPeriod,
                    PositiveNumber.fromInt(75))
                : createMiningParameters(
                    transactionSelectionService,
                    Wei.ZERO,
                    PositiveNumber.fromInt(blockTxsSelectionMaxTime)),
            transactionProcessor,
            blockHeader,
            miningBeneficiary,
            Wei.ZERO,
            transactionSelectionService);

    transactionPool.addRemoteTransactions(transactionsToInject);

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    // no tx is selected since all are invalid or late
    assertThat(results.getSelectedTransactions()).isEmpty();

    // all txs are not selected so wait until all are evaluated
    // before checking the results
    await().until(() -> results.getNotSelectedTransactions().size() == transactionsToInject.size());
    final var expectedEntries = new HashMap<Transaction, TransactionSelectionResult>();
    for (int i = 0; i < txCount - 1; i++) {
      expectedEntries.put(
          transactionsToInject.get(i), TransactionSelectionResult.invalid(txInvalidReason.name()));
    }
    expectedEntries.put(lateTx, longProcessingTxResult);
    assertThat(results.getNotSelectedTransactions())
        .containsExactlyInAnyOrderEntriesOf(expectedEntries);

    assertThat(transactionPool.getTransactionByHash(lateTx.getHash()).isEmpty())
        .isEqualTo(isLongProcessingTxDropped);
  }

  private static Stream<Arguments>
      subsetOfPendingTransactionsIncludedWhenTxSelectionMaxTimeIsOver() {

    return Stream.of(
        Arguments.of(false, true, false, false),
        Arguments.of(false, false, true, false),
        Arguments.of(false, false, false, true),
        Arguments.of(true, true, false, false),
        Arguments.of(true, false, true, false),
        Arguments.of(true, false, false, true));
  }

  protected BlockTransactionSelector createBlockSelectorAndSetupTxPool(
      final MiningConfiguration miningConfiguration,
      final MainnetTransactionProcessor transactionProcessor,
      final ProcessableBlockHeader blockHeader,
      final Address miningBeneficiary,
      final Wei blobGasPrice,
      final TransactionSelectionService transactionSelectionService) {

    transactionPool = createTransactionPool();

    return createBlockSelector(
        miningConfiguration,
        transactionProcessor,
        blockHeader,
        miningBeneficiary,
        blobGasPrice,
        transactionSelectionService);
  }

  protected BlockTransactionSelector createBlockSelector(
      final MiningConfiguration miningConfiguration,
      final MainnetTransactionProcessor transactionProcessor,
      final ProcessableBlockHeader blockHeader,
      final Address miningBeneficiary,
      final Wei blobGasPrice,
      final TransactionSelectionService transactionSelectionService) {
    ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader());
    final var selectorsStateManager = new SelectorsStateManager();
    final BlockTransactionSelector selector =
        new BlockTransactionSelector(
            miningConfiguration,
            transactionProcessor,
            blockchain,
            worldState,
            transactionPool,
            blockHeader,
            protocolSchedule.getByBlockHeader(blockHeader).getTransactionReceiptFactory(),
            miningBeneficiary,
            blobGasPrice,
            protocolSpec,
            transactionSelectionService.createPluginTransactionSelector(
                blockHeader, selectorsStateManager),
            ethScheduler,
            selectorsStateManager,
            Optional.empty());

    return selector;
  }

  protected GasCalculator getGasCalculator() {
    return protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()).getGasCalculator();
  }

  protected Transaction createTransaction(
      final int nonce, final Wei gasPrice, final long gasLimit) {
    return createTransaction(nonce, gasPrice, gasLimit, SENDER1);
  }

  protected Transaction createTransaction(
      final int nonce, final Wei gasPrice, final long gasLimit, final Sender sender) {
    return Transaction.builder()
        .gasLimit(gasLimit)
        .gasPrice(gasPrice)
        .nonce(nonce)
        .payload(Bytes.EMPTY)
        .to(Address.ID)
        .value(Wei.of(nonce))
        .sender(sender.address())
        .chainId(CHAIN_ID)
        .guessType()
        .signAndBuild(sender.keyPair());
  }

  protected Transaction createEIP1559Transaction(
      final int nonce,
      final Wei maxFeePerGas,
      final Wei maxPriorityFeePerGas,
      final long gasLimit) {
    return createEIP1559Transaction(nonce, maxFeePerGas, maxPriorityFeePerGas, gasLimit, SENDER1);
  }

  protected Transaction createEIP1559Transaction(
      final int nonce,
      final Wei maxFeePerGas,
      final Wei maxPriorityFeePerGas,
      final long gasLimit,
      final Sender sender) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .gasLimit(gasLimit)
        .maxFeePerGas(maxFeePerGas)
        .maxPriorityFeePerGas(maxPriorityFeePerGas)
        .nonce(nonce)
        .payload(Bytes.EMPTY)
        .to(Address.ID)
        .value(Wei.of(nonce))
        .sender(sender.address())
        .chainId(CHAIN_ID)
        .signAndBuild(sender.keyPair());
  }

  protected void ensureTransactionIsValid(final Transaction tx) {
    ensureTransactionIsValid(tx, 0, 0);
  }

  protected void ensureTransactionIsValid(
      final Transaction tx, final long gasUsedByTransaction, final long gasRemaining) {
    ensureTransactionIsValid(tx, gasUsedByTransaction, gasRemaining, 0);
  }

  protected void ensureTransactionIsValid(
      final Transaction tx,
      final long gasUsedByTransaction,
      final long gasRemaining,
      final long processingTime) {
    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              if (processingTime > 0) {
                try {
                  Thread.sleep(processingTime);
                } catch (final InterruptedException e) {
                  return TransactionProcessingResult.invalid(
                      ValidationResult.invalid(EXECUTION_INTERRUPTED));
                }
              }
              return TransactionProcessingResult.successful(
                  new ArrayList<>(),
                  gasUsedByTransaction,
                  gasRemaining,
                  Bytes.EMPTY,
                  Optional.empty(),
                  ValidationResult.valid());
            });
    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              if (processingTime > 0) {
                try {
                  Thread.sleep(processingTime);
                } catch (final InterruptedException e) {
                  return TransactionProcessingResult.invalid(
                      ValidationResult.invalid(EXECUTION_INTERRUPTED));
                }
              }
              return TransactionProcessingResult.successful(
                  new ArrayList<>(),
                  gasUsedByTransaction,
                  gasRemaining,
                  Bytes.EMPTY,
                  Optional.empty(),
                  ValidationResult.valid());
            });
  }

  protected void ensureTransactionIsInvalid(
      final Transaction tx, final TransactionInvalidReason invalidReason) {
    ensureTransactionIsInvalid(tx, invalidReason, 0);
  }

  protected void ensureTransactionIsInvalid(
      final Transaction tx,
      final TransactionInvalidReason invalidReason,
      final long processingTime) {
    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              if (processingTime > 0) {
                try {
                  Thread.sleep(processingTime);
                } catch (final InterruptedException e) {
                  return TransactionProcessingResult.invalid(
                      ValidationResult.invalid(EXECUTION_INTERRUPTED));
                }
              }
              return TransactionProcessingResult.invalid(ValidationResult.invalid(invalidReason));
            });
    when(transactionProcessor.processTransaction(
            any(), any(), eq(tx), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              if (processingTime > 0) {
                try {
                  Thread.sleep(processingTime);
                } catch (final InterruptedException e) {
                  return TransactionProcessingResult.invalid(
                      ValidationResult.invalid(EXECUTION_INTERRUPTED));
                }
              }
              return TransactionProcessingResult.invalid(ValidationResult.invalid(invalidReason));
            });
  }

  private BlockHeader blockHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }

  protected MiningConfiguration createMiningParameters(
      final TransactionSelectionService transactionSelectionService,
      final Wei minGasPrice,
      final PositiveNumber txsSelectionMaxTime) {
    return ImmutableMiningConfiguration.builder()
        .mutableInitValues(MutableInitValues.builder().minTransactionGasPrice(minGasPrice).build())
        .transactionSelectionService(transactionSelectionService)
        .posBlockTxsSelectionMaxTime(txsSelectionMaxTime)
        .build();
  }

  protected MiningConfiguration createMiningParameters(
      final TransactionSelectionService transactionSelectionService,
      final Wei minGasPrice,
      final int genesisBlockPeriodSeconds,
      final PositiveNumber minBlockTimePercentage) {
    return ImmutableMiningConfiguration.builder()
        .mutableInitValues(
            MutableInitValues.builder()
                .minTransactionGasPrice(minGasPrice)
                .blockPeriodSeconds(genesisBlockPeriodSeconds)
                .build())
        .transactionSelectionService(transactionSelectionService)
        .poaBlockTxsSelectionMaxTime(minBlockTimePercentage)
        .build();
  }

  private static class PluginTransactionSelectionResult extends TransactionSelectionResult {
    private enum PluginStatus implements Status {
      PLUGIN_INVALID(false, true, false),
      PLUGIN_INVALID_TRANSIENT(false, false, true);

      private final boolean stop;
      private final boolean discard;
      private final boolean penalize;

      PluginStatus(final boolean stop, final boolean discard, final boolean penalize) {
        this.stop = stop;
        this.discard = discard;
        this.penalize = penalize;
      }

      @Override
      public boolean stop() {
        return stop;
      }

      @Override
      public boolean discard() {
        return discard;
      }

      @Override
      public boolean penalize() {
        return penalize;
      }
    }

    public static final TransactionSelectionResult GENERIC_PLUGIN_INVALID_TRANSIENT =
        invalidTransient("GENERIC_PLUGIN_INVALID_TRANSIENT");

    public static final TransactionSelectionResult GENERIC_PLUGIN_INVALID =
        invalid("GENERIC_PLUGIN_INVALID");

    private PluginTransactionSelectionResult(final Status status, final String invalidReason) {
      super(status, invalidReason);
    }

    public static TransactionSelectionResult invalidTransient(final String invalidReason) {
      return new PluginTransactionSelectionResult(
          PluginStatus.PLUGIN_INVALID_TRANSIENT, invalidReason);
    }

    public static TransactionSelectionResult invalid(final String invalidReason) {
      return new PluginTransactionSelectionResult(PluginStatus.PLUGIN_INVALID, invalidReason);
    }
  }

  protected enum Sender {
    // it is important to keep the addresses of the senders sorted, to make the tests reproducible,
    // since a different sender address can change the order in which txs are selected,
    // if all the other sorting fields are equal
    SENDER1(4), // 0x1eff47bc3a10a45d4b230b5d10e37751fe6aa718
    SENDER2(2), // 0x2b5ad5c4795c026514f8317c7a215e218dccd6cf
    SENDER3(3), // 0x6813eb9362372eef6200f3b1dbc3f819671cba69
    SENDER4(1), // 0x7e5f4552091a69125d5dfcb7b8c2659029395bdf
    SENDER5(5); // 0xe1ab8145f7e55dc933d51a18c793f901a3a0b276

    private final KeyPair keyPair;
    private final Address address;

    Sender(final int seed) {
      final var privateKey =
          SignatureAlgorithmFactory.getInstance().createPrivateKey(BigInteger.valueOf(seed));
      final var publicKey = SignatureAlgorithmFactory.getInstance().createPublicKey(privateKey);
      this.keyPair = new KeyPair(privateKey, publicKey);
      this.address =
          Address.extract(Bytes32.wrap(Hash.hash(publicKey.getEncodedBytes()).getBytes()));
    }

    public KeyPair keyPair() {
      return keyPair;
    }

    public Address address() {
      return address;
    }
  }

  private static class CollectorPluginTransactionSelector implements PluginTransactionSelector {
    private final List<Hash> seenPreProcessing = new ArrayList<>();
    private final List<Hash> seenPostProcessing = new ArrayList<>();

    @Override
    public TransactionSelectionResult evaluateTransactionPreProcessing(
        final TransactionEvaluationContext evaluationContext) {
      seenPreProcessing.add(evaluationContext.getPendingTransaction().getTransaction().getHash());
      return SELECTED;
    }

    @Override
    public TransactionSelectionResult evaluateTransactionPostProcessing(
        final TransactionEvaluationContext evaluationContext,
        final org.hyperledger.besu.plugin.data.TransactionProcessingResult processingResult) {
      seenPostProcessing.add(evaluationContext.getPendingTransaction().getTransaction().getHash());
      return SELECTED;
    }

    public List<Hash> getSeenPreProcessing() {
      return seenPreProcessing;
    }

    public List<Hash> getSeenPostProcessing() {
      return seenPostProcessing;
    }
  }
}
