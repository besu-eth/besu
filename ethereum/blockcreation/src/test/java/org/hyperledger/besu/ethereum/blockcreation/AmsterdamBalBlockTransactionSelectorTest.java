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
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER1;
import static org.hyperledger.besu.ethereum.blockcreation.AbstractBlockTransactionSelectorTest.Sender.SENDER2;
import static org.hyperledger.besu.ethereum.core.MiningConfiguration.DEFAULT_POS_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
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
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.services.TransactionSelectionServiceImpl;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * EIP-7928 BAL item budget during block building requires Amsterdam (or later); this class is
 * standalone so it does not inherit the full {@link AbstractBlockTransactionSelectorTest} suite
 * (which targets London-era genesis).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AmsterdamBalBlockTransactionSelectorTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(42L);

  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  private GenesisConfig genesisConfig;
  private MutableBlockchain blockchain;
  private TransactionPool transactionPool;
  private MutableWorldState worldState;
  private ProtocolSchedule protocolSchedule;
  private TransactionSelectionService transactionSelectionService;
  private MiningConfiguration defaultTestMiningConfiguration;

  @Mock private EthScheduler ethScheduler;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ProtocolContext protocolContext;

  @Mock private MainnetTransactionProcessor transactionProcessor;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private EthContext ethContext;

  @BeforeEach
  void setUp() {
    genesisConfig =
        GenesisConfig.fromResource("/block-transaction-selector/amsterdam-genesis.json");
    protocolSchedule =
        new ProtocolScheduleBuilder(
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
    Arrays.stream(AbstractBlockTransactionSelectorTest.Sender.values())
        .map(AbstractBlockTransactionSelectorTest.Sender::address)
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

    transactionPool = createTransactionPool();
    transactionPool.setEnabled();
  }

  private MiningConfiguration createMiningParameters(
      final TransactionSelectionService transactionSelectionService,
      final Wei minGasPrice,
      final PositiveNumber txsSelectionMaxTime) {
    return ImmutableMiningConfiguration.builder()
        .mutableInitValues(MutableInitValues.builder().minTransactionGasPrice(minGasPrice).build())
        .transactionSelectionService(transactionSelectionService)
        .posBlockTxsSelectionMaxTime(txsSelectionMaxTime)
        .build();
  }

  private TransactionPool createTransactionPool() {
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

    return new TransactionPool(
        () -> pendingTransactions,
        protocolSchedule,
        protocolContext,
        mock(TransactionBroadcaster.class),
        ethContext,
        new TransactionPoolMetrics(metricsSystem),
        poolConf,
        new BlobCache());
  }

  private ProcessableBlockHeader createBlock(final long gasLimit, final Wei baseFee) {
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

  private BlockTransactionSelector createBlockSelector(
      final MiningConfiguration miningConfiguration,
      final MainnetTransactionProcessor transactionProcessor,
      final ProcessableBlockHeader blockHeader,
      final Address miningBeneficiary,
      final Wei blobGasPrice,
      final TransactionSelectionService transactionSelectionService,
      final ProtocolSchedule schedule,
      final Optional<BlockAccessList.BlockAccessListBuilder> maybeBalBuilder) {
    final ProtocolSpec protocolSpec = schedule.getByBlockHeader(blockchain.getChainHeadHeader());
    final var selectorsStateManager = new SelectorsStateManager();
    return new BlockTransactionSelector(
        miningConfiguration,
        transactionProcessor,
        blockchain,
        worldState,
        transactionPool,
        blockHeader,
        schedule.getByBlockHeader(blockHeader).getTransactionReceiptFactory(),
        miningBeneficiary,
        blobGasPrice,
        protocolSpec,
        transactionSelectionService.createPluginTransactionSelector(
            blockHeader, selectorsStateManager),
        ethScheduler,
        selectorsStateManager,
        maybeBalBuilder);
  }

  private Transaction createTransaction(
      final int nonce,
      final Wei gasPrice,
      final long gasLimit,
      final AbstractBlockTransactionSelectorTest.Sender sender) {
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

  @Test
  void secondTransactionNotSelectedWhenBlockAccessListItemBudgetWouldBeExceeded() {
    final ProtocolSpec chainSpec =
        spy(protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()));
    final GasCalculator realGasCalculator = chainSpec.getGasCalculator();
    final GasCalculator gasCalculator = mock(GasCalculator.class, delegatesTo(realGasCalculator));
    lenient().when(gasCalculator.getBlockAccessListItemCost()).thenReturn(2_000_000L);
    doReturn(gasCalculator).when(chainSpec).getGasCalculator();
    final ProtocolSchedule scheduleStub = mock(ProtocolSchedule.class);
    when(scheduleStub.getByBlockHeader(any())).thenReturn(chainSpec);

    final ProcessableBlockHeader blockHeader = createBlock(6_000_000, Wei.ONE);
    final BlockAccessList.BlockAccessListBuilder sharedBalBuilder = BlockAccessList.builder();
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService,
            scheduleStub,
            Optional.of(sharedBalBuilder));

    final Transaction tx1 = createTransaction(0, Wei.of(5), 100_000, SENDER1);
    final Transaction tx2 = createTransaction(0, Wei.of(5), 100_000, SENDER2);

    final AtomicInteger evaluationOrder = new AtomicInteger();
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              final int txIndex = evaluationOrder.getAndIncrement();
              return TransactionProcessingResult.successful(
                  List.of(),
                  50_000L,
                  50_000L,
                  Bytes.EMPTY,
                  Optional.of(balPartialAddingTwoEip7928Items(txIndex)),
                  ValidationResult.valid());
            });

    transactionPool.addRemoteTransactions(List.of(tx1, tx2));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(tx1);
    assertThat(results.getNotSelectedTransactions())
        .containsExactly(entry(tx2, BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED));

    final BlockAccessList committedBal = sharedBalBuilder.build();
    assertThat(committedBal.eip7928ItemCount()).isEqualTo(2);

    final Address tx2PartialAccount = Address.fromHexString(String.format("0x%040x", 1L + 100L));
    assertThat(committedBal.accountChanges())
        .extracting(BlockAccessList.AccountChanges::address)
        .doesNotContain(tx2PartialAccount);

    final BlockAccessList.BlockAccessListBuilder expectedBuilder = BlockAccessList.builder();
    expectedBuilder.apply(balPartialAddingTwoEip7928Items(0));
    final BlockAccessList expectedBalOnlyTx1 = expectedBuilder.build();
    assertThat(BodyValidation.balHash(committedBal))
        .isEqualTo(BodyValidation.balHash(expectedBalOnlyTx1));
  }

  @Test
  void bothTransactionsSelectedWhenBlockAccessListItemBudgetAllowsThem() {
    final ProtocolSpec chainSpec =
        spy(protocolSchedule.getByBlockHeader(blockchain.getChainHeadHeader()));
    final GasCalculator realGasCalculator = chainSpec.getGasCalculator();
    final GasCalculator gasCalculator = mock(GasCalculator.class, delegatesTo(realGasCalculator));
    lenient().when(gasCalculator.getBlockAccessListItemCost()).thenReturn(1_000_000L);
    doReturn(gasCalculator).when(chainSpec).getGasCalculator();
    final ProtocolSchedule scheduleStub = mock(ProtocolSchedule.class);
    when(scheduleStub.getByBlockHeader(any())).thenReturn(chainSpec);

    final ProcessableBlockHeader blockHeader = createBlock(6_000_000, Wei.ONE);
    final BlockAccessList.BlockAccessListBuilder sharedBalBuilder = BlockAccessList.builder();
    final BlockTransactionSelector selector =
        createBlockSelector(
            defaultTestMiningConfiguration,
            transactionProcessor,
            blockHeader,
            AddressHelpers.ofValue(1),
            Wei.ZERO,
            transactionSelectionService,
            scheduleStub,
            Optional.of(sharedBalBuilder));

    final Transaction tx1 = createTransaction(0, Wei.of(5), 100_000, SENDER1);
    final Transaction tx2 = createTransaction(0, Wei.of(5), 100_000, SENDER2);

    final AtomicInteger evaluationOrder = new AtomicInteger();
    when(transactionProcessor.processTransaction(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              final int txIndex = evaluationOrder.getAndIncrement();
              return TransactionProcessingResult.successful(
                  List.of(),
                  50_000L,
                  50_000L,
                  Bytes.EMPTY,
                  Optional.of(balPartialAddingTwoEip7928Items(txIndex)),
                  ValidationResult.valid());
            });

    transactionPool.addRemoteTransactions(List.of(tx1, tx2));

    final TransactionSelectionResults results = selector.buildTransactionListForBlock();

    assertThat(results.getSelectedTransactions()).containsExactly(tx1, tx2);
    assertThat(results.getNotSelectedTransactions()).isEmpty();

    final BlockAccessList committedBal = sharedBalBuilder.build();
    assertThat(committedBal.eip7928ItemCount()).isEqualTo(4L);

    final BlockAccessList.BlockAccessListBuilder expectedBuilder = BlockAccessList.builder();
    expectedBuilder.apply(balPartialAddingTwoEip7928Items(0));
    expectedBuilder.apply(balPartialAddingTwoEip7928Items(1));
    final BlockAccessList expectedBalBothTxs = expectedBuilder.build();
    assertThat(BodyValidation.balHash(committedBal))
        .isEqualTo(BodyValidation.balHash(expectedBalBothTxs));
  }

  private static PartialBlockAccessView balPartialAddingTwoEip7928Items(final int txIndex) {
    final Address addr = Address.fromHexString(String.format("0x%040x", txIndex + 100L));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder
        .getOrCreateAccountBuilder(addr)
        .addStorageChange(new StorageSlotKey(UInt256.ONE), UInt256.ZERO);
    return builder.build();
  }
}
