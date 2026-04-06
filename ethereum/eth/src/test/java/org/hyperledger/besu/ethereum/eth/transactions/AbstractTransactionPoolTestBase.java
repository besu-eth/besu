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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.ValidationResult.valid;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredTransactionPoolBaseFeeTest;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderBalanceChecker;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.LegacyTransactionPoolBaseFeeTest;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidatorFactory;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidator;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidatorFactory;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@SuppressWarnings("unchecked")
@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = LENIENT)
public abstract class AbstractTransactionPoolTestBase extends TrustedSetupClassLoaderExtension {

  protected static final KeyPair KEY_PAIR1 =
      SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private static final KeyPair KEY_PAIR2 =
      SignatureAlgorithmFactory.getInstance().generateKeyPair();
  protected static final Wei BASE_FEE_FLOOR = Wei.of(7L);
  protected static final Wei DEFAULT_MIN_GAS_PRICE = Wei.of(50L);

  protected final EthScheduler ethScheduler = new DeterministicEthScheduler();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  protected TransactionValidatorFactory transactionValidatorFactory;

  @Mock protected PendingTransactionAddedListener listener;

  @Mock protected TransactionsMessageSender transactionsMessageSender;
  @Mock protected NewPooledTransactionHashesMessageSender newPooledTransactionHashesMessageSender;
  @Mock protected ProtocolSpec protocolSpec;

  protected ProtocolSchedule protocolSchedule;

  protected final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  protected MutableBlockchain blockchain;
  protected TransactionBroadcaster transactionBroadcaster;

  protected final Transaction transaction0 = createTransaction(0);
  protected final Transaction transaction1 = createTransaction(1);
  private static final BlobTestFixture BLOB_TEST_FIXTURE = new BlobTestFixture();
  protected static final Transaction transactionWithBlobs = createStaticBlobTransaction(2);
  protected static final Transaction transactionWithBlobsReplacement =
      createStaticReplacementTransactionWithBlobs(2);
  protected static final Transaction transactionWithSameBlobs =
      createStaticBlobTransactionWithSameBlobs(
          3, transactionWithBlobs.getBlobsWithCommitments().get());
  protected static final Transaction transactionWithSameBlobsReplacement =
      createStaticReplacementTransactionWithBlobs(3);

  protected final Transaction transactionOtherSender = createTransaction(1, KEY_PAIR2);
  private ExecutionContextTestFixture executionContext;
  protected ProtocolContext protocolContext;
  protected TransactionPool transactionPool;
  protected long blockGasLimit;
  protected EthProtocolManager ethProtocolManager;
  protected EthContext ethContext;
  protected ArgumentCaptor<Runnable> syncTaskCapture;
  protected PeerTransactionTracker peerTransactionTracker;
  protected SenderBalanceChecker senderBalanceChecker = new SenderBalanceChecker.NoOpChecker();

  protected abstract PendingTransactions createPendingTransactions(
      final TransactionPoolConfiguration poolConfig,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester);

  protected TransactionTestFixture createBaseTransactionGasPriceMarket(
      final int transactionNumber) {
    return new TransactionTestFixture()
        .nonce(transactionNumber)
        .gasLimit(blockGasLimit)
        .type(TransactionType.FRONTIER);
  }

  protected TransactionTestFixture createBaseTransactionBaseFeeMarket(final int nonce) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(blockGasLimit)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L)))
        .type(TransactionType.EIP1559);
  }

  protected abstract ExecutionContextTestFixture createExecutionContextTestFixture();

  protected static ExecutionContextTestFixture createExecutionContextTestFixtureBaseFeeMarket() {
    final var genesisConfigFile = GenesisConfig.fromResource("/txpool-test-genesis.json");
    final ProtocolSchedule protocolSchedule =
        new ProtocolScheduleBuilder(
                genesisConfigFile.getConfigOptions(),
                Optional.of(BigInteger.valueOf(1)),
                ProtocolSpecAdapters.create(0, Function.identity()),
                false,
                EvmConfiguration.DEFAULT,
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                BalConfiguration.DEFAULT,
                new NoOpMetricsSystem())
            .createProtocolSchedule();
    final ExecutionContextTestFixture executionContextTestFixture =
        ExecutionContextTestFixture.builder(genesisConfigFile)
            .protocolSchedule(protocolSchedule)
            .build();

    final Block block =
        new Block(
            new BlockHeaderTestFixture()
                .gasLimit(
                    executionContextTestFixture
                        .getBlockchain()
                        .getChainHeadBlock()
                        .getHeader()
                        .getGasLimit())
                .difficulty(Difficulty.ONE)
                .baseFeePerGas(Wei.of(10L))
                .parentHash(executionContextTestFixture.getBlockchain().getChainHeadHash())
                .number(executionContextTestFixture.getBlockchain().getChainHeadBlockNumber() + 1)
                .buildHeader(),
            new BlockBody(List.of(), List.of()));
    executionContextTestFixture.getBlockchain().appendBlock(block, List.of());

    return executionContextTestFixture;
  }

  protected abstract FeeMarket getFeeMarket();

  @BeforeEach
  public void setUp() {
    executionContext = createExecutionContextTestFixture();
    protocolContext = executionContext.getProtocolContext();
    blockchain = executionContext.getBlockchain();
    when(protocolSpec.getTransactionValidatorFactory()).thenReturn(transactionValidatorFactory);
    when(protocolSpec.getFeeMarket()).thenReturn(getFeeMarket());
    protocolSchedule = spy(executionContext.getProtocolSchedule());
    doReturn(protocolSpec).when(protocolSchedule).getByBlockHeader(any());
    blockGasLimit = blockchain.getChainHeadBlock().getHeader().getGasLimit();
    ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(blockchain)
            .build();
    ethContext = spy(ethProtocolManager.ethContext());

    final EthScheduler ethScheduler = spy(ethContext.getScheduler());
    syncTaskCapture = ArgumentCaptor.forClass(Runnable.class);
    doNothing().when(ethScheduler).scheduleSyncWorkerTask(syncTaskCapture.capture());
    doReturn(ethScheduler).when(ethContext).getScheduler();

    peerTransactionTracker =
        new PeerTransactionTracker(
            TransactionPoolConfiguration.DEFAULT,
            ethContext.getEthPeers(),
            ethContext.getScheduler());
    transactionBroadcaster =
        spy(
            new TransactionBroadcaster(
                ethContext,
                peerTransactionTracker,
                transactionsMessageSender,
                newPooledTransactionHashesMessageSender));

    transactionPool = createTransactionPool();
    blockchain.observeBlockAdded(transactionPool);
  }

  protected TransactionPool createTransactionPool() {
    return createTransactionPool(b -> b.minGasPrice(Wei.of(2)));
  }

  TransactionPool createTransactionPool(
      final Consumer<ImmutableTransactionPoolConfiguration.Builder> configConsumer) {
    final ImmutableTransactionPoolConfiguration.Builder configBuilder =
        ImmutableTransactionPoolConfiguration.builder();
    configConsumer.accept(configBuilder);
    final TransactionPoolConfiguration poolConfig = configBuilder.build();

    final TransactionPoolReplacementHandler transactionReplacementHandler =
        new TransactionPoolReplacementHandler(
            poolConfig.getPriceBump(), poolConfig.getBlobPriceBump());

    final BiFunction<PendingTransaction, PendingTransaction, Boolean> transactionReplacementTester =
        (t1, t2) ->
            transactionReplacementHandler.shouldReplace(
                t1, t2, protocolContext.getBlockchain().getChainHeadHeader());

    final TransactionPool txPool =
        new TransactionPool(
            () -> createPendingTransactions(poolConfig, transactionReplacementTester),
            protocolSchedule,
            protocolContext,
            transactionBroadcaster,
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConfig,
            new BlobCache());
    txPool.setEnabled();
    return txPool;
  }

  static TransactionPoolValidatorService getTransactionPoolValidatorServiceReturning(
      final String errorMessage) {
    return new TransactionPoolValidatorService() {
      @Override
      public PluginTransactionPoolValidator createTransactionValidator() {
        return (transaction, isLocal, hasPriority) -> Optional.ofNullable(errorMessage);
      }

      @Override
      public void registerPluginTransactionValidatorFactory(
          final PluginTransactionPoolValidatorFactory pluginTransactionPoolValidatorFactory) {}
    };
  }

  @SuppressWarnings("unused")
  private static boolean isBaseFeeMarket(final ExtensionContext extensionContext) {
    final Class<?> cz = extensionContext.getTestClass().get();

    return cz.equals(LegacyTransactionPoolBaseFeeTest.class)
        || cz.equals(LayeredTransactionPoolBaseFeeTest.class)
        || cz.equals(BlobV1TransactionPoolTest.class);
  }

  protected void assertTransactionNotPending(final Transaction transaction) {
    assertThat(transactionPool.getTransactionByHash(transaction.getHash())).isEmpty();
  }

  protected void addAndAssertRemoteTransactionInvalid(final Transaction tx) {
    transactionPool.addRemoteTransactions(List.of(tx));

    verify(transactionBroadcaster, never()).onTransactionsAdded(singletonList(tx));
    assertTransactionNotPending(tx);
  }

  protected void assertTransactionPending(final Transaction t) {
    assertThat(transactionPool.getTransactionByHash(t.getHash())).contains(t);
  }

  protected void addAndAssertRemoteTransactionsValid(final Transaction... txs) {
    addAndAssertRemoteTransactionsValid(false, txs);
  }

  protected void addAndAssertRemotePriorityTransactionsValid(final Transaction... txs) {
    addAndAssertRemoteTransactionsValid(true, txs);
  }

  protected void addAndAssertRemoteTransactionsValid(
      final boolean hasPriority, final Transaction... txs) {
    transactionPool.addRemoteTransactions(List.of(txs));

    verify(transactionBroadcaster)
        .onTransactionsAdded(
            argThat(btxs -> btxs.size() == txs.length && btxs.containsAll(List.of(txs))));
    Arrays.stream(txs).forEach(this::assertTransactionPending);
    assertThat(getLocalTransactions()).doesNotContain(txs);
    if (hasPriority) {
      assertThat(getPriorityTransactions()).contains(txs);
    }
  }

  protected void addAndAssertTransactionViaApiValid(
      final Transaction tx, final boolean disableLocalPriority) {
    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(tx);

    assertThat(result.isValid()).isTrue();
    assertTransactionPending(tx);
    verify(transactionBroadcaster).onTransactionsAdded(singletonList(tx));
    assertThat(getLocalTransactions()).contains(tx);
    if (disableLocalPriority) {
      assertThat(getPriorityTransactions()).doesNotContain(tx);
    } else {
      assertThat(getPriorityTransactions()).contains(tx);
    }
  }

  protected void addAndAssertTransactionViaApiInvalid(
      final Transaction tx, final TransactionInvalidReason invalidReason) {
    final ValidationResult<TransactionInvalidReason> result =
        transactionPool.addTransactionViaApi(tx);

    assertThat(result.isValid()).isFalse();
    assertThat(result.getInvalidReason()).isEqualTo(invalidReason);
    assertTransactionNotPending(tx);
    verify(transactionBroadcaster, never()).onTransactionsAdded(singletonList(tx));
  }

  @SuppressWarnings("unchecked")
  protected void givenTransactionIsValid(final Transaction transaction) {
    when(transactionValidatorFactory
            .get()
            .validate(eq(transaction), any(Optional.class), any(Optional.class), any()))
        .thenReturn(valid());
    when(transactionValidatorFactory
            .get()
            .validateForSender(
                eq(transaction), nullable(Account.class), any(TransactionValidationParams.class)))
        .thenReturn(valid());
  }

  protected void givenAllTransactionsAreValid() {
    when(transactionValidatorFactory
            .get()
            .validate(any(), any(Optional.class), any(Optional.class), any()))
        .thenReturn(valid());
    when(transactionValidatorFactory
            .get()
            .validateForSender(
                any(), nullable(Account.class), any(TransactionValidationParams.class)))
        .thenReturn(valid());
  }

  protected abstract Block appendBlock(
      final Difficulty difficulty,
      final BlockHeader parentBlock,
      final Transaction... transactionsToAdd);

  protected Transaction createTransactionGasPriceMarket(
      final int transactionNumber, final Wei maxPrice) {
    return createBaseTransaction(transactionNumber).gasPrice(maxPrice).createTransaction(KEY_PAIR1);
  }

  protected Transaction createTransactionBaseFeeMarket(final int nonce, final Wei maxPrice) {
    return createBaseTransaction(nonce)
        .maxFeePerGas(Optional.of(maxPrice))
        .maxPriorityFeePerGas(Optional.of(maxPrice.divide(5L)))
        .createTransaction(KEY_PAIR1);
  }

  protected abstract TransactionTestFixture createBaseTransaction(final int nonce);

  protected Transaction createTransaction(
      final int transactionNumber, final Optional<BigInteger> maybeChainId) {
    return createBaseTransaction(transactionNumber)
        .chainId(maybeChainId)
        .createTransaction(KEY_PAIR1);
  }

  protected abstract Transaction createTransaction(final int nonce, final Wei maxPrice);

  protected Transaction createTransaction(final int nonce) {
    return createTransaction(nonce, Optional.of(BigInteger.ONE));
  }

  protected Transaction createTransaction(final int nonce, final KeyPair keyPair) {
    return createBaseTransaction(nonce).createTransaction(keyPair);
  }

  protected void verifyChainHeadIs(final Block forkBlock2) {
    assertThat(blockchain.getChainHeadHash()).isEqualTo(forkBlock2.getHash());
  }

  protected BlockHeader getHeaderForCurrentChainHead() {
    return blockchain.getBlockHeader(blockchain.getChainHeadHash()).get();
  }

  protected void appendBlock(final Transaction... transactionsToAdd) {
    appendBlock(Difficulty.ONE, getHeaderForCurrentChainHead(), transactionsToAdd);
  }

  protected void protocolSupportsTxReplayProtection(
      final long chainId, final boolean isSupportedAtCurrentBlock) {
    when(protocolSpec.isReplayProtectionSupported()).thenReturn(isSupportedAtCurrentBlock);
    when(protocolSchedule.getChainId()).thenReturn(Optional.of(BigInteger.valueOf(chainId)));
  }

  protected void protocolDoesNotSupportTxReplayProtection() {
    when(protocolSchedule.getChainId()).thenReturn(Optional.empty());
  }

  protected Transaction createTransactionWithoutChainId(final int transactionNumber) {
    return createTransaction(transactionNumber, Optional.empty());
  }

  protected void whenBlockBaseFeeIs(final Wei baseFee) {
    final BlockHeader header =
        BlockHeaderBuilder.fromHeader(blockchain.getChainHeadHeader())
            .baseFee(baseFee)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .parentHash(blockchain.getChainHeadHash())
            .buildBlockHeader();
    blockchain.appendBlock(new Block(header, BlockBody.empty()), emptyList());
  }

  protected Transaction createFrontierTransaction(final int transactionNumber, final Wei gasPrice) {
    return new TransactionTestFixture()
        .nonce(transactionNumber)
        .gasPrice(gasPrice)
        .gasLimit(blockGasLimit)
        .type(TransactionType.FRONTIER)
        .createTransaction(KEY_PAIR1);
  }

  private static Transaction createStaticBlobTransaction(final int nonce) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(0L)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(BLOB_TEST_FIXTURE.createBlobsWithCommitments(6)))
        .createTransaction(KEY_PAIR1);
  }

  private static Transaction createStaticBlobTransactionWithSameBlobs(
      final int nonce, final BlobsWithCommitments blobsWithCommitments) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(0L)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(blobsWithCommitments))
        .createTransaction(KEY_PAIR1);
  }

  private static Transaction createStaticReplacementTransactionWithBlobs(final int nonce) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(0L)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L * 10)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L * 10)))
        .maxFeePerBlobGas(Optional.of(Wei.of(5000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(BLOB_TEST_FIXTURE.createBlobsWithCommitments(6)))
        .createTransaction(KEY_PAIR1);
  }

  protected Transaction createBlobTransaction(final int nonce) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(blockGasLimit)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(new BlobTestFixture().createBlobsWithCommitments(6)))
        .createTransaction(KEY_PAIR1);
  }

  protected Transaction createBlobTransactionWithSameBlobs(
      final int nonce, final BlobsWithCommitments blobsWithCommitments) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(blockGasLimit)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(blobsWithCommitments))
        .createTransaction(KEY_PAIR1);
  }

  protected Transaction createReplacementTransactionWithBlobs(final int nonce) {
    return new TransactionTestFixture()
        .nonce(nonce)
        .gasLimit(blockGasLimit)
        .gasPrice(null)
        .maxFeePerGas(Optional.of(Wei.of(5000L * 10)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1000L * 10)))
        .maxFeePerBlobGas(Optional.of(Wei.of(5000L)))
        .type(TransactionType.BLOB)
        .blobsWithCommitments(Optional.of(new BlobTestFixture().createBlobsWithCommitments(6)))
        .createTransaction(KEY_PAIR1);
  }

  protected int addTxAndGetPendingTxsCount(
      final Wei genesisBaseFee,
      final Wei minGasPrice,
      final Wei lastBlockBaseFee,
      final Wei txMaxFeePerGas,
      final boolean isLocal,
      final boolean hasPriority) {
    when(protocolSpec.getFeeMarket()).thenReturn(FeeMarket.london(0, Optional.of(genesisBaseFee)));
    whenBlockBaseFeeIs(lastBlockBaseFee);

    final Transaction transaction = createTransaction(0, txMaxFeePerGas);
    if (hasPriority) {
      transactionPool =
          createTransactionPool(
              b -> b.minGasPrice(minGasPrice).prioritySenders(Set.of(transaction.getSender())));
    } else {
      transactionPool =
          createTransactionPool(b -> b.minGasPrice(minGasPrice).noLocalPriority(true));
    }

    givenTransactionIsValid(transaction);

    if (isLocal) {
      transactionPool.addTransactionViaApi(transaction);
    } else {
      transactionPool.addRemoteTransactions(List.of(transaction));
    }

    return transactionPool.count();
  }

  protected Block appendBlockGasPriceMarket(
      final Difficulty difficulty,
      final BlockHeader parentBlock,
      final Transaction[] transactionsToAdd) {
    final List<Transaction> transactionList = asList(transactionsToAdd);
    final Block block =
        new Block(
            new BlockHeaderTestFixture()
                .difficulty(difficulty)
                .gasLimit(parentBlock.getGasLimit())
                .parentHash(parentBlock.getHash())
                .number(parentBlock.getNumber() + 1)
                .buildHeader(),
            new BlockBody(transactionList, emptyList()));
    final List<TransactionReceipt> transactionReceipts =
        transactionList.stream()
            .map(transaction -> new TransactionReceipt(1, 1, emptyList(), Optional.empty()))
            .collect(toList());
    blockchain.appendBlock(block, transactionReceipts);
    return block;
  }

  protected Block appendBlockBaseFeeMarket(
      final Difficulty difficulty,
      final BlockHeader parentBlock,
      final Transaction[] transactionsToAdd) {
    final List<Transaction> transactionList = asList(transactionsToAdd);
    final Block block =
        new Block(
            new BlockHeaderTestFixture()
                .baseFeePerGas(Wei.of(10L))
                .gasLimit(parentBlock.getGasLimit())
                .difficulty(difficulty)
                .parentHash(parentBlock.getHash())
                .number(parentBlock.getNumber() + 1)
                .buildHeader(),
            new BlockBody(transactionList, emptyList()));
    final List<TransactionReceipt> transactionReceipts =
        transactionList.stream()
            .map(transaction -> new TransactionReceipt(1, 1, emptyList(), Optional.empty()))
            .collect(toList());
    blockchain.appendBlock(block, transactionReceipts);
    return block;
  }

  protected List<Transaction> getPriorityTransactions() {
    return transactionPool.getPendingTransactions().stream()
        .filter(PendingTransaction::hasPriority)
        .map(PendingTransaction::getTransaction)
        .toList();
  }

  protected List<Transaction> getLocalTransactions() {
    return transactionPool.getPendingTransactions().stream()
        .filter(PendingTransaction::isReceivedFromLocalSource)
        .map(PendingTransaction::getTransaction)
        .toList();
  }
}
