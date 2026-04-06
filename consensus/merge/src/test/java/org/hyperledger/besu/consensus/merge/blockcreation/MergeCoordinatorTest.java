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
package org.hyperledger.besu.consensus.merge.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.Unstable;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.data.AddedBlockContext.EventType;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;

import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MergeCoordinatorTest implements MergeGenesisConfigHelper {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private static final Logger LOG = LoggerFactory.getLogger(MergeCoordinatorTest.class);
  private static final SECPPrivateKey PRIVATE_KEY1 =
      SIGNATURE_ALGORITHM.createPrivateKey(
          Bytes32.fromHexString(
              "ae6ae8e5ccbfb04590405997ee2d52d2b330726137b875053c36d94e974d162f"));
  private static final KeyPair KEYS1 =
      new KeyPair(PRIVATE_KEY1, SIGNATURE_ALGORITHM.createPublicKey(PRIVATE_KEY1));

  private static final long REPETITION_MIN_DURATION = 100;

  private static final BigInteger CHAIN_ID_MAINNET = BigInteger.ONE;
  private static final BigInteger CHAIN_ID_HOODI = BigInteger.valueOf(560048);
  private static final long DEFAULT_TARGET_GAS_LIMIT = 60_000_000L;
  private static final long DEFAULT_TARGET_GAS_LIMIT_TESTNET = 60_000_000L;

  @Mock MergeContext mergeContext;
  @Mock BackwardSyncContext backwardSyncContext;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  EthContext ethContext;

  @Mock EthScheduler ethScheduler;

  private final Address coinbase = genesisAllocations(getPosGenesisConfig()).findFirst().get();

  private MiningConfiguration miningConfiguration =
      ImmutableMiningConfiguration.builder()
          .mutableInitValues(MutableInitValues.builder().coinbase(coinbase).build())
          .unstable(
              Unstable.builder()
                  .posBlockCreationRepetitionMinDuration(REPETITION_MIN_DURATION)
                  .build())
          .build();

  private MergeCoordinator coordinator;
  private ProtocolContext protocolContext;

  private final ProtocolSchedule protocolSchedule = spy(getMergeProtocolSchedule());
  private final GenesisState genesisState =
      GenesisState.fromConfig(getPosGenesisConfig(), protocolSchedule, new CodeCache());

  private final WorldStateArchive worldStateArchive = createInMemoryWorldStateArchive();

  private final MutableBlockchain blockchain =
      spy(createInMemoryBlockchain(genesisState.getBlock()));

  private final Address suggestedFeeRecipient = Address.ZERO;
  private final BlockHeaderTestFixture headerGenerator = new BlockHeaderTestFixture();
  private final BaseFeeMarket feeMarket =
      FeeMarket.london(0, genesisState.getBlock().getHeader().getBaseFee());

  private final org.hyperledger.besu.metrics.StubMetricsSystem metricsSystem =
      new StubMetricsSystem();

  private final TransactionPoolConfiguration poolConf =
      ImmutableTransactionPoolConfiguration.builder()
          .txPoolMaxSize(10)
          .txPoolLimitByAccountPercentage(Fraction.fromPercentage(100))
          .build();
  private final BaseFeePendingTransactionsSorter transactions =
      new BaseFeePendingTransactionsSorter(
          poolConf,
          TestClock.system(ZoneId.systemDefault()),
          metricsSystem,
          MergeCoordinatorTest::mockBlockHeader);

  private TransactionPool transactionPool;

  CompletableFuture<Void> blockCreationTask = CompletableFuture.completedFuture(null);

  private final BadBlockManager badBlockManager = spy(new BadBlockManager());

  @BeforeEach
  public void setUp() {
    when(mergeContext.as(MergeContext.class)).thenReturn(mergeContext);
    when(mergeContext.getTerminalTotalDifficulty())
        .thenReturn(genesisState.getBlock().getHeader().getDifficulty().plus(1L));

    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(badBlockManager)
            .build();
    var mutable = worldStateArchive.getWorldState();
    genesisState.writeStateTo(mutable);
    mutable.persist(null);

    when(ethScheduler.scheduleBlockCreationTask(anyLong(), any()))
        .thenAnswer(
            invocation -> {
              final Runnable runnable = invocation.getArgument(1);
              if (!invocation.toString().contains("MergeCoordinator")) {
                return CompletableFuture.runAsync(runnable);
              }
              blockCreationTask = CompletableFuture.runAsync(runnable);
              return blockCreationTask;
            });

    MergeConfiguration.setMergeEnabled(true);

    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);
    this.transactionPool =
        new TransactionPool(
            () -> transactions,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    this.transactionPool.setEnabled();

    this.coordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            transactionPool,
            miningConfiguration,
            backwardSyncContext);
  }

  @Test
  public void coinbaseShouldMatchSuggestedFeeRecipient() {
    doAnswer(
            invocation -> {
              coordinator.finalizeProposalById(
                  invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, atLeastOnce()).putPayloadById(payloadWrapper.capture());

    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);
    assertThat(payloadWrapper.getValue().blockWithReceipts().getHeader().getCoinbase())
        .isEqualTo(suggestedFeeRecipient);
  }

  @Test
  public void exceptionDuringBuildingBlockShouldNotBeInvalid()
      throws ExecutionException, InterruptedException {

    final int txPerBlock = 7;

    MergeCoordinator.MergeBlockCreatorFactory mergeBlockCreatorFactory =
        (parentHeader, address) -> {
          MergeBlockCreator beingSpiedOn =
              spy(
                  new MergeBlockCreator(
                      miningConfiguration,
                      parent -> Bytes.EMPTY,
                      transactionPool,
                      protocolContext,
                      protocolSchedule,
                      parentHeader,
                      ethScheduler));

          doCallRealMethod()
              .doCallRealMethod()
              .doThrow(new MerkleTrieException("missing leaf"))
              .doCallRealMethod()
              .when(beingSpiedOn)
              .createBlock(
                  any(),
                  any(Bytes32.class),
                  anyLong(),
                  eq(Optional.empty()),
                  eq(Optional.empty()),
                  eq(Optional.empty()),
                  any());
          return beingSpiedOn;
        };

    MergeCoordinator willThrow =
        spy(
            new MergeCoordinator(
                protocolContext,
                protocolSchedule,
                ethScheduler,
                miningConfiguration,
                backwardSyncContext,
                mergeBlockCreatorFactory));

    final AtomicLong retries = new AtomicLong(0);
    doAnswer(
            invocation -> {
              if (retries.getAndIncrement() < txPerBlock) {
                // a new transaction every time a block is built
                transactions.addTransaction(
                    createLocalTransaction(retries.get() - 1), Optional.empty());
              } else {
                // when we have 5 transactions finalize block creation
                willThrow.finalizeProposalById(
                    invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        willThrow.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.random(),
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    blockCreationTask.get();

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, times(txPerBlock + 1))
        .putPayloadById(payloadWrapper.capture()); // +1 for the empty
    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);
    assertThat(
            payloadWrapper
                .getValue()
                .blockWithReceipts()
                .getBlock()
                .getBody()
                .getTransactions()
                .size())
        .isEqualTo(txPerBlock);
    // this only verifies that adding the bad block didn't happen through the mergeCoordinator, it
    // still may be called directly.
    verify(badBlockManager, never()).addBadBlock(any(), any());
  }

  @Test
  public void shouldNotRecordProposedBadBlockToBadBlockManager()
      throws ExecutionException, InterruptedException {
    // set up invalid parent to simulate one of the many conditions that can cause a block
    // validation to fail
    final BlockHeader invalidParentHeader = new BlockHeaderTestFixture().buildHeader();

    blockCreationTask.get();

    coordinator.preparePayload(
        invalidParentHeader,
        System.currentTimeMillis() / 1000,
        Bytes32.ZERO,
        suggestedFeeRecipient,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    verify(badBlockManager, never()).addBadBlock(any(), any());
    assertThat(badBlockManager.getBadBlocks().size()).isEqualTo(0);
  }

  @Test
  public void shouldContinueBuildingBlocksUntilFinalizeIsCalled()
      throws InterruptedException, ExecutionException {
    final AtomicLong retries = new AtomicLong(0);
    doAnswer(
            invocation -> {
              if (retries.getAndIncrement() < 5) {
                // a new transaction every time a block is built
                transactions.addTransaction(
                    createLocalTransaction(retries.get() - 1), Optional.empty());
              } else {
                // when we have 5 transactions finalize block creation
                coordinator.finalizeProposalById(
                    invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    blockCreationTask.get();

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, times(retries.intValue())).putPayloadById(payloadWrapper.capture());

    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);
    assertThat(payloadWrapper.getAllValues().size()).isEqualTo(retries.intValue());
    for (int i = 0; i < retries.intValue(); i++) {
      assertThat(
              payloadWrapper
                  .getAllValues()
                  .get(i)
                  .blockWithReceipts()
                  .getBlock()
                  .getBody()
                  .getTransactions())
          .hasSize(i);
    }
  }

  @Test
  public void blockCreationRepetitionShouldTakeNotLessThanRepetitionMinDuration()
      throws InterruptedException, ExecutionException {
    final AtomicLong retries = new AtomicLong(0);
    final AtomicLong lastPutAt = new AtomicLong();
    final List<Long> repetitionDurations = new ArrayList<>();

    doAnswer(
            invocation -> {
              final long r = retries.getAndIncrement();
              if (r == 0) {
                // ignore first one, that is the empty block
              } else if (r < 5) {
                if (lastPutAt.get() > 0) {
                  // each repetition should take >= REPETITION_MIN_DURATION
                  repetitionDurations.add(System.currentTimeMillis() - lastPutAt.get());
                }
                lastPutAt.set(System.currentTimeMillis());
              } else {
                // finalize after 5 repetitions
                coordinator.finalizeProposalById(
                    invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    blockCreationTask.get();

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, times(retries.intValue())).putPayloadById(payloadWrapper.capture());
    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);

    // check with a tolerance
    assertThat(repetitionDurations)
        .allSatisfy(d -> assertThat(d).isGreaterThanOrEqualTo(REPETITION_MIN_DURATION - 10));
  }

  @Test
  public void shouldRetryBlockCreationOnRecoverableError()
      throws InterruptedException, ExecutionException {
    doAnswer(
            invocation -> {
              if (invocation
                  .getArgument(0, PayloadWrapper.class)
                  .blockWithReceipts()
                  .getBlock()
                  .getBody()
                  .getTransactions()
                  .isEmpty()) {
                // this is called by the first empty block
                doCallRealMethod() // first work
                    .doThrow(new MerkleTrieException("lock")) // second fail
                    .doCallRealMethod() // then work
                    .when(blockchain)
                    .getBlockHeader(any());
              } else {
                // stop block creation loop when we see a not empty block
                coordinator.finalizeProposalById(
                    invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    transactions.addTransaction(createLocalTransaction(0), Optional.empty());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    blockCreationTask.get();

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, times(2)).putPayloadById(payloadWrapper.capture());
    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);

    assertThat(payloadWrapper.getAllValues().size()).isEqualTo(2);
    assertThat(
            payloadWrapper
                .getAllValues()
                .get(0)
                .blockWithReceipts()
                .getBlock()
                .getBody()
                .getTransactions())
        .hasSize(0);
    assertThat(
            payloadWrapper
                .getAllValues()
                .get(1)
                .blockWithReceipts()
                .getBlock()
                .getBody()
                .getTransactions())
        .hasSize(1);
  }

  @Test
  public void shouldStopRetryBlockCreationIfTimeExpired() throws InterruptedException {
    final AtomicLong retries = new AtomicLong(0);
    miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .from(miningConfiguration)
            .unstable(Unstable.builder().posBlockCreationMaxTime(100).build())
            .build();
    doAnswer(
            invocation -> {
              retries.incrementAndGet();
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    try {
      blockCreationTask.get();
      fail("Timeout expected");
    } catch (ExecutionException e) {
      assertThat(e).hasCauseInstanceOf(TimeoutException.class);
    }

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, atLeast(retries.intValue())).putPayloadById(payloadWrapper.capture());
    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);
  }

  @Test
  public void shouldStopInProgressBlockCreationIfFinalizedIsCalled()
      throws InterruptedException, ExecutionException {
    final CountDownLatch waitForBlockCreationInProgress = new CountDownLatch(1);

    doAnswer(
            invocation ->
                // this is called by the first empty block
                doAnswer(
                        i -> {
                          waitForBlockCreationInProgress.countDown();
                          // simulate a long running task
                          try {
                            Thread.sleep(1000);
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }
                          return i.callRealMethod();
                        })
                    .when(blockchain)
                    .getBlockHeader(any()))
        .when(mergeContext)
        .putPayloadById(any());

    var payloadId =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            System.currentTimeMillis() / 1000,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    waitForBlockCreationInProgress.await();
    coordinator.finalizeProposalById(payloadId);

    blockCreationTask.get();

    // check that graceful cancellation completed - may produce 1 or more blocks depending on timing
    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, atLeast(1)).putPayloadById(payloadWrapper.capture());
    // The first payload should be our expected payload ID
    assertThat(payloadWrapper.getAllValues().get(0).payloadIdentifier()).isEqualTo(payloadId);

    assertThat(payloadWrapper.getAllValues().size()).isGreaterThanOrEqualTo(1);
    // The first block should be empty (since cancellation was called during creation)
    assertThat(
            payloadWrapper
                .getAllValues()
                .get(0)
                .blockWithReceipts()
                .getBlock()
                .getBody()
                .getTransactions())
        .hasSize(0);
  }

  @Test
  public void shouldNotStartAnotherBlockCreationJobIfCalledAgainWithTheSamePayloadId()
      throws ExecutionException, InterruptedException {
    final AtomicLong retries = new AtomicLong(0);
    doAnswer(
            invocation -> {
              if (retries.getAndIncrement() < 5) {
                // add a new transaction every time a block is built
                transactions.addTransaction(
                    createLocalTransaction(retries.get() - 1), Optional.empty());
              } else {
                // when we have 5 transactions finalize block creation
                coordinator.finalizeProposalById(
                    invocation.getArgument(0, PayloadWrapper.class).payloadIdentifier());
              }
              return null;
            })
        .when(mergeContext)
        .putPayloadById(any());

    final long timestamp = System.currentTimeMillis() / 1000;

    var payloadId1 =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            timestamp,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    final CompletableFuture<Void> task1 = blockCreationTask;

    var payloadId2 =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            timestamp,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    assertThat(payloadId1).isEqualTo(payloadId2);

    final CompletableFuture<Void> task2 = blockCreationTask;

    assertThat(task1).isSameAs(task2);

    blockCreationTask.get();

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, times(retries.intValue())).putPayloadById(payloadWrapper.capture());

    assertThat(payloadWrapper.getAllValues().size()).isEqualTo(retries.intValue());
    for (int i = 0; i < retries.intValue(); i++) {
      assertThat(
              payloadWrapper
                  .getAllValues()
                  .get(i)
                  .blockWithReceipts()
                  .getBlock()
                  .getBody()
                  .getTransactions())
          .hasSize(i);
      assertThat(payloadWrapper.getAllValues().get(i).blockWithReceipts().getReceipts()).hasSize(i);
    }
  }

  @Test
  public void shouldCancelPreviousBlockCreationJobIfCalledAgainWithNewPayloadId() {

    final long timestamp = System.currentTimeMillis() / 1000;

    var payloadId1 =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            timestamp,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    assertThat(coordinator.isBlockCreationCancelled(payloadId1)).isFalse();

    var payloadId2 =
        coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            timestamp + 1,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    assertThat(payloadId1).isNotEqualTo(payloadId2);
    assertThat(coordinator.isBlockCreationCancelled(payloadId1)).isTrue();
    assertThat(coordinator.isBlockCreationCancelled(payloadId2)).isFalse();
  }

  @Test
  public void shouldUseExtraDataFromMiningParameters() {
    final Bytes extraData = Bytes.fromHexString("0x1234");

    miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(MutableInitValues.builder().extraData(extraData).build())
            .build();

    this.coordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            transactionPool,
            miningConfiguration,
            backwardSyncContext);

    final PayloadIdentifier payloadId =
        this.coordinator.preparePayload(
            genesisState.getBlock().getHeader(),
            1L,
            Bytes32.ZERO,
            suggestedFeeRecipient,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    ArgumentCaptor<PayloadWrapper> payloadWrapper = ArgumentCaptor.forClass(PayloadWrapper.class);

    verify(mergeContext, atLeastOnce()).putPayloadById(payloadWrapper.capture());

    assertThat(payloadWrapper.getValue().payloadIdentifier()).isEqualTo(payloadId);
    assertThat(payloadWrapper.getValue().blockWithReceipts().getHeader().getExtraData())
        .isEqualTo(extraData);
  }

  @Test
  public void childTimestampExceedsParentsFails() {
    BlockHeader terminalHeader = terminalPowBlock();
    sendNewPayloadAndForkchoiceUpdate(
        new Block(terminalHeader, BlockBody.empty()), Optional.empty(), Hash.ZERO);

    BlockHeader parentHeader = nextBlockHeader(terminalHeader);
    Block parent = new Block(parentHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(parent, Optional.empty(), terminalHeader.getHash());

    BlockHeader childHeader = nextBlockHeader(parentHeader, parentHeader.getTimestamp());
    Block child = new Block(childHeader, BlockBody.empty());
    coordinator.rememberBlock(child);

    ForkchoiceResult result =
        coordinator.updateForkChoice(
            childHeader, terminalHeader.getHash(), terminalHeader.getHash());

    assertThat(result.shouldNotProceedToPayloadBuildProcess()).isTrue();
    assertThat(result.getErrorMessage()).isPresent();
    assertThat(result.getErrorMessage().get())
        .isEqualTo("new head timestamp not greater than parent");

    verify(blockchain, never()).setFinalized(childHeader.getHash());
    verify(mergeContext, never()).setFinalized(childHeader);
    verify(blockchain, never()).setSafeBlock(childHeader.getHash());
    verify(mergeContext, never()).setSafeBlock(childHeader);
  }

  @Test
  public void latestValidAncestorDescendsFromFinalizedBlock() {
    BlockHeader terminalHeader = terminalPowBlock();
    sendNewPayloadAndForkchoiceUpdate(
        new Block(terminalHeader, BlockBody.empty()), Optional.empty(), Hash.ZERO);

    BlockHeader grandParentHeader = nextBlockHeader(terminalHeader);
    Block grandParent = new Block(grandParentHeader, BlockBody.empty());

    // if latest valid ancestor is PoW, then latest valid hash should be Hash.ZERO
    var lvh = this.coordinator.getLatestValidAncestor(grandParentHeader);
    assertThat(lvh).isPresent();
    assertThat(lvh.get()).isEqualTo(Hash.ZERO);

    sendNewPayloadAndForkchoiceUpdate(grandParent, Optional.empty(), terminalHeader.getHash());
    BlockHeader parentHeader = nextBlockHeader(grandParentHeader);
    Block parent = new Block(parentHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        parent, Optional.of(grandParentHeader), grandParentHeader.getHash());

    BlockHeader childHeader = nextBlockHeader(parentHeader);
    Block child = new Block(childHeader, BlockBody.empty());
    coordinator.validateBlock(child);

    var nextLvh = this.coordinator.getLatestValidAncestor(childHeader);
    assertThat(nextLvh).isPresent();
    assertThat(nextLvh.get()).isEqualTo(parentHeader.getHash());

    verify(mergeContext, never()).getTerminalPoWBlock();
  }

  @Test
  public void updateForkChoiceShouldPersistFirstFinalizedBlockHash() {
    BlockHeader terminalHeader = terminalPowBlock();
    sendNewPayloadAndForkchoiceUpdate(
        new Block(terminalHeader, BlockBody.empty()), Optional.empty(), Hash.ZERO);

    BlockHeader firstFinalizedHeader = nextBlockHeader(terminalHeader);
    Block firstFinalizedBlock = new Block(firstFinalizedHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        firstFinalizedBlock, Optional.empty(), terminalHeader.getHash());

    BlockHeader headBlockHeader = nextBlockHeader(firstFinalizedHeader);
    Block headBlock = new Block(headBlockHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        headBlock, Optional.of(firstFinalizedHeader), firstFinalizedHeader.getHash());

    verify(blockchain).setFinalized(firstFinalizedBlock.getHash());
    verify(mergeContext).setFinalized(firstFinalizedHeader);
    verify(blockchain).setSafeBlock(firstFinalizedBlock.getHash());
    verify(mergeContext).setSafeBlock(firstFinalizedHeader);
  }

  @Test
  public void reorgAroundLogBloomCacheUpdate() {
    // generate 5 blocks, remember them in order, all need to be parented correctly.
    BlockHeader prevParent = genesisState.getBlock().getHeader();

    AtomicReference<BlockAddedEvent> lastBlockAddedEvent = new AtomicReference<>();
    blockchain.observeBlockAdded(
        new BlockAddedObserver() {
          @Override
          public void onBlockAdded(final BlockAddedEvent event) {
            LOG.info(event.toString());
            lastBlockAddedEvent.set(event);
          }
        });
    for (int i = 0; i <= 5; i++) {
      BlockHeader nextBlock =
          nextBlockHeader(
              prevParent, genesisState.getBlock().getHeader().getTimestamp() + i * 1000);
      coordinator.rememberBlock(new Block(nextBlock, BlockBody.empty()));
      prevParent = nextBlock;
    }

    coordinator.updateForkChoice(
        prevParent, genesisState.getBlock().getHash(), genesisState.getBlock().getHash());
    Hash expectedCommonAncestor = blockchain.getBlockHeader(2).get().getBlockHash();

    // generate from 3' down to some other head. Remember those.
    BlockHeader forkPoint = blockchain.getBlockHeader(2).get();
    prevParent = forkPoint;
    for (int i = 3; i <= 5; i++) {
      BlockHeader nextPrime =
          nextBlockHeader(
              prevParent, genesisState.getBlock().getHeader().getTimestamp() + i * 1001);
      coordinator.rememberBlock(new Block(nextPrime, BlockBody.empty()));
      prevParent = nextPrime;
    }
    coordinator.updateForkChoice(
        prevParent, genesisState.getBlock().getHash(), genesisState.getBlock().getHash());
    assertThat(lastBlockAddedEvent.get().getCommonAncestorHash()).isEqualTo(expectedCommonAncestor);
    assertThat(lastBlockAddedEvent.get().getEventType()).isEqualTo(EventType.CHAIN_REORG);
    assertThat(lastBlockAddedEvent.get().getHeader().getHash())
        .isEqualTo(prevParent.getBlockHash());
  }

  @Test
  public void updateForkChoiceShouldPersistLastFinalizedBlockHash() {
    BlockHeader terminalHeader = terminalPowBlock();
    sendNewPayloadAndForkchoiceUpdate(
        new Block(terminalHeader, BlockBody.empty()), Optional.empty(), Hash.ZERO);

    BlockHeader prevFinalizedHeader = nextBlockHeader(terminalHeader);
    Block prevFinalizedBlock = new Block(prevFinalizedHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        prevFinalizedBlock, Optional.empty(), terminalHeader.getHash());

    BlockHeader lastFinalizedHeader = nextBlockHeader(prevFinalizedHeader);
    Block lastFinalizedBlock = new Block(lastFinalizedHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        lastFinalizedBlock, Optional.of(prevFinalizedHeader), prevFinalizedHeader.getHash());

    BlockHeader headBlockHeader = nextBlockHeader(lastFinalizedHeader);
    Block headBlock = new Block(headBlockHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(
        headBlock, Optional.of(lastFinalizedHeader), lastFinalizedHeader.getHash());

    verify(blockchain).setFinalized(lastFinalizedBlock.getHash());
    verify(mergeContext).setFinalized(lastFinalizedHeader);
    verify(blockchain).setSafeBlock(lastFinalizedBlock.getHash());
    verify(mergeContext).setSafeBlock(lastFinalizedHeader);
  }

  @Test
  public void assertGetOrSyncForBlockAlreadyPresent() {
    BlockHeader mockHeader =
        headerGenerator.parentHash(Hash.fromHexStringLenient("0xdead")).buildHeader();
    when(blockchain.getBlockHeader(mockHeader.getHash())).thenReturn(Optional.of(mockHeader));
    var res = coordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO);

    assertThat(res).isPresent();
  }

  @Test
  public void assertGetOrSyncForBlockNotPresent() {
    BlockHeader mockHeader =
        headerGenerator.parentHash(Hash.fromHexStringLenient("0xbeef")).buildHeader();
    when(backwardSyncContext.syncBackwardsUntil(mockHeader.getBlockHash()))
        .thenReturn(CompletableFuture.completedFuture(null));

    var res = coordinator.getOrSyncHeadByHash(mockHeader.getHash(), Hash.ZERO);

    assertThat(res).isNotPresent();
  }

  @Test
  public void forkchoiceUpdateShouldIgnoreAncestorOfChainHead() {
    BlockHeader terminalHeader = terminalPowBlock();
    sendNewPayloadAndForkchoiceUpdate(
        new Block(terminalHeader, BlockBody.empty()), Optional.empty(), Hash.ZERO);

    BlockHeader parentHeader = nextBlockHeader(terminalHeader);
    Block parent = new Block(parentHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(parent, Optional.empty(), terminalHeader.getHash());

    BlockHeader childHeader = nextBlockHeader(parentHeader);
    Block child = new Block(childHeader, BlockBody.empty());
    sendNewPayloadAndForkchoiceUpdate(child, Optional.empty(), parent.getHash());

    ForkchoiceResult res =
        coordinator.updateForkChoice(parentHeader, Hash.ZERO, terminalHeader.getHash());

    assertThat(res.getStatus()).isEqualTo(ForkchoiceResult.Status.IGNORE_UPDATE_TO_OLD_HEAD);
    assertThat(res.shouldNotProceedToPayloadBuildProcess()).isTrue();
    assertThat(res.getNewHead().isEmpty()).isTrue();
    assertThat(res.getLatestValid().isPresent()).isTrue();
    assertThat(res.getLatestValid().get()).isEqualTo(parentHeader.getHash());
    assertThat(res.getErrorMessage().isEmpty()).isTrue();

    verify(blockchain, never()).rewindToBlock(any());
  }

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("getGasLimits")
  public void shouldSetCorrectTargetGasLimit(final ArgumentsAccessor argumentsAccessor) {
    final long chainId = argumentsAccessor.getLong(1);
    final long expectedTargetGasLimit = argumentsAccessor.getLong(2);

    final MiningConfiguration mockMiningConfiguration = mock(MiningConfiguration.class);
    when(mockMiningConfiguration.getTargetGasLimit()).thenReturn(OptionalLong.empty());
    when(protocolSchedule.getChainId()).thenReturn(Optional.of(BigInteger.valueOf(chainId)));
    doNothing().when(backwardSyncContext).subscribeBadChainListener(any());

    MergeCoordinator testTargetGasLimitCoordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            transactionPool,
            mockMiningConfiguration,
            backwardSyncContext);

    assertThat(testTargetGasLimitCoordinator).isNotNull();
    verify(mockMiningConfiguration).setTargetGasLimit(expectedTargetGasLimit);
  }

  @Test
  public void shouldReturnExpectedTargetGasLimitForMainnet() {
    final long targetGasLimitMainnet =
        MergeCoordinator.getDefaultGasLimitByChainId(Optional.of(CHAIN_ID_MAINNET)).orElseThrow();
    assertThat(targetGasLimitMainnet).isEqualTo(DEFAULT_TARGET_GAS_LIMIT);
  }

  @Test
  public void shouldReturnExpectedTargetGasLimitForTestnet() {
    final long targetGasLimitMainnet =
        MergeCoordinator.getDefaultGasLimitByChainId(Optional.of(CHAIN_ID_HOODI)).orElseThrow();
    assertThat(targetGasLimitMainnet).isEqualTo(DEFAULT_TARGET_GAS_LIMIT_TESTNET);
  }

  public static Stream<Arguments> getGasLimits() {
    return Stream.of(
        Arguments.of("mainnet", 1L, 60_000_000L),
        Arguments.of("holesky", 17_000L, 60_000_000L),
        Arguments.of("sepolia", 11_155_111L, 60_000_000L),
        Arguments.of("hoodi", 560_048L, 60_000_000L),
        Arguments.of("ephemery", 39_438_135L, 60_000_000L));
  }

  private void sendNewPayloadAndForkchoiceUpdate(
      final Block block, final Optional<BlockHeader> finalizedHeader, final Hash safeHash) {

    assertThat(coordinator.rememberBlock(block).getYield()).isPresent();
    assertThat(
            coordinator
                .updateForkChoice(
                    block.getHeader(),
                    finalizedHeader.map(BlockHeader::getHash).orElse(Hash.ZERO),
                    safeHash)
                .shouldNotProceedToPayloadBuildProcess())
        .isFalse();

    when(mergeContext.getFinalized()).thenReturn(finalizedHeader);
  }

  private BlockHeader terminalPowBlock() {
    return headerGenerator
        .difficulty(Difficulty.MAX_VALUE)
        .parentHash(genesisState.getBlock().getHash())
        .number(genesisState.getBlock().getHeader().getNumber() + 1)
        .baseFeePerGas(
            feeMarket.computeBaseFee(
                genesisState.getBlock().getHeader().getNumber() + 1,
                genesisState.getBlock().getHeader().getBaseFee().orElse(Wei.of(0x3b9aca00)),
                0,
                15000000L))
        .timestamp(1)
        .gasLimit(genesisState.getBlock().getHeader().getGasLimit())
        .stateRoot(genesisState.getBlock().getHeader().getStateRoot())
        .buildHeader();
  }

  private BlockHeader nextBlockHeader(
      final BlockHeader parentHeader, final long... optionalTimestamp) {

    return headerGenerator
        .difficulty(Difficulty.ZERO)
        .parentHash(parentHeader.getHash())
        .gasLimit(genesisState.getBlock().getHeader().getGasLimit())
        .number(parentHeader.getNumber() + 1)
        .stateRoot(genesisState.getBlock().getHeader().getStateRoot())
        .timestamp(
            optionalTimestamp.length > 0 ? optionalTimestamp[0] : parentHeader.getTimestamp() + 12)
        .baseFeePerGas(
            feeMarket.computeBaseFee(
                genesisState.getBlock().getHeader().getNumber() + 1,
                parentHeader.getBaseFee().orElse(Wei.of(0x3b9aca00)),
                0,
                15000000L))
        .buildHeader();
  }

  private PendingTransaction createLocalTransaction(final long transactionNumber) {
    return PendingTransaction.newPendingTransaction(
        new TransactionTestFixture()
            .value(Wei.of(transactionNumber + 1))
            .to(Optional.of(Address.ZERO))
            .gasLimit(53000L)
            .gasPrice(
                Wei.fromHexString(
                    "0x00000000000000000000000000000000000000000000000000000013b9aca00"))
            .maxFeePerGas(
                Optional.of(
                    Wei.fromHexString(
                        "0x00000000000000000000000000000000000000000000000000000013b9aca00")))
            .maxPriorityFeePerGas(Optional.of(Wei.of(100_000)))
            .nonce(transactionNumber)
            .createTransaction(KEYS1),
        true,
        true,
        MAX_SCORE);
  }

  private static BlockHeader mockBlockHeader() {
    final BlockHeader blockHeader = mock(BlockHeader.class);
    when(blockHeader.getBaseFee()).thenReturn(Optional.of(Wei.ONE));
    return blockHeader;
  }
}
