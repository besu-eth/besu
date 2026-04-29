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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.merge.MergeProtocolSchedule;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestUtil;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTaskExecutorAnswer;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardChain;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContextTest;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.BaseFeePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.TrieLogLayerBudgetExceededException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.number.Fraction;

import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Integration coverage for {@link MergeCoordinator#updateForkChoice} with a real {@link
 * BackwardSyncContext}, {@link EthProtocolManager}, and {@link PeerTaskExecutor} answers (mirrored
 * remote chain), plus the trie-log budget path returning {@link ForkchoiceResult.Status#SYNCING}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MergeCoordinatorForkchoiceBackwardSyncIntegrationTest
    implements MergeGenesisConfigHelper {

  /** Large budget so trie-log rolls during chain construction stay well under the limit. */
  private static final long BONSAI_TRIE_LOG_LAYERS_FOR_TESTS = 512L;

  private static final int COMMON_POST_TERMINAL_BLOCKS = 5;
  private static final int SHORT_BRANCH_BLOCKS = 2;
  private static final int LONG_BRANCH_BLOCKS = 12;

  @Mock private PeerTaskExecutor peerTaskExecutor;
  @Mock private SyncState syncState;

  private final PostMergeContext mergeContext = new PostMergeContext();
  private ProtocolSchedule protocolSchedule;
  private GenesisState genesisState;
  private final BlockHeaderTestFixture headerGenerator = new BlockHeaderTestFixture();
  private BaseFeeMarket feeMarket;
  private final Address coinbase =
      genesisAllocations(getPosGenesisConfig()).findFirst().orElseThrow();
  private final MiningConfiguration miningConfiguration =
      ImmutableMiningConfiguration.builder()
          .mutableInitValues(MutableInitValues.builder().coinbase(coinbase).build())
          .build();

  private MutableBlockchain blockchain;
  private MutableBlockchain remoteBlockchain;
  private WorldStateArchive worldStateArchive;
  private WorldStateArchive remoteWorldStateArchive;
  private ProtocolContext protocolContext;
  private ProtocolContext remoteProtocolContext;
  private BadBlockManager badBlockManager;
  private EthScheduler ethScheduler;

  @BeforeEach
  void setUp() {
    MergeConfiguration.setMergeEnabled(false);
    badBlockManager = new BadBlockManager();
    protocolSchedule =
        MergeProtocolSchedule.create(
            getPosGenesisConfig().getConfigOptions(),
            false,
            MiningConfiguration.MINING_DISABLED,
            badBlockManager,
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem(),
            EvmConfiguration.DEFAULT);
    genesisState =
        GenesisState.fromConfig(getPosGenesisConfig(), protocolSchedule, new CodeCache());
    feeMarket = FeeMarket.london(0, genesisState.getBlock().getHeader().getBaseFee());
    MergeConfiguration.setMergeEnabled(true);

    blockchain = createInMemoryBlockchain(genesisState.getBlock());
    remoteBlockchain = createInMemoryBlockchain(genesisState.getBlock());
    final PathBasedExtraStorageConfiguration pathConfig =
        ImmutablePathBasedExtraStorageConfiguration.builder()
            .from(PathBasedExtraStorageConfiguration.DEFAULT)
            .maxLayersToLoad(BONSAI_TRIE_LOG_LAYERS_FOR_TESTS)
            .build();
    worldStateArchive =
        createBonsaiInMemoryWorldStateArchive(
            blockchain, EvmConfiguration.DEFAULT, null, pathConfig);
    remoteWorldStateArchive =
        createBonsaiInMemoryWorldStateArchive(
            remoteBlockchain, EvmConfiguration.DEFAULT, null, pathConfig);

    mergeContext.setTerminalTotalDifficulty(
        Difficulty.of(
            getPosGenesisConfig()
                .getConfigOptions()
                .getTerminalTotalDifficulty()
                .orElseThrow()
                .toBigInteger()
                .longValueExact()));
    mergeContext.setSyncState(syncState);
    when(syncState.hasReachedTerminalDifficulty()).thenReturn(Optional.of(Boolean.TRUE));
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    when(syncState.isInSync()).thenReturn(true);

    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(badBlockManager)
            .build();

    final var localMutable = worldStateArchive.getWorldState();
    genesisState.writeStateTo(localMutable);
    localMutable.persist(genesisState.getBlock().getHeader());

    remoteProtocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(remoteBlockchain)
            .withWorldStateArchive(remoteWorldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(badBlockManager)
            .build();
    final var remoteMutable = remoteWorldStateArchive.getWorldState();
    genesisState.writeStateTo(remoteMutable);
    remoteMutable.persist(genesisState.getBlock().getHeader());

    blockchain.observeBlockAdded(
        event ->
            blockchain
                .getTotalDifficultyByHash(event.getHeader().getHash())
                .ifPresent(mergeContext::setIsPostMerge));
    remoteBlockchain.observeBlockAdded(
        event ->
            remoteBlockchain
                .getTotalDifficultyByHash(event.getHeader().getHash())
                .ifPresent(mergeContext::setIsPostMerge));

    ethScheduler = new EthScheduler(2, 2, 2, new NoOpMetricsSystem());
  }

  @AfterEach
  void tearDown() {
    MergeConfiguration.setMergeEnabled(false);
  }

  @Test
  void deepForkReorgCompletes() throws Exception {
    final MergeCoordinator coordinator =
        newMergeCoordinatorWithBackwardSync(BackwardSyncContextTest.inMemoryBackwardChain());

    final Block firstPos = new Block(firstPostGenesisHeader(), BlockBody.empty());
    importAndForkchoiceToHead(coordinator, firstPos);
    mirrorBlockToRemote(firstPos);

    for (int i = 0; i < COMMON_POST_TERMINAL_BLOCKS; i++) {
      final Block b = nextEmptyBlock(blockchain.getChainHeadHeader());
      importAndForkchoiceToHead(coordinator, b);
      mirrorBlockToRemote(b);
    }
    final Hash forkParentHash = blockchain.getChainHeadHash();

    Block shortTip = null;
    for (int i = 0; i < SHORT_BRANCH_BLOCKS; i++) {
      final Block b = nextEmptyBlock(blockchain.getChainHeadHeader());
      importAndForkchoiceToHead(coordinator, b);
      mirrorBlockToRemote(b);
      shortTip = b;
    }

    BlockHeader longBranchParent = blockchain.getBlockHeader(forkParentHash).orElseThrow();
    Block longTip = null;
    for (int i = 0; i < LONG_BRANCH_BLOCKS; i++) {
      final Block b = nextEmptyBlock(longBranchParent);
      rememberOnly(coordinator, b);
      mirrorBlockToRemote(b);
      longTip = b;
      longBranchParent = b.getHeader();
    }

    assertThat(shortTip).isNotNull();
    assertThat(longTip).isNotNull();
    assertThat(blockchain.getChainHeadHash()).isEqualTo(shortTip.getHash());

    final BlockHeader longOnChain =
        protocolContext.getBlockchain().getBlockByHash(longTip.getHash()).orElseThrow().getHeader();
    final ForkchoiceResult res = coordinator.updateForkChoice(longOnChain, Hash.ZERO, Hash.ZERO);
    assertThat(res.getStatus()).isEqualTo(ForkchoiceResult.Status.VALID);
    assertThat(blockchain.getChainHeadHash()).isEqualTo(longTip.getHash());
  }

  @Test
  void forkchoiceReturnsSyncingAndStartsBackwardSyncWhenTrieLogRollBudgetExceeded() {
    final BonsaiWorldStateProvider spyArchive = spy((BonsaiWorldStateProvider) worldStateArchive);
    final AtomicReference<Hash> trieBudgetExceededWhenRollingTo = new AtomicReference<>();
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(spyArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(badBlockManager)
            .build();
    final var localMutable = spyArchive.getWorldState();
    genesisState.writeStateTo(localMutable);
    localMutable.persist(genesisState.getBlock().getHeader());

    doAnswer(
            invocation -> {
              final WorldStateQueryParams params = invocation.getArgument(0);
              final Hash failAt = trieBudgetExceededWhenRollingTo.get();
              if (params.shouldEnforceTrieRollLayerBudget()
                  && params.shouldWorldStateUpdateHead()
                  && failAt != null
                  && failAt.equals(params.getBlockHash())) {
                throw new TrieLogLayerBudgetExceededException(100, 4);
              }
              return invocation.callRealMethod();
            })
        .when(spyArchive)
        .getWorldState(any(WorldStateQueryParams.class));

    final BackwardChain backwardChain = BackwardSyncContextTest.inMemoryBackwardChain();
    final EthContext ethContext = createEthContextAndWirePeers();
    final BackwardSyncContext backwardSyncContext =
        spy(
            new BackwardSyncContext(
                protocolContext,
                protocolSchedule,
                SynchronizerConfiguration.builder().build(),
                new NoOpMetricsSystem(),
                ethContext,
                syncState,
                backwardChain,
                1,
                25));
    final MergeCoordinator coordinator = newMergeCoordinator(ethContext, backwardSyncContext);

    final Block firstPos = new Block(firstPostGenesisHeader(), BlockBody.empty());
    importAndForkchoiceToHead(coordinator, firstPos);

    final Block b1 = nextEmptyBlock(blockchain.getChainHeadHeader());
    importAndForkchoiceToHead(coordinator, b1);

    final Block b2 = nextEmptyBlock(blockchain.getChainHeadHeader());
    trieBudgetExceededWhenRollingTo.set(b2.getHash());
    rememberOnly(coordinator, b2);

    final BlockHeader b2OnChain =
        protocolContext.getBlockchain().getBlockByHash(b2.getHash()).orElseThrow().getHeader();
    final ForkchoiceResult syncing = coordinator.updateForkChoice(b2OnChain, Hash.ZERO, Hash.ZERO);
    assertThat(syncing.getStatus()).isEqualTo(ForkchoiceResult.Status.SYNCING);
    verify(backwardSyncContext)
        .syncBackwardsUntil(blockchain.getBlockByHash(b2.getHash()).orElseThrow());
  }

  private EthContext createEthContextAndWirePeers() {
    final EthProtocolManager ethProtocolManager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(protocolSchedule)
            .setBlockchain(blockchain)
            .setPeerTaskExecutor(peerTaskExecutor)
            .setEthScheduler(ethScheduler)
            .build();
    EthProtocolManagerTestUtil.createPeer(ethProtocolManager);
    final EthContext ethContext = ethProtocolManager.ethContext();

    when(peerTaskExecutor.execute(any(GetHeadersFromPeerTask.class)))
        .thenAnswer(
            new GetHeadersFromPeerTaskExecutorAnswer(remoteBlockchain, ethContext.getEthPeers()));
    when(peerTaskExecutor.execute(any(GetBodiesFromPeerTask.class)))
        .thenAnswer(
            new GetBodiesFromPeerTaskExecutorAnswer(remoteBlockchain, ethContext.getEthPeers()));
    return ethContext;
  }

  private MergeCoordinator newMergeCoordinatorWithBackwardSync(final BackwardChain backwardChain) {
    final EthContext ethContext = createEthContextAndWirePeers();
    final BackwardSyncContext backwardSyncContext =
        new BackwardSyncContext(
            protocolContext,
            protocolSchedule,
            SynchronizerConfiguration.builder().build(),
            new NoOpMetricsSystem(),
            ethContext,
            syncState,
            backwardChain,
            1,
            25);
    return newMergeCoordinator(ethContext, backwardSyncContext);
  }

  private MergeCoordinator newMergeCoordinator(
      final EthContext ethContext, final BackwardSyncContext backwardSyncContext) {
    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder()
            .txPoolMaxSize(10)
            .txPoolLimitByAccountPercentage(Fraction.fromPercentage(100))
            .build();
    final var metrics = new StubMetricsSystem();
    final var transactions =
        new BaseFeePendingTransactionsSorter(
            poolConf,
            TestClock.system(ZoneId.systemDefault()),
            metrics,
            MergeCoordinatorForkchoiceBackwardSyncIntegrationTest::minimalHeader);
    final TransactionPool transactionPool =
        new TransactionPool(
            () -> transactions,
            protocolSchedule,
            protocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metrics),
            poolConf,
            new BlobCache());
    transactionPool.setEnabled();

    return new MergeCoordinator(
        protocolContext,
        protocolSchedule,
        ethScheduler,
        transactionPool,
        miningConfiguration,
        backwardSyncContext);
  }

  private static BlockHeader minimalHeader() {
    final var h = mock(BlockHeader.class);
    when(h.getBaseFee()).thenReturn(Optional.of(org.hyperledger.besu.datatypes.Wei.ONE));
    return h;
  }

  private void mirrorBlockToRemote(final Block block) {
    final BlockProcessingResult result =
        protocolSchedule
            .getByBlockHeader(block.getHeader())
            .getBlockValidator()
            .validateAndProcessBlock(
                remoteProtocolContext, block, HeaderValidationMode.FULL, HeaderValidationMode.NONE);
    assertThat(result.isSuccessful()).withFailMessage(() -> result.toString()).isTrue();
    result
        .getYield()
        .ifPresent(
            y -> remoteBlockchain.appendBlock(block, y.getReceipts(), y.getBlockAccessList()));
  }

  /**
   * With {@code posAtGenesis.json} the TTD is already satisfied at genesis; the first child must be
   * a PoS header (difficulty zero), not a synthetic terminal PoW block.
   */
  private BlockHeader firstPostGenesisHeader() {
    final BlockHeader genesisHeader = genesisState.getBlock().getHeader();
    return headerGenerator
        .difficulty(Difficulty.ZERO)
        .parentHash(genesisHeader.getHash())
        .number(genesisHeader.getNumber() + 1)
        .baseFeePerGas(
            feeMarket.computeBaseFee(
                genesisHeader.getNumber() + 1,
                genesisHeader.getBaseFee().orElseThrow(),
                0,
                15_000_000L))
        .timestamp(genesisHeader.getTimestamp() + 12)
        .gasLimit(genesisHeader.getGasLimit())
        .stateRoot(genesisHeader.getStateRoot())
        .buildHeader();
  }

  private Block nextEmptyBlock(final BlockHeader parent) {
    final BlockHeader h =
        headerGenerator
            .difficulty(Difficulty.ZERO)
            .parentHash(parent.getHash())
            .number(parent.getNumber() + 1)
            .gasLimit(parent.getGasLimit())
            .stateRoot(parent.getStateRoot())
            .timestamp(parent.getTimestamp() + 12)
            .baseFeePerGas(
                feeMarket.computeBaseFee(
                    parent.getNumber() + 1, parent.getBaseFee().orElseThrow(), 0, 15_000_000L))
            .buildHeader();
    return new Block(h, BlockBody.empty());
  }

  private void importAndForkchoiceToHead(final MergeCoordinator coordinator, final Block block) {
    rememberOnly(coordinator, block);
    final Block stored =
        protocolContext.getBlockchain().getBlockByHash(block.getHash()).orElseThrow();
    final ForkchoiceResult res =
        coordinator.updateForkChoice(stored.getHeader(), Hash.ZERO, Hash.ZERO);
    assertThat(res.getStatus()).isEqualTo(ForkchoiceResult.Status.VALID);
  }

  private void rememberOnly(final MergeCoordinator coordinator, final Block block) {
    final BlockProcessingResult result = coordinator.rememberBlock(block);
    assertThat(result.isSuccessful()).withFailMessage(() -> result.toString()).isTrue();
  }
}
