/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.consensus.qbft.core.support;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.JsonQbftConfigOptions;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.config.QbftFork;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.consensus.common.BftValidatorOverrides;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.BftRoundExpiryTimeCalculator;
import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.EventMultiplexer;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.SynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.UniqueMessageMulticaster;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftProposerSelector;
import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.common.bft.inttest.DefaultValidatorPeer;
import org.hyperledger.besu.consensus.common.bft.inttest.NetworkLayout;
import org.hyperledger.besu.consensus.common.bft.inttest.NodeParams;
import org.hyperledger.besu.consensus.common.bft.inttest.StubValidatorMulticaster;
import org.hyperledger.besu.consensus.common.bft.inttest.StubbedSynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.inttest.TestTransitions;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.common.validator.blockbased.BlockValidatorProvider;
import org.hyperledger.besu.consensus.qbft.FutureMessageSynchronizerHandler;
import org.hyperledger.besu.consensus.qbft.MutableQbftConfigOptions;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.QbftForksSchedulesFactory;
import org.hyperledger.besu.consensus.qbft.QbftProtocolScheduleBuilder;
import org.hyperledger.besu.consensus.qbft.adaptor.BftEventHandlerAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftBlockCodecAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftBlockCreatorFactoryAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftBlockInterfaceAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftBlockchainAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftFinalStateImpl;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftProtocolScheduleAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftValidatorModeTransitionLoggerAdaptor;
import org.hyperledger.besu.consensus.qbft.adaptor.QbftValidatorProviderAdaptor;
import org.hyperledger.besu.consensus.qbft.blockcreation.QbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftBlockHeightManagerFactory;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftController;
import org.hyperledger.besu.consensus.qbft.core.statemachine.QbftRoundFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.types.QbftEventHandler;
import org.hyperledger.besu.consensus.qbft.core.types.QbftFinalState;
import org.hyperledger.besu.consensus.qbft.core.types.QbftGossiper;
import org.hyperledger.besu.consensus.qbft.core.types.QbftMessage;
import org.hyperledger.besu.consensus.qbft.core.types.QbftMinedBlockObserver;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;
import org.hyperledger.besu.consensus.qbft.network.QbftGossiperImpl;
import org.hyperledger.besu.consensus.qbft.validator.ForkingValidatorProvider;
import org.hyperledger.besu.consensus.qbft.validator.TransactionValidatorProvider;
import org.hyperledger.besu.consensus.qbft.validator.ValidatorContractController;
import org.hyperledger.besu.consensus.qbft.validator.ValidatorModeTransitionLogger;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.ImmutableTransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionBroadcaster;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.sorter.GasPricePendingTransactionsSorter;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.testutil.TestClock;
import org.hyperledger.besu.util.Subscribers;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.apache.tuweni.bytes.Bytes;

public class TestContextBuilder {
  @SuppressWarnings(
      "UnusedVariable") // false positive https://github.com/google/error-prone/issues/2713
  private record ControllerAndState(
      BftExecutors bftExecutors,
      QbftEventHandler eventHandler,
      QbftFinalState finalState,
      EventMultiplexer eventMultiplexer,
      MessageFactory messageFactory,
      ValidatorProvider validatorProvider,
      ProposerSelector proposerSelector) {}

  private static final MetricsSystem metricsSystem = new NoOpMetricsSystem();
  private boolean useValidatorContract;
  private boolean useLondonMilestone = false;
  private boolean useShanghaiMilestone = false;
  private boolean useZeroBaseFee = false;
  private boolean useFixedBaseFee = false;
  public static final int EPOCH_LENGTH = 10_000;
  public static final int BLOCK_TIMER_SEC = 3;
  public static final int ROUND_TIMER_SEC = 12;
  public static final int MESSAGE_QUEUE_LIMIT = 1000;
  public static final int GOSSIPED_HISTORY_LIMIT = 100;
  public static final int DUPLICATE_MESSAGE_LIMIT = 100;
  public static final int FUTURE_MESSAGES_MAX_DISTANCE = 10;
  public static final int FUTURE_MESSAGES_LIMIT = 1000;
  public static final Address VALIDATOR_CONTRACT_ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000008888");
  private static final QbftExtraDataCodec BFT_EXTRA_DATA_ENCODER = new QbftExtraDataCodec();
  private static final BftBlockInterface BFT_BLOCK_INTERFACE =
      new BftBlockInterface(BFT_EXTRA_DATA_ENCODER);

  private Clock clock = Clock.fixed(Instant.MIN, ZoneId.of("UTC"));
  private BftEventQueue bftEventQueue = new BftEventQueue(MESSAGE_QUEUE_LIMIT);
  private int validatorCount = 4;
  private int indexOfFirstLocallyProposedBlock = 0; // Meaning first block is from remote peer.
  private boolean useGossip = false;
  private Optional<String> genesisFile = Optional.empty();
  private List<NodeParams> nodeParams = Collections.emptyList();
  private List<QbftFork> qbftForks = Collections.emptyList();
  private QbftBlockInterface qbftBlockInterface =
      new QbftBlockInterfaceAdaptor(BFT_BLOCK_INTERFACE);

  public TestContextBuilder clock(final Clock clock) {
    this.clock = clock;
    return this;
  }

  public TestContextBuilder eventQueue(final BftEventQueue bftEventQueue) {
    this.bftEventQueue = bftEventQueue;
    return this;
  }

  public TestContextBuilder validatorCount(final int validatorCount) {
    this.validatorCount = validatorCount;
    return this;
  }

  public TestContextBuilder indexOfFirstLocallyProposedBlock(
      final int indexOfFirstLocallyProposedBlock) {
    this.indexOfFirstLocallyProposedBlock = indexOfFirstLocallyProposedBlock;
    return this;
  }

  public TestContextBuilder nodeParams(final List<NodeParams> nodeParams) {
    this.nodeParams = nodeParams;
    return this;
  }

  public TestContextBuilder useGossip(final boolean useGossip) {
    this.useGossip = useGossip;
    return this;
  }

  public TestContextBuilder genesisFile(final String genesisFile) {
    this.genesisFile = Optional.of(genesisFile);
    return this;
  }

  public TestContextBuilder useValidatorContract(final boolean useValidatorContract) {
    this.useValidatorContract = useValidatorContract;
    return this;
  }

  public TestContextBuilder useLondonMilestone(final boolean useLondonMilestone) {
    this.useLondonMilestone = useLondonMilestone;
    return this;
  }

  public TestContextBuilder useShanghaiMilestone(final boolean useShanghaiMilestone) {
    this.useShanghaiMilestone = useShanghaiMilestone;
    return this;
  }

  public TestContextBuilder useZeroBaseFee(final boolean useZeroBaseFee) {
    this.useZeroBaseFee = useZeroBaseFee;
    return this;
  }

  public TestContextBuilder useFixedBaseFee(final boolean useFixedBaseFee) {
    this.useFixedBaseFee = useFixedBaseFee;
    return this;
  }

  public TestContextBuilder qbftForks(final List<QbftFork> qbftForks) {
    this.qbftForks = qbftForks;
    return this;
  }

  public TestContextBuilder qbftBlockInterface(final QbftBlockInterface qbftBlockInterface) {
    this.qbftBlockInterface = qbftBlockInterface;
    return this;
  }

  public TestContext build() {
    final NetworkLayout networkNodes;
    if (nodeParams.isEmpty()) {
      networkNodes =
          NetworkLayout.createNetworkLayout(validatorCount, indexOfFirstLocallyProposedBlock);
    } else {
      final TreeMap<Address, NodeParams> addressKeyMap = new TreeMap<>();
      for (NodeParams params : nodeParams) {
        addressKeyMap.put(params.getAddress(), params);
      }
      final NodeParams localNode =
          Iterables.get(addressKeyMap.values(), indexOfFirstLocallyProposedBlock);
      networkNodes = new NetworkLayout(localNode, addressKeyMap);
    }

    final QbftBlockCodec blockEncoder = new QbftBlockCodecAdaptor(BFT_EXTRA_DATA_ENCODER);
    final MutableBlockchain blockChain;
    final ForestWorldStateArchive worldStateArchive = createInMemoryWorldStateArchive();

    if (genesisFile.isPresent()) {
      try {
        final GenesisState genesisState = createGenesisBlock(genesisFile.get());
        blockChain =
            createInMemoryBlockchain(
                genesisState.getBlock(),
                BftBlockHeaderFunctions.forOnchainBlock(BFT_EXTRA_DATA_ENCODER));
        genesisState.writeStateTo(worldStateArchive.getWorldState());
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    } else {
      final Block genesisBlock = createGenesisBlock(networkNodes.getValidatorAddresses());
      blockChain =
          createInMemoryBlockchain(
              genesisBlock, BftBlockHeaderFunctions.forOnchainBlock(BFT_EXTRA_DATA_ENCODER));
    }

    // Use a stubbed version of the multicaster, to prevent creating PeerConnections etc.
    final StubValidatorMulticaster multicaster = new StubValidatorMulticaster();
    final UniqueMessageMulticaster uniqueMulticaster =
        new UniqueMessageMulticaster(multicaster, GOSSIPED_HISTORY_LIMIT);

    final QbftGossiper gossiper =
        useGossip
            ? new QbftGossiperImpl(uniqueMulticaster, blockEncoder)
            : mock(QbftGossiper.class);

    final StubbedSynchronizerUpdater synchronizerUpdater = new StubbedSynchronizerUpdater();

    final ControllerAndState controllerAndState =
        createControllerAndFinalState(
            blockChain,
            worldStateArchive,
            multicaster,
            networkNodes.getLocalNode().getNodeKey(),
            clock,
            bftEventQueue,
            gossiper,
            synchronizerUpdater,
            useValidatorContract,
            useLondonMilestone,
            useShanghaiMilestone,
            useZeroBaseFee,
            useFixedBaseFee,
            qbftForks,
            blockEncoder,
            qbftBlockInterface);

    // Add each networkNode to the Multicaster (such that each can receive msgs from local node).
    // NOTE: the remotePeers needs to be ordered based on Address (as this is used to determine
    // the proposer order which must be managed in test).
    final Map<Address, ValidatorPeer> remotePeers =
        networkNodes.getRemotePeers().stream()
            .collect(
                Collectors.toMap(
                    NodeParams::getAddress,
                    nodeParams ->
                        new ValidatorPeer(
                            nodeParams,
                            new MessageFactory(nodeParams.getNodeKey(), blockEncoder),
                            controllerAndState.eventMultiplexer()),
                    (u, v) -> {
                      throw new IllegalStateException(String.format("Duplicate key %s", u));
                    },
                    LinkedHashMap::new));

    final List<DefaultValidatorPeer> peerCollection = new ArrayList<>(remotePeers.values());
    multicaster.addNetworkPeers(peerCollection);
    synchronizerUpdater.addNetworkPeers(peerCollection);

    return new TestContext(
        remotePeers,
        blockChain,
        controllerAndState.bftExecutors(),
        controllerAndState.eventHandler(),
        controllerAndState.finalState(),
        controllerAndState.eventMultiplexer(),
        controllerAndState.messageFactory(),
        controllerAndState.validatorProvider(),
        controllerAndState.proposerSelector(),
        BFT_EXTRA_DATA_ENCODER,
        blockEncoder);
  }

  public TestContext buildAndStart() {
    TestContext testContext = build();
    testContext.start();
    return testContext;
  }

  private static Block createGenesisBlock(final Set<Address> validators) {
    final Address coinbase = Iterables.get(validators, 0);
    final BlockHeaderTestFixture headerTestFixture = new BlockHeaderTestFixture();
    final BftExtraData extraData =
        new BftExtraData(
            Bytes.wrap(new byte[32]), Collections.emptyList(), Optional.empty(), 0, validators);
    headerTestFixture.extraData(BFT_EXTRA_DATA_ENCODER.encode(extraData));
    headerTestFixture.mixHash(BftHelpers.EXPECTED_MIX_HASH);
    headerTestFixture.difficulty(Difficulty.ONE);
    headerTestFixture.ommersHash(Hash.EMPTY_LIST_HASH);
    headerTestFixture.nonce(0);
    headerTestFixture.timestamp(0);
    headerTestFixture.parentHash(Hash.ZERO);
    headerTestFixture.gasLimit(5000);
    headerTestFixture.coinbase(coinbase);

    final BlockHeader genesisHeader = headerTestFixture.buildHeader();
    return new Block(
        genesisHeader, new BlockBody(Collections.emptyList(), Collections.emptyList()));
  }

  private GenesisState createGenesisBlock(final String genesisFile) throws IOException {
    return GenesisState.fromConfig(
        GenesisConfig.fromSource(Path.of(genesisFile).toUri().toURL()),
        ProtocolScheduleFixture.TESTING_NETWORK,
        new CodeCache());
  }

  private static ControllerAndState createControllerAndFinalState(
      final MutableBlockchain blockChain,
      final WorldStateArchive worldStateArchive,
      final StubValidatorMulticaster multicaster,
      final NodeKey nodeKey,
      final Clock clock,
      final BftEventQueue bftEventQueue,
      final QbftGossiper gossiper,
      final SynchronizerUpdater synchronizerUpdater,
      final boolean useValidatorContract,
      final boolean useLondonMilestone,
      final boolean useShanghaiMilestone,
      final boolean useZeroBaseFee,
      final boolean useFixedBaseFee,
      final List<QbftFork> qbftForks,
      final QbftBlockCodec blockEncoder,
      final QbftBlockInterface qbftBlockInterface) {

    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(
                MutableInitValues.builder()
                    .isMiningEnabled(true)
                    .minTransactionGasPrice(Wei.ZERO)
                    .extraData(Bytes.wrap("Qbft Int tests".getBytes(UTF_8)))
                    .coinbase(AddressHelpers.ofValue(1))
                    .build())
            .build();

    final StubGenesisConfigOptions genesisConfigOptions = new StubGenesisConfigOptions();
    final Map<String, Object> qbftConfigValues =
        useValidatorContract
            ? Map.of(
                JsonQbftConfigOptions.VALIDATOR_CONTRACT_ADDRESS,
                VALIDATOR_CONTRACT_ADDRESS.getBytes().toHexString())
            : Collections.emptyMap();
    final QbftConfigOptions qbftConfigOptions = createGenesisConfig(useValidatorContract);

    if (useLondonMilestone) {
      genesisConfigOptions.londonBlock(0);
    } else if (useShanghaiMilestone) {
      genesisConfigOptions.shanghaiTime(10);
    } else {
      genesisConfigOptions.berlinBlock(0);
    }
    if (useZeroBaseFee) {
      genesisConfigOptions.zeroBaseFee(true);
    }
    if (useFixedBaseFee) {
      genesisConfigOptions.fixedBaseFee(true);
    }
    genesisConfigOptions.qbftConfigOptions(
        new JsonQbftConfigOptions(JsonUtil.objectNodeFromMap(qbftConfigValues)));
    genesisConfigOptions.transitions(TestTransitions.createQbftTestTransitions(qbftForks));
    genesisConfigOptions.qbftConfigOptions(qbftConfigOptions);

    final EpochManager epochManager = new EpochManager(EPOCH_LENGTH);

    final ForksSchedule<QbftConfigOptions> forksSchedule =
        QbftForksSchedulesFactory.create(genesisConfigOptions);

    final BftProtocolSchedule protocolSchedule =
        QbftProtocolScheduleBuilder.create(
            genesisConfigOptions,
            forksSchedule,
            BFT_EXTRA_DATA_ENCODER,
            EvmConfiguration.DEFAULT,
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());

    final BftValidatorOverrides validatorOverrides = convertBftForks(qbftForks);
    final TransactionSimulator transactionSimulator =
        new TransactionSimulator(
            blockChain, worldStateArchive, protocolSchedule, miningConfiguration, 0L);

    final BlockValidatorProvider blockValidatorProvider =
        BlockValidatorProvider.forkingValidatorProvider(
            blockChain, epochManager, BFT_BLOCK_INTERFACE, validatorOverrides);
    final TransactionValidatorProvider transactionValidatorProvider =
        new TransactionValidatorProvider(
            blockChain, new ValidatorContractController(transactionSimulator), forksSchedule);
    final ValidatorProvider validatorProvider =
        new ForkingValidatorProvider(
            blockChain, forksSchedule, blockValidatorProvider, transactionValidatorProvider);
    final QbftValidatorProvider qbftValidatorProvider =
        new QbftValidatorProviderAdaptor(validatorProvider);

    final ProtocolContext bftProtocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockChain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(
                new BftContext(validatorProvider, epochManager, BFT_BLOCK_INTERFACE))
            .build();

    final TransactionPoolConfiguration poolConf =
        ImmutableTransactionPoolConfiguration.builder().txPoolMaxSize(1).build();

    final GasPricePendingTransactionsSorter pendingTransactions =
        new GasPricePendingTransactionsSorter(
            poolConf, clock, metricsSystem, blockChain::getChainHeadHeader);

    final EthContext ethContext = mock(EthContext.class, RETURNS_DEEP_STUBS);
    when(ethContext.getEthPeers().subscribeConnect(any())).thenReturn(1L);

    final TransactionPool transactionPool =
        new TransactionPool(
            () -> pendingTransactions,
            protocolSchedule,
            bftProtocolContext,
            mock(TransactionBroadcaster.class),
            ethContext,
            new TransactionPoolMetrics(metricsSystem),
            poolConf,
            new BlobCache());

    transactionPool.setEnabled();

    final EthScheduler ethScheduler = new DeterministicEthScheduler();

    final Address localAddress = Util.publicKeyToAddress(nodeKey.getPublicKey());
    final QbftBlockCreatorFactory blockCreatorFactory =
        new QbftBlockCreatorFactory(
            transactionPool, // changed from QbftBesuController
            bftProtocolContext,
            protocolSchedule,
            forksSchedule,
            miningConfiguration,
            localAddress,
            BFT_EXTRA_DATA_ENCODER,
            ethScheduler);

    final ProposerSelector proposerSelector =
        new BftProposerSelector(blockChain, BFT_BLOCK_INTERFACE, true, validatorProvider);

    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);
    final QbftFinalState finalState =
        new QbftFinalStateImpl(
            validatorProvider,
            nodeKey,
            Util.publicKeyToAddress(nodeKey.getPublicKey()),
            proposerSelector,
            multicaster,
            new RoundTimer(
                bftEventQueue,
                new BftRoundExpiryTimeCalculator(Duration.ofSeconds(ROUND_TIMER_SEC)),
                bftExecutors),
            new BlockTimer(bftEventQueue, forksSchedule, bftExecutors, TestClock.fixed()),
            new QbftBlockCreatorFactoryAdaptor(blockCreatorFactory, BFT_EXTRA_DATA_ENCODER),
            clock);

    final MessageFactory messageFactory = new MessageFactory(nodeKey, blockEncoder);

    final QbftProtocolScheduleAdaptor qbftProtocolSchedule =
        new QbftProtocolScheduleAdaptor(protocolSchedule, bftProtocolContext);
    final MessageValidatorFactory messageValidatorFactory =
        new MessageValidatorFactory(
            proposerSelector, qbftProtocolSchedule, qbftValidatorProvider, qbftBlockInterface);

    final Subscribers<QbftMinedBlockObserver> minedBlockObservers = Subscribers.create();

    final MessageTracker duplicateMessageTracker = new MessageTracker(DUPLICATE_MESSAGE_LIMIT);
    final FutureMessageBuffer<QbftMessage> futureMessageBuffer =
        new FutureMessageBuffer<>(
            FUTURE_MESSAGES_MAX_DISTANCE,
            FUTURE_MESSAGES_LIMIT,
            blockChain.getChainHeadBlockNumber(),
            new FutureMessageSynchronizerHandler(synchronizerUpdater));
    final QbftValidatorModeTransitionLoggerAdaptor validatorModeTransitionLogger =
        new QbftValidatorModeTransitionLoggerAdaptor(
            new ValidatorModeTransitionLogger(forksSchedule));

    final QbftController qbftController =
        new QbftController(
            new QbftBlockchainAdaptor(blockChain),
            finalState,
            new QbftBlockHeightManagerFactory(
                finalState,
                new QbftRoundFactory(
                    finalState,
                    qbftBlockInterface,
                    qbftProtocolSchedule,
                    minedBlockObservers,
                    messageValidatorFactory,
                    messageFactory),
                messageValidatorFactory,
                messageFactory,
                qbftValidatorProvider,
                validatorModeTransitionLogger),
            gossiper,
            duplicateMessageTracker,
            futureMessageBuffer,
            blockEncoder);

    final EventMultiplexer eventMultiplexer =
        new EventMultiplexer(new BftEventHandlerAdaptor(qbftController));
    //////////////////////////// END QBFT BesuController ////////////////////////////

    return new ControllerAndState(
        bftExecutors,
        qbftController,
        finalState,
        eventMultiplexer,
        messageFactory,
        validatorProvider,
        proposerSelector);
  }

  private static QbftConfigOptions createGenesisConfig(final boolean useValidatorContract) {
    final MutableQbftConfigOptions qbftConfigOptions =
        new MutableQbftConfigOptions(JsonQbftConfigOptions.DEFAULT);
    qbftConfigOptions.setBlockPeriodSeconds(BLOCK_TIMER_SEC);
    if (useValidatorContract) {
      qbftConfigOptions.setValidatorContractAddress(
          Optional.of(VALIDATOR_CONTRACT_ADDRESS.getBytes().toHexString()));
    }
    return qbftConfigOptions;
  }

  private static BftValidatorOverrides convertBftForks(final List<QbftFork> bftForks) {
    final Map<Long, List<Address>> result = new HashMap<>();

    for (final BftFork fork : bftForks) {
      fork.getValidators()
          .ifPresent(
              validators ->
                  result.put(
                      fork.getForkBlock(),
                      validators.stream()
                          .map(Address::fromHexString)
                          .collect(Collectors.toList())));
    }

    return new BftValidatorOverrides(result);
  }
}
