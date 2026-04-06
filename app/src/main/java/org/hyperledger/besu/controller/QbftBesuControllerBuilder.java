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
package org.hyperledger.besu.controller;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.config.BftConfigOptions;
import org.hyperledger.besu.config.BftFork;
import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.config.QbftFork;
import org.hyperledger.besu.consensus.common.BftValidatorOverrides;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.ForksSchedule;
import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProcessor;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.BftRoundExpiryTimeCalculator;
import org.hyperledger.besu.consensus.common.bft.BlockTimer;
import org.hyperledger.besu.consensus.common.bft.EthSynchronizerUpdater;
import org.hyperledger.besu.consensus.common.bft.EventMultiplexer;
import org.hyperledger.besu.consensus.common.bft.MessageTracker;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.UniqueMessageMulticaster;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftMiningCoordinator;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftProposerSelector;
import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.common.bft.network.ValidatorPeers;
import org.hyperledger.besu.consensus.common.bft.protocol.BftProtocolManager;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.consensus.common.bft.statemachine.FutureMessageBuffer;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.consensus.common.validator.blockbased.BlockValidatorProvider;
import org.hyperledger.besu.consensus.qbft.FutureMessageSynchronizerHandler;
import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.QbftForksSchedulesFactory;
import org.hyperledger.besu.consensus.qbft.QbftProtocolScheduleBuilder;
import org.hyperledger.besu.consensus.qbft.adaptor.AdaptorUtil;
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
import org.hyperledger.besu.consensus.qbft.core.types.QbftMessage;
import org.hyperledger.besu.consensus.qbft.core.types.QbftMinedBlockObserver;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.consensus.qbft.core.types.QbftValidatorProvider;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidatorFactory;
import org.hyperledger.besu.consensus.qbft.jsonrpc.QbftJsonRpcMethods;
import org.hyperledger.besu.consensus.qbft.network.QbftGossiperImpl;
import org.hyperledger.besu.consensus.qbft.protocol.Istanbul100SubProtocol;
import org.hyperledger.besu.consensus.qbft.validator.ForkingValidatorProvider;
import org.hyperledger.besu.consensus.qbft.validator.TransactionValidatorProvider;
import org.hyperledger.besu.consensus.qbft.validator.ValidatorContractController;
import org.hyperledger.besu.consensus.qbft.validator.ValidatorModeTransitionLogger;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethods;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapProtocolManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.config.SubProtocolConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.util.Subscribers;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft Besu controller builder. */
public class QbftBesuControllerBuilder extends BesuControllerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(QbftBesuControllerBuilder.class);
  private BftEventQueue bftEventQueue;
  private QbftConfigOptions qbftConfig;
  private ForksSchedule<QbftConfigOptions> qbftForksSchedule;
  private ValidatorPeers peers;
  private TransactionValidatorProvider transactionValidatorProvider;
  private BftConfigOptions bftConfigOptions;
  private QbftExtraDataCodec qbftExtraDataCodec;
  private BftBlockInterface bftBlockInterface;

  /** Default Constructor. */
  public QbftBesuControllerBuilder() {}

  @Override
  protected void prepForBuild() {
    qbftConfig = genesisConfigOptions.getQbftConfigOptions();
    bftEventQueue = new BftEventQueue(qbftConfig.getMessageQueueLimit());
    qbftForksSchedule = QbftForksSchedulesFactory.create(genesisConfigOptions);
    bftConfigOptions = qbftConfig;
    qbftExtraDataCodec = new QbftExtraDataCodec();
    bftBlockInterface = new BftBlockInterface(qbftExtraDataCodec);
  }

  @Override
  protected JsonRpcMethods createAdditionalJsonRpcMethodFactory(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final MiningConfiguration miningConfiguration) {

    return new QbftJsonRpcMethods(
        protocolContext,
        protocolSchedule,
        miningConfiguration,
        createReadOnlyValidatorProvider(protocolContext.getBlockchain()),
        bftConfigOptions);
  }

  private ValidatorProvider createReadOnlyValidatorProvider(final Blockchain blockchain) {
    checkNotNull(
        transactionValidatorProvider, "transactionValidatorProvider should have been initialised");
    final long startBlock =
        qbftConfig.getStartBlock().isPresent() ? qbftConfig.getStartBlock().getAsLong() : 0;
    final EpochManager epochManager = new EpochManager(qbftConfig.getEpochLength(), startBlock);
    // Must create our own voteTallyCache as using this would pollute the main voteTallyCache
    final BlockValidatorProvider readOnlyBlockValidatorProvider =
        BlockValidatorProvider.nonForkingValidatorProvider(
            blockchain, epochManager, bftBlockInterface);
    return new ForkingValidatorProvider(
        blockchain,
        qbftForksSchedule,
        readOnlyBlockValidatorProvider,
        transactionValidatorProvider);
  }

  @Override
  protected SubProtocolConfiguration createSubProtocolConfiguration(
      final EthProtocolManager ethProtocolManager,
      final Optional<SnapProtocolManager> maybeSnapProtocolManager) {
    final SubProtocolConfiguration subProtocolConfiguration =
        new SubProtocolConfiguration()
            .withSubProtocol(EthProtocol.get(), ethProtocolManager)
            .withSubProtocol(
                Istanbul100SubProtocol.get(),
                new BftProtocolManager(
                    bftEventQueue,
                    peers,
                    Istanbul100SubProtocol.ISTANBUL_100,
                    Istanbul100SubProtocol.get().getName()));
    maybeSnapProtocolManager.ifPresent(
        snapProtocolManager ->
            subProtocolConfiguration.withSubProtocol(SnapProtocol.get(), snapProtocolManager));
    return subProtocolConfiguration;
  }

  @Override
  protected MiningCoordinator createMiningCoordinator(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager) {
    final MutableBlockchain blockchain = protocolContext.getBlockchain();
    final BftExecutors bftExecutors =
        BftExecutors.create(metricsSystem, BftExecutors.ConsensusType.QBFT);
    final QbftBlockCodec blockEncoder = new QbftBlockCodecAdaptor(qbftExtraDataCodec);

    final Address localAddress = Util.publicKeyToAddress(nodeKey.getPublicKey());
    final BftProtocolSchedule bftProtocolSchedule = (BftProtocolSchedule) protocolSchedule;
    QbftProtocolSchedule qbftProtocolSchedule =
        new QbftProtocolScheduleAdaptor(bftProtocolSchedule, protocolContext);
    final QbftBlockCreatorFactory blockCreatorFactory =
        new QbftBlockCreatorFactory(
            transactionPool,
            protocolContext,
            bftProtocolSchedule,
            qbftForksSchedule,
            miningConfiguration,
            localAddress,
            qbftExtraDataCodec,
            ethProtocolManager.ethContext().getScheduler());

    final ValidatorProvider validatorProvider;
    if (qbftConfig.getStartBlock().isPresent()) {
      validatorProvider =
          protocolContext
              .getConsensusContext(BftContext.class, qbftConfig.getStartBlock().getAsLong())
              .getValidatorProvider();
    } else {
      validatorProvider =
          protocolContext.getConsensusContext(BftContext.class).getValidatorProvider();
    }
    final QbftValidatorProvider qbftValidatorProvider =
        new QbftValidatorProviderAdaptor(validatorProvider);

    final QbftBlockInterface qbftBlockInterface = new QbftBlockInterfaceAdaptor(bftBlockInterface);

    final ProposerSelector proposerSelector =
        new BftProposerSelector(blockchain, bftBlockInterface, true, validatorProvider);

    // NOTE: peers should not be used for accessing the network as it does not enforce the
    // "only send once" filter applied by the UniqueMessageMulticaster.
    peers = new ValidatorPeers(validatorProvider, Istanbul100SubProtocol.NAME);

    final UniqueMessageMulticaster uniqueMessageMulticaster =
        new UniqueMessageMulticaster(peers, qbftConfig.getGossipedHistoryLimit());

    final QbftGossiperImpl gossiper = new QbftGossiperImpl(uniqueMessageMulticaster, blockEncoder);

    final QbftFinalState finalState =
        new QbftFinalStateImpl(
            validatorProvider,
            nodeKey,
            Util.publicKeyToAddress(nodeKey.getPublicKey()),
            proposerSelector,
            uniqueMessageMulticaster,
            new RoundTimer(
                bftEventQueue,
                new BftRoundExpiryTimeCalculator(
                    Duration.ofSeconds(qbftConfig.getRequestTimeoutSeconds())),
                bftExecutors),
            new BlockTimer(bftEventQueue, qbftForksSchedule, bftExecutors, clock),
            new QbftBlockCreatorFactoryAdaptor(blockCreatorFactory, qbftExtraDataCodec),
            clock);

    final MessageValidatorFactory messageValidatorFactory =
        new MessageValidatorFactory(
            proposerSelector, qbftProtocolSchedule, qbftValidatorProvider, qbftBlockInterface);

    final Subscribers<QbftMinedBlockObserver> minedBlockObservers = Subscribers.create();
    minedBlockObservers.subscribe(
        qbftBlock -> ethProtocolManager.blockMined(AdaptorUtil.toBesuBlock(qbftBlock)));
    minedBlockObservers.subscribe(
        qbftBlock ->
            blockLogger(transactionPool, localAddress)
                .blockMined(AdaptorUtil.toBesuBlock(qbftBlock)));

    final EthSynchronizerUpdater synchronizerUpdater =
        new EthSynchronizerUpdater(ethProtocolManager.ethContext().getEthPeers());
    final FutureMessageBuffer<QbftMessage> futureMessageBuffer =
        new FutureMessageBuffer<>(
            qbftConfig.getFutureMessagesMaxDistance(),
            qbftConfig.getFutureMessagesLimit(),
            blockchain.getChainHeadBlockNumber(),
            new FutureMessageSynchronizerHandler(synchronizerUpdater));
    final MessageTracker duplicateMessageTracker =
        new MessageTracker(qbftConfig.getDuplicateMessageLimit());

    final MessageFactory messageFactory = new MessageFactory(nodeKey, blockEncoder);

    QbftRoundFactory qbftRoundFactory =
        new QbftRoundFactory(
            finalState,
            qbftBlockInterface,
            qbftProtocolSchedule,
            minedBlockObservers,
            messageValidatorFactory,
            messageFactory);
    QbftBlockHeightManagerFactory qbftBlockHeightManagerFactory =
        new QbftBlockHeightManagerFactory(
            finalState,
            qbftRoundFactory,
            messageValidatorFactory,
            messageFactory,
            qbftValidatorProvider,
            new QbftValidatorModeTransitionLoggerAdaptor(
                new ValidatorModeTransitionLogger(qbftForksSchedule)));

    qbftBlockHeightManagerFactory.isEarlyRoundChangeEnabled(isEarlyRoundChangeEnabled);

    final QbftEventHandler qbftController =
        new QbftController(
            new QbftBlockchainAdaptor(blockchain),
            finalState,
            qbftBlockHeightManagerFactory,
            gossiper,
            duplicateMessageTracker,
            futureMessageBuffer,
            blockEncoder);
    final BftEventHandler bftEventHandler = new BftEventHandlerAdaptor(qbftController);

    final EventMultiplexer eventMultiplexer = new EventMultiplexer(bftEventHandler);
    final BftProcessor bftProcessor = new BftProcessor(bftEventQueue, eventMultiplexer);

    final MiningCoordinator miningCoordinator =
        new BftMiningCoordinator(
            bftExecutors,
            bftEventHandler,
            bftProcessor,
            blockCreatorFactory,
            blockchain,
            bftEventQueue,
            syncState);

    // Update the next block period in seconds according to the transition schedule
    protocolContext
        .getBlockchain()
        .observeBlockAdded(
            o -> {
              miningConfiguration.setBlockPeriodSeconds(
                  qbftForksSchedule
                      .getFork(o.getHeader().getNumber() + 1, o.getHeader().getTimestamp())
                      .getValue()
                      .getBlockPeriodSeconds());
              miningConfiguration.setEmptyBlockPeriodSeconds(
                  qbftForksSchedule
                      .getFork(o.getHeader().getNumber() + 1, o.getHeader().getTimestamp())
                      .getValue()
                      .getEmptyBlockPeriodSeconds());
            });

    return miningCoordinator;
  }

  @Override
  protected PluginServiceFactory createAdditionalPluginServices(
      final Blockchain blockchain, final ProtocolContext protocolContext) {
    final ValidatorProvider validatorProvider =
        protocolContext.getConsensusContext(BftContext.class).getValidatorProvider();
    return new BftQueryPluginServiceFactory(
        blockchain, qbftExtraDataCodec, validatorProvider, nodeKey, "qbft");
  }

  @Override
  protected ProtocolSchedule createProtocolSchedule() {
    return QbftProtocolScheduleBuilder.create(
        genesisConfigOptions,
        qbftForksSchedule,
        isRevertReasonEnabled,
        qbftExtraDataCodec,
        evmConfiguration,
        miningConfiguration,
        badBlockManager,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  @Override
  protected void validateContext(final ProtocolContext context) {
    final BlockHeader genesisBlockHeader = context.getBlockchain().getGenesisBlock().getHeader();

    if (usingValidatorContractModeButSignersExistIn(genesisBlockHeader)) {
      LOG.warn(
          "Using validator contract mode but genesis block contains signers - the genesis block signers will not be used.");
    }

    if (usingValidatorBlockHeaderModeButNoSignersIn(genesisBlockHeader)) {
      LOG.warn("Genesis block contains no signers - chain will not progress.");
    }
  }

  private boolean usingValidatorContractModeButSignersExistIn(
      final BlockHeader genesisBlockHeader) {
    return isValidatorContractMode() && signersExistIn(genesisBlockHeader);
  }

  private boolean usingValidatorBlockHeaderModeButNoSignersIn(
      final BlockHeader genesisBlockHeader) {
    return !isValidatorContractMode() && !signersExistIn(genesisBlockHeader);
  }

  private boolean isValidatorContractMode() {
    return genesisConfigOptions.getQbftConfigOptions().isValidatorContractMode();
  }

  private boolean signersExistIn(final BlockHeader genesisBlockHeader) {
    return !bftBlockInterface.validatorsInBlock(genesisBlockHeader).isEmpty();
  }

  @Override
  protected BftContext createConsensusContext(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule) {
    final long startBlock =
        qbftConfig.getStartBlock().isPresent() ? qbftConfig.getStartBlock().getAsLong() : 0;
    final EpochManager epochManager = new EpochManager(qbftConfig.getEpochLength(), startBlock);

    final BftValidatorOverrides validatorOverrides =
        convertBftForks(genesisConfigOptions.getTransitions().getQbftForks());
    final BlockValidatorProvider blockValidatorProvider =
        BlockValidatorProvider.forkingValidatorProvider(
            blockchain, epochManager, bftBlockInterface, validatorOverrides);

    transactionValidatorProvider =
        new TransactionValidatorProvider(
            blockchain, new ValidatorContractController(transactionSimulator), qbftForksSchedule);

    final ValidatorProvider validatorProvider =
        new ForkingValidatorProvider(
            blockchain, qbftForksSchedule, blockValidatorProvider, transactionValidatorProvider);

    return new BftContext(validatorProvider, epochManager, bftBlockInterface);
  }

  private BftValidatorOverrides convertBftForks(final List<QbftFork> bftForks) {
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

  private static MinedBlockObserver blockLogger(
      final TransactionPool transactionPool, final Address localAddress) {
    return block ->
        LOG.info(
            String.format(
                "%s %s #%,d / %d tx / %d pending / %,d (%01.1f%%) gas / (%s)",
                block.getHeader().getCoinbase().equals(localAddress) ? "Produced" : "Imported",
                block.getBody().getTransactions().isEmpty() ? "empty block" : "block",
                block.getHeader().getNumber(),
                block.getBody().getTransactions().size(),
                transactionPool.count(),
                block.getHeader().getGasUsed(),
                (block.getHeader().getGasUsed() * 100.0) / block.getHeader().getGasLimit(),
                block.getHash().getBytes().toHexString()));
  }
}
