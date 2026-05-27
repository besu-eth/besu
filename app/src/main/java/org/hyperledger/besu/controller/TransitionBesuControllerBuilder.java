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
package org.hyperledger.besu.controller;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.TransitionBackwardSyncContext;
import org.hyperledger.besu.consensus.merge.TransitionContext;
import org.hyperledger.besu.consensus.merge.TransitionProtocolSchedule;
import org.hyperledger.besu.consensus.merge.blockcreation.TransitionCoordinator;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.MergePeerFilter;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.DefaultSynchronizer;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncAlgorithmFactory;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.permissioning.NodeMessagePermissioningProvider;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Transition besu controller builder. */
public class TransitionBesuControllerBuilder extends BesuControllerBuilder {
  private final BesuControllerBuilder preMergeBesuControllerBuilder;
  private final MergeBesuControllerBuilder mergeBesuControllerBuilder;

  private static final Logger LOG = LoggerFactory.getLogger(TransitionBesuControllerBuilder.class);
  private TransitionProtocolSchedule transitionProtocolSchedule;

  /**
   * Instantiates a new Transition besu controller builder.
   *
   * @param preMergeBesuControllerBuilder the pre merge besu controller builder
   * @param mergeBesuControllerBuilder the merge besu controller builder
   */
  public TransitionBesuControllerBuilder(
      final BesuControllerBuilder preMergeBesuControllerBuilder,
      final MergeBesuControllerBuilder mergeBesuControllerBuilder) {
    this.preMergeBesuControllerBuilder = preMergeBesuControllerBuilder;
    this.mergeBesuControllerBuilder = mergeBesuControllerBuilder;
  }

  @Override
  protected void prepForBuild() {
    preMergeBesuControllerBuilder.prepForBuild();
    mergeBesuControllerBuilder.prepForBuild();
  }

  @Override
  protected MiningCoordinator createMiningCoordinator(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager) {

    // cast to transition schedule for explicit access to pre and post objects:
    final TransitionProtocolSchedule transitionProtocolSchedule =
        (TransitionProtocolSchedule) protocolSchedule;

    // PoA consensus mines by default, get consensus-specific mining parameters for
    // TransitionCoordinator:
    preMergeBesuControllerBuilder.overrideMiningConfiguration(miningConfiguration);

    // construct a transition backward sync context
    BackwardSyncContext transitionBackwardsSyncContext =
        new TransitionBackwardSyncContext(
            protocolContext,
            transitionProtocolSchedule,
            syncConfig,
            metricsSystem,
            ethProtocolManager.ethContext(),
            syncState,
            storageProvider,
            new BackwardSyncAlgorithmFactory());

    final TransitionCoordinator composedCoordinator =
        new TransitionCoordinator(
            preMergeBesuControllerBuilder.createMiningCoordinator(
                transitionProtocolSchedule.getPreMergeSchedule(),
                protocolContext,
                transactionPool,
                // special case: we need to disable mining during transition
                ImmutableMiningConfiguration.builder()
                    .from(miningConfiguration)
                    .mutableInitValues(
                        ImmutableMiningConfiguration.MutableInitValues.builder()
                            .isMiningEnabled(false)
                            .build())
                    .build(),
                syncState,
                ethProtocolManager),
            mergeBesuControllerBuilder.createTransitionMiningCoordinator(
                transitionProtocolSchedule,
                protocolContext,
                transactionPool,
                miningConfiguration,
                syncState,
                transitionBackwardsSyncContext,
                ethProtocolManager.ethContext().getScheduler()),
            mergeBesuControllerBuilder.getPostMergeContext());
    initTransitionWatcher(protocolContext, composedCoordinator);
    return composedCoordinator;
  }

  @Override
  protected EthProtocolManager createEthProtocolManager(
      final ProtocolContext protocolContext,
      final SynchronizerConfiguration synchronizerConfiguration,
      final TransactionPool transactionPool,
      final EthProtocolConfiguration ethereumWireProtocolConfiguration,
      final EthPeers ethPeers,
      final EthContext ethContext,
      final EthMessages ethMessages,
      final EthScheduler scheduler,
      final List<PeerValidator> peerValidators,
      final Optional<MergePeerFilter> mergePeerFilter,
      final ForkIdManager forkIdManager) {
    return mergeBesuControllerBuilder.createEthProtocolManager(
        protocolContext,
        synchronizerConfiguration,
        transactionPool,
        ethereumWireProtocolConfiguration,
        ethPeers,
        ethContext,
        ethMessages,
        scheduler,
        peerValidators,
        mergePeerFilter,
        forkIdManager);
  }

  @Override
  protected ProtocolSchedule createProtocolSchedule() {
    transitionProtocolSchedule =
        new TransitionProtocolSchedule(
            preMergeBesuControllerBuilder.createProtocolSchedule(),
            mergeBesuControllerBuilder.createProtocolSchedule(),
            mergeBesuControllerBuilder.getPostMergeContext());
    return transitionProtocolSchedule;
  }

  @Override
  protected ProtocolContext createProtocolContext(
      final MutableBlockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ConsensusContext consensusContext,
      final ServiceManager serviceManager) {
    final ProtocolContext protocolContext =
        super.createProtocolContext(
            blockchain, worldStateArchive, consensusContext, serviceManager);
    transitionProtocolSchedule.setProtocolContext(protocolContext);
    return protocolContext;
  }

  @Override
  protected ConsensusContext createConsensusContext(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule) {
    return new TransitionContext(
        preMergeBesuControllerBuilder.createConsensusContext(
            blockchain, worldStateArchive, protocolSchedule),
        mergeBesuControllerBuilder.createConsensusContext(
            blockchain, worldStateArchive, protocolSchedule));
  }

  @Override
  protected PluginServiceFactory createAdditionalPluginServices(
      final Blockchain blockchain, final ProtocolContext protocolContext) {
    return new NoopPluginServiceFactory();
  }

  @Override
  protected DefaultSynchronizer createSynchronizer(
      final ProtocolSchedule protocolSchedule,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext,
      final EthContext ethContext,
      final PeerTaskExecutor peerTaskExecutor,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager,
      final PivotBlockSelector pivotBlockSelector) {

    DefaultSynchronizer sync =
        super.createSynchronizer(
            protocolSchedule,
            worldStateStorageCoordinator,
            protocolContext,
            ethContext,
            peerTaskExecutor,
            syncState,
            ethProtocolManager,
            pivotBlockSelector);

    if (genesisConfigOptions.getTerminalTotalDifficulty().isPresent()) {
      LOG.info(
          "TTD present, creating DefaultSynchronizer that stops propagating after finalization");
      protocolContext
          .getConsensusContext(MergeContext.class)
          .addNewUnverifiedForkchoiceListener(sync);
    }

    return sync;
  }

  @SuppressWarnings("UnusedVariable")
  private void initTransitionWatcher(
      final ProtocolContext protocolContext, final TransitionCoordinator composedCoordinator) {

    PostMergeContext postMergeContext = mergeBesuControllerBuilder.getPostMergeContext();
    postMergeContext.observeNewIsPostMergeState(
        (isPoS, priorState, difficultyStoppedAt) -> {
          if (isPoS) {
            // if we transitioned to post-merge, stop and disable any mining
            composedCoordinator.getPreMergeObject().disable();
            composedCoordinator.getPreMergeObject().stop();
            // set the blockchoiceRule to never reorg, rely on forkchoiceUpdated instead
            protocolContext
                .getBlockchain()
                .setBlockChoiceRule((newBlockHeader, currentBlockHeader) -> -1);

          } else if (composedCoordinator.isMiningBeforeMerge()) {
            // if our merge state is set to mine pre-merge and we are mining, start mining
            composedCoordinator.getPreMergeObject().enable();
            composedCoordinator.getPreMergeObject().start();
          }
        });

    // initialize our merge context merge status before we would start either
    Blockchain blockchain = protocolContext.getBlockchain();
    blockchain
        .getTotalDifficultyByHash(blockchain.getChainHeadHash())
        .ifPresent(postMergeContext::setIsPostMerge);
  }

  @Override
  public BesuControllerBuilder storageProvider(final StorageProvider storageProvider) {
    super.storageProvider(storageProvider);
    return propagateConfig(z -> z.storageProvider(storageProvider));
  }

  @Override
  public BesuController build() {
    final BesuController controller = super.build();
    mergeBesuControllerBuilder.getPostMergeContext().setSyncState(controller.getSyncState());
    return controller;
  }

  @Override
  public BesuControllerBuilder evmConfiguration(final EvmConfiguration evmConfiguration) {
    super.evmConfiguration(evmConfiguration);
    return propagateConfig(z -> z.evmConfiguration(evmConfiguration));
  }

  @Override
  public BesuControllerBuilder genesisConfig(final GenesisConfig genesisConfig) {
    super.genesisConfig(genesisConfig);
    return propagateConfig(z -> z.genesisConfig(genesisConfig));
  }

  @Override
  public BesuControllerBuilder synchronizerConfiguration(
      final SynchronizerConfiguration synchronizerConfig) {
    super.synchronizerConfiguration(synchronizerConfig);
    return propagateConfig(z -> z.synchronizerConfiguration(synchronizerConfig));
  }

  @Override
  public BesuControllerBuilder ethProtocolConfiguration(
      final EthProtocolConfiguration ethProtocolConfiguration) {
    super.ethProtocolConfiguration(ethProtocolConfiguration);
    return propagateConfig(z -> z.ethProtocolConfiguration(ethProtocolConfiguration));
  }

  @Override
  public BesuControllerBuilder networkId(final BigInteger networkId) {
    super.networkId(networkId);
    return propagateConfig(z -> z.networkId(networkId));
  }

  @Override
  public BesuControllerBuilder miningParameters(final MiningConfiguration miningConfiguration) {
    super.miningParameters(miningConfiguration);
    return propagateConfig(z -> z.miningParameters(miningConfiguration));
  }

  @Override
  public BesuControllerBuilder messagePermissioningProviders(
      final List<NodeMessagePermissioningProvider> messagePermissioningProviders) {
    super.messagePermissioningProviders(messagePermissioningProviders);
    return propagateConfig(z -> z.messagePermissioningProviders(messagePermissioningProviders));
  }

  @Override
  public BesuControllerBuilder nodeKey(final NodeKey nodeKey) {
    super.nodeKey(nodeKey);
    return propagateConfig(z -> z.nodeKey(nodeKey));
  }

  @Override
  public BesuControllerBuilder metricsSystem(final ObservableMetricsSystem metricsSystem) {
    super.metricsSystem(metricsSystem);
    return propagateConfig(z -> z.metricsSystem(metricsSystem));
  }

  @Override
  public BesuControllerBuilder dataDirectory(final Path dataDirectory) {
    super.dataDirectory(dataDirectory);
    return propagateConfig(z -> z.dataDirectory(dataDirectory));
  }

  @Override
  public BesuControllerBuilder clock(final Clock clock) {
    super.clock(clock);
    return propagateConfig(z -> z.clock(clock));
  }

  @Override
  public BesuControllerBuilder transactionPoolConfiguration(
      final TransactionPoolConfiguration transactionPoolConfiguration) {
    super.transactionPoolConfiguration(transactionPoolConfiguration);
    return propagateConfig(z -> z.transactionPoolConfiguration(transactionPoolConfiguration));
  }

  @Override
  public BesuControllerBuilder isParallelTxProcessingEnabled(
      final boolean isParallelTxProcessingEnabled) {
    super.isParallelTxProcessingEnabled(isParallelTxProcessingEnabled);
    return propagateConfig(z -> z.isParallelTxProcessingEnabled(isParallelTxProcessingEnabled));
  }

  @Override
  public BesuControllerBuilder balConfiguration(final BalConfiguration balConfiguration) {
    super.balConfiguration(balConfiguration);
    return propagateConfig(z -> z.balConfiguration(balConfiguration));
  }

  @Override
  public BesuControllerBuilder requiredBlocks(final Map<Long, Hash> requiredBlocks) {
    super.requiredBlocks(requiredBlocks);
    return propagateConfig(z -> z.requiredBlocks(requiredBlocks));
  }

  @Override
  public BesuControllerBuilder reorgLoggingThreshold(final long reorgLoggingThreshold) {
    super.reorgLoggingThreshold(reorgLoggingThreshold);
    return propagateConfig(z -> z.reorgLoggingThreshold(reorgLoggingThreshold));
  }

  @Override
  public BesuControllerBuilder dataStorageConfiguration(
      final DataStorageConfiguration dataStorageConfiguration) {
    super.dataStorageConfiguration(dataStorageConfiguration);
    return propagateConfig(z -> z.dataStorageConfiguration(dataStorageConfiguration));
  }

  private BesuControllerBuilder propagateConfig(final Consumer<BesuControllerBuilder> toPropagate) {
    toPropagate.accept(preMergeBesuControllerBuilder);
    toPropagate.accept(mergeBesuControllerBuilder);
    return this;
  }
}
