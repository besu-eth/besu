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

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.MergeProtocolSchedule;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.TransitionBestPeerComparator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeCoordinator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
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
import org.hyperledger.besu.ethereum.eth.peervalidation.RequiredBlocksPeerValidator;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardChain;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncAlgorithmFactory;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Merge besu controller builder. */
public class MergeBesuControllerBuilder extends BesuControllerBuilder {
  private final AtomicReference<SyncState> syncState = new AtomicReference<>();
  private static final Logger LOG = LoggerFactory.getLogger(MergeBesuControllerBuilder.class);
  private final PostMergeContext postMergeContext = new PostMergeContext();

  /** Default constructor. */
  public MergeBesuControllerBuilder() {}

  @Override
  protected MiningCoordinator createMiningCoordinator(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final SyncState syncState,
      final EthProtocolManager ethProtocolManager) {
    return createTransitionMiningCoordinator(
        protocolSchedule,
        protocolContext,
        transactionPool,
        miningConfiguration,
        syncState,
        new BackwardSyncContext(
            protocolContext,
            protocolSchedule,
            syncConfig,
            metricsSystem,
            ethProtocolManager.ethContext(),
            syncState,
            BackwardChain.from(
                storageProvider, ScheduleBasedBlockHeaderFunctions.create(protocolSchedule)),
            new BackwardSyncAlgorithmFactory()),
        ethProtocolManager.ethContext().getScheduler());
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

    var mergeContext = protocolContext.getConsensusContext(MergeContext.class);

    var mergeBestPeerComparator =
        new TransitionBestPeerComparator(
            genesisConfigOptions.getTerminalTotalDifficulty().map(Difficulty::of).orElseThrow());
    ethPeers.setBestPeerComparator(mergeBestPeerComparator);
    mergeContext.observeNewIsPostMergeState(mergeBestPeerComparator);

    Optional<MergePeerFilter> filterToUse = Optional.of(new MergePeerFilter());

    if (mergePeerFilter.isPresent()) {
      filterToUse = mergePeerFilter;
    }
    mergeContext.observeNewIsPostMergeState(filterToUse.get());
    mergeContext.addNewUnverifiedForkchoiceListener(filterToUse.get());

    EthProtocolManager ethProtocolManager =
        super.createEthProtocolManager(
            protocolContext,
            synchronizerConfiguration,
            transactionPool,
            ethereumWireProtocolConfiguration,
            ethPeers,
            ethContext,
            ethMessages,
            scheduler,
            peerValidators,
            filterToUse,
            forkIdManager);

    return ethProtocolManager;
  }

  /**
   * Create transition mining coordinator.
   *
   * @param protocolSchedule the protocol schedule
   * @param protocolContext the protocol context
   * @param transactionPool the transaction pool
   * @param miningConfiguration the mining parameters
   * @param syncState the sync state
   * @param backwardSyncContext the backward sync context
   * @param ethScheduler the scheduler
   * @return the mining coordinator
   */
  protected MiningCoordinator createTransitionMiningCoordinator(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final TransactionPool transactionPool,
      final MiningConfiguration miningConfiguration,
      final SyncState syncState,
      final BackwardSyncContext backwardSyncContext,
      final EthScheduler ethScheduler) {

    this.syncState.set(syncState);

    return new MergeCoordinator(
        protocolContext,
        protocolSchedule,
        ethScheduler,
        transactionPool,
        miningConfiguration,
        backwardSyncContext);
  }

  @Override
  protected ProtocolSchedule createProtocolSchedule() {
    return MergeProtocolSchedule.create(
        genesisConfigOptions,
        isRevertReasonEnabled,
        miningConfiguration,
        badBlockManager,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem,
        evmConfiguration);
  }

  @Override
  protected MergeContext createConsensusContext(
      final Blockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ProtocolSchedule protocolSchedule) {

    final OptionalLong terminalBlockNumber = genesisConfigOptions.getTerminalBlockNumber();
    final Optional<Hash> terminalBlockHash = genesisConfigOptions.getTerminalBlockHash();
    final boolean isPostMergeAtGenesis =
        genesisConfigOptions.getTerminalTotalDifficulty().isPresent()
            && genesisConfigOptions.getTerminalTotalDifficulty().get().isZero()
            && blockchain.getGenesisBlockHeader().getDifficulty().isZero();

    final MergeContext mergeContext =
        postMergeContext
            .setSyncState(syncState.get())
            .setTerminalTotalDifficulty(
                genesisConfigOptions
                    .getTerminalTotalDifficulty()
                    .map(Difficulty::of)
                    .orElse(Difficulty.ZERO))
            .setPostMergeAtGenesis(isPostMergeAtGenesis);

    blockchain
        .getFinalized()
        .flatMap(blockchain::getBlockHeader)
        .ifPresent(mergeContext::setFinalized);

    blockchain
        .getSafeBlock()
        .flatMap(blockchain::getBlockHeader)
        .ifPresent(mergeContext::setSafeBlock);

    if (terminalBlockNumber.isPresent() && terminalBlockHash.isPresent()) {
      Optional<BlockHeader> termBlock = blockchain.getBlockHeader(terminalBlockNumber.getAsLong());
      mergeContext.setTerminalPoWBlock(termBlock);
    }
    blockchain.observeBlockAdded(
        blockAddedEvent ->
            blockchain
                .getTotalDifficultyByHash(blockAddedEvent.getHeader().getHash())
                .ifPresent(mergeContext::setIsPostMerge));

    return mergeContext;
  }

  @Override
  protected PluginServiceFactory createAdditionalPluginServices(
      final Blockchain blockchain, final ProtocolContext protocolContext) {
    return new NoopPluginServiceFactory();
  }

  @Override
  protected List<PeerValidator> createPeerValidators(
      final ProtocolSchedule protocolSchedule, final PeerTaskExecutor peerTaskExecutor) {
    List<PeerValidator> retval = super.createPeerValidators(protocolSchedule, peerTaskExecutor);
    final OptionalLong powTerminalBlockNumber = genesisConfigOptions.getTerminalBlockNumber();
    final Optional<Hash> powTerminalBlockHash = genesisConfigOptions.getTerminalBlockHash();
    if (powTerminalBlockHash.isPresent() && powTerminalBlockNumber.isPresent()) {
      retval.add(
          new RequiredBlocksPeerValidator(
              protocolSchedule,
              peerTaskExecutor,
              powTerminalBlockNumber.getAsLong(),
              powTerminalBlockHash.get(),
              0));
    } else {
      LOG.debug("unable to validate peers with terminal difficulty blocks");
    }
    return retval;
  }

  @Override
  public BesuController build() {
    final BesuController controller = super.build();
    postMergeContext.setSyncState(syncState.get());
    return controller;
  }

  /**
   * Gets post merge context.
   *
   * @return the post merge context
   */
  public PostMergeContext getPostMergeContext() {
    return postMergeContext;
  }
}
