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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.consensus.clique.BlockHeaderValidationRulesetFactory;
import org.hyperledger.besu.consensus.clique.CliqueContext;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.TransitionProtocolSchedule;
import org.hyperledger.besu.consensus.merge.blockcreation.TransitionCoordinator;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration.MutableInitValues;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * We only bother testing transitionControllerBuilder for PoW and Clique since those are the only
 * network types that are transitioning to PoS.
 */
@ExtendWith(MockitoExtension.class)
public class TransitionControllerBuilderTest {

  @Mock ProtocolSchedule preMergeProtocolSchedule;
  @Mock ProtocolSchedule postMergeProtocolSchedule;
  @Mock ProtocolContext protocolContext;
  @Mock MutableBlockchain mockBlockchain;
  @Mock TransactionPool transactionPool;
  @Mock PeerTaskExecutor peerTaskExecutor;
  @Mock SyncState syncState;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  EthProtocolManager ethProtocolManager;

  @Mock PostMergeContext mergeContext;
  StorageProvider storageProvider = new InMemoryKeyValueStorageProvider();

  CliqueBesuControllerBuilder cliqueBuilder = new CliqueBesuControllerBuilder();
  BesuControllerBuilder powBuilder = new MainnetBesuControllerBuilder();
  MergeBesuControllerBuilder postMergeBuilder = new MergeBesuControllerBuilder();
  MiningConfiguration miningConfiguration;

  TransitionProtocolSchedule transitionProtocolSchedule;

  @BeforeEach
  public void setup() {
    transitionProtocolSchedule =
        spy(
            new TransitionProtocolSchedule(
                preMergeProtocolSchedule, postMergeProtocolSchedule, mergeContext));
    transitionProtocolSchedule.setProtocolContext(protocolContext);
    cliqueBuilder.nodeKey(NodeKeyUtils.generate());
    cliqueBuilder.genesisConfig(GenesisConfig.DEFAULT);
    powBuilder.genesisConfig(GenesisConfig.DEFAULT);
    postMergeBuilder.genesisConfig(GenesisConfig.DEFAULT);
    postMergeBuilder.storageProvider(storageProvider);
    lenient().when(protocolContext.getBlockchain()).thenReturn(mockBlockchain);
    lenient()
        .when(transitionProtocolSchedule.getPostMergeSchedule())
        .thenReturn(postMergeProtocolSchedule);
    lenient()
        .when(transitionProtocolSchedule.getPreMergeSchedule())
        .thenReturn(preMergeProtocolSchedule);
    lenient()
        .when(protocolContext.getConsensusContext(CliqueContext.class))
        .thenReturn(mock(CliqueContext.class));
    lenient()
        .when(protocolContext.getConsensusContext(PostMergeContext.class))
        .thenReturn(mergeContext);
    lenient()
        .when(protocolContext.getConsensusContext(MergeContext.class))
        .thenReturn(mergeContext);
    when(ethProtocolManager.ethContext().getScheduler())
        .thenReturn(new DeterministicEthScheduler());
    miningConfiguration = MiningConfiguration.newDefault();
  }

  @Test
  public void assertPoWIsNotMiningPreMerge() {
    assertThat(miningConfiguration.isMiningEnabled()).isFalse();
    var transCoordinator = buildTransitionCoordinator(powBuilder, postMergeBuilder);
    assertThat(transCoordinator.isMiningBeforeMerge()).isFalse();
  }

  @Test
  public void assertPowMiningPreMerge() {
    miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .mutableInitValues(MutableInitValues.builder().isMiningEnabled(true).build())
            .build();
    var transCoordinator = buildTransitionCoordinator(powBuilder, postMergeBuilder);
    assertThat(transCoordinator.isMiningBeforeMerge()).isTrue();
  }

  @Test
  public void assertPreMergeScheduleForNotPostMerge() {
    var mockBlock = new BlockHeaderTestFixture().buildHeader();
    var preMergeProtocolSpec = mock(ProtocolSpec.class);
    when(mergeContext.isPostMerge()).thenReturn(Boolean.FALSE);
    when(mergeContext.getFinalized()).thenReturn(Optional.empty());
    when(preMergeProtocolSchedule.getByBlockHeader(any())).thenReturn(preMergeProtocolSpec);
    assertThat(transitionProtocolSchedule.getByBlockHeaderWithTransitionReorgHandling(mockBlock))
        .isEqualTo(preMergeProtocolSpec);
  }

  @Test
  public void assertPostMergeScheduleForAnyBlockWhenPostMergeAndFinalized() {
    var mockBlock = new BlockHeaderTestFixture().buildHeader();
    var postMergeProtocolSpec = mock(ProtocolSpec.class);
    when(mergeContext.getFinalized()).thenReturn(Optional.of(mockBlock));
    when(postMergeProtocolSchedule.getByBlockHeader(any())).thenReturn(postMergeProtocolSpec);
    assertThat(transitionProtocolSchedule.getByBlockHeaderWithTransitionReorgHandling(mockBlock))
        .isEqualTo(postMergeProtocolSpec);
  }

  @Test
  public void assertPreMergeScheduleForBelowTerminalBlockWhenPostMergeIfNotFinalized() {
    var mockParentBlock = new BlockHeaderTestFixture().number(100).buildHeader();
    var mockBlock =
        new BlockHeaderTestFixture()
            .number(101)
            .difficulty(Difficulty.of(1L))
            .parentHash(mockParentBlock.getHash())
            .buildHeader();

    var preMergeProtocolSpec = mock(ProtocolSpec.class);

    when(mergeContext.getTerminalTotalDifficulty()).thenReturn(Difficulty.of(1337L));
    when(mergeContext.isPostMerge()).thenReturn(Boolean.TRUE);
    when(mergeContext.getFinalized()).thenReturn(Optional.empty());
    when(mockBlockchain.getTotalDifficultyByHash(any()))
        .thenReturn(Optional.of(Difficulty.of(1335L)));

    when(preMergeProtocolSchedule.getByBlockHeader(any())).thenReturn(preMergeProtocolSpec);
    assertThat(transitionProtocolSchedule.getByBlockHeaderWithTransitionReorgHandling(mockBlock))
        .isEqualTo(preMergeProtocolSpec);
  }

  @Test
  public void assertPostMergeScheduleForPostMergeExactlyAtTerminalDifficultyIfNotFinalized() {
    var mockParentBlock = new BlockHeaderTestFixture().number(100).buildHeader();
    var mockBlock =
        new BlockHeaderTestFixture()
            .number(101)
            .difficulty(Difficulty.of(0L))
            .parentHash(mockParentBlock.getHash())
            .buildHeader();

    var postMergeProtocolSpec = mock(ProtocolSpec.class);

    when(mergeContext.getTerminalTotalDifficulty()).thenReturn(Difficulty.of(1337L));
    when(mergeContext.isPostMerge()).thenReturn(Boolean.TRUE);
    when(mergeContext.getFinalized()).thenReturn(Optional.empty());
    when(mockBlockchain.getTotalDifficultyByHash(any()))
        .thenReturn(Optional.of(Difficulty.of(1337L)));

    when(postMergeProtocolSchedule.getByBlockHeader(any())).thenReturn(postMergeProtocolSpec);
    assertThat(transitionProtocolSchedule.getByBlockHeaderWithTransitionReorgHandling(mockBlock))
        .isEqualTo(postMergeProtocolSpec);
  }

  @Test
  public void assertCliqueDetachedHeaderValidationPreMerge() {
    BlockHeaderValidator cliqueValidator =
        BlockHeaderValidationRulesetFactory.cliqueBlockHeaderValidator(
                5L, true, new EpochManager(5L), Optional.of(FeeMarket.london(1L)), true)
            .build();
    assertDetachedRulesForPostMergeBlocks(cliqueValidator);
  }

  @Test
  public void assertPoWDetachedHeaderValidationPreMerge() {
    BlockHeaderValidator powValidator =
        MainnetBlockHeaderValidator.createBaseFeeMarketValidator(FeeMarket.london(1L), true)
            .build();
    assertDetachedRulesForPostMergeBlocks(powValidator);
  }

  void assertDetachedRulesForPostMergeBlocks(final BlockHeaderValidator validator) {
    var preMergeProtocolSpec = mock(ProtocolSpec.class);
    when(preMergeProtocolSpec.getBlockHeaderValidator()).thenReturn(validator);

    when(preMergeProtocolSchedule.getByBlockHeader(any())).thenReturn(preMergeProtocolSpec);

    var mockParentBlock =
        new BlockHeaderTestFixture()
            .number(100L)
            .baseFeePerGas(Wei.of(7L))
            .gasLimit(5000L)
            .buildHeader();

    var mockBlock =
        new BlockHeaderTestFixture()
            .number(101L)
            .timestamp(1000L)
            .baseFeePerGas(Wei.of(7L))
            .gasLimit(5000L)
            .difficulty(Difficulty.of(0L))
            .parentHash(mockParentBlock.getHash())
            .buildHeader();

    var mergeFriendlyValidation =
        transitionProtocolSchedule
            .getPreMergeSchedule()
            .getByBlockHeader(mockBlock)
            .getBlockHeaderValidator()
            .validateHeader(
                mockBlock, mockParentBlock, protocolContext, HeaderValidationMode.DETACHED_ONLY);
    assertThat(mergeFriendlyValidation).isTrue();
  }

  TransitionCoordinator buildTransitionCoordinator(
      final BesuControllerBuilder preMerge, final MergeBesuControllerBuilder postMerge) {
    var builder = new TransitionBesuControllerBuilder(preMerge, postMerge);
    builder.genesisConfig(GenesisConfig.mainnet());
    builder.storageProvider(storageProvider);
    builder.metricsSystem(new NoOpMetricsSystem());
    var coordinator =
        builder.createMiningCoordinator(
            transitionProtocolSchedule,
            protocolContext,
            transactionPool,
            miningConfiguration,
            syncState,
            ethProtocolManager);

    assertThat(coordinator).isInstanceOf(TransitionCoordinator.class);
    var transCoordinator = (TransitionCoordinator) coordinator;
    return transCoordinator;
  }
}
