/*
 * Copyright contributors to Besu.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
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
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSyncContext;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Fork choices to a head too far from the head world state to move to within the call: the world
 * state is moved in the background, and a later fork choice completes the move.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MergeCoordinatorBatchedHeadRollTest implements MergeGenesisConfigHelper {

  private static final int TRIE_LOGS_PER_BATCH = 2;

  @Mock private SyncState syncState;
  @Mock private EthScheduler ethScheduler;

  private final List<Runnable> serviceTasks = new ArrayList<>();
  private final PostMergeContext mergeContext = new PostMergeContext();
  private final BlockHeaderTestFixture headerGenerator = new BlockHeaderTestFixture();

  private ProtocolSchedule protocolSchedule;
  private GenesisState genesisState;
  private BaseFeeMarket feeMarket;
  private MutableBlockchain blockchain;
  private BonsaiWorldStateProvider worldStateArchive;
  private MergeCoordinator coordinator;

  private Block shortBranchTip;
  private Block longBranchTip;

  @BeforeEach
  void setUp() {
    MergeConfiguration.setMergeEnabled(false);
    protocolSchedule = getMergeProtocolSchedule();
    genesisState =
        GenesisState.fromConfig(getPosGenesisConfig(), protocolSchedule, new BonsaiCodeCache());
    feeMarket = FeeMarket.london(0, genesisState.getBlock().getHeader().getBaseFee());
    MergeConfiguration.setMergeEnabled(true);

    blockchain = createInMemoryBlockchain(genesisState.getBlock());
    worldStateArchive = createBonsaiInMemoryWorldStateArchive(blockchain);
    worldStateArchive.setTrieLogsPerRollBatch(TRIE_LOGS_PER_BATCH);

    mergeContext.setTerminalTotalDifficulty(genesisState.getBlock().getHeader().getDifficulty());
    mergeContext.setSyncState(syncState);
    when(syncState.hasReachedTerminalDifficulty()).thenReturn(Optional.of(Boolean.TRUE));
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    when(syncState.isInSync()).thenReturn(true);

    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(mergeContext)
            .withBadBlockManager(new BadBlockManager())
            .build();
    final var genesisWorldState = worldStateArchive.getWorldState();
    genesisState.writeStateTo(genesisWorldState);
    genesisWorldState.persist(genesisState.getBlock().getHeader());
    blockchain.observeBlockAdded(
        event ->
            blockchain
                .getTotalDifficultyByHash(event.getHeader().getHash())
                .ifPresent(mergeContext::setIsPostMerge));

    // background tasks only run when the test says so
    when(ethScheduler.scheduleServiceTask(any(Runnable.class)))
        .thenAnswer(
            invocation -> {
              serviceTasks.add(invocation.getArgument(0));
              return CompletableFuture.completedFuture(null);
            });

    final Address coinbase = genesisAllocations(getPosGenesisConfig()).findFirst().orElseThrow();
    coordinator =
        new MergeCoordinator(
            protocolContext,
            protocolSchedule,
            ethScheduler,
            mock(TransactionPool.class),
            ImmutableMiningConfiguration.builder()
                .mutableInitValues(MutableInitValues.builder().coinbase(coinbase).build())
                .build(),
            mock(BackwardSyncContext.class));

    // a common chain, then a one block branch that becomes the head and a four block branch that
    // is only imported, so either tip is five trie logs from the other
    BlockHeader parent = genesisState.getBlock().getHeader();
    for (int i = 0; i < 3; i++) {
      parent = importAndMakeHead(nextEmptyBlock(parent)).getHeader();
    }
    final BlockHeader forkPoint = parent;
    shortBranchTip = importAndMakeHead(nextEmptyBlock(forkPoint));
    parent = forkPoint;
    for (int i = 0; i < 4; i++) {
      longBranchTip = nextEmptyBlock(parent);
      assertThat(coordinator.rememberBlock(longBranchTip).isSuccessful()).isTrue();
      parent = longBranchTip.getHeader();
    }
  }

  @AfterEach
  void tearDown() {
    MergeConfiguration.setMergeEnabled(false);
  }

  @Test
  void deepForkChoiceMovesWorldStateInBackgroundThenHead() {
    assertDeepForkChoiceCompletesAfterBackgroundMove(longBranchTip, shortBranchTip);
  }

  @Test
  void deepForkChoiceToLowerBlockMovesWorldStateInBackgroundThenHead() {
    assertDeepForkChoiceCompletesAfterBackgroundMove(longBranchTip, shortBranchTip);

    assertDeepForkChoiceCompletesAfterBackgroundMove(shortBranchTip, longBranchTip);
  }

  @Test
  void failedBackgroundMoveIsReportedOnceThenRetried() {
    worldStateArchive
        .getWorldStateKeyValueStorage()
        .pruneTrieLog(longBranchTip.getHeader().getParentHash());

    assertThat(forkChoice(longBranchTip).getStatus()).isEqualTo(ForkchoiceResult.Status.SYNCING);
    serviceTasks.getFirst().run();

    assertThat(forkChoice(longBranchTip).getStatus())
        .isEqualTo(ForkchoiceResult.Status.INTERNAL_ERROR);
    assertThat(forkChoice(longBranchTip).getStatus()).isEqualTo(ForkchoiceResult.Status.SYNCING);
    assertThat(serviceTasks).hasSize(2);
    assertThat(blockchain.getChainHeadHash()).isEqualTo(shortBranchTip.getHash());
  }

  private void assertDeepForkChoiceCompletesAfterBackgroundMove(
      final Block newHead, final Block oldHead) {
    serviceTasks.clear();

    assertThat(forkChoice(newHead).getStatus()).isEqualTo(ForkchoiceResult.Status.SYNCING);
    // a repeated fork choice while the move is running does not start another one
    assertThat(forkChoice(newHead).getStatus()).isEqualTo(ForkchoiceResult.Status.SYNCING);
    assertThat(serviceTasks).hasSize(1);
    assertThat(blockchain.getChainHeadHash()).isEqualTo(oldHead.getHash());

    serviceTasks.getFirst().run();
    assertThat(((PathBasedWorldState) worldStateArchive.getWorldState()).blockHash())
        .isEqualTo(newHead.getHash());
    assertThat(blockchain.getChainHeadHash()).isEqualTo(oldHead.getHash());

    assertThat(forkChoice(newHead).getStatus()).isEqualTo(ForkchoiceResult.Status.VALID);
    assertThat(blockchain.getChainHeadHash()).isEqualTo(newHead.getHash());
    assertThat(serviceTasks).hasSize(1);
  }

  private ForkchoiceResult forkChoice(final Block head) {
    return coordinator.updateForkChoice(head.getHeader(), Hash.ZERO, Hash.ZERO);
  }

  private Block importAndMakeHead(final Block block) {
    assertThat(coordinator.rememberBlock(block).isSuccessful()).isTrue();
    assertThat(forkChoice(block).getStatus()).isEqualTo(ForkchoiceResult.Status.VALID);
    return block;
  }

  private Block nextEmptyBlock(final BlockHeader parent) {
    final BlockHeader header =
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
    return new Block(header, BlockBody.empty());
  }
}
