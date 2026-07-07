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
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.MergeConfiguration;
import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
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
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;
import org.hyperledger.besu.util.LogConfigurator;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MergeReorgTest implements MergeGenesisConfigHelper {

  @Mock TransactionPool mockTransactionPool;

  private MergeCoordinator coordinator;

  private final MergeContext mergeContext = new PostMergeContext();
  private final ProtocolSchedule mockProtocolSchedule = getMergeProtocolSchedule();
  private final GenesisState genesisState =
      GenesisState.fromConfig(
          getPowGenesisConfig(), mockProtocolSchedule, new PathBasedCodeCache());

  private final WorldStateArchive worldStateArchive = createInMemoryWorldStateArchive();
  private final MutableBlockchain blockchain = createInMemoryBlockchain(genesisState.getBlock());
  private final EthScheduler ethScheduler = new DeterministicEthScheduler();
  private final ProtocolContext protocolContext =
      new ProtocolContext.Builder()
          .withBlockchain(blockchain)
          .withWorldStateArchive(worldStateArchive)
          .withConsensusContext(mergeContext)
          .build();

  private final Address coinbase = genesisAllocations(getPowGenesisConfig()).findFirst().get();
  private final BlockHeaderTestFixture headerGenerator = new BlockHeaderTestFixture();
  private final BaseFeeMarket feeMarket =
      FeeMarket.london(0, genesisState.getBlock().getHeader().getBaseFee());

  @BeforeEach
  public void setUp() {
    var mutable = worldStateArchive.getWorldState();
    genesisState.writeStateTo(mutable);
    mutable.persist(null);
    mergeContext.setTerminalTotalDifficulty(Difficulty.of(1001));
    MergeConfiguration.setMergeEnabled(true);
    this.coordinator =
        new MergeCoordinator(
            protocolContext,
            mockProtocolSchedule,
            ethScheduler,
            mockTransactionPool,
            ImmutableMiningConfiguration.builder()
                .mutableInitValues(MutableInitValues.builder().coinbase(coinbase).build())
                .build(),
            mock(BackwardSyncContext.class));
    mergeContext.setIsPostMerge(genesisState.getBlock().getHeader().getDifficulty());
    blockchain.observeBlockAdded(
        blockAddedEvent ->
            blockchain
                .getTotalDifficultyByHash(blockAddedEvent.getHeader().getHash())
                .ifPresent(mergeContext::setIsPostMerge));
  }

  /* Validation scenario as described over Discord:
  as long as a post-merge PoS block has not been finalized,
  then you can and should be able to re-org to a different pre-TTD block
  say there is viable TTD block A and B, then we can have a PoS chain build on A for a while
      and then see another PoS chain build on B that has a higher fork choice weight and causes a re-org
  once any post-merge PoS chain is finalized though, you'd never re-org any PoW blocks in the tree ever again */

  @Test
  public void reorgsAcrossTDDToDifferentTargetsWhenNotFinal() {
    // Add N blocks to chain from genesis, where total diff is < TTD
    LogConfigurator.setLevel(BlockHeaderValidator.class.getName(), "DEBUG");
    List<Block> endOfWork = subChain(genesisState.getBlock().getHeader(), 10, Difficulty.of(100L));
    endOfWork.stream().forEach(this::appendBlock);
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(10L);
    BlockHeader tddPenultimate = this.blockchain.getChainHeadHeader();
    // Add TTD block A to chain as child of N.
    Block ttdA = new Block(terminalPowBlock(tddPenultimate, Difficulty.ONE), BlockBody.empty());
    appendBlock(ttdA);
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(11L);
    assertThat(blockchain.getTotalDifficultyByHash(ttdA.getHash())).isPresent();
    Difficulty tdd = blockchain.getTotalDifficultyByHash(ttdA.getHash()).get();
    assertThat(tdd.getAsBigInteger())
        .isGreaterThan(
            getPosGenesisConfig()
                .getConfigOptions()
                .getTerminalTotalDifficulty()
                .get()
                .toBigInteger());
    assertThat(mergeContext.isPostMerge()).isTrue();
    List<Block> builtOnTTDA = subChain(ttdA.getHeader(), 5, Difficulty.of(0L));
    builtOnTTDA.stream().forEach(this::appendBlock);
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(16);
    assertThat(blockchain.getChainHead().getHash())
        .isEqualTo(builtOnTTDA.get(builtOnTTDA.size() - 1).getHash());

    Block ttdB = new Block(terminalPowBlock(tddPenultimate, Difficulty.of(2L)), BlockBody.empty());
    appendBlock(ttdB);
    List<Block> builtOnTTDB = subChain(ttdB.getHeader(), 10, Difficulty.of(0L));
    builtOnTTDB.stream().forEach(this::appendBlock);
    assertThat(blockchain.getChainHead().getHeight()).isEqualTo(21);
    assertThat(blockchain.getChainHead().getHash())
        .isEqualTo(builtOnTTDB.get(builtOnTTDB.size() - 1).getHash());
    // don't finalize
    // Create a new chain back to A which has

  }

  private void appendBlock(final Block block) {
    final BlockProcessingResult result = coordinator.validateBlock(block);

    if (result.isSuccessful()) {
      result
          .getYield()
          .ifPresentOrElse(
              outputs -> blockchain.appendBlock(block, outputs.getReceipts()),
              () -> {
                if (result.causedBy().isPresent()) {
                  throw new RuntimeException(result.errorMessage.get(), result.causedBy().get());
                }
                throw new RuntimeException(result.errorMessage.get());
              });
    } else {
      if (result.causedBy().isPresent()) {
        throw new RuntimeException(result.errorMessage.get(), result.causedBy().get());
      }
      throw new RuntimeException(result.errorMessage.get());
    }
  }

  private List<Block> subChain(
      final BlockHeader parentHeader, final long length, final Difficulty each) {
    BlockHeader newParent = parentHeader;
    List<Block> retval = new ArrayList<>();
    for (long i = 1; i <= length; i++) {
      headerGenerator
          .parentHash(newParent.getHash())
          .number(newParent.getNumber() + 1)
          .baseFeePerGas(
              feeMarket.computeBaseFee(
                  genesisState.getBlock().getHeader().getNumber() + 1,
                  newParent.getBaseFee().orElse(Wei.of(0x3b9aca00)),
                  0,
                  15000000))
          .gasLimit(newParent.getGasLimit())
          .timestamp(newParent.getTimestamp() + 1)
          .stateRoot(newParent.getStateRoot());
      if (each.greaterOrEqualThan(Difficulty.ZERO)) {
        headerGenerator.difficulty(each);
      }
      BlockHeader h = headerGenerator.buildHeader();
      retval.add(new Block(h, BlockBody.empty()));
      newParent = h;
    }
    return retval;
  }

  private BlockHeader terminalPowBlock(final BlockHeader parent, final Difficulty diff) {

    BlockHeader terminal =
        headerGenerator
            .difficulty(diff)
            .parentHash(parent.getHash())
            .number(parent.getNumber() + 1)
            .baseFeePerGas(
                feeMarket.computeBaseFee(
                    genesisState.getBlock().getHeader().getNumber() + 1,
                    parent.getBaseFee().orElse(Wei.of(0x3b9aca00)),
                    0,
                    15000000l))
            .gasLimit(parent.getGasLimit())
            .timestamp(parent.getTimestamp() + 1)
            .stateRoot(parent.getStateRoot())
            .buildHeader();
    return terminal;
  }
}
