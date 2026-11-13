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
package org.hyperledger.besu.ethereum.eth.sync.fullsync.ereprepipeline;

import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.consensus.merge.MergeProtocolSchedule;
import org.hyperledger.besu.consensus.merge.PostMergeContext;
import org.hyperledger.besu.consensus.merge.TransitionContext;
import org.hyperledger.besu.consensus.merge.TransitionProtocolSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ConsensusContextFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManager;
import org.hyperledger.besu.ethereum.eth.manager.EthProtocolManagerTestBuilder;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;

/** Real Shanghai execution fixtures with independently calculated withdrawal state roots. */
final class ErePosTestChain implements AutoCloseable {
  static final Address RECIPIENT =
      Address.fromHexString("0x0000000000000000000000000000000000000001");
  final ProtocolSchedule schedule;
  final MutableBlockchain blockchain;
  final WorldStateArchive archive;
  final ProtocolContext context;
  final EthProtocolManager manager;
  final List<Block> blocks = new ArrayList<>();
  final Difficulty terminalDifficulty;
  final boolean transition;

  ErePosTestChain(final int lastNumber) throws Exception {
    this(lastNumber, false);
  }

  ErePosTestChain(final int lastNumber, final boolean transition) throws Exception {
    this.transition = transition;
    terminalDifficulty = Difficulty.of(transition ? 2 : 0);
    final var config =
        GenesisConfig.fromConfig(
            """
        {"config":{"chainId":11244,"londonBlock":0,
          "shanghaiTime":%d,"terminalTotalDifficulty":%d},
         "nonce":"0x0","timestamp":"0x0","difficulty":"%s",
         "gasLimit":"0x1c9c380","baseFeePerGas":"0x3b9aca00","alloc":{}}
        """
                .formatted(transition ? 24 : 0, transition ? 2 : 0, transition ? "0x1" : "0x0"));
    final var metrics = new NoOpMetricsSystem();
    final var badBlocks = new BadBlockManager();
    final var merge = new PostMergeContext().setTerminalTotalDifficulty(terminalDifficulty);
    final var postSchedule =
        MergeProtocolSchedule.create(
            config.getConfigOptions(),
            false,
            MiningConfiguration.MINING_DISABLED,
            badBlocks,
            false,
            BalConfiguration.DEFAULT,
            metrics,
            EvmConfiguration.DEFAULT);
    schedule =
        transition
            ? new TransitionProtocolSchedule(
                MainnetProtocolSchedule.fromConfig(
                    config.getConfigOptions(),
                    EvmConfiguration.DEFAULT,
                    MiningConfiguration.MINING_DISABLED,
                    badBlocks,
                    false,
                    BalConfiguration.DEFAULT,
                    metrics),
                postSchedule,
                merge)
            : postSchedule;
    final var genesis = GenesisState.fromConfig(config, schedule, new BonsaiCodeCache());
    blockchain = createInMemoryBlockchain(genesis.getBlock());
    archive = createBonsaiInMemoryWorldStateArchive(blockchain);
    genesis.writeStateTo(archive.getWorldState());
    merge.setIsPostMerge(blockchain.getChainHead().getTotalDifficulty());
    context =
        new ProtocolContext.Builder()
            .withBlockchain(blockchain)
            .withWorldStateArchive(archive)
            .withConsensusContext(
                transition ? new TransitionContext(new ConsensusContextFixture(), merge) : merge)
            .withBadBlockManager(badBlocks)
            .build();
    if (schedule instanceof TransitionProtocolSchedule transitionSchedule) {
      transitionSchedule.setProtocolContext(context);
      blockchain.observeBlockAdded(
          event -> {
            if (event.isNewCanonicalHead()) {
              merge.setIsPostMerge(blockchain.getChainHead().getTotalDifficulty());
            }
          });
    }
    manager =
        EthProtocolManagerTestBuilder.builder()
            .setProtocolSchedule(schedule)
            .setBlockchain(blockchain)
            .setWorldStateArchive(archive)
            .setGenesisConfigFile(config)
            .setGenesisState(genesis)
            .setTransactionPool(mock(TransactionPool.class))
            .setEthScheduler(new EthScheduler(1, 1, 1, 1, metrics))
            .setEthereumWireProtocolConfiguration(EthProtocolConfiguration.DEFAULT)
            .build();
    blocks.add(genesis.getBlock());
    BlockHeader previous = genesis.getBlock().getHeader();
    try (var expected = createInMemoryWorldState()) {
      for (int number = 1; number <= lastNumber; number++) {
        final boolean proofOfWork = transition && number == 1;
        final var updater = expected.updater();
        updater
            .getOrCreate(proofOfWork ? Address.ZERO : RECIPIENT)
            .incrementBalance(
                proofOfWork ? Wei.of(2_000_000_000_000_000_000L) : GWei.ONE.getAsWei());
        updater.commit();
        final var withdrawals =
            List.of(new Withdrawal(UInt64.valueOf(number), UInt64.ZERO, RECIPIENT, GWei.ONE));
        final var feeMarket = (BaseFeeMarket) schedule.getByBlockHeader(previous).getFeeMarket();
        final var header =
            new BlockHeaderTestFixture()
                .parentHash(previous.getHash())
                .number(number)
                .timestamp(number * 12L)
                .difficulty(proofOfWork ? Difficulty.ONE : Difficulty.ZERO)
                .gasLimit(previous.getGasLimit())
                .gasUsed(0)
                .stateRoot(expected.rootHash())
                .coinbase(Address.ZERO)
                .nonce(0)
                .baseFeePerGas(
                    feeMarket.computeBaseFee(
                        number,
                        previous.getBaseFee().orElse(Wei.ZERO),
                        previous.getGasUsed(),
                        previous.getGasLimit()))
                .withdrawalsRoot(proofOfWork ? null : BodyValidation.withdrawalsRoot(withdrawals))
                .buildHeader();
        blocks.add(
            new Block(
                header,
                new BlockBody(
                    List.of(),
                    List.of(),
                    proofOfWork ? Optional.empty() : Optional.of(withdrawals))));
        previous = header;
      }
    }
  }

  @Override
  public void close() throws IOException {
    manager.stop();
    archive.close();
  }
}
