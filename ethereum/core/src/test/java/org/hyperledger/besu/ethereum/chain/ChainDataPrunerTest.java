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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import jakarta.validation.constraints.NotNull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ChainDataPrunerTest {

  @Test
  public void singleChainPruning() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 0, 0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator to create blocks with BAL automatically
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    final int retention = 512;
    final int chainLength = retention + 8; // just past the pruning threshold

    gen.blockSequenceWithAccessList(genesisBlock, chainLength)
        .forEach(
            blockWithBal -> {
              final Block blk = blockWithBal.getBlock();

              // Store the generated BAL in blockchain storage
              blockWithBal
                  .getBlockAccessList()
                  .ifPresent(
                      bal -> {
                        assertThat(blk.getHeader().getBalHash()).isPresent();

                        final BlockchainStorage.Updater updater = blockchainStorage.updater();
                        updater.putBlockAccessList(blk.getHash(), bal);
                        updater.commit();
                      });

              blockchain.appendBlock(blk, gen.receipts(blk));
              long number = blk.getHeader().getNumber();

              // Verify balHash is set in the header
              assertThat(blk.getHeader().getBalHash()).isPresent();

              // Genesis block (block 0) is always kept
              assertThat(blockchain.getBlockHeader(0)).isPresent();

              if (number <= retention) {
                // No pruning has occurred yet
                assertThat(blockchain.getBlockHeader(1)).isPresent();
              } else {
                // Prune block number - retention only
                assertThat(blockchain.getBlockHeader(number - retention)).isEmpty();
                assertThat(blockchain.getBlockHeader(number - retention + 1)).isPresent();
              }
            });
  }

  @Test
  public void forkPruning() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 0, 0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    final int retention = 512;
    final int forkLength = 16;
    // need retention + forkLength + 1 canonical blocks so pruning mark covers all fork blocks
    final int canonicalLength = retention + forkLength + 3;

    List<BlockDataGenerator.BlockWithAccessList> canonicalChain =
        gen.blockSequenceWithAccessList(genesisBlock, canonicalLength);
    List<BlockDataGenerator.BlockWithAccessList> forkChain =
        gen.blockSequenceWithAccessList(genesisBlock, forkLength);

    // Store fork blocks with their BAL
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : forkChain) {
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.storeBlock(blk, gen.receipts(blk));
    }

    // Import first retention blocks of canonical chain
    for (int i = 0; i < retention; i++) {
      BlockDataGenerator.BlockWithAccessList blockWithBal = canonicalChain.get(i);
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // No pruning has occurred yet
    assertThat(blockchain.getBlockHeader(0)).isPresent();
    assertThat(blockchain.getBlockHeader(canonicalChain.get(0).getBlock().getHash())).isPresent();
    assertThat(blockchain.getBlockHeader(forkChain.get(0).getBlock().getHash())).isPresent();

    // Continue importing canonical chain past the retention threshold
    for (int i = retention; i < retention + forkLength - 1; i++) {
      BlockDataGenerator.BlockWithAccessList blockWithBal = canonicalChain.get(i);
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.appendBlock(blk, gen.receipts(blk));

      // Genesis is always kept
      assertThat(blockchain.getBlockHeader(0)).isPresent();

      if (i > retention) {
        // Prune block on canonical chain and fork for i - retention only
        assertThat(
                blockchain.getBlockHeader(canonicalChain.get(i - retention).getBlock().getHash()))
            .isEmpty();
        assertThat(blockchain.getBlockHeader(forkChain.get(i - retention).getBlock().getHash()))
            .isEmpty();
      }

      assertThat(
              blockchain.getBlockHeader(canonicalChain.get(i - retention + 1).getBlock().getHash()))
          .isPresent();
      assertThat(blockchain.getBlockHeader(forkChain.get(i - retention + 1).getBlock().getHash()))
          .isPresent();
    }
  }

  @Test
  public void balOnlyPruning() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE, // never prune blocks
                512,
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    final int retention = 512;
    final int chainLength = retention + 8; // just past the pruning threshold

    gen.blockSequenceWithAccessList(genesisBlock, chainLength)
        .forEach(
            blockWithBal -> {
              final Block blk = blockWithBal.getBlock();

              // Store the generated BAL
              blockWithBal
                  .getBlockAccessList()
                  .ifPresent(
                      bal -> {
                        assertThat(blk.getHeader().getBalHash()).isPresent();

                        final BlockchainStorage.Updater updater = blockchainStorage.updater();
                        updater.putBlockAccessList(blk.getHash(), bal);
                        updater.commit();
                      });

              blockchain.appendBlock(blk, gen.receipts(blk));
              long number = blk.getHeader().getNumber();

              // Genesis is always kept
              assertThat(blockchain.getBlockHeader(0)).isPresent();
              // Chain data should ALWAYS be present in BAL mode
              assertThat(blockchain.getBlockHeader(1)).isPresent();
              blockchain
                  .getBlockHeader(1)
                  .ifPresent(
                      header -> {
                        assertThat(blockchainStorage.getBlockBody(header.getBlockHash()))
                            .isPresent();
                        assertThat(blockchainStorage.getTransactionReceipts(header.getBlockHash()))
                            .isPresent();
                      });

              if (number > retention) {
                assertThat(blockchain.getBlockHeader(number - retention)).isPresent();
                blockchain
                    .getBlockHeader(number - retention)
                    .ifPresent(
                        oldHeader -> {
                          // Block data still exists
                          assertThat(blockchainStorage.getBlockBody(oldHeader.getBlockHash()))
                              .isPresent();
                          assertThat(
                                  blockchainStorage.getTransactionReceipts(
                                      oldHeader.getBlockHash()))
                              .isPresent();
                          // But BAL is pruned
                          assertThat(blockchainStorage.getBlockAccessList(oldHeader.getBlockHash()))
                              .isEmpty();
                        });
                assertThat(blockchain.getBlockHeader(number - retention + 1)).isPresent();
                blockchain
                    .getBlockHeader(number - retention + 1)
                    .ifPresent(
                        recentHeader -> {
                          // Recent blocks should have BAL
                          assertThat(
                                  blockchainStorage.getBlockAccessList(recentHeader.getBlockHash()))
                              .isPresent();
                        });
              }
            });
  }

  @Test
  public void pruningWithFrequency() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 256, 256, 256, 100, 0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Batch boundary derivation:
    // storedMark starts at 1 (genesis always kept). First prune fires when
    // (blockNumber - retention) - storedMark >= frequency, i.e. blockNumber >= retention +
    // frequency + 1
    final int retention = 256;
    final int frequency = 100;
    final int firstBatchAt = retention + frequency + 1; // 357
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 101
    final int chainLength =
        firstBatchAt
            + 13; // past first batch, not enough for second (needs retention + 2*freq + 1 = 457)

    List<Block> blocks = gen.blockSequence(genesisBlock, chainLength);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // At block chainLength (= firstBatchAt + 13 = 370):
    // - blockPruningMark = chainLength - retention (= 114)
    // - Genesis (block 0) is ALWAYS kept, pruning starts from block 1
    //
    // Pruning timeline:
    // block firstBatchAt (= 357): blockPruningMark = firstBatchPrunedUpTo (= 101)
    //                             blocksToBePruned = firstBatchPrunedUpTo - 1 = frequency
    //                             → first batch fires, prunes blocks 1..firstBatchPrunedUpTo
    // block chainLength (= 370):  blockPruningMark = chainLength - retention (= 114)
    //                             blocksToBePruned = 13 < frequency
    //                             → second batch not fired; needs block retention + 2*freq + 1
    // (= 457)

    // Genesis (block 0) is ALWAYS kept
    assertThat(blockchain.getBlockHeader(0)).as("Genesis block should always be kept").isPresent();

    // Blocks pruned in first batch
    for (int i = 1; i <= firstBatchPrunedUpTo; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should be pruned (first batch at block %d)", i, firstBatchAt)
          .isEmpty();
    }

    // Blocks past first batch mark but below pruning mark: pending second batch
    final int pruningMark = chainLength - retention;
    for (int i = firstBatchPrunedUpTo + 1; i <= pruningMark; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as(
              "Block %d should exist (second batch needs block %d)",
              i, retention + 2 * frequency + 1)
          .isPresent();
    }

    // Blocks within retention window
    for (int i = pruningMark + 1; i <= chainLength; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (within retention)", i)
          .isPresent();
    }
  }

  @Test
  public void balRetentionLowerThanBlockRetentionInBalMode() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE, // NEVER prune blocks
                256, // prune BALs after 256
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    // Generate 270 blocks with BAL - just past the 256-block BAL retention threshold
    gen.blockSequenceWithAccessList(genesisBlock, 270)
        .forEach(
            blockWithBal -> {
              final Block blk = blockWithBal.getBlock();

              // Store the generated BAL
              blockWithBal
                  .getBlockAccessList()
                  .ifPresent(
                      bal -> {
                        assertThat(blk.getHeader().getBalHash()).isPresent();

                        final BlockchainStorage.Updater updater = blockchainStorage.updater();
                        updater.putBlockAccessList(blk.getHash(), bal);
                        updater.commit();
                      });

              blockchain.appendBlock(blk, gen.receipts(blk));
              long number = blk.getHeader().getNumber();

              // Verify balHash is set
              assertThat(blk.getHeader().getBalHash()).isPresent();

              // Genesis (block 0) is ALWAYS kept
              assertThat(blockchain.getBlockHeader(0))
                  .as("Genesis block should always be kept")
                  .isPresent();
              // ALL blocks should always exist (never pruned in BAL mode)
              assertThat(blockchain.getBlockHeader(1)).isPresent();
              assertThat(blockchain.getBlockHeader(number)).isPresent();

              if (number > 256) {
                // BAL should be pruned for old blocks
                assertThat(blockchain.getBlockHeader(number - 256)).isPresent();
                blockchain
                    .getBlockHeader(number - 256)
                    .ifPresent(
                        oldHeader -> {
                          // Block still exists
                          assertThat(blockchainStorage.getBlockBody(oldHeader.getBlockHash()))
                              .isPresent();
                          assertThat(
                                  blockchainStorage.getTransactionReceipts(
                                      oldHeader.getBlockHash()))
                              .isPresent();

                          // But BAL is pruned
                          assertThat(blockchainStorage.getBlockAccessList(oldHeader.getBlockHash()))
                              .isEmpty();
                        });

                // Recent blocks should have BAL
                assertThat(blockchain.getBlockHeader(number - 255)).isPresent();
                blockchain
                    .getBlockHeader(number - 255)
                    .ifPresent(
                        recentHeader -> {
                          assertThat(
                                  blockchainStorage.getBlockAccessList(recentHeader.getBlockHash()))
                              .isPresent();
                        });
              }
            });
  }

  @Test
  public void equalBalAndBlockRetention() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 0, 0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);
    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    final int retention = 512;
    final int chainLength = retention + 8; // just past the pruning threshold

    gen.blockSequence(genesisBlock, chainLength)
        .forEach(
            blk -> {
              blockchain.appendBlock(blk, gen.receipts(blk));
              long number = blk.getHeader().getNumber();

              // Genesis (block 0) is ALWAYS kept
              assertThat(blockchain.getBlockHeader(0))
                  .as("Genesis block should always be kept")
                  .isPresent();
              if (number > retention) {
                assertThat(blockchain.getBlockHeader(number - retention)).isEmpty();
                assertThat(blockchain.getBlockHeader(number - retention + 1)).isPresent();
              }
            });
  }

  @Test
  public void noPruningWhenBelowRetentionThreshold() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 1000, 1000, 1000, 0, 0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.blockSequence(genesisBlock, 50)
        .forEach(
            blk -> {
              blockchain.appendBlock(blk, gen.receipts(blk));
              assertThat(blockchain.getBlockHeader(1)).isPresent();
              assertThat(blockchain.getBlockHeader(blk.getHeader().getNumber())).isPresent();
            });
  }

  @Test
  public void balPruningWithDifferentFrequency() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                256,
                Long.MAX_VALUE,
                100, // prune every 100 blocks
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    // Add BAL for genesis block
    final BlockAccessList genesisBal = gen.blockAccessList();
    final BlockchainStorage.Updater genesisUpdater = blockchainStorage.updater();
    genesisUpdater.putBlockAccessList(genesisBlock.getHash(), genesisBal);
    genesisUpdater.commit();

    final int retention = 256;
    final int frequency = 100;
    final int firstBatchAt = retention + frequency + 1; // 357
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 101
    final int chainLength = firstBatchAt + 13; // past first batch, not enough for second

    List<BlockDataGenerator.BlockWithAccessList> blocks =
        gen.blockSequenceWithAccessList(genesisBlock, chainLength);
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : blocks) {
      final Block blk = blockWithBal.getBlock();

      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                assertThat(blk.getHeader().getBalHash()).isPresent();

                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });

      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    final int secondBatchAt = retention + 2 * frequency + 1; // 457

    // Genesis (block 0): Block and BAL should ALWAYS exist
    assertThat(blockchain.getBlockHeader(0))
        .as("Genesis block should exist (BAL mode never prunes blocks)")
        .isPresent();
    assertThat(blockchainStorage.getBlockAccessList(genesisBlock.getHash()))
        .as("Genesis BAL should always be kept")
        .isPresent();

    // Blocks 1..firstBatchPrunedUpTo: BALs pruned in first batch
    for (int i = 1; i <= firstBatchPrunedUpTo; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should be pruned (first batch at block %d)", i, firstBatchAt)
          .isEmpty();
    }

    // Remaining blocks: BALs still exist (second batch needs block secondBatchAt)
    for (int i = firstBatchPrunedUpTo + 1; i <= chainLength; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i)).as("Block %d should exist", i).isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should exist (next pruning at block %d)", i, secondBatchAt)
          .isPresent();
    }
  }

  @Test
  public void balPruningWithTwoBatches() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE, // never prune blocks
                256, // keep 256 BALs
                Long.MAX_VALUE,
                100, // prune every 100 blocks
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    // Add BAL for genesis block
    final BlockAccessList genesisBal = gen.blockAccessList();
    final BlockchainStorage.Updater genesisUpdater = blockchainStorage.updater();
    genesisUpdater.putBlockAccessList(genesisBlock.getHash(), genesisBal);
    genesisUpdater.commit();

    final int retention = 256;
    final int frequency = 100;
    final int firstBatchAt = retention + frequency + 1; // 357
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 101
    final int secondBatchAt = firstBatchAt + frequency; // 457
    final int secondBatchPrunedUpTo = secondBatchAt - retention; // 201
    final int chainLength = secondBatchAt + 13; // past second batch, not enough for third

    // Generate blocks to trigger two pruning batches
    List<BlockDataGenerator.BlockWithAccessList> blocks =
        gen.blockSequenceWithAccessList(genesisBlock, chainLength);
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : blocks) {
      final Block blk = blockWithBal.getBlock();

      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                assertThat(blk.getHeader().getBalHash()).isPresent();

                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });

      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    final int pruningMark = chainLength - retention;
    final int thirdBatchAt = secondBatchAt + frequency; // 557

    // At block chainLength (= secondBatchAt + 13 = 470):
    // - balPruningMark = chainLength - retention (= 214)
    // - Genesis (block 0) BAL is ALWAYS kept, pruning starts from block 1
    //
    // Pruning timeline:
    // block firstBatchAt (= 357):  balPruningMark = firstBatchPrunedUpTo (= 101)
    //                              blocksToBePruned = firstBatchPrunedUpTo - 1 = frequency
    //                              → first batch fires, prunes BALs 1..firstBatchPrunedUpTo
    // block secondBatchAt (= 457): balPruningMark = secondBatchPrunedUpTo (= 201)
    //                              blocksToBePruned = secondBatchPrunedUpTo - firstBatchPrunedUpTo
    // = frequency
    //                              → second batch fires, prunes BALs
    // firstBatchPrunedUpTo+1..secondBatchPrunedUpTo
    // block chainLength (= 470):   balPruningMark = pruningMark (= 214)
    //                              blocksToBePruned = pruningMark - secondBatchPrunedUpTo = 13 <
    // frequency
    //                              → third batch not fired; needs block thirdBatchAt (= 557)

    // Genesis (block 0): Block and BAL should ALWAYS exist
    assertThat(blockchain.getBlockHeader(0))
        .as("Genesis block should exist (BAL mode never prunes blocks)")
        .isPresent();
    assertThat(blockchainStorage.getBlockAccessList(genesisBlock.getHash()))
        .as("Genesis BAL should always be kept")
        .isPresent();

    // First batch: BALs pruned
    for (int i = 1; i <= firstBatchPrunedUpTo; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should be pruned (first batch at block %d)", i, firstBatchAt)
          .isEmpty();
    }

    // Second batch: BALs pruned
    for (int i = firstBatchPrunedUpTo + 1; i <= secondBatchPrunedUpTo; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should be pruned (second batch at block %d)", i, secondBatchAt)
          .isEmpty();
    }

    // Past second batch mark but not enough for third: pending
    for (int i = secondBatchPrunedUpTo + 1; i <= pruningMark; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i)).as("Block %d should exist", i).isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should exist (third batch needs block %d)", i, thirdBatchAt)
          .isPresent();
    }

    // Within retention window
    for (int i = pruningMark + 1; i <= chainLength; i++) {
      final Block block = blocks.get(i - 1).getBlock();
      assertThat(blockchain.getBlockHeader(i)).as("Block %d should exist", i).isPresent();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should exist (within retention of %d blocks)", i, retention)
          .isPresent();
    }
  }

  @Test
  public void forkBlocksRemovedInBalOnlyMode() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final InMemoryKeyValueStorage storage = new InMemoryKeyValueStorage();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            storage,
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final InMemoryKeyValueStorage prunerKvStorage = new InMemoryKeyValueStorage();
    final ChainDataPrunerStorage prunerStorage = new ChainDataPrunerStorage(prunerKvStorage);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            prunerStorage,
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE, // never prune blocks
                256, // prune BALs after 256
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    // Create canonical chain and fork - need 272+ blocks so pruningMark(=chain-256) covers all 16
    // fork blocks
    List<BlockDataGenerator.BlockWithAccessList> canonicalChain =
        gen.blockSequenceWithAccessList(genesisBlock, 280);
    List<BlockDataGenerator.BlockWithAccessList> forkChain =
        gen.blockSequenceWithAccessList(genesisBlock, 16);

    // Store fork blocks
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : forkChain) {
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.storeBlock(blk, gen.receipts(blk));
    }

    // Import canonical chain
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : canonicalChain) {
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // At block 280, balPruningMark = 280 - 256 = 24
    // Both canonical and fork blocks should have:
    // - Blocks still present (BAL mode never prunes blocks)
    // - BALs pruned for blocks <= 24
    // - Fork blocks metadata removed for blocks <= 24

    // Verify fork blocks 1-16 (all should be pruned since 16 < 24)
    for (int i = 1; i <= 16; i++) {
      final Block forkBlock = forkChain.get(i - 1).getBlock();
      final Block canonicalBlock = canonicalChain.get(i - 1).getBlock();

      // Blocks should still exist (BAL mode doesn't prune blocks)
      assertThat(blockchain.getBlockByHash(forkBlock.getHash()))
          .as("Fork block %d should still exist in BAL mode", i)
          .isPresent();
      assertThat(blockchain.getBlockByHash(canonicalBlock.getHash()))
          .as("Canonical block %d should still exist", i)
          .isPresent();

      // BALs should be pruned (since i <= 24, the pruning mark)
      assertThat(blockchainStorage.getBlockAccessList(forkBlock.getHash()))
          .as("Fork block %d BAL should be pruned", i)
          .isEmpty();
      assertThat(blockchainStorage.getBlockAccessList(canonicalBlock.getHash()))
          .as("Canonical block %d BAL should be pruned", i)
          .isEmpty();

      // Fork blocks metadata should be removed
      assertThat(prunerStorage.getForkBlocks(i))
          .as("Fork blocks metadata for block %d should be removed in BAL mode", i)
          .isEmpty();
    }

    // Verify blocks 25-280 still have BALs and fork blocks metadata
    for (int i = 25; i <= 280; i++) {
      final Block canonicalBlock = canonicalChain.get(i - 1).getBlock();
      assertThat(blockchainStorage.getBlockAccessList(canonicalBlock.getHash()))
          .as("Canonical block %d BAL should exist (within retention)", i)
          .isPresent();
      assertThat(prunerStorage.getForkBlocks(i))
          .as("Fork blocks metadata for block %d should be present in BAL mode", i)
          .isNotEmpty();
    }
  }

  @Test
  public void forkBlocksRemovedOnlyWithChainDataInAllMode() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final InMemoryKeyValueStorage storage = new InMemoryKeyValueStorage();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            storage,
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final InMemoryKeyValueStorage prunerKvStorage = new InMemoryKeyValueStorage();
    final ChainDataPrunerStorage prunerStorage = new ChainDataPrunerStorage(prunerKvStorage);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            prunerStorage,
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                400, // prune blocks after 400
                200, // prune BALs after 200
                200,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Configure generator with BAL generation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    // Create canonical chain and fork
    List<BlockDataGenerator.BlockWithAccessList> canonicalChain =
        gen.blockSequenceWithAccessList(genesisBlock, 500);
    List<BlockDataGenerator.BlockWithAccessList> forkChain =
        gen.blockSequenceWithAccessList(genesisBlock, 150);

    // Store fork blocks
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : forkChain) {
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.storeBlock(blk, gen.receipts(blk));
    }

    // Import canonical chain
    for (BlockDataGenerator.BlockWithAccessList blockWithBal : canonicalChain) {
      final Block blk = blockWithBal.getBlock();
      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // At block 500:
    // blockPruningMark = 500 - 400 = 100
    // balPruningMark = 500 - 200 = 300

    // Verify blocks 1-100: Everything pruned including fork blocks
    for (int i = 1; i <= 100; i++) {
      final Block canonicalBlock = canonicalChain.get(i - 1).getBlock();

      // Chain data should be pruned
      assertThat(blockchain.getBlockByHash(canonicalBlock.getHash()))
          .as("Canonical block %d should be pruned", i)
          .isEmpty();

      // Fork blocks metadata should be removed (blocks are pruned)
      assertThat(prunerStorage.getForkBlocks(i))
          .as("Fork blocks metadata for block %d should be removed (blocks pruned)", i)
          .isEmpty();
    }

    // Verify blocks 101-150: Blocks exist, BALs pruned, but fork blocks metadata still present
    for (int i = 101; i <= 150; i++) {
      final Block forkBlock = forkChain.get(i - 1).getBlock();
      final Block canonicalBlock = canonicalChain.get(i - 1).getBlock();

      // Blocks should still exist (101-150 < blockPruningMark + retention)
      assertThat(blockchain.getBlockByHash(canonicalBlock.getHash()))
          .as("Canonical block %d should exist (within block retention)", i)
          .isPresent();
      assertThat(blockchain.getBlockByHash(forkBlock.getHash()))
          .as("Fork block %d should exist (within block retention)", i)
          .isPresent();

      // BALs should be pruned (101-150 <= balPruningMark = 300)
      assertThat(blockchainStorage.getBlockAccessList(canonicalBlock.getHash()))
          .as("Canonical block %d BAL should be pruned", i)
          .isEmpty();
      assertThat(blockchainStorage.getBlockAccessList(forkBlock.getHash()))
          .as("Fork block %d BAL should be pruned", i)
          .isEmpty();

      // Fork blocks metadata should STILL exist (blocks not pruned yet)
      assertThat(prunerStorage.getForkBlocks(i))
          .as("Fork blocks metadata for block %d should exist (blocks not pruned yet)", i)
          .isNotEmpty();
    }

    // Verify blocks 301-500: Everything should exist
    for (int i = 301; i <= 500; i++) {
      final Block canonicalBlock = canonicalChain.get(i - 1).getBlock();
      assertThat(blockchain.getBlockByHash(canonicalBlock.getHash()))
          .as("Canonical block %d should exist", i)
          .isPresent();
      assertThat(blockchainStorage.getBlockAccessList(canonicalBlock.getHash()))
          .as("Canonical block %d BAL should exist", i)
          .isPresent();
    }
  }

  @Test
  public void pruningBeforeBalActivation() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                256,
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Disable BAL generation for pre-activation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());

    // Generate 50 blocks WITHOUT BAL (pre-activation)
    List<Block> blocks = gen.blockSequence(genesisBlock, 50);
    for (Block blk : blocks) {
      // No BAL added - simulating pre-BAL activation
      blockchain.appendBlock(blk, gen.receipts(blk));

      // Verify no balHash is set
      assertThat(blk.getHeader().getBalHash()).isEmpty();
    }

    // All blocks should exist (BAL mode never prunes blocks)
    for (int i = 0; i <= 50; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
    }
  }

  @Test
  public void balPruningTransitionFromNoBalToBalActivated() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                100, // Keep 100 BALs
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Phase 1: Generate 20 blocks WITHOUT BAL (simulating pre-activation)
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());
    List<Block> preBalBlocks = gen.blockSequence(genesisBlock, 20);
    for (Block blk : preBalBlocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
      assertThat(blk.getHeader().getBalHash()).isEmpty();
    }

    // Phase 2: BAL activation at block 21 - generate 130 more blocks WITH BAL
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    List<BlockDataGenerator.BlockWithAccessList> postBalBlocks =
        gen.blockSequenceWithAccessList(preBalBlocks.get(19), 130);

    for (BlockDataGenerator.BlockWithAccessList blockWithBal : postBalBlocks) {
      final Block blk = blockWithBal.getBlock();

      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                assertThat(blk.getHeader().getBalHash()).isPresent();

                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });

      blockchain.appendBlock(blk, gen.receipts(blk));

      // Verify balHash is set for post-activation blocks
      assertThat(blk.getHeader().getBalHash()).isPresent();
    }

    // At block 150 (20 pre-BAL + 130 post-BAL):
    // - We want to keep 100 BALs (blocks 51-150)
    // - Blocks 1-20 never had BAL (should be skipped)
    // - Blocks 21-50 had BAL but should be pruned (balPruningMark = 150 - 100 = 50)

    // All blocks should still exist (BAL mode never prunes blocks)
    for (int i = 0; i <= 150; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
    }

    // Blocks 1-20: No BAL to prune (never existed)
    for (int i = 1; i <= 20; i++) {
      final Block block = preBalBlocks.get(i - 1);
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("Block %d never had BAL", i)
          .isEmpty();
    }

    // Blocks 21-50: BAL should be pruned (outside retention window)
    for (int i = 21; i <= 50; i++) {
      final Block block = postBalBlocks.get(i - 21).getBlock();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should be pruned (outside retention)", i)
          .isEmpty();
    }

    // Blocks 51-150: BAL should exist (within retention window)
    for (int i = 51; i <= 150; i++) {
      final Block block = postBalBlocks.get(i - 21).getBlock();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should exist (within retention)", i)
          .isPresent();
    }
  }

  @Test
  public void allModePruningBeforeBalActivation() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                100, // Keep 100 blocks
                100, // Keep 100 BALs
                100,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Generate 200 blocks WITHOUT BAL (pre-activation)
    List<Block> blocks = gen.blockSequence(genesisBlock, 200);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // Genesis should exist
    assertThat(blockchain.getBlockHeader(0)).isPresent();

    // Blocks 1-100: Should be pruned (outside retention)
    for (int i = 1; i <= 100; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should be pruned (outside retention)", i)
          .isEmpty();
    }

    // Blocks 101-200: Should exist (within retention)
    for (int i = 101; i <= 200; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (within retention)", i)
          .isPresent();
    }
  }

  @Test
  public void allModePruningTransitionFromNoBalToBalActivated() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                50, // Keep 50 blocks
                100, // Keep 100 BALs (more than blocks)
                50,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Phase 1: Generate 100 blocks WITHOUT BAL
    List<Block> preBalBlocks = gen.blockSequence(genesisBlock, 100);
    for (Block blk : preBalBlocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // Phase 2: BAL activation - generate 100 more blocks WITH BAL
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    List<BlockDataGenerator.BlockWithAccessList> postBalBlocks =
        gen.blockSequenceWithAccessList(preBalBlocks.get(99), 100);

    for (BlockDataGenerator.BlockWithAccessList blockWithBal : postBalBlocks) {
      final Block blk = blockWithBal.getBlock();

      blockWithBal
          .getBlockAccessList()
          .ifPresent(
              bal -> {
                assertThat(blk.getHeader().getBalHash()).isPresent();

                final BlockchainStorage.Updater updater = blockchainStorage.updater();
                updater.putBlockAccessList(blk.getHash(), bal);
                updater.commit();
              });

      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // At block 200:
    // - Keep blocks 151-200 (50 blocks retained)
    // - Keep BALs from blocks 101-200 (100 BALs retained)
    // - Blocks 1-150 should be pruned
    // - Blocks 101-150 pruned as blocks but BAL never existed

    // Genesis always kept
    assertThat(blockchain.getBlockHeader(0)).isPresent();

    // Blocks 1-150: Should be pruned
    for (int i = 1; i <= 150; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should be pruned (outside retention)", i)
          .isEmpty();
    }

    // Blocks 151-200: Should exist
    for (int i = 151; i <= 200; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (within retention)", i)
          .isPresent();
    }

    // Blocks 101-150: Were pruned as blocks, so no BAL check possible

    // Blocks 151-200: BAL should exist (within retention)
    for (int i = 151; i <= 200; i++) {
      final Block block = postBalBlocks.get(i - 101).getBlock();
      assertThat(blockchainStorage.getBlockAccessList(block.getHash()))
          .as("BAL for block %d should exist (within retention)", i)
          .isPresent();
    }
  }

  @Test
  public void balPruningMarkAdvancesAutomaticallyWhenNoBalPresent() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);

    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());

    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            prunerStorage,
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                256,
                Long.MAX_VALUE,
                100,
                0),
            new BlockingExecutor());

    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Add blocks without BAL - marker should advance automatically
    List<Block> blocks = gen.blockSequence(genesisBlock, 50);
    for (int i = 0; i < 50; i++) {
      Block blk = blocks.get(i);
      blockchain.appendBlock(blk, gen.receipts(blk));

      // Verify BAL pruning mark advances with each block added
      assertThat(prunerStorage.getBalPruningMark())
          .as("BAL pruning mark should advance to block %d", i + 1)
          .isPresent()
          .hasValue((long) i + 1);
    }

    // All blocks should still exist
    for (int i = 0; i <= 50; i++) {
      assertThat(blockchain.getBlockHeader(i)).isPresent();
    }
  }

  @Test
  public void noBalPruningWarningBeforeGlamsterdam() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                256,
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());

    final List<LogEvent> capturedEvents =
        withLogCapture(
            ChainDataPruner.class,
            () -> {
              List<Block> canonicalChain = gen.blockSequence(genesisBlock, 258);
              List<Block> forkChain = gen.blockSequence(genesisBlock, 16);

              for (Block blk : canonicalChain) {
                blockchain.appendBlock(blk, gen.receipts(blk));
              }

              // Fork blocks at lower heights: blockNumber < storedBalPruningMark,
              // but no warning expected because BAL is not activated yet.
              for (Block blk : forkChain) {
                blockchain.storeBlock(blk, gen.receipts(blk));
              }
            });

    assertThat(
            capturedEvents.stream()
                .filter(e -> e.getLevel().equals(Level.WARN))
                .map(e -> e.getMessage().getFormattedMessage())
                .anyMatch(msg -> msg.contains("is less than BAL pruning mark")))
        .as("No BAL pruning warning should be emitted for pre-Glamsterdam blocks")
        .isFalse();
  }

  @Test
  public void balPruningWarningAfterGlamsterdam() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            0,
            ChainDataPruner.PruningMode.CHAIN_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                256,
                Long.MAX_VALUE,
                0,
                0),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    final List<LogEvent> capturedEvents =
        withLogCapture(
            ChainDataPruner.class,
            () -> {
              List<BlockDataGenerator.BlockWithAccessList> canonicalChain =
                  gen.blockSequenceWithAccessList(genesisBlock, 258);
              List<BlockDataGenerator.BlockWithAccessList> forkChain =
                  gen.blockSequenceWithAccessList(genesisBlock, 16);

              for (BlockDataGenerator.BlockWithAccessList blockWithBal : canonicalChain) {
                final Block blk = blockWithBal.getBlock();
                blockWithBal
                    .getBlockAccessList()
                    .ifPresent(
                        bal -> {
                          final BlockchainStorage.Updater updater = blockchainStorage.updater();
                          updater.putBlockAccessList(blk.getHash(), bal);
                          updater.commit();
                        });
                blockchain.appendBlock(blk, gen.receipts(blk));
              }

              // Fork blocks with BAL at lower heights (1-16).
              // blockNumber < storedBalPruningMark → warning expected.
              for (BlockDataGenerator.BlockWithAccessList blockWithBal : forkChain) {
                final Block blk = blockWithBal.getBlock();
                blockWithBal
                    .getBlockAccessList()
                    .ifPresent(
                        bal -> {
                          final BlockchainStorage.Updater updater = blockchainStorage.updater();
                          updater.putBlockAccessList(blk.getHash(), bal);
                          updater.commit();
                        });
                blockchain.storeBlock(blk, gen.receipts(blk));
              }
            });

    assertThat(
            capturedEvents.stream()
                .filter(e -> e.getLevel().equals(Level.WARN))
                .map(e -> e.getMessage().getFormattedMessage())
                .anyMatch(msg -> msg.contains("is less than BAL pruning mark")))
        .as("BAL pruning warning should be emitted for post-Glamsterdam fork blocks")
        .isTrue();
  }

  @Test
  public void testPreMergePruningAction() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    gen.blockSequence(genesisBlock, 20)
        .forEach((block) -> blockchain.appendBlock(block, gen.receipts(block)));
    final int mergeBlock = 11;
    final int pruningQuantity = 6;
    // Chain now has 20 blocks, including the genesis block
    Assertions.assertEquals(20, blockchain.getChainHeadBlockNumber());

    // Set up the pruner to prune blocks 1 to 10 in batches of 6
    ChainDataPruner pruner =
        new ChainDataPruner(
            blockchainStorage,
            () -> {},
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            mergeBlock,
            ChainDataPruner.PruningMode.PRE_MERGE_PRUNING,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 0, 0, 0, 0, pruningQuantity),
            new BlockingExecutor());

    BlockAddedEvent blockAddedEvent = Mockito.mock(BlockAddedEvent.class);
    Mockito.when(blockAddedEvent.isNewCanonicalHead()).thenReturn(true);

    // On the first prune, we're expecting blocks 1 to 6 to be removed (full pruning batch size)
    pruner.onBlockAdded(blockAddedEvent);

    checkBlocks(blockchain, 1, pruningQuantity, Optional::isEmpty);
    checkBlocks(
        blockchain, pruningQuantity + 1, blockchain.getChainHeadBlockNumber(), Optional::isPresent);

    // On the second prune, we're expecting blocks 7 to 10 to be removed (limited by merge block)
    pruner.onBlockAdded(blockAddedEvent);

    checkBlocks(blockchain, 1, mergeBlock - 1, Optional::isEmpty);
    checkBlocks(blockchain, mergeBlock, blockchain.getChainHeadBlockNumber(), Optional::isPresent);
  }

  /**
   * Attaches a temporary log4j appender to the given class's logger, runs the action, and returns
   * all captured log events. The appender is properly stopped and removed regardless of outcome.
   */
  @SuppressWarnings("BannedMethod")
  private static List<LogEvent> withLogCapture(final Class<?> loggerClass, final Runnable action) {
    final Logger logger = (Logger) LogManager.getLogger(loggerClass);
    final List<LogEvent> events = new CopyOnWriteArrayList<>();
    final AbstractAppender appender =
        new AbstractAppender("test-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    return events;
  }

  /**
   * Helper method to check if blocks in a range satisfy a given predicate.
   *
   * @param blockchain the blockchain to query
   * @param start the starting block number (inclusive)
   * @param end the ending block number (inclusive)
   * @param test the predicate to test each block against
   */
  private void checkBlocks(
      final Blockchain blockchain,
      final long start,
      final long end,
      final Predicate<Optional<Block>> test) {
    for (long prunedBlockNumber = start; prunedBlockNumber <= end; prunedBlockNumber++) {
      blockchain
          .getBlockHeader(prunedBlockNumber)
          .ifPresentOrElse(
              (blockHeader) ->
                  Assertions.assertTrue(
                      test.test(blockchain.getBlockByHash(blockHeader.getBlockHash()))),
              () -> Assertions.fail("Failed to find header expected to exist"));
    }
  }

  /** Blocking executor for testing purposes. Executes tasks synchronously in the calling thread. */
  protected static class BlockingExecutor extends AbstractExecutorService {
    @Override
    public void shutdown() {}

    @NotNull
    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return true;
    }

    @Override
    public boolean isTerminated() {
      return true;
    }

    @Override
    public boolean awaitTermination(final long timeout, final @NotNull TimeUnit unit) {
      return true;
    }

    @Override
    public void execute(final @NotNull Runnable command) {
      command.run();
    }
  }
}
