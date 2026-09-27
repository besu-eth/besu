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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.NotNull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 0, 0),
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
                assertThat(blockchain.getBlockHeader(number - retention)).isPresent();
                assertThat(blockchain.getBlockByNumber(number - retention)).isEmpty();
                assertThat(blockchain.getBlockByNumber(number - retention + 1)).isPresent();
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 0, 0),
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
        // The canonical header survives; the fork header at the same height does not.
        assertThat(
                blockchain.getBlockHeader(canonicalChain.get(i - retention).getBlock().getHash()))
            .isPresent();
        assertThat(blockchain.getBlockBody(canonicalChain.get(i - retention).getBlock().getHash()))
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 512, Long.MAX_VALUE, 0, 0),
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 256, 256, 100, 0),
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
      assertThat(blockchain.getBlockByNumber(i))
          .as("Block %d body should be pruned (first batch at block %d)", i, firstBatchAt)
          .isEmpty();
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d header should be retained after pruning", i)
          .isPresent();
    }

    // Blocks past first batch mark but below pruning mark: pending second batch
    final int pruningMark = chainLength - retention;
    for (int i = firstBatchPrunedUpTo + 1; i <= pruningMark; i++) {
      assertThat(blockchain.getBlockByNumber(i))
          .as(
              "Block %d should exist (second batch needs block %d)",
              i, retention + 2 * frequency + 1)
          .isPresent();
    }

    // Blocks within retention window
    for (int i = pruningMark + 1; i <= chainLength; i++) {
      assertThat(blockchain.getBlockByNumber(i))
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 0, 0),
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
                assertThat(blockchain.getBlockHeader(number - retention)).isPresent();
                assertThat(blockchain.getBlockByNumber(number - retention)).isEmpty();
                assertThat(blockchain.getBlockByNumber(number - retention + 1)).isPresent();
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 1000, 1000, 0, 0),
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 256, Long.MAX_VALUE, 100, 0),
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
                256, // keep 256 BALs
                Long.MAX_VALUE,
                100,
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
    // - pruningMark = chainLength - retention (= 214)
    // - Genesis (block 0) BAL is ALWAYS kept, pruning starts from block 1
    //
    // Pruning timeline:
    // block firstBatchAt (= 357):  pruningMark = firstBatchPrunedUpTo (= 101)
    //                              blocksToBePruned = firstBatchPrunedUpTo - 1 = frequency
    //                              → first batch fires, prunes BALs 1..firstBatchPrunedUpTo
    // block secondBatchAt (= 457): pruningMark = secondBatchPrunedUpTo (= 201)
    //                              blocksToBePruned = secondBatchPrunedUpTo - firstBatchPrunedUpTo
    // = frequency
    //                              → second batch fires, prunes BALs
    // firstBatchPrunedUpTo+1..secondBatchPrunedUpTo
    // block chainLength (= 470):   pruningMark = 214
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 256, Long.MAX_VALUE, 0, 0),
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
  public void balOnlyModeWritesNoMarkBeforeBalActivation() {
    // The early return must not advance the mark: with one mark, doing so would make a later
    // switch to ALL skip every block below it.
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
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 10, Long.MAX_VALUE, 0, 0),
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    // Disable BAL generation for pre-activation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());

    // Generate 50 blocks WITHOUT BAL (pre-activation), well past the 10-block retention
    List<Block> blocks = gen.blockSequence(genesisBlock, 50);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
      assertThat(blk.getHeader().getBalHash()).isEmpty();
    }

    assertThat(prunerStorage.getChainPruningMark()).isEmpty();

    // All blocks should exist (BAL mode never prunes blocks, and no mark was ever written)
    for (int i = 0; i <= 50; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
    }
  }

  @Test
  public void recordsForkBlocksInBothModes() {
    // With a single mark, the sweep clears fork-index entries in both modes, so both modes can
    // safely record them: there is no longer a mode that needs the entries to leak.
    final BlockDataGenerator allModeGen = new BlockDataGenerator();
    final BlockchainStorage allModeBlockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage allModePrunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final ChainDataPruner allModePruner =
        new ChainDataPruner(
            allModeBlockchainStorage,
            allModePrunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, Long.MAX_VALUE, Long.MAX_VALUE, 0, 0),
            new BlockingExecutor());
    final Block allModeGenesisBlock = allModeGen.genesisBlock();
    final MutableBlockchain allModeBlockchain =
        DefaultBlockchain.createMutable(
            allModeGenesisBlock, allModeBlockchainStorage, new NoOpMetricsSystem(), 0);
    allModeBlockchain.observeBlockAdded(allModePruner);

    allModeGen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());

    allModeGen
        .blockSequence(allModeGenesisBlock, 20)
        .forEach(blk -> allModeBlockchain.appendBlock(blk, allModeGen.receipts(blk)));

    for (int i = 1; i <= 20; i++) {
      assertThat(allModePrunerStorage.getForkBlocks(i))
          .as("ALL mode should record a fork-index entry for block %d", i)
          .isNotEmpty();
    }

    final BlockDataGenerator balModeGen = new BlockDataGenerator();
    final BlockchainStorage balModeBlockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage balModePrunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final ChainDataPruner balModePruner =
        new ChainDataPruner(
            balModeBlockchainStorage,
            balModePrunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, Long.MAX_VALUE, Long.MAX_VALUE, 0, 0),
            new BlockingExecutor());
    final Block balModeGenesisBlock = balModeGen.genesisBlock();
    final MutableBlockchain balModeBlockchain =
        DefaultBlockchain.createMutable(
            balModeGenesisBlock, balModeBlockchainStorage, new NoOpMetricsSystem(), 0);
    balModeBlockchain.observeBlockAdded(balModePruner);

    balModeGen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    balModeGen
        .blockSequenceWithAccessList(balModeGenesisBlock, 50)
        .forEach(
            blockWithBal -> {
              final Block blk = blockWithBal.getBlock();
              assertThat(blk.getHeader().getBalHash()).isPresent();
              blockWithBal
                  .getBlockAccessList()
                  .ifPresent(
                      bal -> {
                        final BlockchainStorage.Updater updater =
                            balModeBlockchainStorage.updater();
                        updater.putBlockAccessList(blk.getHash(), bal);
                        updater.commit();
                      });
              balModeBlockchain.appendBlock(blk, balModeGen.receipts(blk));
            });

    for (int i = 1; i <= 50; i++) {
      assertThat(balModePrunerStorage.getForkBlocks(i))
          .as("BAL mode should record a fork-index entry for block %d", i)
          .isNotEmpty();
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL,
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
    // - Blocks 21-50 had BAL but should be pruned (pruningMark = 150 - 100 = 50)

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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                100, // Keep 100 blocks and BALs
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
      assertThat(blockchain.getBlockByNumber(i))
          .as("Block %d body should be pruned (outside retention)", i)
          .isEmpty();
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d header should be retained after pruning", i)
          .isPresent();
    }

    // Blocks 101-200: Should exist (within retention)
    for (int i = 101; i <= 200; i++) {
      assertThat(blockchain.getBlockByNumber(i))
          .as("Block %d should exist (within retention)", i)
          .isPresent();
    }
  }

  @Test
  public void pruningSlackRetainsMoreBlocksThanAreAdvertised() {
    // retained 100, slack 50: at head 200 deletes to 200-100-50=50, advertises 200-100+1=101.
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                100, // Keep 100 blocks and BALs (before slack)
                100,
                0,
                50 /* pruneSlackBlocks */),
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    List<Block> blocks = gen.blockSequence(genesisBlock, 200);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(head).isEqualTo(200L);

    assertThat(blockchain.getBlockByNumber(50)).isEmpty();
    assertThat(blockchain.getBlockByNumber(51)).isPresent();
    // Advertised above the lowest block still held, which is the point of the slack.
    assertThat(chainDataPruner.getEarliestRetainedBlock(head)).contains(101L);
  }

  @Test
  public void pruningSlackAppliesInBalOnlyMode() {
    // BAL-only mode applies the slack to the single pruning mark just like ALL mode does; this
    // confirms the slack shows up here too. BALs retained 100, slack 50: at head 200 deleted to
    // 50.
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 100, Long.MAX_VALUE, 0, 50),
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    gen.blockSequenceWithAccessList(genesisBlock, 200)
        .forEach(
            blockWithBal -> {
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
            });

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(200L);

    final Hash prunedBal = blockchain.getBlockHashByNumber(50).orElseThrow();
    final Hash retainedBal = blockchain.getBlockHashByNumber(51).orElseThrow();

    assertThat(blockchainStorage.getBlockAccessList(prunedBal)).isEmpty();
    // Without the slack on the pruning mark it would be 100 and this BAL would be gone too.
    assertThat(blockchainStorage.getBlockAccessList(retainedBal)).isPresent();
    // BAL-only mode never touches block data.
    assertThat(blockchain.getBlockByNumber(50)).isPresent();
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 256, Long.MAX_VALUE, 0, 0),
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

              // Fork blocks at lower heights: pre-activation BAL mode writes no mark, so the
              // stored mark stays at its default and no block number can be below it.
              for (Block blk : forkChain) {
                blockchain.storeBlock(blk, gen.receipts(blk));
              }
            });

    assertThat(
            capturedEvents.stream()
                .filter(e -> e.getLevel().equals(Level.WARN))
                .map(e -> e.getMessage().getFormattedMessage())
                .anyMatch(msg -> msg.contains("is less than the pruning mark")))
        .as("No pruning-mark warning should be emitted for pre-Glamsterdam blocks")
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
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 256, Long.MAX_VALUE, 0, 0),
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
              // blockNumber < storedPruningMark → warning expected.
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
                .anyMatch(msg -> msg.contains("is less than the pruning mark")))
        .as("Pruning-mark warning should be emitted for post-Glamsterdam fork blocks")
        .isTrue();
  }

  @Test
  public void keepsTheBackfillFloorReadAPruningRunAgo() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner chainDataPruner =
        backfillPruner(blockchainStorage, ChainDataPruner.ChainPruningStrategy.ALL);
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.blockSequence(genesisBlock, 40)
        .forEach(
            blk -> {
              blockchain.appendBlock(blk, gen.receipts(blk));
              // a back-fill that read its floor a run ago, 4 blocks, stores that block now
              final long floor = chainDataPruner.lowestToBackfill(blk.getHeader().getNumber() - 4);
              assertThat(blockchain.getBlockByNumber(floor)).isPresent();
            });
    // a run later, the pruner keeps the blocks from 44 - 10 + 1
    assertThat(chainDataPruner.lowestToBackfill(40)).isEqualTo(35);
  }

  @Test
  public void leavesNoMarginWhenOnlyBlockAccessListsArePruned() {
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPruner balPruner =
        backfillPruner(blockchainStorage, ChainDataPruner.ChainPruningStrategy.BAL);

    // pruning the block access lists only keeps every body
    assertThat(balPruner.lowestToBackfill(1_000)).isZero();
  }

  // keeps 10 blocks, runs every 4 blocks
  private static ChainDataPruner backfillPruner(
      final BlockchainStorage blockchainStorage,
      final ChainDataPruner.ChainPruningStrategy strategy) {
    return new ChainDataPruner(
        blockchainStorage,
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
        new ChainPrunerConfiguration(strategy, 10, 10, 4, 0),
        new BlockingExecutor());
  }

  @Test
  public void prunesBlocksImportedWithoutBlockAddedEvents() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);

    final int importedWithoutObserver = 40;
    gen.blockSequence(genesisBlock, importedWithoutObserver)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final int retention = 8;
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, retention, retention, 0, 0),
            new BlockingExecutor());
    blockchain.observeBlockAdded(chainDataPruner);

    gen.blockSequence(blockchain.getChainHeadBlock(), 1)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(blockchain.getBlockHeader(0)).isPresent();
    assertThat(blockchain.getBlockHeader(head - retention)).isPresent();
    assertThat(blockchain.getBlockByNumber(head - retention)).isEmpty();
    assertThat(blockchain.getBlockByNumber(head - retention + 1)).isPresent();
  }

  @Test
  public void prunesBlocksDrivenBySyncImportHookWithoutObserverEvents() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);

    final int importedWithoutObserver = 40;
    gen.blockSequence(genesisBlock, importedWithoutObserver)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final int retention = 8;
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, retention, retention, 0, 0),
            new BlockingExecutor());

    // The snap-sync unsafe import path never fires BlockAddedEvents, so the pruner is not
    // registered as an observer. Catch-up pruning is driven by pruneForSyncedHead from the
    // sync pipeline (ImportSyncBlocksStep) after each unsafe batch commits.
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadBlock().getHeader());

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(blockchain.getBlockHeader(0)).isPresent();
    assertThat(blockchain.getBlockHeader(head - retention)).isPresent();
    assertThat(blockchain.getBlockByNumber(head - retention)).isEmpty();
    assertThat(blockchain.getBlockByNumber(head - retention + 1)).isPresent();
  }

  @Test
  public void doesNotInitializePruningMarkToHeadOnFirstEvent() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);

    gen.blockSequence(genesisBlock, 30)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final int retention = 10;
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, retention, retention, 0, 0),
            new BlockingExecutor());
    blockchain.observeBlockAdded(chainDataPruner);

    gen.blockSequence(blockchain.getChainHeadBlock(), 1)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(prunerStorage.getChainPruningMark()).hasValue(head - retention);
    assertThat(blockchain.getBlockHeader(head)).isPresent();
    assertThat(blockchain.getBlockHeader(head - retention)).isPresent();
    assertThat(blockchain.getBlockByNumber(head - retention)).isEmpty();
  }

  @Test
  public void allModeStartsFromTheFirstBlockWithABodyNotAStaleMark() {
    // A mark left by BAL-only pruning says "swept to here" for access lists, not for block data.
    // ALL mode must derive its start point from the data instead of trusting it.
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final KeyValueStorageTransaction tx = prunerStorage.startTransaction();
    prunerStorage.setChainPruningMark(tx, 400);
    tx.commit();

    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 100, 100, 0, 0),
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);

    gen.blockSequence(genesisBlock, 500)
        .forEach(blk -> blockchain.appendBlock(blk, gen.receipts(blk)));

    // Bodies were never pruned, so the derived start is genesis and everything below
    // head - retained is swept despite the stale mark.
    assertThat(blockchain.getBlockByNumber(1)).isEmpty();
    assertThat(blockchain.getBlockHeader(1)).isPresent();
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

  private static BlockchainStorage inMemoryBlockchainStorage() {
    return new KeyValueStoragePrefixedKeyBlockchainStorage(
        new InMemoryKeyValueStorage(),
        new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
        new MainnetBlockHeaderFunctions(),
        false);
  }

  private static ChainDataPruner pruner(
      final ChainDataPrunerStorage prunerStorage, final ChainPrunerConfiguration config) {
    return new ChainDataPruner(
        inMemoryBlockchainStorage(), prunerStorage, config, new BlockingExecutor());
  }

  @Test
  public void earliestRetainedBlockInAllModeIsHeadMinusRetained() {
    // retained 512, slack 0, nothing pruned yet: bound = head - retained + 1
    // = 2000 - 512 + 1 = 1489
    final ChainDataPruner chainDataPruner =
        pruner(
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 10, 0));

    assertThat(chainDataPruner.getEarliestRetainedBlock(2000)).contains(1489L);
  }

  @Test
  public void earliestRetainedBlockInAllModeIsAtLeastStoredMarkPlusOne() {
    // Operator raised retention after blocks <= 1800 were already deleted; the stored mark
    // must win over the bound (2000 - 512 + 1 = 1489 < 1801).
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final KeyValueStorageTransaction tx = prunerStorage.startTransaction();
    prunerStorage.setChainPruningMark(tx, 1800);
    tx.commit();
    final ChainDataPruner chainDataPruner =
        pruner(
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 10, 0));

    assertThat(chainDataPruner.getEarliestRetainedBlock(2000)).contains(1801L);
  }

  @Test
  public void earliestRetainedBlockNeverReportsBelowGenesisOnUnprunedChain() {
    // Large retention (10_000) with head 100 makes the bound negative:
    // 100 - 10_000 + 1 = -9899. Nothing pruned yet, so the floor is genesis (0).
    final ChainDataPruner chainDataPruner =
        pruner(
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 10_000, 10_000, 10, 0));

    assertThat(chainDataPruner.getEarliestRetainedBlock(100)).contains(0L);
  }

  @Test
  public void earliestRetainedBlockIsEmptyInBalOnlyMode() {
    final ChainDataPruner chainDataPruner =
        pruner(
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 512, 512, 10, 0));

    assertThat(chainDataPruner.getEarliestRetainedBlock(2000)).isEmpty();
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
