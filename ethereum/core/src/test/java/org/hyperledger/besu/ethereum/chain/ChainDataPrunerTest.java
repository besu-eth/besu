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

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

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
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
                ChainDataPruner.ChainPruningStrategy.BAL, 512, 512, Long.MAX_VALUE, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
                ChainDataPruner.ChainPruningStrategy.ALL, 256, 256, 256, 100),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    // Batch boundary derivation:
    // Nothing pruned yet (genesis always kept). First prune fires when
    // (blockNumber - retention) - lastPruned >= frequency, i.e. blockNumber >= retention +
    // frequency
    final int retention = 256;
    final int frequency = 100;
    final int firstBatchAt = retention + frequency; // 356
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 100
    final int chainLength =
        firstBatchAt
            + 13; // past first batch, not enough for second (needs retention + 2*freq = 456)

    List<Block> blocks = gen.blockSequence(genesisBlock, chainLength);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // At block chainLength (= firstBatchAt + 13 = 370):
    // - blockPruningMark = chainLength - retention (= 113)
    // - Genesis (block 0) is ALWAYS kept, pruning starts from block 1
    //
    // Pruning timeline:
    // block firstBatchAt (= 356): blockPruningMark = firstBatchPrunedUpTo (= 100)
    //                             blocksToBePruned = firstBatchPrunedUpTo = frequency
    //                             → first batch fires, prunes blocks 1..firstBatchPrunedUpTo
    // block chainLength (= 369):  blockPruningMark = chainLength - retention (= 113)
    //                             blocksToBePruned = 13 < frequency
    //                             → second batch not fired; needs block retention + 2*freq + 1
    // (= 456)

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
          .as("Block %d should exist (second batch needs block %d)", i, retention + 2 * frequency)
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
                ChainDataPruner.ChainPruningStrategy.ALL, 1000, 1000, 1000, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    gen.blockSequence(genesisBlock, 50)
        .forEach(
            blk -> {
              blockchain.appendBlock(blk, gen.receipts(blk));
              final Hash firstBlock = blockchain.getBlockHashByNumber(1).orElseThrow();
              assertThat(blockchainStorage.getBlockBody(firstBlock)).isPresent();
              assertThat(blockchainStorage.getTransactionReceipts(firstBlock)).isPresent();
              assertThat(blockchainStorage.getBlockBody(blk.getHash())).isPresent();
              assertThat(blockchainStorage.getTransactionReceipts(blk.getHash())).isPresent();
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
                ChainDataPruner.ChainPruningStrategy.BAL, 256, 256, Long.MAX_VALUE, 100),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
    final int firstBatchAt = retention + frequency; // 356
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 100
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

    final int secondBatchAt = retention + 2 * frequency; // 456

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
                256,
                256, // keep 256 BALs
                Long.MAX_VALUE,
                100),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
    final int firstBatchAt = retention + frequency; // 356
    final int firstBatchPrunedUpTo = firstBatchAt - retention; // 100
    final int secondBatchAt = firstBatchAt + frequency; // 456
    final int secondBatchPrunedUpTo = secondBatchAt - retention; // 200
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
    final int thirdBatchAt = secondBatchAt + frequency; // 556

    // At block chainLength (= secondBatchAt + 13 = 470):
    // - pruningMark = chainLength - retention (= 213)
    // - Genesis (block 0) BAL is ALWAYS kept, pruning starts from block 1
    //
    // Pruning timeline:
    // block firstBatchAt (= 356):  pruningMark = firstBatchPrunedUpTo (= 100)
    //                              blocksToBePruned = firstBatchPrunedUpTo = frequency
    //                              → first batch fires, prunes BALs 1..firstBatchPrunedUpTo
    // block secondBatchAt (= 456): pruningMark = secondBatchPrunedUpTo (= 200)
    //                              blocksToBePruned = secondBatchPrunedUpTo - firstBatchPrunedUpTo
    // = frequency
    //                              → second batch fires, prunes BALs
    // firstBatchPrunedUpTo+1..secondBatchPrunedUpTo
    // block chainLength (= 469):   pruningMark = 214
    //                              blocksToBePruned = pruningMark - secondBatchPrunedUpTo = 13 <
    // frequency
    //                              → third batch not fired; needs block thirdBatchAt (= 556)

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
  public void balOnlyModeWritesNoMarkBeforeBalActivation() {
    // Neither progress marker is initialized until BAL pruning is activated.
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
                ChainDataPruner.ChainPruningStrategy.BAL, 10, 10, Long.MAX_VALUE, 0),
            false,
            0,
            header -> false,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    // Disable BAL generation for pre-activation
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withoutGeneratedBlockAccessList());

    // Generate 50 blocks WITHOUT BAL (pre-activation), well past the 10-block retention
    List<Block> blocks = gen.blockSequence(genesisBlock, 50);
    for (Block blk : blocks) {
      blockchain.appendBlock(blk, gen.receipts(blk));
      assertThat(blk.getHeader().getBalHash()).isEmpty();
    }

    assertThat(prunerStorage.getBalPruningMark()).isEmpty();
    assertThat(prunerStorage.getBlockPruningMark()).isEmpty();

    // All blocks should exist (BAL mode never prunes blocks, and no mark was ever written)
    for (int i = 0; i <= 50; i++) {
      assertThat(blockchain.getBlockHeader(i))
          .as("Block %d should exist (BAL mode never prunes blocks)", i)
          .isPresent();
    }
  }

  @Test
  public void recordsForkBlocksInBothModes() {
    // BAL-only pruning must retain fork indices for a subsequent switch to ALL.
    final BlockDataGenerator allModeGen = new BlockDataGenerator();
    final BlockchainStorage allModeBlockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage allModePrunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final ChainDataPruner allModePruner =
        new ChainDataPruner(
            allModeBlockchainStorage,
            allModePrunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block allModeGenesisBlock = allModeGen.genesisBlock();
    final MutableBlockchain allModeBlockchain =
        DefaultBlockchain.createMutable(
            allModeGenesisBlock, allModeBlockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(allModeBlockchain, allModePruner);

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
                ChainDataPruner.ChainPruningStrategy.BAL,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block balModeGenesisBlock = balModeGen.genesisBlock();
    final MutableBlockchain balModeBlockchain =
        DefaultBlockchain.createMutable(
            balModeGenesisBlock, balModeBlockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(balModeBlockchain, balModePruner);

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
                ChainDataPruner.ChainPruningStrategy.BAL, 100, 100, Long.MAX_VALUE, 0),
            false,
            0,
            header -> header.getNumber() >= 21,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
                ChainDataPruner.ChainPruningStrategy.ALL, 100, 100, 100, 0),
            false,
            0,
            header -> false,
            lowestToBackfill -> false,
            new BlockingExecutor());
    Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void bftPrunesFromHeadRegardlessOfSafeBlock(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final int retained = 4;
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, retained, retained, retained, 0),
            true,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());

    for (final BlockDataGenerator.BlockWithAccessList blockWithBal :
        gen.blockSequenceWithAccessList(genesisBlock, retained + 4)) {
      final Block block = blockWithBal.getBlock();
      final long number = block.getHeader().getNumber();
      if (number == retained + 3) {
        blockchain.setSafeBlock(genesisBlock.getHash());
      } else if (number == retained + 4) {
        final Hash unknownSafe = gen.blockSequence(block, 1).getFirst().getHash();
        blockchain.setSafeBlock(unknownSafe);
        assertThat(blockchainStorage.getBlockHeader(unknownSafe)).isEmpty();
      } else {
        assertThat(blockchainStorage.getSafeBlock()).isEmpty();
      }

      final BlockchainStorage.Updater updater = blockchainStorage.updater();
      updater.putBlockAccessList(block.getHash(), blockWithBal.getBlockAccessList().orElseThrow());
      updater.commit();
      blockchain.appendBlock(block, gen.receipts(block));

      assertThat(blockchain.getBlockByNumber(0)).isPresent();
      if (number <= retained) {
        assertThat(prunerStorage.getBalPruningMark()).isEmpty();
        assertThat(prunerStorage.getBlockPruningMark()).isEmpty();
        assertThat(blockchain.getBlockByNumber(1)).isPresent();
        assertThat(
                blockchainStorage.getBlockAccessList(
                    blockchain.getBlockHashByNumber(1).orElseThrow()))
            .isPresent();
        continue;
      }

      final long target = number - retained;
      final Hash prunedHash = blockchain.getBlockHashByNumber(target).orElseThrow();
      final Hash retainedHash = blockchain.getBlockHashByNumber(target + 1).orElseThrow();
      assertProgress(prunerStorage, strategy, target);
      assertThat(blockchain.getBlockHeader(target)).isPresent();
      assertThat(blockchainStorage.getBlockAccessList(prunedHash)).isEmpty();
      assertThat(blockchainStorage.getBlockAccessList(retainedHash)).isPresent();
      assertThat(blockchain.getBlockByNumber(target + 1)).isPresent();
      assertThat(blockchainStorage.getTransactionReceipts(retainedHash)).isPresent();
      if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
        assertThat(blockchain.getBlockByNumber(target)).isEmpty();
        assertThat(blockchainStorage.getTransactionReceipts(prunedHash)).isEmpty();
      } else {
        assertThat(blockchain.getBlockByNumber(target)).isPresent();
        assertThat(blockchainStorage.getTransactionReceipts(prunedHash)).isPresent();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void pruningPausesUntilSafeHeaderIsKnown(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final CountingPrunerStorage prunerStorage = new CountingPrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, 100, 100, 100, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);
    appendBlocks(gen, blockchain, 200);

    assertThat(blockchainStorage.getSafeBlock()).isEmpty();
    assertThat(executor.queued()).isEmpty();

    final Block unknownSafeBlock = gen.blockSequence(blockchain.getChainHeadBlock(), 1).getFirst();
    blockchain.setSafeBlock(unknownSafeBlock.getHash());
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());
    assertThat(blockchainStorage.getBlockHeader(unknownSafeBlock.getHash())).isEmpty();
    assertThat(executor.queued()).isEmpty();
    assertThat(prunerStorage.getBalPruningMark()).isEmpty();
    assertThat(prunerStorage.getBlockPruningMark()).isEmpty();
    assertThat(blockchain.getBlockByNumber(1)).isPresent();

    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(150).orElseThrow());
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());
    assertThat(executor.queued()).hasSize(1);
    executor.runNext();
    assertProgress(prunerStorage, strategy, 50L);
  }

  @Test
  public void allModeUsesLaggingSafeCutoffWhileAdvertisingHeadSnapshot() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPruner chainDataPruner = allModePruner(blockchainStorage, 100);
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    blockchain.observeBlockAdded(chainDataPruner);
    appendBlocks(gen, blockchain, 200);
    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(150).orElseThrow());
    appendBlocks(gen, blockchain, 1);

    assertThat(blockchain.getBlockByNumber(50)).isEmpty();
    assertThat(blockchain.getBlockByNumber(51)).isPresent();
    assertThat(blockchain.getBlockByNumber(101)).isPresent();
    assertThat(chainDataPruner.getEarliestRetainedBlock(201)).contains(102L);
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void queuedJobRechecksRetreatingSafeBlock(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final CountingPrunerStorage prunerStorage = new CountingPrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, 100, 100, 100, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    appendBlocks(gen, blockchain, 200);
    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(180).orElseThrow());
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());
    assertThat(executor.queued()).hasSize(1);

    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(140).orElseThrow());
    executor.runNext();
    assertProgress(prunerStorage, strategy, 40L);
    assertThat(blockchain.getBlockByNumber(41)).isPresent();
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void queuedJobPrunesAgainstTheCurrentHeadAndSafeBlock(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final CountingPrunerStorage prunerStorage = new CountingPrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, 100, 100, 100, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    appendBlocks(gen, blockchain, 200);
    final BlockHeader eventHead = blockchain.getChainHeadHeader();
    appendBlocks(gen, blockchain, 50);
    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(140).orElseThrow());
    chainDataPruner.pruneForSyncedHead(eventHead);
    assertThat(executor.queued()).hasSize(1);

    blockchain.setSafeBlock(blockchain.getChainHeadHash());
    executor.runNext();
    assertProgress(prunerStorage, strategy, 150L);
    assertThat(blockchain.getBlockByNumber(151)).isPresent();

    chainDataPruner.pruneForSyncedHead(eventHead);
    assertThat(executor.queued()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void queuedJobPausesWhenSafeHeaderBecomesUnknown(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final CountingPrunerStorage prunerStorage = new CountingPrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, 100, 100, 100, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    appendBlocks(gen, blockchain, 200);
    blockchain.setSafeBlock(blockchain.getChainHeadHash());
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());
    assertThat(executor.queued()).hasSize(1);

    blockchain.setSafeBlock(
        gen.blockSequence(blockchain.getChainHeadBlock(), 1).getFirst().getHash());
    executor.runNext();
    assertThat(prunerStorage.transactionsStarted()).isZero();
    assertThat(prunerStorage.getBalPruningMark()).isEmpty();
    assertThat(prunerStorage.getBlockPruningMark()).isEmpty();
    assertThat(blockchain.getBlockByNumber(1)).isPresent();
  }

  @Test
  public void balOnlyModePrunesRelativeToLaggingSafeBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 100, 100, Long.MAX_VALUE, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);

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

    blockchain.setSafeBlock(blockchain.getBlockHashByNumber(150).orElseThrow());
    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());

    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(200L);

    final Hash prunedBal = blockchain.getBlockHashByNumber(50).orElseThrow();
    final Hash retainedBal = blockchain.getBlockHashByNumber(51).orElseThrow();

    assertThat(blockchainStorage.getBlockAccessList(prunedBal)).isEmpty();
    assertThat(blockchainStorage.getBlockAccessList(retainedBal)).isPresent();
    // BAL-only mode never touches block data.
    assertThat(blockchain.getBlockByNumber(50)).isPresent();
  }

  private static ChainDataPruner allModePruner(
      final BlockchainStorage blockchainStorage, final long retained) {
    return new ChainDataPruner(
        blockchainStorage,
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
        new ChainPrunerConfiguration(
            ChainDataPruner.ChainPruningStrategy.ALL, retained, retained, retained, 0),
        false,
        0,
        header -> true,
        lowestToBackfill -> false,
        new BlockingExecutor());
  }

  /** Marks each imported canonical head safe before notifying the pruner in retention tests. */
  private static void observePrunerWithSafeHead(
      final MutableBlockchain blockchain, final ChainDataPruner chainDataPruner) {
    blockchain.observeBlockAdded(
        event -> {
          if (event.isNewCanonicalHead()) {
            blockchain.setSafeBlock(event.getHeader().getHash());
          }
          chainDataPruner.onBlockAdded(event);
        });
  }

  @Test
  public void allModeRetainsBlocksLongerThanBals() {
    final PruningFixture fixture = new PruningFixture(20, 1);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 10, 3, 0, header -> true);

    assertThat(fixture.marks.getBlockPruningMark()).contains(10L);
    assertThat(fixture.marks.getBalPruningMark()).contains(17L);
    assertThat(fixture.storage.getBlockBody(fixture.hash(10))).isEmpty();
    assertThat(fixture.storage.getTransactionReceipts(fixture.hash(10))).isEmpty();
    assertThat(fixture.storage.getBlockBody(fixture.hash(11))).isPresent();
    assertThat(fixture.storage.getTransactionReceipts(fixture.hash(11))).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(17))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(18))).isPresent();
    assertThat(fixture.blockchain.getBlockHeader(10)).isPresent();
  }

  @Test
  public void missingMarksPersistDistinctBoundariesAndRestartTrustsThem() {
    final ProtocolSchedule schedule =
        MainnetProtocolSchedule.fromConfig(
            new StubGenesisConfigOptions()
                .homesteadBlock(0)
                .byzantiumBlock(0)
                .constantinopleBlock(0)
                .petersburgBlock(0)
                .istanbulBlock(0)
                .berlinBlock(0)
                .londonBlock(0)
                .shanghaiTime(0)
                .cancunTime(0)
                .pragueTime(0)
                .osakaTime(0)
                .amsterdamTime(11),
            Optional.empty(),
            Optional.empty(),
            MiningConfiguration.MINING_DISABLED,
            new BadBlockManager(),
            false,
            BalConfiguration.DEFAULT,
            new NoOpMetricsSystem());
    final Predicate<BlockHeader> balEnabled =
        header -> schedule.getByBlockHeader(header).getBlockAccessListFactory().isPresent();
    final InMemoryKeyValueStorage data = new InMemoryKeyValueStorage();
    final InMemoryKeyValueStorage variables = new InMemoryKeyValueStorage();
    final InMemoryKeyValueStorage progress = new InMemoryKeyValueStorage();
    final BlockchainStorage storage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            data,
            new VariablesKeyValueStorage(variables),
            new MainnetBlockHeaderFunctions(),
            false);
    final BlockDataGenerator gen = new BlockDataGenerator();
    final List<BlockHeader> headers = new ArrayList<>();
    final List<BlockAccessList> bals = new ArrayList<>();
    final BlockchainStorage.Updater importTransaction = storage.updater();
    Hash parentHash = Hash.ZERO;
    for (int height = 1; height <= 20; height++) {
      final BlockAccessList bal = gen.blockAccessList();
      final BlockHeader header =
          BlockHeaderBuilder.createDefault()
              .parentHash(parentHash)
              .number(height)
              .timestamp(height)
              .transactionsRoot(Hash.EMPTY_TRIE_HASH)
              .receiptsRoot(Hash.EMPTY_TRIE_HASH)
              .baseFee(Wei.ONE)
              .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
              .blobGasUsed(0L)
              .excessBlobGas(BlobGas.ZERO)
              .parentBeaconBlockRoot(Bytes32.ZERO)
              .requestsHash(BodyValidation.requestsHash(List.of()))
              .balHash(height >= 11 ? BodyValidation.balHash(bal) : null)
              .slotNumber(height >= 11 ? (long) height : null)
              .buildBlockHeader();
      headers.add(header);
      bals.add(bal);
      importTransaction.putBlockHeader(header.getHash(), header);
      importTransaction.putBlockHash(height, header.getHash());
      if (height > 10) {
        importTransaction.putBlockBody(header.getHash(), BlockBody.empty());
        importTransaction.putTransactionReceipts(header.getHash(), List.of());
      }
      if (height >= 16) {
        importTransaction.putBlockAccessList(header.getHash(), bal);
      }
      parentHash = header.getHash();
    }
    importTransaction.setChainHead(parentHash);
    importTransaction.setSafeBlock(parentHash);
    importTransaction.commit();

    final BlockchainStorage reloaded =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            data,
            new VariablesKeyValueStorage(variables),
            new MainnetBlockHeaderFunctions(),
            false);
    final BlockHeader head = reloaded.getBlockHeader(parentHash).orElseThrow();
    final BlockHeader firstAmsterdam =
        reloaded.getBlockHeader(headers.get(10).getHash()).orElseThrow();
    assertThat(balEnabled.test(reloaded.getBlockHeader(headers.get(9).getHash()).orElseThrow()))
        .isFalse();
    assertThat(balEnabled.test(firstAmsterdam)).isTrue();
    assertThat(firstAmsterdam.getBalHash()).contains(BodyValidation.balHash(bals.get(10)));
    assertThat(firstAmsterdam.getOptionalSlotNumber()).contains(11L);
    assertThat(head.getBalHash()).contains(BodyValidation.balHash(bals.get(19)));
    assertThat(head.getOptionalSlotNumber()).contains(20L);

    final ChainDataPrunerStorage marks = new ChainDataPrunerStorage(progress);
    new ChainDataPruner(
            reloaded,
            marks,
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 5, 2, 0, 100),
            false,
            0,
            balEnabled,
            lowestToBackfill -> false,
            new BlockingExecutor())
        .pruneForSyncedHead(head);
    assertThat(marks.getBlockPruningMark()).contains(10L);
    assertThat(marks.getBalPruningMark()).contains(15L);
    assertThat(reloaded.getBlockBody(headers.get(10).getHash())).isPresent();
    assertThat(reloaded.getBlockAccessList(headers.get(15).getHash()))
        .hasValueSatisfying(
            bal ->
                assertThat(BodyValidation.balHash(bal))
                    .isEqualTo(headers.get(15).getBalHash().orElseThrow()));

    // Restored old data must not cause a restarted worker to rediscover either boundary.
    final BlockchainStorage.Updater restoration = reloaded.updater();
    for (int height = 1; height <= 10; height++) {
      restoration.putBlockBody(headers.get(height - 1).getHash(), BlockBody.empty());
    }
    for (int height = 11; height <= 15; height++) {
      restoration.putBlockAccessList(headers.get(height - 1).getHash(), bals.get(height - 1));
    }
    restoration.commit();
    final BlockchainStorage restartedStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            data,
            new VariablesKeyValueStorage(variables),
            new MainnetBlockHeaderFunctions(),
            false);
    final ChainDataPrunerStorage restartedMarks = new ChainDataPrunerStorage(progress);
    new ChainDataPruner(
            restartedStorage,
            restartedMarks,
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 5, 2, 0, 0),
            false,
            0,
            balEnabled,
            lowestToBackfill -> false,
            new BlockingExecutor())
        .pruneForSyncedHead(restartedStorage.getBlockHeader(parentHash).orElseThrow());
    assertThat(restartedMarks.getBlockPruningMark()).contains(15L);
    assertThat(restartedMarks.getBalPruningMark()).contains(18L);
    assertThat(restartedStorage.getBlockBody(headers.getFirst().getHash())).isPresent();
    assertThat(restartedStorage.getBlockAccessList(firstAmsterdam.getHash())).isPresent();
    assertThat(restartedStorage.getBlockBody(headers.get(9).getHash())).isPresent();
    assertThat(restartedStorage.getBlockAccessList(headers.get(14).getHash())).isPresent();
    assertThat(restartedStorage.getBlockBody(headers.get(14).getHash())).isEmpty();
    assertThat(restartedStorage.getBlockBody(headers.get(15).getHash())).isPresent();
    assertThat(restartedStorage.getBlockAccessList(headers.get(17).getHash())).isEmpty();
    assertThat(restartedStorage.getBlockAccessList(headers.get(18).getHash()))
        .hasValueSatisfying(
            bal ->
                assertThat(BodyValidation.balHash(bal))
                    .isEqualTo(headers.get(18).getBalHash().orElseThrow()));
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void preAmsterdamDoesNotReadOrPruneAccidentalBals(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final PruningFixture fixture = new PruningFixture(20, 1);
    fixture.storage.balReads = 0;
    fixture.prune(strategy, 10, 2, 0, header -> header.getNumber() >= 21);
    assertThat(fixture.storage.balReads).isZero();
    assertThat(fixture.marks.getBalPruningMark()).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(1))).isPresent();
    if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
      assertThat(fixture.marks.getBlockPruningMark()).contains(10L);
      assertThat(fixture.storage.getBlockBody(fixture.hash(10))).isEmpty();
    } else {
      assertThat(fixture.marks.getBlockPruningMark()).isEmpty();
    }
  }

  @Test
  public void activationFindsFirstStoredBalRatherThanFirstActivatedHeader() {
    final PruningFixture fixture = new PruningFixture(20, 16);
    fixture.prune(
        ChainDataPruner.ChainPruningStrategy.BAL, 10, 2, 0, header -> header.getNumber() >= 11);
    assertThat(fixture.marks.getBalPruningMark()).contains(18L);
    assertThat(fixture.marks.getBlockPruningMark()).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(16))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(19))).isPresent();
  }

  @Test
  public void missingBalDataLeavesMarkAbsentAndCanBeDiscoveredLater() {
    final PruningFixture fixture = new PruningFixture(20, 21);
    final ChainDataPruner pruner =
        fixture.pruner(ChainDataPruner.ChainPruningStrategy.BAL, 10, 2, 0, header -> true);
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBalPruningMark()).isEmpty();
    fixture.putBals(16);
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBalPruningMark()).contains(18L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(16))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(19))).isPresent();
  }

  @Test
  public void existingZeroMarksAreProgressNotMissingBoundaries() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final KeyValueStorageTransaction transaction = fixture.marks.startTransaction();
    fixture.marks.setBlockPruningMark(transaction, 0);
    fixture.marks.setBalPruningMark(transaction, 0);
    transaction.commit();
    fixture.storage.balReads = 0;
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 4, 2, 0, header -> true);
    assertThat(fixture.storage.balReads).isZero();
    assertThat(fixture.marks.getBlockPruningMark()).contains(8L);
    assertThat(fixture.marks.getBalPruningMark()).contains(10L);
    assertThat(fixture.storage.getBlockBody(fixture.hash(1))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(1))).isEmpty();
  }

  @Test
  public void legacyAmbiguousMarksCannotSkipEitherDataStream() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final KeyValueStorageTransaction transaction = fixture.marks.startTransaction();
    for (final String key : List.of("pruningMark", "balPruningMark")) {
      transaction.put(
          Bytes.concatenate(Bytes.of(1), Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)))
              .toArrayUnsafe(),
          UInt256.valueOf(1000).toArrayUnsafe());
    }
    transaction.commit();
    assertThat(fixture.marks.getBlockPruningMark()).isEmpty();
    assertThat(fixture.marks.getBalPruningMark()).isEmpty();
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 4, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(8L);
    assertThat(fixture.marks.getBalPruningMark()).contains(10L);
    assertThat(fixture.storage.getBlockBody(fixture.hash(1))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(1))).isEmpty();
  }

  @Test
  public void switchingAllToBalAndBackKeepsIndependentProgressAndForks() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final Block fork = fixture.addForkAt(6);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 8, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(4L);
    assertThat(fixture.marks.getBalPruningMark()).contains(10L);
    assertThat(fixture.marks.getForkBlocks(6)).contains(fork.getHash());
    assertThat(fixture.storage.getBlockBody(fork.getHash())).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fork.getHash())).isEmpty();

    fixture.append(2);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.BAL, 8, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(4L);
    assertThat(fixture.marks.getBalPruningMark()).contains(12L);
    assertThat(fixture.marks.getForkBlocks(6)).contains(fork.getHash());
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 8, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(6L);
    assertThat(fixture.marks.getBalPruningMark()).contains(12L);
    assertThat(fixture.marks.getForkBlocks(6)).isEmpty();
    assertThat(fixture.storage.getBlockHeader(fork.getHash())).isEmpty();
    assertThat(fixture.storage.getBlockBody(fork.getHash())).isEmpty();
    assertThat(fixture.storage.getTransactionReceipts(fork.getHash())).isEmpty();
    assertThat(fixture.blockchain.getBlockHeader(6)).isPresent();
  }

  @Test
  public void balOnlyForkIndexSurvivesUntilFirstAllModeSweep() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final Block fork = fixture.addForkAt(2);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.BAL, 4, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).isEmpty();
    assertThat(fixture.marks.getForkBlocks(2)).contains(fork.getHash());
    assertThat(fixture.storage.getBlockBody(fork.getHash())).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fork.getHash())).isEmpty();
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 4, 2, 0, header -> true);
    assertThat(fixture.marks.getForkBlocks(2)).isEmpty();
    assertThat(fixture.storage.getBlockHeader(fork.getHash())).isEmpty();
    assertThat(fixture.storage.getBlockBody(fork.getHash())).isEmpty();
  }

  @Test
  public void eachPassMakesBoundedProgressWithoutStarvingTheOther() {
    final InMemoryKeyValueStorage persistedMarks = new InMemoryKeyValueStorage();
    final PruningFixture fixture =
        new PruningFixture(30, 1, new ChainDataPrunerStorage(persistedMarks));
    final ChainDataPruner pruner =
        fixture.pruner(ChainDataPruner.ChainPruningStrategy.ALL, 8, 2, 2, header -> true);
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBlockPruningMark()).contains(6L);
    assertThat(fixture.marks.getBalPruningMark()).contains(6L);
    assertThat(fixture.storage.getBlockBody(fixture.hash(6))).isEmpty();
    assertThat(fixture.storage.getBlockBody(fixture.hash(7))).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(6))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(7))).isPresent();
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBlockPruningMark()).contains(12L);
    assertThat(fixture.marks.getBalPruningMark()).contains(12L);

    final ChainDataPruner restarted =
        new ChainDataPruner(
            fixture.storage,
            new ChainDataPrunerStorage(persistedMarks),
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 8, 2, 0, 2),
            true,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    restarted.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBlockPruningMark()).contains(18L);
    assertThat(fixture.marks.getBalPruningMark()).contains(18L);
    restarted.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBlockPruningMark()).contains(22L);
    assertThat(fixture.marks.getBalPruningMark()).contains(24L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(24))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(25))).isPresent();
    restarted.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(fixture.marks.getBlockPruningMark()).contains(22L);
    assertThat(fixture.marks.getBalPruningMark()).contains(28L);
    for (int height = 1; height <= 30; height++) {
      assertThat(fixture.storage.getBlockBody(fixture.hash(height)).isPresent())
          .as("body at height %s after restart", height)
          .isEqualTo(height > 22);
      assertThat(fixture.storage.getTransactionReceipts(fixture.hash(height)).isPresent())
          .as("receipts at height %s after restart", height)
          .isEqualTo(height > 22);
      assertThat(fixture.storage.getBlockAccessList(fixture.hash(height)).isPresent())
          .as("BAL at height %s after restart", height)
          .isEqualTo(height > 28);
    }
  }

  @Test
  public void allModeSchedulesBalsBeforeBlockRetentionIsReached() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 20, 2, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).isEmpty();
    assertThat(fixture.marks.getBalPruningMark()).contains(10L);
    assertThat(fixture.storage.getBlockBody(fixture.hash(1))).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(1))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(11))).isPresent();
  }

  @Test
  public void preAmsterdamDoesNotAdvanceAnExistingBalMark() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final KeyValueStorageTransaction transaction = fixture.marks.startTransaction();
    fixture.marks.setBalPruningMark(transaction, 2);
    transaction.commit();
    fixture.storage.balReads = 0;
    fixture.prune(
        ChainDataPruner.ChainPruningStrategy.ALL, 4, 2, 0, header -> header.getNumber() >= 13);
    assertThat(fixture.storage.balReads).isZero();
    assertThat(fixture.marks.getBlockPruningMark()).contains(8L);
    assertThat(fixture.marks.getBalPruningMark()).contains(2L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(3))).isPresent();
  }

  @Test
  public void forkIndexWaitsForBalPassWhenBlockPassFinishesFirst() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    final Block fork = fixture.addForkAt(6);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 2, 8, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(10L);
    assertThat(fixture.marks.getBalPruningMark()).contains(4L);
    assertThat(fixture.storage.getBlockHeader(fork.getHash())).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fork.getHash())).isPresent();
    assertThat(fixture.marks.getForkBlocks(6)).contains(fork.getHash());
    fixture.append(2);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 2, 8, 0, header -> true);
    assertThat(fixture.marks.getBalPruningMark()).contains(6L);
    assertThat(fixture.storage.getBlockAccessList(fork.getHash())).isEmpty();
    assertThat(fixture.marks.getForkBlocks(6)).isEmpty();
  }

  @Test
  public void zeroBalRetentionPrunesThroughTheReferenceHead() {
    final PruningFixture fixture = new PruningFixture(12, 1);
    fixture.prune(ChainDataPruner.ChainPruningStrategy.ALL, 4, 0, 0, header -> true);
    assertThat(fixture.marks.getBlockPruningMark()).contains(8L);
    assertThat(fixture.marks.getBalPruningMark()).contains(12L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(12))).isEmpty();
    assertThat(fixture.storage.getBlockBody(fixture.hash(12))).isPresent();
  }

  @Test
  public void encodedEmptyBalIsDataAndGenesisBalIsNeverPruned() {
    final PruningFixture fixture = new PruningFixture(12, 13);
    final BlockchainStorage.Updater updater = fixture.storage.updater();
    for (int height = 0; height <= 12; height++) {
      updater.putBlockAccessList(fixture.hash(height), new BlockAccessList(List.of()));
    }
    updater.commit();
    fixture.prune(ChainDataPruner.ChainPruningStrategy.BAL, 4, 2, 0, header -> true);
    assertThat(fixture.marks.getBalPruningMark()).contains(10L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(0))).isPresent();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(1))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(11))).isPresent();
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void failedMetadataCommitRetriesAlreadyDeletedDataWithoutAdvancingMemory(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final FailingOncePrunerStorage marks = new FailingOncePrunerStorage();
    marks.failed = true;
    final KeyValueStorageTransaction transaction = marks.startTransaction();
    marks.setBlockPruningMark(transaction, 0);
    marks.setBalPruningMark(transaction, 0);
    transaction.commit();
    marks.failed = false;
    final PruningFixture fixture = new PruningFixture(12, 1, marks);
    final ChainDataPruner pruner = fixture.pruner(strategy, 4, 2, 0, header -> true);
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(marks.getBlockPruningMark()).contains(0L);
    assertThat(marks.getBalPruningMark())
        .contains(strategy == ChainDataPruner.ChainPruningStrategy.ALL ? 10L : 0L);
    for (int height = 1; height <= 12; height++) {
      final boolean bodyRetained =
          strategy == ChainDataPruner.ChainPruningStrategy.BAL || height > 8;
      assertThat(fixture.storage.getBlockBody(fixture.hash(height)).isPresent())
          .isEqualTo(bodyRetained);
      assertThat(fixture.storage.getTransactionReceipts(fixture.hash(height)).isPresent())
          .isEqualTo(bodyRetained);
      assertThat(fixture.storage.getBlockAccessList(fixture.hash(height)).isPresent())
          .isEqualTo(height > 10);
    }
    if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
      assertThat(pruner.getEarliestRetainedBlock(0))
          .as("the retention floor must not mask uncommitted in-memory progress")
          .contains(0L);
    }
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(marks.getBalPruningMark()).contains(10L);
    assertThat(marks.getBlockPruningMark())
        .contains(strategy == ChainDataPruner.ChainPruningStrategy.ALL ? 8L : 0L);
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(10))).isEmpty();
    assertThat(fixture.storage.getBlockAccessList(fixture.hash(11))).isPresent();
    if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
      assertThat(fixture.storage.getBlockBody(fixture.hash(8))).isEmpty();
      assertThat(fixture.storage.getBlockBody(fixture.hash(9))).isPresent();
      assertThat(pruner.getEarliestRetainedBlock(0)).contains(9L);
    }
  }

  @ParameterizedTest
  @CsvSource({"ALL, true", "ALL, false", "BAL, false"})
  public void failedDeletionCommitDiscardsPendingDeletesAndRetriesWithoutSkippingData(
      final ChainDataPruner.ChainPruningStrategy strategy, final boolean failBlockPass) {
    final FailingOnceBlockchainKeyValueStorage data = new FailingOnceBlockchainKeyValueStorage();
    final ChainDataPrunerStorage marks = new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final KeyValueStorageTransaction transaction = marks.startTransaction();
    marks.setBlockPruningMark(transaction, 0);
    marks.setBalPruningMark(transaction, 0);
    transaction.commit();
    final PruningFixture fixture = new PruningFixture(12, 1, marks, data);
    final ChainDataPruner pruner = fixture.pruner(strategy, 4, 2, 0, header -> true);
    final boolean blockPassSucceeds =
        strategy == ChainDataPruner.ChainPruningStrategy.ALL && !failBlockPass;
    data.commitsUntilFailure = blockPassSucceeds ? 2 : 1;

    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());

    assertThat(data.discardedDeletes).isPositive();
    assertThat(marks.getBlockPruningMark()).contains(blockPassSucceeds ? 8L : 0L);
    assertThat(marks.getBalPruningMark()).contains(failBlockPass ? 10L : 0L);
    if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
      assertThat(pruner.getEarliestRetainedBlock(0)).contains(blockPassSucceeds ? 9L : 0L);
    }
    for (int height = 1; height <= 12; height++) {
      assertThat(fixture.storage.getBlockBody(fixture.hash(height)).isPresent())
          .as("body at height %s after failed deletion", height)
          .isEqualTo(!blockPassSucceeds || height > 8);
      assertThat(fixture.storage.getTransactionReceipts(fixture.hash(height)).isPresent())
          .as("receipts at height %s after failed deletion", height)
          .isEqualTo(!blockPassSucceeds || height > 8);
      assertThat(fixture.storage.getBlockAccessList(fixture.hash(height)).isPresent())
          .as("BAL at height %s after failed deletion", height)
          .isEqualTo(!failBlockPass || height > 10);
    }

    // Reusing this worker also detects an unpersisted advance of either in-memory mark.
    pruner.pruneForSyncedHead(fixture.blockchain.getChainHeadHeader());
    assertThat(marks.getBlockPruningMark())
        .contains(strategy == ChainDataPruner.ChainPruningStrategy.ALL ? 8L : 0L);
    assertThat(marks.getBalPruningMark()).contains(10L);
    for (int height = 1; height <= 12; height++) {
      final boolean bodyRetained =
          strategy == ChainDataPruner.ChainPruningStrategy.BAL || height > 8;
      assertThat(fixture.storage.getBlockBody(fixture.hash(height)).isPresent())
          .as("body at height %s after retry", height)
          .isEqualTo(bodyRetained);
      assertThat(fixture.storage.getTransactionReceipts(fixture.hash(height)).isPresent())
          .as("receipts at height %s after retry", height)
          .isEqualTo(bodyRetained);
      assertThat(fixture.storage.getBlockAccessList(fixture.hash(height)).isPresent())
          .as("BAL at height %s after retry", height)
          .isEqualTo(height > 10);
    }
  }

  private static class PruningFixture {
    private final BlockDataGenerator gen = new BlockDataGenerator();
    private final ObservedBlockchainStorage storage;
    private final ChainDataPrunerStorage marks;
    private final MutableBlockchain blockchain;
    private final List<Block> blocks;

    PruningFixture(final int height, final int firstBal) {
      this(height, firstBal, new ChainDataPrunerStorage(new InMemoryKeyValueStorage()));
    }

    PruningFixture(final int height, final int firstBal, final ChainDataPrunerStorage marks) {
      this(height, firstBal, marks, new InMemoryKeyValueStorage());
    }

    PruningFixture(
        final int height,
        final int firstBal,
        final ChainDataPrunerStorage marks,
        final InMemoryKeyValueStorage data) {
      storage = new ObservedBlockchainStorage(data);
      blockchain =
          DefaultBlockchain.createMutable(gen.genesisBlock(), storage, new NoOpMetricsSystem(), 0);
      this.marks = marks;
      blocks = gen.blockSequence(blockchain.getChainHeadBlock(), height);
      blocks.forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));
      putBals(firstBal);
    }

    Hash hash(final int height) {
      return blockchain.getBlockHashByNumber(height).orElseThrow();
    }

    void putBals(final int firstBal) {
      final BlockchainStorage.Updater updater = storage.updater();
      for (int height = firstBal; height <= blockchain.getChainHeadBlockNumber(); height++) {
        updater.putBlockAccessList(hash(height), gen.blockAccessList());
      }
      updater.commit();
    }

    void append(final int count) {
      final int first = (int) blockchain.getChainHeadBlockNumber() + 1;
      appendBlocks(gen, blockchain, count);
      putBals(first);
    }

    Block addForkAt(final int height) {
      final Block fork =
          gen.blockSequence(blockchain.getBlockByNumber(height - 1).orElseThrow(), 1).getFirst();
      blockchain.storeBlock(fork, gen.receipts(fork), Optional.of(gen.blockAccessList()));
      marks.addForkBlock(height, fork.getHash());
      return fork;
    }

    ChainDataPruner pruner(
        final ChainDataPruner.ChainPruningStrategy strategy,
        final long blocksRetained,
        final long balsRetained,
        final int frequency,
        final Predicate<BlockHeader> activation) {
      return new ChainDataPruner(
          storage,
          marks,
          new ChainPrunerConfiguration(strategy, blocksRetained, balsRetained, 0, frequency),
          true,
          0,
          activation,
          lowestToBackfill -> false,
          new BlockingExecutor());
    }

    void prune(
        final ChainDataPruner.ChainPruningStrategy strategy,
        final long blocksRetained,
        final long balsRetained,
        final int frequency,
        final Predicate<BlockHeader> activation) {
      pruner(strategy, blocksRetained, balsRetained, frequency, activation)
          .pruneForSyncedHead(blockchain.getChainHeadHeader());
    }
  }

  private static class ObservedBlockchainStorage
      extends KeyValueStoragePrefixedKeyBlockchainStorage {
    private int balReads;

    ObservedBlockchainStorage(final InMemoryKeyValueStorage data) {
      super(
          data,
          new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
          new MainnetBlockHeaderFunctions(),
          false);
    }

    @Override
    public Optional<BlockAccessList> getBlockAccessList(final Hash blockHash) {
      balReads++;
      return super.getBlockAccessList(blockHash);
    }
  }

  private static class FailingOnceBlockchainKeyValueStorage extends InMemoryKeyValueStorage {
    private int commitsUntilFailure;
    private int discardedDeletes;

    @Override
    public KeyValueStorageTransaction startTransaction() {
      final KeyValueStorageTransaction transaction = super.startTransaction();
      if (commitsUntilFailure == 0) {
        return transaction;
      }
      return new KeyValueStorageTransaction() {
        private int pendingDeletes;

        @Override
        public void put(final byte[] key, final byte[] value) {
          transaction.put(key, value);
        }

        @Override
        public void remove(final byte[] key) {
          transaction.remove(key);
          pendingDeletes++;
        }

        @Override
        public void commit() {
          if (--commitsUntilFailure == 0) {
            discardedDeletes = pendingDeletes;
            transaction.rollback();
            throw new StorageException("simulated failure before blockchain deletion commit");
          }
          transaction.commit();
        }

        @Override
        public void rollback() {
          transaction.rollback();
        }

        @Override
        public void close() {
          transaction.close();
        }
      };
    }
  }

  private static void appendBlocks(
      final BlockDataGenerator gen, final MutableBlockchain blockchain, final int count) {
    for (final BlockDataGenerator.BlockWithAccessList generated :
        gen.blockSequenceWithAccessList(blockchain.getChainHeadBlock(), count)) {
      final Block blk = generated.getBlock();
      blockchain.appendBlock(blk, gen.receipts(blk), generated.getBlockAccessList());
    }
  }

  private static void assertProgress(
      final ChainDataPrunerStorage storage,
      final ChainDataPruner.ChainPruningStrategy strategy,
      final long expected) {
    assertThat(storage.getBalPruningMark()).contains(expected);
    if (strategy == ChainDataPruner.ChainPruningStrategy.ALL) {
      assertThat(storage.getBlockPruningMark()).contains(expected);
    } else {
      assertThat(storage.getBlockPruningMark()).isEmpty();
    }
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
    observePrunerWithSafeHead(blockchain, chainDataPruner);

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
        new ChainPrunerConfiguration(strategy, 10, 10, 10, 4),
        false,
        0,
        header -> true,
        lowestToBackfill -> false,
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
                ChainDataPruner.ChainPruningStrategy.ALL, retention, retention, retention, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    gen.blockSequence(blockchain.getChainHeadBlock(), 1)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(blockchain.getBlockHeader(0)).isPresent();
    assertThat(blockchain.getBlockHeader(head - retention)).isPresent();
    assertThat(blockchain.getBlockByNumber(head - retention)).isEmpty();
    assertThat(blockchain.getBlockByNumber(head - retention + 1)).isPresent();
  }

  @ParameterizedTest
  @EnumSource(
      value = ChainDataPruner.ChainPruningStrategy.class,
      names = {"ALL", "BAL"})
  public void neverPrunesWithinTheMinimumHeadDistance(
      final ChainDataPruner.ChainPruningStrategy strategy) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(strategy, 100, 100, 100, 0),
            false,
            75,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    appendBlocks(gen, blockchain, 200);
    // stored beyond the head, so that the safe block stands 50 blocks ahead of it
    Hash lastStored = blockchain.getChainHeadHash();
    for (final Block block : gen.blockSequence(blockchain.getChainHeadBlock(), 50)) {
      blockchain.storeBlock(block, gen.receipts(block));
      lastStored = block.getHash();
    }
    blockchain.setSafeBlock(lastStored);

    chainDataPruner.pruneForSyncedHead(blockchain.getChainHeadHeader());

    // min(250 - 100, 200 - 75)
    assertProgress(prunerStorage, strategy, 125L);
    assertThat(blockchain.getBlockByNumber(126)).isPresent();
  }

  @Test
  public void blockBoundaryWaitsWhileTheBackfillMissesBodies() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final AtomicBoolean backfilling = new AtomicBoolean(true);
    final AtomicLong askedFor = new AtomicLong();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 100, 100, 100, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> {
              askedFor.set(lowestToBackfill);
              return backfilling.get();
            },
            new BlockingExecutor());
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    appendBlocks(gen, blockchain, 200);
    blockchain.setSafeBlock(blockchain.getChainHeadHash());
    final BlockHeader head = blockchain.getChainHeadHeader();

    chainDataPruner.pruneForSyncedHead(head);

    assertThat(prunerStorage.getBlockPruningMark()).isEmpty();
    assertThat(prunerStorage.getBalPruningMark()).contains(100L);
    assertThat(blockchain.getBlockByNumber(1)).isPresent();
    // lowestToBackfill(200) with no frequency
    assertThat(askedFor.get()).isEqualTo(101L);

    backfilling.set(false);
    chainDataPruner.pruneForSyncedHead(head);

    assertThat(prunerStorage.getBlockPruningMark()).contains(100L);
    assertThat(blockchain.getBlockByNumber(100)).isEmpty();
    assertThat(blockchain.getBlockByNumber(101)).isPresent();
  }

  @Test
  public void lowestToBackfillStaysAboveWhatPruningRemoved() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 10, 10, 10, 0),
            false,
            0,
            header -> false,
            lowestToBackfill -> false,
            new BlockingExecutor());
    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);
    gen.blockSequence(genesisBlock, 40)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));
    assertThat(prunerStorage.getBlockPruningMark()).contains(30L);

    // retention raised after blocks up to 30 were pruned
    final ChainDataPruner raisedRetention =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(ChainDataPruner.ChainPruningStrategy.ALL, 100, 100, 10, 0),
            false,
            0,
            header -> false,
            lowestToBackfill -> false,
            new BlockingExecutor());

    assertThat(raisedRetention.lowestToBackfill(40)).isEqualTo(31L);
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
                ChainDataPruner.ChainPruningStrategy.ALL, retention, retention, retention, 0),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            new BlockingExecutor());
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    gen.blockSequence(blockchain.getChainHeadBlock(), 1)
        .forEach(block -> blockchain.appendBlock(block, gen.receipts(block)));

    final long head = blockchain.getChainHeadBlockNumber();
    assertThat(prunerStorage.getBlockPruningMark()).hasValue(head - retention);
    assertThat(blockchain.getBlockHeader(head)).isPresent();
    assertThat(blockchain.getBlockHeader(head - retention)).isPresent();
    assertThat(blockchain.getBlockByNumber(head - retention)).isEmpty();
  }

  @Test
  public void allModeStartsFromBodiesDespiteAnExistingBalMark() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final KeyValueStorageTransaction tx = prunerStorage.startTransaction();
    prunerStorage.setBalPruningMark(tx, 400);
    tx.commit();

    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 100, 100, 100, 0),
            false,
            0,
            header -> false,
            lowestToBackfill -> false,
            new BlockingExecutor());

    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    observePrunerWithSafeHead(blockchain, chainDataPruner);

    gen.blockSequence(genesisBlock, 101)
        .forEach(blk -> blockchain.appendBlock(blk, gen.receipts(blk)));

    assertThat(prunerStorage.getBlockPruningMark()).contains(1L);
    assertThat(prunerStorage.getBalPruningMark()).contains(400L);
    assertThat(blockchain.getBlockByNumber(1)).isEmpty();
    assertThat(blockchain.getBlockHeader(1)).isPresent();
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
        inMemoryBlockchainStorage(),
        prunerStorage,
        config,
        false,
        0,
        header -> true,
        lowestToBackfill -> false,
        new BlockingExecutor());
  }

  @Test
  public void earliestRetainedBlockInAllModeIsHeadMinusRetained() {
    // Retained 512, nothing pruned yet: bound = head - retained + 1
    // = 2000 - 512 + 1 = 1489
    final ChainDataPruner chainDataPruner =
        pruner(
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 10));

    assertThat(chainDataPruner.getEarliestRetainedBlock(2000)).contains(1489L);
  }

  @Test
  public void earliestRetainedBlockInAllModeIsAtLeastStoredMarkPlusOne() {
    // Operator raised retention after blocks <= 1800 were already deleted; the stored mark
    // must win over the bound (2000 - 512 + 1 = 1489 < 1801).
    final ChainDataPrunerStorage prunerStorage =
        new ChainDataPrunerStorage(new InMemoryKeyValueStorage());
    final KeyValueStorageTransaction tx = prunerStorage.startTransaction();
    prunerStorage.setBlockPruningMark(tx, 1800);
    tx.commit();
    final ChainDataPruner chainDataPruner =
        pruner(
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 512, 512, 512, 10));

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
                ChainDataPruner.ChainPruningStrategy.ALL, 10_000, 10_000, 10_000, 10));

    assertThat(chainDataPruner.getEarliestRetainedBlock(100)).contains(0L);
  }

  @Test
  public void earliestRetainedBlockIsEmptyInBalOnlyMode() {
    final ChainDataPruner chainDataPruner =
        pruner(
            new ChainDataPrunerStorage(new InMemoryKeyValueStorage()),
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.BAL, 512, 512, 512, 10));

    assertThat(chainDataPruner.getEarliestRetainedBlock(2000)).isEmpty();
  }

  @Test
  public void queuedJobRereadsTheMarkAndSkipsWorkAnEarlierJobAlreadyDid() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            new InMemoryKeyValueStorage(),
            new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
            new MainnetBlockHeaderFunctions(),
            false);
    final CountingPrunerStorage prunerStorage = new CountingPrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 256, 256, 256, 100),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);

    final Block genesisBlock = gen.genesisBlock();
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesisBlock, blockchainStorage, new NoOpMetricsSystem(), 0);
    for (final Block blk : gen.blockSequence(genesisBlock, 400)) {
      blockchain.appendBlock(blk, gen.receipts(blk));
    }

    // Two heads arrive while the single pruning thread is busy, so both jobs queue with the same
    // view of the mark. Pruning mark for head 400 is 400 - 256 - 0 = 144, which one batch covers.
    final BlockHeader head = blockchain.getChainHeadHeader();
    blockchain.setSafeBlock(head.getHash());
    chainDataPruner.pruneForSyncedHead(head);
    chainDataPruner.pruneForSyncedHead(head);
    assertThat(executor.queued()).hasSize(2);

    executor.runNext();
    assertThat(prunerStorage.getBlockPruningMark()).contains(144L);
    final int transactionsAfterFirstJob = prunerStorage.transactionsStarted();
    assertThat(transactionsAfterFirstJob).isPositive();

    executor.runNext();

    assertThat(prunerStorage.transactionsStarted())
        .as("second job should re-read the advanced mark and do nothing")
        .isEqualTo(transactionsAfterFirstJob);
    assertThat(prunerStorage.getBlockPruningMark()).contains(144L);
  }

  @Test
  public void failedMarkInitializationLeavesBlockDataForTheNextJobToRetry() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final BlockchainStorage blockchainStorage = inMemoryBlockchainStorage();
    final FailingOncePrunerStorage prunerStorage = new FailingOncePrunerStorage();
    final DeferredExecutor executor = new DeferredExecutor();
    final ChainDataPruner chainDataPruner =
        new ChainDataPruner(
            blockchainStorage,
            prunerStorage,
            new ChainPrunerConfiguration(
                ChainDataPruner.ChainPruningStrategy.ALL, 256, 256, 256, 100),
            false,
            0,
            header -> true,
            lowestToBackfill -> false,
            executor);

    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            gen.genesisBlock(), blockchainStorage, new NoOpMetricsSystem(), 0);
    gen.setBlockOptionsSupplier(
        () -> BlockDataGenerator.BlockOptions.create().withGeneratedBlockAccessList());
    appendBlocks(gen, blockchain, 400);
    final BlockHeader head = blockchain.getChainHeadHeader();
    blockchain.setSafeBlock(head.getHash());

    chainDataPruner.pruneForSyncedHead(head);
    executor.runNext();
    assertThat(prunerStorage.getBlockPruningMark()).isEmpty();
    final Hash firstHash = blockchain.getBlockHashByNumber(1).orElseThrow();
    final Hash cutoffHash = blockchain.getBlockHashByNumber(144).orElseThrow();
    assertThat(blockchainStorage.getBlockBody(firstHash)).isPresent();
    assertThat(blockchainStorage.getTransactionReceipts(firstHash)).isPresent();
    assertThat(blockchainStorage.getBlockBody(cutoffHash)).isPresent();
    assertThat(blockchainStorage.getTransactionReceipts(cutoffHash)).isPresent();
    assertThat(prunerStorage.getBalPruningMark()).contains(144L);
    assertThat(blockchainStorage.getBlockAccessList(firstHash)).isEmpty();
    assertThat(blockchainStorage.getBlockAccessList(cutoffHash)).isEmpty();
    final Hash retainedHash = blockchain.getBlockHashByNumber(145).orElseThrow();
    assertThat(blockchainStorage.getBlockAccessList(retainedHash)).isPresent();
    assertThat(chainDataPruner.getEarliestRetainedBlock(0))
        .as("failed initialization must not publish an in-memory block mark")
        .contains(0L);

    // 400 - 256 = 144: the retry covers the same range and commits it.
    chainDataPruner.pruneForSyncedHead(head);
    executor.runNext();
    assertThat(prunerStorage.getBlockPruningMark()).contains(144L);
    assertThat(blockchain.getBlockByNumber(144)).isEmpty();
    assertThat(blockchainStorage.getBlockBody(firstHash)).isEmpty();
    assertThat(blockchainStorage.getTransactionReceipts(firstHash)).isEmpty();
    assertThat(blockchainStorage.getTransactionReceipts(cutoffHash)).isEmpty();
    assertThat(chainDataPruner.getEarliestRetainedBlock(0)).contains(145L);
    assertThat(blockchain.getBlockByNumber(145)).isPresent();
    assertThat(blockchainStorage.getTransactionReceipts(retainedHash)).isPresent();
  }

  /** Fails the next pruning-mark transaction, either initialization or post-deletion progress. */
  private static class FailingOncePrunerStorage extends ChainDataPrunerStorage {
    private boolean failed = false;

    FailingOncePrunerStorage() {
      super(new InMemoryKeyValueStorage());
    }

    @Override
    public KeyValueStorageTransaction startTransaction() {
      final KeyValueStorageTransaction transaction = super.startTransaction();
      if (failed) {
        return transaction;
      }
      failed = true;
      return new KeyValueStorageTransaction() {
        @Override
        public void put(final byte[] key, final byte[] value) {
          transaction.put(key, value);
        }

        @Override
        public void remove(final byte[] key) {
          transaction.remove(key);
        }

        @Override
        public void commit() {
          transaction.rollback();
          throw new StorageException("simulated");
        }

        @Override
        public void rollback() {
          transaction.rollback();
        }

        @Override
        public void close() {
          transaction.close();
        }
      };
    }
  }

  /** Counts how many pruning transactions were opened, i.e. how many jobs did real work. */
  private static class CountingPrunerStorage extends ChainDataPrunerStorage {
    private int transactionsStarted = 0;

    CountingPrunerStorage() {
      super(new InMemoryKeyValueStorage());
    }

    @Override
    public KeyValueStorageTransaction startTransaction() {
      transactionsStarted++;
      return super.startTransaction();
    }

    int transactionsStarted() {
      return transactionsStarted;
    }
  }

  /** Queues tasks instead of running them, so a test controls when each job executes. */
  private static class DeferredExecutor extends AbstractExecutorService {
    private final List<Runnable> queued = new CopyOnWriteArrayList<>();

    List<Runnable> queued() {
      return queued;
    }

    void runNext() {
      queued.remove(0).run();
    }

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
      queued.add(command);
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
