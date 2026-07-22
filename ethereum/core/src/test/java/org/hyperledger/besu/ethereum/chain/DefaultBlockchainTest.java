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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.MetricsSystemFactory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class DefaultBlockchainTest {

  @Test
  public void initializeNew() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    assertBlockDataIsStored(blockchain, genesisBlock, Collections.emptyList());
    assertBlockIsHead(blockchain, genesisBlock);
    assertTotalDifficultiesAreConsistent(blockchain, genesisBlock);
    assertThat(blockchain.getForks()).isEmpty();
  }

  @Test
  public void initializeExisting() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    // Write to kv store
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    // Initialize a new blockchain store with kvStore that already contains data
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    assertBlockDataIsStored(blockchain, genesisBlock, Collections.emptyList());
    assertBlockIsHead(blockchain, genesisBlock);
    assertTotalDifficultiesAreConsistent(blockchain, genesisBlock);
    assertThat(blockchain.getForks()).isEmpty();
  }

  @Test
  public void initializeExistingWithWrongGenesisBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    // Write to kv store
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    // Initialize a new blockchain store with same kvStore, but different genesis block
    assertThatThrownBy(
            () ->
                createMutableBlockchain(
                    kvStore, kvStoreVariables, gen.genesisBlock(), "/test/path"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Supplied genesis block does not match chain data stored in /test/path\n"
                + "Please specify a different data directory with --data-path, specify the original genesis file with "
                + "--genesis-file or supply a testnet/mainnet option with --network.");
  }

  @Test
  public void initializeReadOnly_withGenesisBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();

    // Write genesis block to storage
    createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    // Create read only chain
    final Blockchain blockchain = createBlockchain(kvStore, kvStoreVariables);

    assertBlockDataIsStored(blockchain, genesisBlock, Collections.emptyList());
    assertBlockIsHead(blockchain, genesisBlock);
    assertTotalDifficultiesAreConsistent(blockchain, genesisBlock);
  }

  @Test
  public void initializeReadOnly_withSmallChain() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final List<Block> blocks = gen.blockSequence(10);
    final List<List<TransactionReceipt>> blockReceipts = new ArrayList<>(blocks.size());
    blockReceipts.add(Collections.emptyList());

    // Write small chain to storage
    final MutableBlockchain mutableBlockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, blocks.get(0));
    for (int i = 1; i < blocks.size(); i++) {
      final Block block = blocks.get(i);
      final List<TransactionReceipt> receipts = gen.receipts(block);
      blockReceipts.add(receipts);
      mutableBlockchain.appendBlock(block, receipts);
    }

    // Create read only chain
    final Blockchain blockchain = createBlockchain(kvStore, kvStoreVariables);

    for (int i = 0; i < blocks.size(); i++) {
      assertBlockDataIsStored(blockchain, blocks.get(i), blockReceipts.get(i));
    }
    final Block lastBlock = blocks.get(blocks.size() - 1);
    assertBlockIsHead(blockchain, lastBlock);
    assertTotalDifficultiesAreConsistent(blockchain, lastBlock);
  }

  @Test
  public void initializeReadOnly_withGiantDifficultyAndLiveMetrics() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    gen.setBlockOptionsSupplier(
        () -> BlockOptions.create().setDifficulty(Difficulty.of(Long.MAX_VALUE)));
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final ArrayDeque<Block> blocks = new ArrayDeque<>(gen.blockSequence(10));
    final List<List<TransactionReceipt>> blockReceipts = new ArrayList<>(blocks.size());
    blockReceipts.add(Collections.emptyList());

    // Write small chain to storage
    final MutableBlockchain mutableBlockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, blocks.getFirst());
    blocks.stream()
        .filter(block -> !block.equals(blocks.getFirst()))
        .forEach(
            block -> {
              final List<TransactionReceipt> receipts = gen.receipts(block);
              blockReceipts.add(receipts);
              mutableBlockchain.appendBlock(block, receipts);
            });

    // To make sure there are no surprising NPEs during DefaultBlockchain.<init>
    BlockchainStorage blockchainStorage = createStorage(kvStore, kvStoreVariables);
    BlockchainStorage.Updater updater = blockchainStorage.updater();
    updater.setFinalized(blocks.getLast().getHash());
    updater.setSafeBlock(blocks.getLast().getHash());
    updater.setChainHead(blocks.getLast().getHash());
    updater.commit();

    // Create read only chain
    final Blockchain blockchain =
        DefaultBlockchain.create(
            blockchainStorage,
            MetricsSystemFactory.create(MetricsConfiguration.builder().enabled(true).build()),
            0);

    assertThat(blockReceipts.size()).isEqualTo(blocks.size());
    Iterator<List<TransactionReceipt>> it = blockReceipts.iterator();
    blocks.forEach(block -> assertBlockDataIsStored(blockchain, block, it.next()));
    assertBlockIsHead(blockchain, blocks.getLast());
    assertTotalDifficultiesAreConsistent(blockchain, blocks.getLast());
  }

  @Test
  public void initializeReadOnly_emptyStorage() {
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    assertThatThrownBy(() -> createBlockchain(kvStore, kvStoreVariables))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot create Blockchain from empty storage");
  }

  @Test
  public void appendBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .addTransaction(gen.transactions(5))
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);
    blockchain.observeBlockAdded(
        (event ->
            assertThat(event.getLogsWithMetadata())
                .containsExactly(
                    LogWithMetadata.generate(newBlock, receipts, false)
                        .toArray(new LogWithMetadata[] {}))));
    blockchain.appendBlock(newBlock, receipts);

    assertBlockIsHead(blockchain, newBlock);
    assertTotalDifficultiesAreConsistent(blockchain, newBlock);
    assertThat(blockchain.getForks()).isEmpty();
  }

  @Test
  public void unsafeImportSyncBodiesAndReceiptsDoesNotPublishHeadBeforeCommit() throws Exception {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final CommitBlockingKeyValueStorage chainStorage = new CommitBlockingKeyValueStorage();
    final KeyValueStorage variablesStorage = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(chainStorage, variablesStorage, genesisBlock);
    final Block importedBlock =
        gen.block(
            new BlockDataGenerator.BlockOptions()
                .setBlockNumber(1L)
                .setParentHash(genesisBlock.getHash()));
    blockchain.storeBlockHeaders(List.of(importedBlock.getHeader()));
    final SyncBlockWithReceipts syncBlockWithReceipts = toSyncBlockWithReceipts(importedBlock);
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    chainStorage.blockNextCommit();
    final Future<?> importFuture =
        executor.submit(
            () ->
                blockchain.unsafeImportSyncBodiesAndReceipts(
                    List.of(syncBlockWithReceipts), false));
    try {
      assertThat(chainStorage.awaitCommit()).isTrue();
      assertThat(blockchain.getChainHeadBlockNumber())
          .isEqualTo(genesisBlock.getHeader().getNumber());
      assertThat(blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber())).isPresent();

      chainStorage.releaseCommit();
      importFuture.get(10, TimeUnit.SECONDS);

      assertThat(blockchain.getChainHeadBlockNumber())
          .isEqualTo(importedBlock.getHeader().getNumber());
      assertThat(blockchain.getBlockByNumber(blockchain.getChainHeadBlockNumber())).isPresent();
    } finally {
      chainStorage.releaseCommit();
      executor.shutdownNow();
    }
  }

  @Test
  public void appendUnconnectedBlock() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions().setBlockNumber(1L).setParentHash(Hash.ZERO);
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);
    assertThatThrownBy(() -> blockchain.appendBlock(newBlock, receipts))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void appendBlockWithMismatchedReceipts() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);
    receipts.add(gen.receipt());
    assertThatThrownBy(() -> blockchain.appendBlock(newBlock, receipts))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createSmallChain() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final List<Block> chain = gen.blockSequence(3);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
    }

    for (int i = 1; i < chain.size(); i++) {
      assertBlockDataIsStored(blockchain, chain.get(i), blockReceipts.get(i));
    }

    final Block head = chain.get(chain.size() - 1);
    assertBlockIsHead(blockchain, head);
    assertTotalDifficultiesAreConsistent(blockchain, head);
    assertThat(blockchain.getForks()).isEmpty();
  }

  @Test
  public void appendBlockWithReorgToChainAtEqualHeight() {
    final BlockDataGenerator gen = new BlockDataGenerator(1);

    // Setup
    final int chainLength = 3;
    final List<Block> chain = gen.blockSequence(chainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));

    // Listen to block events and add the Logs here
    List<LogWithMetadata> logsWithMetadata = new ArrayList<>();
    blockchain.observeBlockAdded(event -> logsWithMetadata.addAll(event.getLogsWithMetadata()));
    List<LogWithMetadata> expectedLogsWithMetadata = new ArrayList<>();

    // Add initial blocks
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(chain.get(i), blockReceipts.get(i), false));
    }
    assertThat(blockchain.getForks()).isEmpty();
    final Block originalHead = chain.get(chainLength - 1);

    // Create parallel fork of length 1
    final int forkBlock = 2;
    final int commonAncestor = 1;
    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkBlock)
            .setDifficulty(chain.get(forkBlock).getHeader().getDifficulty().add(10L));
    final Block fork = gen.block(options);
    final List<TransactionReceipt> forkReceipts = gen.receipts(fork);
    final List<Block> reorgedChain = new ArrayList<>(chain.subList(0, forkBlock));
    reorgedChain.add(fork);
    final List<List<TransactionReceipt>> reorgedReceipts =
        new ArrayList<>(blockReceipts.subList(0, forkBlock));
    reorgedReceipts.add(forkReceipts);

    // Add fork
    blockchain.appendBlock(fork, forkReceipts);

    // Check chain has reorganized
    for (int i = 0; i < reorgedChain.size(); i++) {
      assertBlockDataIsStored(blockchain, reorgedChain.get(i), reorgedReceipts.get(i));
    }
    // Check old transactions have been removed
    for (final Transaction tx : originalHead.getBody().getTransactions()) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }

    // LogWithMetadata reflecting removal of originalHead's logs
    final List<LogWithMetadata> removedLogs =
        Lists.reverse(
            LogWithMetadata.generate(
                originalHead, blockchain.getTxReceipts(originalHead.getHash()).get(), true));
    expectedLogsWithMetadata.addAll(removedLogs);
    // LogWithMetadata reflecting addition of originalHead's logs
    expectedLogsWithMetadata.addAll(LogWithMetadata.generate(fork, forkReceipts, false));

    assertBlockIsHead(blockchain, fork);
    assertTotalDifficultiesAreConsistent(blockchain, fork);
    // Old chain head should now be tracked as a fork.
    final Set<Hash> forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(1);
    assertThat(forks.stream().anyMatch(f -> f.equals(originalHead.getHash()))).isTrue();
    // Old chain should not be on canonical chain.
    for (int i = commonAncestor + 1; i < chainLength; i++) {
      assertThat(blockchain.blockIsOnCanonicalChain(chain.get(i).getHash())).isFalse();
    }
    assertThat(logsWithMetadata)
        .containsExactly(expectedLogsWithMetadata.toArray(new LogWithMetadata[] {}));
  }

  @Test
  public void appendBlockWithReorgToShorterChain() {
    final BlockDataGenerator gen = new BlockDataGenerator(2);

    // Setup an initial blockchain
    final int originalChainLength = 4;
    final List<Block> chain = gen.blockSequence(originalChainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    // Listen to block events and add the Logs here
    List<LogWithMetadata> logsWithMetadata = new ArrayList<>();
    blockchain.observeBlockAdded(event -> logsWithMetadata.addAll(event.getLogsWithMetadata()));
    List<LogWithMetadata> expectedLogsWithMetadata = new ArrayList<>();
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(chain.get(i), blockReceipts.get(i), false));
    }
    final Block originalHead = chain.get(originalChainLength - 1);

    // Create parallel fork of length 2 from 3 blocks back
    final List<Block> forkBlocks = new ArrayList<>();
    final int forkStart = 1;
    final int commonAncestor = 0;
    // Generate first block
    BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkStart)
            .setDifficulty(chain.get(forkStart).getHeader().getDifficulty().subtract(5L));
    forkBlocks.add(gen.block(options));
    // Generate second block
    final Difficulty remainingDifficultyToOutpace =
        chain
            .get(forkStart + 1)
            .getHeader()
            .getDifficulty()
            .add(chain.get(forkStart + 2).getHeader().getDifficulty());
    options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(forkBlocks.get(0).getHash())
            .setBlockNumber(forkStart + 1)
            .setDifficulty(remainingDifficultyToOutpace.add(10L));
    forkBlocks.add(gen.block(options));
    // Generate corresponding receipts
    final List<List<TransactionReceipt>> forkReceipts =
        forkBlocks.stream().map(gen::receipts).collect(Collectors.toList());

    // Collect fork data
    final List<Block> reorgedChain = new ArrayList<>(chain.subList(0, forkStart));
    reorgedChain.addAll(forkBlocks);
    final List<List<TransactionReceipt>> reorgedReceipts =
        new ArrayList<>(blockReceipts.subList(0, forkStart));
    reorgedReceipts.addAll(forkReceipts);

    // Add first block in fork, which should not cause a reorg
    blockchain.appendBlock(forkBlocks.get(0), forkReceipts.get(0));
    // Check chain has not reorganized
    for (int i = 0; i < chain.size(); i++) {
      assertBlockDataIsStored(blockchain, chain.get(i), blockReceipts.get(i));
    }
    assertBlockIsHead(blockchain, originalHead);
    assertTotalDifficultiesAreConsistent(blockchain, originalHead);
    // Check transactions were not indexed
    for (final Transaction tx : forkBlocks.get(0).getBody().getTransactions()) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }
    // Appended block should be tracked as a fork
    assertThat(blockchain.blockIsOnCanonicalChain(forkBlocks.get(0).getHash())).isFalse();
    Set<Hash> forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(1);
    assertThat(forks.stream().anyMatch(f -> f.equals(forkBlocks.get(0).getHash()))).isTrue();

    // Add second block in fork, which should cause a reorg
    blockchain.appendBlock(forkBlocks.get(1), forkReceipts.get(1));
    // Check chain has reorganized
    for (int i = 0; i < reorgedChain.size(); i++) {
      assertBlockDataIsStored(blockchain, reorgedChain.get(i), reorgedReceipts.get(i));
    }
    assertBlockIsHead(blockchain, forkBlocks.get(1));
    assertTotalDifficultiesAreConsistent(blockchain, forkBlocks.get(1));
    // Check old transactions have been removed
    final List<Transaction> removedTransactions = new ArrayList<>();
    for (int i = forkStart; i < originalChainLength; i++) {
      removedTransactions.addAll(chain.get(i).getBody().getTransactions());
    }
    for (final Transaction tx : removedTransactions) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }
    // LogWithMetadata reflecting removal of logs
    for (int i = originalChainLength - 1; i >= forkStart; i--) {
      final Block currentBlock = chain.get(i);
      expectedLogsWithMetadata.addAll(
          Lists.reverse(
              LogWithMetadata.generate(
                  currentBlock, blockchain.getTxReceipts(currentBlock.getHash()).get(), true)));
    }
    // LogWithMetadata reflecting addition of logs
    for (int i = 0; i < forkBlocks.size(); i++) {
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(forkBlocks.get(i), forkReceipts.get(i), false));
    }

    // Check that blockNumber index for previous chain head has been removed
    assertThat(blockchain.getBlockHashByNumber(originalChainLength - 1)).isNotPresent();
    // Old chain head should now be tracked as a fork.
    forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(1);
    assertThat(forks.stream().anyMatch(f -> f.equals(originalHead.getHash()))).isTrue();
    // Old chain should not be on canonical chain.
    for (int i = commonAncestor + 1; i < originalChainLength; i++) {
      assertThat(blockchain.blockIsOnCanonicalChain(chain.get(i).getHash())).isFalse();
    }
    assertThat(logsWithMetadata)
        .containsExactly(expectedLogsWithMetadata.toArray(new LogWithMetadata[] {}));
  }

  @Test
  public void appendBlockWithReorgToLongerChain() {
    final BlockDataGenerator gen = new BlockDataGenerator(2);

    // Setup an initial blockchain
    final int originalChainLength = 4;
    final List<Block> chain = gen.blockSequence(originalChainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    // Listen to block events and add the Logs here
    List<LogWithMetadata> logsWithMetadata = new ArrayList<>();
    blockchain.observeBlockAdded(event -> logsWithMetadata.addAll(event.getLogsWithMetadata()));
    List<LogWithMetadata> expectedLogsWithMetadata = new ArrayList<>();
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(chain.get(i), blockReceipts.get(i), false));
    }
    final Block originalHead = chain.get(originalChainLength - 1);

    // Create parallel fork of length 2 from 3 blocks back
    final List<Block> forkBlocks = new ArrayList<>();
    final int forkStart = 3;
    final int commonAncestor = 2;
    // Generate first block
    BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkStart)
            .setDifficulty(chain.get(forkStart).getHeader().getDifficulty().subtract(5L));
    forkBlocks.add(gen.block(options));
    // Generate second block
    options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(forkBlocks.get(0).getHash())
            .setBlockNumber(forkStart + 1)
            .setDifficulty(Difficulty.of(10L));
    forkBlocks.add(gen.block(options));
    // Generate corresponding receipts
    final List<List<TransactionReceipt>> forkReceipts =
        forkBlocks.stream().map(gen::receipts).collect(Collectors.toList());

    // Collect fork data
    final List<Block> reorgedChain = new ArrayList<>(chain.subList(0, forkStart));
    reorgedChain.addAll(forkBlocks);
    final List<List<TransactionReceipt>> reorgedReceipts =
        new ArrayList<>(blockReceipts.subList(0, forkStart));
    reorgedReceipts.addAll(forkReceipts);

    // Add first block in fork, which should not cause a reorg
    blockchain.appendBlock(forkBlocks.get(0), forkReceipts.get(0));
    // Check chain has not reorganized
    for (int i = 0; i < chain.size(); i++) {
      assertBlockDataIsStored(blockchain, chain.get(i), blockReceipts.get(i));
    }
    assertBlockIsHead(blockchain, originalHead);
    assertTotalDifficultiesAreConsistent(blockchain, originalHead);
    // Check transactions were not indexed
    for (final Transaction tx : forkBlocks.get(0).getBody().getTransactions()) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }
    // Appended block should be tracked as a fork
    assertThat(blockchain.blockIsOnCanonicalChain(forkBlocks.get(0).getHash())).isFalse();
    Set<Hash> forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(1);
    assertThat(forks.stream().anyMatch(f -> f.equals(forkBlocks.get(0).getHash()))).isTrue();

    // Add second block in fork, which should cause a reorg
    blockchain.appendBlock(forkBlocks.get(1), forkReceipts.get(1));
    // Check chain has reorganized
    for (int i = 0; i < reorgedChain.size(); i++) {
      assertBlockDataIsStored(blockchain, reorgedChain.get(i), reorgedReceipts.get(i));
    }
    assertBlockIsHead(blockchain, forkBlocks.get(1));
    assertTotalDifficultiesAreConsistent(blockchain, forkBlocks.get(1));
    // Check old transactions have been removed
    final List<Transaction> removedTransactions = new ArrayList<>();
    for (int i = forkStart; i < originalChainLength; i++) {
      removedTransactions.addAll(chain.get(i).getBody().getTransactions());
    }
    for (final Transaction tx : removedTransactions) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }
    // LogWithMetadata reflecting removal of logs
    for (int i = originalChainLength - 1; i >= forkStart; i--) {
      final Block currentBlock = chain.get(i);
      expectedLogsWithMetadata.addAll(
          Lists.reverse(
              LogWithMetadata.generate(
                  currentBlock, blockchain.getTxReceipts(currentBlock.getHash()).get(), true)));
    }
    // LogWithMetadata reflecting addition of logs
    for (int i = 0; i < forkBlocks.size(); i++) {
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(forkBlocks.get(i), forkReceipts.get(i), false));
    }
    // Old chain head should now be tracked as a fork.
    forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(1);
    assertThat(forks.stream().anyMatch(f -> f.equals(originalHead.getHash()))).isTrue();
    // Old chain should not be on canonical chain.
    for (int i = commonAncestor + 1; i < originalChainLength; i++) {
      assertThat(blockchain.blockIsOnCanonicalChain(chain.get(i).getHash())).isFalse();
    }
    assertThat(logsWithMetadata)
        .containsExactly(expectedLogsWithMetadata.toArray(new LogWithMetadata[] {}));
  }

  @Test
  public void reorgWithOverlappingTransactions() {
    final BlockDataGenerator gen = new BlockDataGenerator(1);

    // Setup an initial blockchain
    final int chainLength = 3;
    final List<Block> chain = gen.blockSequence(chainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    // Listen to block events and add the Logs here
    List<LogWithMetadata> logsWithMetadata = new ArrayList<>();
    blockchain.observeBlockAdded(event -> logsWithMetadata.addAll(event.getLogsWithMetadata()));
    List<LogWithMetadata> expectedLogsWithMetadata = new ArrayList<>();
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(chain.get(i), blockReceipts.get(i), false));
    }
    final Transaction overlappingTx = chain.get(chainLength - 1).getBody().getTransactions().get(0);

    // Create parallel fork of length 1
    final int forkBlock = 2;
    final int commonAncestor = 1;
    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkBlock)
            .setDifficulty(chain.get(forkBlock).getHeader().getDifficulty().add(10L))
            .addTransaction(overlappingTx)
            .addTransaction(gen.transaction());
    final Block fork = gen.block(options);
    final List<TransactionReceipt> forkReceipts = gen.receipts(fork);
    final List<Block> reorgedChain = new ArrayList<>(chain.subList(0, forkBlock));
    reorgedChain.add(fork);
    final List<List<TransactionReceipt>> reorgedReceipts =
        new ArrayList<>(blockReceipts.subList(0, forkBlock));
    reorgedReceipts.add(forkReceipts);

    // Add fork
    blockchain.appendBlock(fork, forkReceipts);

    // Check chain has reorganized
    for (int i = 0; i < reorgedChain.size(); i++) {
      assertBlockDataIsStored(blockchain, reorgedChain.get(i), reorgedReceipts.get(i));
    }

    // Check old transactions have been removed
    for (final Transaction tx : chain.get(chainLength - 1).getBody().getTransactions()) {
      final Optional<Transaction> actualTransaction = blockchain.getTransactionByHash(tx.getHash());
      if (tx.equals(overlappingTx)) {
        assertThat(actualTransaction).isPresent();
      } else {
        assertThat(actualTransaction).isNotPresent();
      }
    }
    // LogWithMetadata reflecting removal of logs
    for (int i = chainLength - 1; i >= forkBlock; i--) {
      final Block currentBlock = chain.get(i);
      expectedLogsWithMetadata.addAll(
          Lists.reverse(
              LogWithMetadata.generate(
                  currentBlock, blockchain.getTxReceipts(currentBlock.getHash()).get(), true)));
    }
    // LogWithMetadata reflecting addition of logs
    expectedLogsWithMetadata.addAll(LogWithMetadata.generate(fork, forkReceipts, false));
    assertThat(logsWithMetadata)
        .containsExactly(expectedLogsWithMetadata.toArray(new LogWithMetadata[] {}));
  }

  @Test
  public void rewindChain() {
    final BlockDataGenerator gen = new BlockDataGenerator(2);

    // Setup an initial blockchain
    final int originalChainLength = 4;
    final List<Block> chain = gen.blockSequence(originalChainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
    }
    final Block originalHead = blockchain.getChainHeadBlock();
    final Block targetHead =
        blockchain.getBlockByHash(originalHead.getHeader().getParentHash()).get();

    // rewind it by 1 block
    blockchain.rewindToBlock(targetHead.getHeader().getNumber());

    // Check chain has the expected blocks
    for (int i = 0; i < chain.size() - 1; i++) {
      assertBlockDataIsStored(blockchain, chain.get(i), blockReceipts.get(i));
    }
    assertBlockIsHead(blockchain, targetHead);

    // Check transactions were not indexed
    for (final Transaction tx : originalHead.getBody().getTransactions()) {
      assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
    }

    // Check that blockNumber index for previous chain head has been removed
    assertThat(blockchain.getBlockHashByNumber(originalHead.getHeader().getNumber()))
        .isNotPresent();
    // Old chain head should not be tracked.
    assertThat(blockchain.blockIsOnCanonicalChain(originalHead.getHash())).isFalse();
  }

  @Test
  public void appendBlockForFork() {
    final BlockDataGenerator gen = new BlockDataGenerator(2);

    // Setup an initial blockchain
    final int originalChainLength = 4;
    final List<Block> chain = gen.blockSequence(originalChainLength);
    final List<List<TransactionReceipt>> blockReceipts =
        chain.stream().map(gen::receipts).collect(Collectors.toList());
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, chain.get(0));
    // Listen to block events and add the Logs here
    List<LogWithMetadata> logsWithMetadata = new ArrayList<>();
    blockchain.observeBlockAdded(event -> logsWithMetadata.addAll(event.getLogsWithMetadata()));
    List<LogWithMetadata> expectedLogsWithMetadata = new ArrayList<>();
    for (int i = 1; i < chain.size(); i++) {
      blockchain.appendBlock(chain.get(i), blockReceipts.get(i));
      expectedLogsWithMetadata.addAll(
          LogWithMetadata.generate(chain.get(i), blockReceipts.get(i), false));
    }
    final Block originalHead = chain.get(originalChainLength - 1);

    // Create fork of length 2
    final List<Block> forkBlocks = new ArrayList<>();
    final int forkStart = 2;
    final int commonAncestor = 1;
    // Generate first block
    BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkStart)
            .setDifficulty(chain.get(forkStart).getHeader().getDifficulty().subtract(5L));
    forkBlocks.add(gen.block(options));
    // Generate second block
    options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(forkBlocks.get(0).getHash())
            .setBlockNumber(forkStart + 1)
            .setDifficulty(chain.get(forkStart + 1).getHeader().getDifficulty().subtract(5L));
    forkBlocks.add(gen.block(options));
    // Generate corresponding receipts
    final List<List<TransactionReceipt>> forkReceipts =
        forkBlocks.stream().map(gen::receipts).collect(Collectors.toList());

    // Add fork blocks, which should not cause a reorg
    for (int i = 0; i < forkBlocks.size(); i++) {
      final Block forkBlock = forkBlocks.get(i);
      blockchain.appendBlock(forkBlock, forkReceipts.get(i));
      // Check chain has not reorganized
      for (int j = 0; j < chain.size(); j++) {
        assertBlockDataIsStored(blockchain, chain.get(j), blockReceipts.get(j));
      }
      assertBlockIsHead(blockchain, originalHead);
      assertTotalDifficultiesAreConsistent(blockchain, originalHead);
      // Check transactions were not indexed
      for (final Transaction tx : forkBlock.getBody().getTransactions()) {
        assertThat(blockchain.getTransactionByHash(tx.getHash())).isNotPresent();
      }
      // Appended block should be tracked as a fork
      assertThat(blockchain.blockIsOnCanonicalChain(forkBlock.getHash())).isFalse();
      final Set<Hash> forks = blockchain.getForks();
      assertThat(forks.size()).isEqualTo(1);
      final Optional<Hash> trackedFork =
          forks.stream().filter(f -> f.equals(forkBlock.getHash())).findAny();
      assertThat(trackedFork).isPresent();
    }

    // Add another independent fork
    options =
        new BlockDataGenerator.BlockOptions()
            .setParentHash(chain.get(commonAncestor).getHash())
            .setBlockNumber(forkStart)
            .setDifficulty(chain.get(forkStart).getHeader().getDifficulty().subtract(5L));
    final Block secondFork = gen.block(options);
    blockchain.appendBlock(secondFork, gen.receipts(secondFork));

    // We should now be tracking 2 forks
    assertThat(blockchain.blockIsOnCanonicalChain(secondFork.getHash())).isFalse();
    final Set<Hash> forks = blockchain.getForks();
    assertThat(forks.size()).isEqualTo(2);
    final Optional<Hash> trackedFork =
        forks.stream().filter(f -> f.equals(secondFork.getHash())).findAny();
    assertThat(trackedFork).isPresent();

    // Head should not have changed
    assertBlockIsHead(blockchain, originalHead);
    // We should only have the log events from when we initially created the chain. None from forks.
    assertThat(logsWithMetadata)
        .containsExactly(expectedLogsWithMetadata.toArray(new LogWithMetadata[] {}));
  }

  @Test
  public void blockAddedObserver_removeNonexistentObserver() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    assertThat(blockchain.removeObserver(7)).isFalse();
  }

  @Test
  public void blockAddedObserver_addRemoveSingle() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final long observerId = blockchain.observeBlockAdded(__ -> {});
    assertThat(blockchain.observerCount()).isEqualTo(1);

    assertThat(blockchain.removeObserver(observerId)).isTrue();
    assertThat(blockchain.observerCount()).isEqualTo(0);
  }

  @Test
  public void blockAddedObserver_nullObserver() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    assertThatThrownBy(() -> blockchain.observeBlockAdded(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void blockAddedObserver_addRemoveMultiple() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final long observerId1 = blockchain.observeBlockAdded(__ -> {});
    assertThat(blockchain.observerCount()).isEqualTo(1);

    final long observerId2 = blockchain.observeBlockAdded(__ -> {});
    assertThat(blockchain.observerCount()).isEqualTo(2);

    final long observerId3 = blockchain.observeBlockAdded(__ -> {});
    assertThat(blockchain.observerCount()).isEqualTo(3);

    assertThat(blockchain.removeObserver(observerId1)).isTrue();
    assertThat(blockchain.observerCount()).isEqualTo(2);

    assertThat(blockchain.removeObserver(observerId2)).isTrue();
    assertThat(blockchain.observerCount()).isEqualTo(1);

    assertThat(blockchain.removeObserver(observerId3)).isTrue();
    assertThat(blockchain.observerCount()).isEqualTo(0);
  }

  @Test
  public void blockAddedObserver_invokedSingle() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);

    final AtomicBoolean observerInvoked = new AtomicBoolean(false);
    blockchain.observeBlockAdded(__ -> observerInvoked.set(true));

    blockchain.appendBlock(newBlock, receipts);

    assertThat(observerInvoked.get()).isTrue();
  }

  @Test
  public void blockAddedObserver_invokedMultiple() {
    final BlockDataGenerator gen = new BlockDataGenerator();

    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);

    final AtomicBoolean observer1Invoked = new AtomicBoolean(false);
    blockchain.observeBlockAdded(__ -> observer1Invoked.set(true));

    final AtomicBoolean observer2Invoked = new AtomicBoolean(false);
    blockchain.observeBlockAdded(__ -> observer2Invoked.set(true));

    final AtomicBoolean observer3Invoked = new AtomicBoolean(false);
    blockchain.observeBlockAdded(__ -> observer3Invoked.set(true));

    blockchain.appendBlock(newBlock, receipts);

    assertThat(observer1Invoked.get()).isTrue();
    assertThat(observer2Invoked.get()).isTrue();
    assertThat(observer3Invoked.get()).isTrue();
  }

  @Test
  public void testCacheEmptyWhenNumberOfBlocksToCacheIsZero() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock);

    assertThat(blockchain.getBlockHeadersCache()).isEmpty();
    assertThat(blockchain.getBlockBodiesCache()).isEmpty();
    assertThat(blockchain.getTransactionReceiptsCache()).isEmpty();
    assertThat(blockchain.getTotalDifficultyCache()).isEmpty();
  }

  @Test
  public void testCacheUsedWhenNumberOfBlocksToCacheNotZero() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock, "/data/test", 512, 0);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);

    assertThat(blockchain.getBlockHeadersCache()).isNotEmpty();
    assertThat(blockchain.getBlockBodiesCache()).isNotEmpty();
    assertThat(blockchain.getTransactionReceiptsCache()).isNotEmpty();
    assertThat(blockchain.getTotalDifficultyCache()).isNotEmpty();

    assertThat(blockchain.getBlockHeadersCache().get().size()).isEqualTo(0);
    assertThat(blockchain.getBlockBodiesCache().get().size()).isEqualTo(0);
    assertThat(blockchain.getTransactionReceiptsCache().get().size()).isEqualTo(0);
    assertThat(blockchain.getTotalDifficultyCache().get().size()).isEqualTo(0);

    blockchain.appendBlock(newBlock, receipts);

    assertThat(blockchain.getBlockHeadersCache().get().size()).isEqualTo(1);
    assertThat(blockchain.getBlockHeadersCache().get().getIfPresent(newBlock.getHash()))
        .isEqualTo(newBlock.getHeader());

    assertThat(blockchain.getBlockBodiesCache().get().size()).isEqualTo(1);
    assertThat(blockchain.getBlockBodiesCache().get().getIfPresent(newBlock.getHash()))
        .isEqualTo(newBlock.getBody());

    assertThat(blockchain.getTransactionReceiptsCache().get().size()).isEqualTo(1);
    assertThat(blockchain.getTransactionReceiptsCache().get().getIfPresent(newBlock.getHash()))
        .isEqualTo(receipts);

    assertThat(blockchain.getTotalDifficultyCache().get().size()).isEqualTo(1);
    assertThat(blockchain.getTotalDifficultyCache().get().getIfPresent(newBlock.getHash()))
        .isEqualTo(newBlock.getHeader().getDifficulty());
  }

  @Test
  public void testCacheUsedWhenNumberOfBlockHeadersToCacheNotZero() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final KeyValueStorage kvStore = new InMemoryKeyValueStorage();
    final KeyValueStorage kvStoreVariables = new InMemoryKeyValueStorage();
    final Block genesisBlock = gen.genesisBlock();
    final DefaultBlockchain blockchain =
        createMutableBlockchain(kvStore, kvStoreVariables, genesisBlock, "/data/test", 0, 10000);

    final BlockDataGenerator.BlockOptions options =
        new BlockDataGenerator.BlockOptions()
            .setBlockNumber(1L)
            .setParentHash(genesisBlock.getHash());
    final Block newBlock = gen.block(options);
    final List<TransactionReceipt> receipts = gen.receipts(newBlock);

    assertThat(blockchain.getBlockHeadersCache()).isNotEmpty();
    assertThat(blockchain.getBlockBodiesCache()).isEmpty();
    assertThat(blockchain.getTransactionReceiptsCache()).isEmpty();
    assertThat(blockchain.getTotalDifficultyCache()).isEmpty();

    assertThat(blockchain.getBlockHeadersCache().get().size()).isEqualTo(0);

    blockchain.appendBlock(newBlock, receipts);

    assertThat(blockchain.getBlockHeadersCache().get().size()).isEqualTo(1);
    assertThat(blockchain.getBlockHeadersCache().get().getIfPresent(newBlock.getHash()))
        .isEqualTo(newBlock.getHeader());
  }

  /*
   * Check that block header, block body, block number, transaction locations, and receipts for this
   * block are all stored.
   */
  private void assertBlockDataIsStored(
      final Blockchain blockchain, final Block block, final List<TransactionReceipt> receipts) {
    final Hash hash = block.getHash();
    assertThat(blockchain.getBlockHashByNumber(block.getHeader().getNumber()).get())
        .isEqualTo(hash);
    assertThat(blockchain.getBlockHeader(block.getHeader().getNumber()).get())
        .isEqualTo(block.getHeader());
    assertThat(blockchain.getBlockHeader(hash).get()).isEqualTo(block.getHeader());
    assertThat(blockchain.getBlockBody(hash).get()).isEqualTo(block.getBody());
    assertThat(blockchain.blockIsOnCanonicalChain(block.getHash())).isTrue();

    final List<Transaction> txs = block.getBody().getTransactions();
    for (int i = 0; i < txs.size(); i++) {
      final Transaction expected = txs.get(i);
      final Transaction actual = blockchain.getTransactionByHash(expected.getHash()).get();
      assertThat(actual).isEqualTo(expected);
    }
    final List<TransactionReceipt> actualReceipts = blockchain.getTxReceipts(hash).get();
    assertThat(actualReceipts).isEqualTo(receipts);
  }

  private void assertBlockIsHead(final Blockchain blockchain, final Block head) {
    assertThat(blockchain.getChainHeadHash()).isEqualTo(head.getHash());
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(head.getHeader().getNumber());
    assertThat(blockchain.getChainHead().getHash()).isEqualTo(head.getHash());
  }

  private void assertTotalDifficultiesAreConsistent(final Blockchain blockchain, final Block head) {
    // Check that total difficulties are summed correctly
    long num = BlockHeader.GENESIS_BLOCK_NUMBER;
    Difficulty td = Difficulty.ZERO;
    while (num <= head.getHeader().getNumber()) {
      final Hash curHash = blockchain.getBlockHashByNumber(num).get();
      final BlockHeader curHead = blockchain.getBlockHeader(curHash).get();
      td = td.add(curHead.getDifficulty());
      assertThat(blockchain.getTotalDifficultyByHash(curHash).get()).isEqualTo(td);

      num += 1;
    }

    // Check reported chainhead td
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(td);
  }

  private BlockchainStorage createStorage(
      final KeyValueStorage kvStoreChain, final KeyValueStorage kvStorageVariables) {
    return new KeyValueStoragePrefixedKeyBlockchainStorage(
        kvStoreChain,
        new VariablesKeyValueStorage(kvStorageVariables),
        new MainnetBlockHeaderFunctions(),
        false);
  }

  private SyncBlockWithReceipts toSyncBlockWithReceipts(final Block block) {
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    block.getBody().writeWrappedBodyTo(output);
    final SyncBlockBody body =
        SyncBlockBody.readWrappedBodyFrom(
            new BytesValueRLPInput(output.encoded(), false),
            false,
            new DefaultProtocolSchedule(Optional.of(BigInteger.ONE)));
    return new SyncBlockWithReceipts(
        new SyncBlock(block.getHeader(), body), Collections.emptyList());
  }

  private DefaultBlockchain createMutableBlockchain(
      final KeyValueStorage kvStore,
      final KeyValueStorage kvStorageVariables,
      final Block genesisBlock) {
    return (DefaultBlockchain)
        DefaultBlockchain.createMutable(
            genesisBlock, createStorage(kvStore, kvStorageVariables), new NoOpMetricsSystem(), 0);
  }

  private DefaultBlockchain createMutableBlockchain(
      final KeyValueStorage kvStore,
      final KeyValueStorage kvStorageVariables,
      final Block genesisBlock,
      final String dataDirectory) {
    return (DefaultBlockchain)
        DefaultBlockchain.createMutable(
            genesisBlock,
            createStorage(kvStore, kvStorageVariables),
            new NoOpMetricsSystem(),
            0,
            dataDirectory);
  }

  private Blockchain createBlockchain(
      final KeyValueStorage kvStore, final KeyValueStorage kvStorageVariables) {
    return DefaultBlockchain.create(
        createStorage(kvStore, kvStorageVariables), new NoOpMetricsSystem(), 0);
  }

  private DefaultBlockchain createMutableBlockchain(
      final KeyValueStorage kvStore,
      final KeyValueStorage kvStorageVariables,
      final Block genesisBlock,
      final String dataDirectory,
      final int numberOfBlocksToCache,
      final int numberOfBlockHeadersToCache) {
    return (DefaultBlockchain)
        DefaultBlockchain.createMutable(
            genesisBlock,
            createStorage(kvStore, kvStorageVariables),
            new NoOpMetricsSystem(),
            0,
            dataDirectory,
            numberOfBlocksToCache,
            numberOfBlockHeadersToCache);
  }

  @Test
  void testGetEarliestBlockNumber() {
    final long earliestBlockNumber = 3L;
    final long lastBlockNumber = 5L;
    DefaultBlockchain blockchain = mockMutableBlockchain(earliestBlockNumber, lastBlockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(earliestBlockNumber);
  }

  @Test
  void testGetEarliestBlockNumberWhenNoBlocksExist() {
    DefaultBlockchain blockchain = mockMutableBlockchain(0L, 0L);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isEmpty();
  }

  @Test
  void testGetEarliestBlockNumberWithSingleBlock() {
    final long blockNumber = 2L;
    DefaultBlockchain blockchain = mockMutableBlockchain(blockNumber, blockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(blockNumber);
  }

  @Test
  void testGetEarliestBlockNumberWithSingleBlockIsGenesis() {
    final long blockNumber = 1L;
    DefaultBlockchain blockchain = mockMutableBlockchain(blockNumber, blockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(BlockHeader.GENESIS_BLOCK_NUMBER);
  }

  @Test
  void testGetEarliestBlockNumberWithAllBlocks() {
    final long earliestBlockNumber = 0L;
    final long lastBlockNumber = 10L;
    DefaultBlockchain blockchain = mockMutableBlockchain(earliestBlockNumber, lastBlockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(earliestBlockNumber);
  }

  @Test
  void testGetEarliestBlockNumberWithWholeChainIsGenesis() {
    final long earliestBlockNumber = 1L;
    final long lastBlockNumber = 10L;
    DefaultBlockchain blockchain = mockMutableBlockchain(earliestBlockNumber, lastBlockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(BlockHeader.GENESIS_BLOCK_NUMBER);
  }

  @Test
  void testGetEarliestBlockNumberWhenChainHeadIsEarliest() {
    final long blockNumber = 5L;
    DefaultBlockchain blockchain = mockMutableBlockchain(blockNumber, blockNumber);
    Optional<Long> result = blockchain.getEarliestBlockNumber();
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(blockNumber);
  }

  private DefaultBlockchain mockMutableBlockchain(
      final long earliestBlockNumber, final long lastBlockNumber) {
    DefaultBlockchain blockchain = mock(DefaultBlockchain.class);
    when(blockchain.getEarliestBlockNumber()).thenCallRealMethod();
    for (long i = earliestBlockNumber; i <= lastBlockNumber; i++) {
      Block block = mock(Block.class);
      BlockHeader blockHeader = mock(BlockHeader.class);
      when(blockHeader.getNumber()).thenReturn(i);
      when(block.getHeader()).thenReturn(blockHeader);
      when(blockchain.getBlockByNumber(i)).thenReturn(Optional.of(block));
    }
    when(blockchain.getChainHeadBlockNumber()).thenReturn(lastBlockNumber);
    return blockchain;
  }

  private static class CommitBlockingKeyValueStorage extends InMemoryKeyValueStorage {
    private final AtomicBoolean blockNextCommit = new AtomicBoolean();
    private final CountDownLatch commitStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCommit = new CountDownLatch(1);

    void blockNextCommit() {
      blockNextCommit.set(true);
    }

    boolean awaitCommit() throws InterruptedException {
      return commitStarted.await(10, TimeUnit.SECONDS);
    }

    void releaseCommit() {
      releaseCommit.countDown();
    }

    @Override
    public KeyValueStorageTransaction startTransaction() {
      final KeyValueStorageTransaction transaction = super.startTransaction();
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
          if (blockNextCommit.compareAndSet(true, false)) {
            commitStarted.countDown();
            try {
              if (!releaseCommit.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to release the commit");
              }
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("Interrupted while waiting to release the commit", e);
            }
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
}
