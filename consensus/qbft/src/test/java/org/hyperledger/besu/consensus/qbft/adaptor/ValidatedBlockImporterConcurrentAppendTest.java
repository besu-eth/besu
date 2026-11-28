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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus.ALREADY_IMPORTED;
import static org.hyperledger.besu.ethereum.mainnet.BlockImportResult.BlockImportStatus.IMPORTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStoragePrefixedKeyBlockchainStorage;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * The QBFT round and a NewBlock import can import the same block (same on-chain hash) at the same
 * time. DefaultBlockchain.appendBlock caches the header before it commits the block, so while the
 * other import is committing, the block can be found by hash but its total difficulty is not stored
 * yet. A caller told ALREADY_IMPORTED then fails in EthProtocolManager.blockMined with "Unable to
 * get total difficulty from blockchain for mined block".
 */
class ValidatedBlockImporterConcurrentAppendTest {

  private static class CommitBlockingKeyValueStorage extends InMemoryKeyValueStorage {
    private final AtomicBoolean blockNextCommit = new AtomicBoolean();
    private final CountDownLatch commitStarted = new CountDownLatch(1);
    private final CountDownLatch releaseCommit = new CountDownLatch(1);

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
              releaseCommit.await(10, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
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

  @Test
  void blockIsNotReportedAsImportedWhileAnotherImportIsStillStoringIt() throws Exception {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final Block genesis = gen.genesisBlock();
    final CommitBlockingKeyValueStorage chainStorage = new CommitBlockingKeyValueStorage();
    // --cache-last-blocks defaults to 128, so the header cache is on
    final MutableBlockchain blockchain =
        DefaultBlockchain.createMutable(
            genesis,
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                chainStorage,
                new VariablesKeyValueStorage(new InMemoryKeyValueStorage()),
                new MainnetBlockHeaderFunctions(),
                false),
            new NoOpMetricsSystem(),
            0,
            "/data/test",
            DefaultBlockchain.DEFAULT_BLOCKS_TO_CACHE,
            0,
            0,
            false);
    final Block block =
        gen.block(
            BlockDataGenerator.BlockOptions.create()
                .setParentHash(genesis.getHash())
                .setBlockNumber(1));
    final List<TransactionReceipt> receipts = gen.receipts(block);

    final ProtocolContext context = mock(ProtocolContext.class);
    when(context.getBlockchain()).thenReturn(blockchain);
    // the first import executes the block and appends it, like MainnetBlockImporter
    final BlockImporter executingImporter = mock(BlockImporter.class);
    when(executingImporter.importBlock(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final MutableBlockchain chain =
                  invocation.<ProtocolContext>getArgument(0).getBlockchain();
              if (chain.contains(block.getHash())) {
                return new BlockImportResult(ALREADY_IMPORTED);
              }
              chain.appendBlock(block, receipts, Optional.empty());
              return new BlockImportResult(IMPORTED);
            });
    final ValidatedBlockImporter importer =
        new ValidatedBlockImporter(
            executingImporter, mock(BlockValidator.class), new ValidatedBlockCache());

    // NewBlock import: paused while it commits the block
    chainStorage.blockNextCommit.set(true);
    final CompletableFuture<BlockImportResult> networkImport =
        CompletableFuture.supplyAsync(() -> importBlock(importer, context, block));
    assertThat(chainStorage.commitStarted.await(10, TimeUnit.SECONDS)).isTrue();

    // the QBFT round imports the block it has just committed
    final CompletableFuture<Boolean> roundSeesStoredBlock = new CompletableFuture<>();
    final Thread roundThread =
        new Thread(
            () -> {
              final BlockImportResult result = importBlock(importer, context, block);
              // what EthProtocolManager.blockMined needs right after a successful import
              roundSeesStoredBlock.complete(
                  result.isImported()
                      && blockchain.getTotalDifficultyByHash(block.getHash()).isPresent());
            });
    roundThread.start();
    // it either returns, or waits for the import that is committing the block
    await()
        .atMost(Duration.ofSeconds(10))
        .until(
            () ->
                roundSeesStoredBlock.isDone()
                    || roundThread.getState() == Thread.State.BLOCKED
                    || roundThread.getState() == Thread.State.WAITING
                    || roundThread.getState() == Thread.State.TIMED_WAITING);
    assertThat(roundSeesStoredBlock.isDone())
        .as("the round's import returned while the block was still being committed")
        .isFalse();

    chainStorage.releaseCommit.countDown();
    assertThat(networkImport.get(10, TimeUnit.SECONDS).getStatus()).isEqualTo(IMPORTED);
    assertThat(roundSeesStoredBlock.get(10, TimeUnit.SECONDS)).isTrue();
    roundThread.join(Duration.ofSeconds(10));
  }

  private static BlockImportResult importBlock(
      final ValidatedBlockImporter importer, final ProtocolContext context, final Block block) {
    return importer.importBlock(
        context, block, HeaderValidationMode.FULL, HeaderValidationMode.FULL, Optional.empty());
  }
}
