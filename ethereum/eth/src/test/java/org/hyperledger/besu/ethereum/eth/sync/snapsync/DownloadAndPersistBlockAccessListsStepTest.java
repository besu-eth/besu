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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DownloadAndPersistBlockAccessListsStepTest {

  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator(1);

  private DefaultBlockchain blockchain;

  @BeforeEach
  void setUp() {
    final Block genesisBlock =
        blockDataGenerator.genesisBlock(new BlockOptions().setDifficulty(Difficulty.ZERO));
    blockchain =
        (DefaultBlockchain) InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisBlock);
  }

  @Test
  void shouldPersistDownloadedAccessListsForBalEnabledHeadersOnly() {
    final BlockAccessList firstBal = blockDataGenerator.blockAccessList();
    final BlockAccessList secondBal = blockDataGenerator.blockAccessList();

    final BlockHeader nonBalHeader = nonBalHeader(101);
    final BlockHeader firstBalHeader = balEnabledHeader(100, firstBal);
    final BlockHeader secondBalHeader = balEnabledHeader(102, secondBal);

    final List<SyncBlockWithReceipts> syncBlocks =
        List.of(
            syncBlockWithHeader(nonBalHeader),
            syncBlockWithHeader(firstBalHeader),
            syncBlockWithHeader(secondBalHeader));

    final AtomicReference<List<BlockHeader>> requestedHeaders = new AtomicReference<>();
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            Duration.ofSeconds(2),
            headers -> {
              requestedHeaders.set(headers);
              return CompletableFuture.completedFuture(List.of(firstBal, secondBal));
            });

    final List<SyncBlockWithReceipts> result = step.apply(syncBlocks).join();

    assertThat(result).isSameAs(syncBlocks);
    assertThat(requestedHeaders.get()).containsExactly(firstBalHeader, secondBalHeader);
    assertThat(blockchain.getBlockAccessList(nonBalHeader.getHash())).isEmpty();
    assertThat(blockchain.getBlockAccessList(firstBalHeader.getHash())).contains(firstBal);
    assertThat(blockchain.getBlockAccessList(secondBalHeader.getHash())).contains(secondBal);
  }

  @Test
  void shouldPersistOnlyAvailableSubsetWhenDownloaderReturnsFewerAccessListsThanHeaders() {
    final BlockAccessList firstBal = blockDataGenerator.blockAccessList();
    final BlockAccessList secondBal = blockDataGenerator.blockAccessList();

    final BlockHeader firstBalHeader = balEnabledHeader(200, firstBal);
    final BlockHeader secondBalHeader = balEnabledHeader(201, secondBal);

    final List<SyncBlockWithReceipts> syncBlocks =
        List.of(syncBlockWithHeader(firstBalHeader), syncBlockWithHeader(secondBalHeader));

    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            Duration.ofSeconds(2),
            headers -> CompletableFuture.completedFuture(List.of(firstBal)));

    step.apply(syncBlocks).join();

    assertThat(blockchain.getBlockAccessList(firstBalHeader.getHash())).contains(firstBal);
    assertThat(blockchain.getBlockAccessList(secondBalHeader.getHash())).isEmpty();
  }

  @Test
  void shouldSkipPersistingWhenDownloaderFails() {
    final BlockAccessList expectedBal = blockDataGenerator.blockAccessList();
    final BlockHeader balHeader = balEnabledHeader(300, expectedBal);

    final List<SyncBlockWithReceipts> syncBlocks = List.of(syncBlockWithHeader(balHeader));

    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            Duration.ofSeconds(2),
            headers -> CompletableFuture.failedFuture(new RuntimeException("download failed")));

    final List<SyncBlockWithReceipts> result = step.apply(syncBlocks).join();

    assertThat(result).isSameAs(syncBlocks);
    assertThat(blockchain.getBlockAccessList(balHeader.getHash())).isEmpty();
  }

  @Test
  void shouldSkipPersistingWhenDownloaderReturnsNullResponse() {
    final BlockAccessList expectedBal = blockDataGenerator.blockAccessList();
    final BlockHeader balHeader = balEnabledHeader(400, expectedBal);

    final List<SyncBlockWithReceipts> syncBlocks = List.of(syncBlockWithHeader(balHeader));

    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain, Duration.ofSeconds(2), headers -> CompletableFuture.completedFuture(null));

    step.apply(syncBlocks).join();

    assertThat(blockchain.getBlockAccessList(balHeader.getHash())).isEmpty();
  }

  @Test
  void shouldReturnImmediatelyWhenNoBalEnabledHeadersExist() {
    final BlockHeader firstHeader = nonBalHeader(500);
    final BlockHeader secondHeader = nonBalHeader(501);
    final List<SyncBlockWithReceipts> syncBlocks =
        List.of(syncBlockWithHeader(firstHeader), syncBlockWithHeader(secondHeader));

    final AtomicReference<Boolean> wasDownloaderInvoked = new AtomicReference<>(false);
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            Duration.ofSeconds(2),
            headers -> {
              wasDownloaderInvoked.set(true);
              return CompletableFuture.completedFuture(List.of());
            });

    final List<SyncBlockWithReceipts> result = step.apply(syncBlocks).join();

    assertThat(result).isSameAs(syncBlocks);
    assertThat(wasDownloaderInvoked.get()).isFalse();
    assertThat(blockchain.getBlockAccessList(firstHeader.getHash())).isEmpty();
    assertThat(blockchain.getBlockAccessList(secondHeader.getHash())).isEmpty();
  }

  private SyncBlockWithReceipts syncBlockWithHeader(final BlockHeader header) {
    return new SyncBlockWithReceipts(new SyncBlock(header, null), List.of());
  }

  private BlockHeader balEnabledHeader(final long number, final BlockAccessList bal) {
    return new BlockHeaderTestFixture()
        .number(number)
        .balHash(BodyValidation.balHash(bal))
        .buildHeader();
  }

  private BlockHeader nonBalHeader(final long number) {
    return new BlockHeaderTestFixture().number(number).buildHeader();
  }
}
