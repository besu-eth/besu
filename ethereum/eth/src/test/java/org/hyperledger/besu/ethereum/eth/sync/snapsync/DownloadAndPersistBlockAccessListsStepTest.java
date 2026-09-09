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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

class DownloadAndPersistBlockAccessListsStepTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final BlockDataGenerator generator = new BlockDataGenerator(1);
  private final DefaultBlockchain blockchain =
      (DefaultBlockchain)
          InMemoryKeyValueStorageProvider.createInMemoryBlockchain(generator.genesisBlock());

  private final Function<List<BlockHeader>, CompletableFuture<List<SyncBlockAccessList>>>
      neverCalledDownloader = this::neverCalledDownloader;

  private CompletableFuture<List<SyncBlockAccessList>> neverCalledDownloader(
      final List<BlockHeader> headers) {
    throw new AssertionError("downloader must not be called in this test");
  }

  @Test
  void persistsDownloadedBlockAccessLists() {
    final List<BlockHeader> headers = List.of(balHeader(1), balHeader(2));
    final List<BlockAccessList> bals =
        List.of(generator.blockAccessList(), generator.blockAccessList());

    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            TIMEOUT,
            requested -> {
              assertThat(requested).isEqualTo(headers);
              return CompletableFuture.completedFuture(
                  bals.stream().map(bal -> new SyncBlockAccessList(bal.encode())).toList());
            });

    assertThat(step.apply(headers)).isCompletedWithValue(headers);

    for (int i = 0; i < headers.size(); i++) {
      assertThat(
              blockchain.getBlockAccessList(headers.get(i).getHash()).map(BlockAccessList::encode))
          .contains(bals.get(i).encode());
    }
  }

  @Test
  void emptyHeaderListSkipsDownload() {
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(blockchain, TIMEOUT, neverCalledDownloader);

    assertThat(step.apply(List.of())).isCompletedWithValue(List.of());
  }

  @Test
  void rejectsHeadersMissingBalHash() {
    final BlockHeader headerWithoutBalHash = new BlockHeaderTestFixture().number(1).buildHeader();
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(blockchain, TIMEOUT, neverCalledDownloader);

    assertThatThrownBy(() -> step.apply(List.of(headerWithoutBalHash)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsUnavailableBlockAccessList() {
    final List<BlockHeader> headers = List.of(balHeader(1), balHeader(2));
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            TIMEOUT,
            requested ->
                CompletableFuture.completedFuture(
                    List.of(
                        new SyncBlockAccessList(generator.blockAccessList().encode()),
                        new SyncBlockAccessList(RLP.NULL))));

    assertThatThrownBy(() -> step.apply(headers).join())
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining(headers.get(1).getHash().toHexString());
  }

  @Test
  void rejectsIncompleteDownload() {
    final List<BlockHeader> headers = List.of(balHeader(1), balHeader(2));
    final DownloadAndPersistBlockAccessListsStep step =
        new DownloadAndPersistBlockAccessListsStep(
            blockchain,
            TIMEOUT,
            requested ->
                CompletableFuture.completedFuture(
                    List.of(new SyncBlockAccessList(generator.blockAccessList().encode()))));

    assertThatThrownBy(() -> step.apply(headers).join())
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  private BlockHeader balHeader(final long number) {
    return new BlockHeaderTestFixture()
        .number(number)
        .balHash(Hash.hash(generator.blockAccessList().encode()))
        .buildHeader();
  }
}
