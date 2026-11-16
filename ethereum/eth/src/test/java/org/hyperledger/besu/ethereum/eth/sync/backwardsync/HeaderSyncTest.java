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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.HeaderSync.Link;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.ChainSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HeaderSyncTest {

  private static final int HEADERS_PER_REQUEST = 10;

  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final BadBlockManager badBlocks = new BadBlockManager();
  private final PendingChain pending =
      new PendingChain(
          new InMemoryKeyValueStorage(),
          new InMemoryKeyValueStorage(),
          new InMemoryKeyValueStorage(),
          new MainnetBlockHeaderFunctions());
  // the chain the local node has, and the one its peers serve
  private MutableBlockchain local;
  private MutableBlockchain remote;

  @Test
  void linksAChildOfTheLocalHead() {
    final List<BlockHeader> localChain = chain(21, 0);
    final List<BlockHeader> remoteChain = extend(localChain, 21, 0);
    open(localChain, remoteChain);

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompleted();
    final Link result = link.join();
    assertThat(result.top()).isEqualTo(remoteChain.getLast());
    assertThat(result.sharedNumber()).isEqualTo(20);
    assertThat(result.replacing()).isEmpty();
    assertThat(pending.header(21L)).contains(remoteChain.getLast());
  }

  @Test
  void downloadsALongRangeInParallelSegments() {
    final List<BlockHeader> localChain = chain(21, 0);
    final List<BlockHeader> remoteChain = extend(localChain, 120, 0);
    open(localChain, remoteChain);

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompleted();
    assertThat(link.join().sharedNumber()).isEqualTo(20);
    assertThat(link.join().replacing()).isEmpty();
    for (final BlockHeader header : remoteChain.subList(21, 121)) {
      assertThat(pending.header(header.getNumber())).contains(header);
      assertThat(pending.header(header.getHash())).contains(header);
    }
  }

  @Test
  void resolvesAForkBelowTheLocalHead() {
    // the local chain and the one the peers serve part at block 20, the local one reaching block 30
    final List<BlockHeader> common = chain(21, 0);
    final List<BlockHeader> localChain = extend(common, 30, 0);
    final List<BlockHeader> remoteChain = extend(common, 40, 1);
    open(localChain, remoteChain);

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompleted();
    final Link result = link.join();
    assertThat(result.sharedNumber()).isEqualTo(20);
    // what the download did not store: the fork down to the local head
    assertThat(result.replacing()).containsExactlyElementsOf(remoteChain.subList(21, 31));
    assertThat(pending.header(31L)).contains(remoteChain.get(31));
    assertThat(pending.header(40L)).contains(remoteChain.getLast());
  }

  @Test
  void resolvesAHeadBelowTheLocalHead() {
    final List<BlockHeader> common = chain(11, 0);
    final List<BlockHeader> localChain = extend(common, 30, 0);
    final List<BlockHeader> remoteChain = extend(common, 25, 1);
    open(localChain, remoteChain);

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompleted();
    final Link result = link.join();
    assertThat(result.sharedNumber()).isEqualTo(10);
    // the head itself is part of what replaces the local blocks, and nothing is stored
    assertThat(result.replacing()).containsExactlyElementsOf(remoteChain.subList(11, 26));
    assertThat(pending.isEmpty()).isTrue();
  }

  @Test
  void failsWhenTheForkIsBelowTheFinalizedBlock() {
    final List<BlockHeader> common = chain(21, 0);
    final List<BlockHeader> localChain = extend(common, 30, 0);
    final List<BlockHeader> remoteChain = extend(common, 40, 1);
    open(localChain, remoteChain);
    local.setFinalized(localChain.get(25).getHash());

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompletedExceptionally();
    assertThatThrownBy(link::join)
        .hasCauseInstanceOf(WrongChainException.class)
        .hasMessageContaining("above the finalized block");
  }

  @Test
  void failsWithTheBadHeaderOfTheChain() {
    final List<BlockHeader> localChain = chain(21, 0);
    final List<BlockHeader> remoteChain = extend(localChain, 120, 0);
    open(localChain, remoteChain);
    final BlockHeader bad = remoteChain.get(70);
    badBlocks.addBadBlock(
        new Block(bad, BlockBody.empty()), BadBlockCause.fromValidationFailure("invalid"));

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());
    awaitDone(link);

    assertThat(link).isCompletedExceptionally();
    assertThatThrownBy(link::join)
        .hasCauseInstanceOf(BadHeaderException.class)
        .cause()
        .extracting(error -> ((BadHeaderException) error).header())
        .isEqualTo(bad);
  }

  @Test
  void failsAtOnceWhenTheHeadIsABadBlock() {
    final List<BlockHeader> localChain = chain(21, 0);
    final List<BlockHeader> remoteChain = extend(localChain, 40, 0);
    open(localChain, remoteChain);
    badBlocks.addBadBlock(
        new Block(remoteChain.getLast(), BlockBody.empty()),
        BadBlockCause.fromValidationFailure("invalid"));

    final CompletableFuture<Link> link = headerSync().extendTo(remoteChain.getLast());

    assertThat(link).isCompletedExceptionally();
    assertThatThrownBy(link::join).hasCauseInstanceOf(BadHeaderException.class);
    assertThat(pending.isEmpty()).isTrue();
  }

  @Test
  void aHeadAtTheLocalHeadIsLinkedWithoutDownload() {
    final List<BlockHeader> localChain = chain(21, 0);
    open(localChain, localChain);

    final CompletableFuture<Link> link = headerSync().extendTo(localChain.getLast());

    assertThat(link).isCompleted();
    assertThat(link.join().sharedNumber()).isEqualTo(20);
    assertThat(link.join().replacing()).isEmpty();
    assertThat(pending.isEmpty()).isTrue();
  }

  @Test
  void aStoppedDownloadIsCancelled() {
    final List<BlockHeader> localChain = chain(21, 0);
    final List<BlockHeader> remoteChain = extend(localChain, 120, 0);
    open(localChain, remoteChain);
    final HeaderSync headerSync = headerSync();

    final CompletableFuture<Link> link = headerSync.extendTo(remoteChain.getLast());
    headerSync.stop();
    awaitDone(link);

    assertThat(link).isCancelled();
    assertThat(headerSync.progress()).isEqualTo(new HeaderSync.Progress(0, 0));
    // a new download may start
    final CompletableFuture<Link> again = headerSync.extendTo(localChain.getLast());
    assertThat(again).isCompleted();
  }

  @AfterEach
  void stopExecutor() {
    executor.shutdownNow();
  }

  // the pipeline stages run on their own threads, so the download takes as long as it takes
  private static void awaitDone(final CompletableFuture<?> link) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!link.isDone() && System.nanoTime() < deadline) {
      Thread.onSpinWait();
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private HeaderSync headerSync() {
    final ChainSender sender = new ChainSender(remote);
    return new HeaderSync(
        sender,
        ProtocolScheduleFixture.MAINNET,
        pending,
        local,
        badBlocks,
        new HeaderFetcher(
            sender, ProtocolScheduleFixture.MAINNET, Optional::empty, HEADERS_PER_REQUEST),
        executor,
        new BackwardSyncConfiguration(
            HEADERS_PER_REQUEST,
            // more than the segments, so the pipeline buffers hold them all
            16,
            64L << 20,
            8192,
            Duration.ofSeconds(10),
            3,
            10,
            Duration.ofSeconds(5),
            3,
            1024,
            64L << 20,
            Duration.ofSeconds(30),
            true),
        new NoOpMetricsSystem());
  }

  private void open(final List<BlockHeader> localChain, final List<BlockHeader> remoteChain) {
    local = blockchain(localChain);
    remote = blockchain(remoteChain);
  }

  private static MutableBlockchain blockchain(final List<BlockHeader> headers) {
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(
            new Block(headers.getFirst(), BlockBody.empty()));
    headers.stream()
        .skip(1)
        .forEach(header -> blockchain.appendBlock(new Block(header, BlockBody.empty()), List.of()));
    return blockchain;
  }

  // headers 0 to count - 1, each the parent of the next
  private static List<BlockHeader> chain(final int count, final int fork) {
    return extend(List.of(new BlockHeaderTestFixture().number(0).buildHeader()), count - 1, fork);
  }

  // the base, then a chain of its own up to the highest block
  private static List<BlockHeader> extend(
      final List<BlockHeader> base, final long highest, final int fork) {
    final List<BlockHeader> headers = new ArrayList<>(base);
    while (headers.getLast().getNumber() < highest) {
      final BlockHeader parent = headers.getLast();
      final long number = parent.getNumber() + 1;
      headers.add(
          new BlockHeaderTestFixture()
              .number(number)
              .parentHash(parent.getHash())
              .timestamp(1_000L * fork + number)
              .buildHeader());
    }
    return headers;
  }
}
