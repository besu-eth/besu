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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.diskFailure;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.notCommitted;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

/**
 * Runs the downloads of the chain on the test thread: the header download when the test runs its
 * executor, a download that failed starting again when it runs the resumes, and the window when it
 * runs the workers of the window. A peer answers from a chain of empty blocks.
 */
class SyncChainTest {

  private static final int HEAD = 30;
  // the first proof-of-stake block, the checkpoint of the tests that have one
  private static final int MERGE = 6;
  private static final int PIVOT = 20;
  private static final int WINDOW = 8;
  // more requests at once than segments, as the stages of the header download share the test thread
  private static final int HEADER_PARALLELISM = 16;
  private static final int TOTAL_DIFFICULTIES_PER_COMMIT = 8;

  private final BlockDataGenerator gen = new BlockDataGenerator(7);
  private final List<Block> blocks = chain();
  private final MutableBlockchain source =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(blocks.getFirst());
  private final MutableBlockchain blockchain =
      spy(InMemoryKeyValueStorageProvider.createInMemoryBlockchain(blocks.getFirst()));
  private final ChainSender sender = new ChainSender(source);
  private final QueuedExecutor executor = new QueuedExecutor();
  private final QueuedExecutor resumes = new QueuedExecutor();
  private final QueuedExecutor windowWorkers = new QueuedExecutor();
  private final List<Long> pauses = new ArrayList<>();

  @BeforeEach
  void serveTheChain() {
    blocks.subList(1, blocks.size()).forEach(block -> source.appendBlock(block, List.of()));
  }

  @Test
  void storesTheHeadersDownToGenesisAndLeavesTheHeadThere() {
    final SyncChain chain = chain(Optional.empty(), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
    assertThat(blockchain.getSyncFloor()).contains(0L);
  }

  @Test
  void storesTheCheckpointThenTheHeadersDownToIt() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    // downloaded, then stored as the head until the bodies above it are stored
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(MERGE));
    assertThat(blockchain.getSyncFloor()).contains((long) MERGE);
    verify(blockchain, never()).unsafeSetChainHead(any(), any());
    assertHeadersStored(MERGE, PIVOT);
    assertThat(blockchain.getBlockHeader(MERGE - 1)).isEmpty();
    // above a proof-of-stake checkpoint, every header has the total difficulty of the checkpoint
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT - 1).getHash()))
        .contains(totalDifficulty(MERGE));
  }

  @Test
  void storesTheProofOfWorkHeadersBelowTheCheckpointWithoutTotalDifficulty() {
    final SyncChain chain = chain(Optional.of(checkpoint()), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(blockchain.getTotalDifficultyByHash(header(MERGE - 2).getHash())).isEmpty();
  }

  @Test
  void storesTheHeadersAboveTheLastProofOfWorkBlockWithItsTotalDifficulty() {
    // its difficulty is not zero, as with a genesis of difficulty 1 followed by proof of stake
    final SyncChain chain =
        chain(
            Optional.of(
                checkpoint(MERGE - 1, header(MERGE - 1).getHash(), totalDifficulty(MERGE - 1))),
            true);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertThat(blockchain.getTotalDifficultyByHash(header(PIVOT - 1).getHash()))
        .contains(totalDifficulty(MERGE - 1));
  }

  @Test
  void storesTheHeadersInStartWhenTheFirstPivotIsJustAboveTheCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    chain.start(header(MERGE + 1));

    // nothing left to download
    assertThat(chain.headersDone()).isCompleted();
    chain.downloadWindow(header(MERGE + 1));
    executor.runAll();
    windowWorkers.runAll();
    chain.setChainHead();
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(MERGE + 1));
  }

  @Test
  void failsWhenTheCheckpointIsNotOnTheChainOfThePivot() {
    // a checkpoint of another chain at the same height, stored already
    final BlockHeader other =
        new BlockHeaderTestFixture().number(MERGE).difficulty(Difficulty.ZERO).buildHeader();
    blockchain.unsafeStoreHeader(other, Difficulty.ONE);
    final SyncChain chain =
        chain(Optional.of(checkpoint(MERGE, other.getHash(), Difficulty.ONE)), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone().exceptionNow()).isInstanceOf(WrongChainException.class);
  }

  @Test
  void failsOnProofOfWorkHeadersAboveAProofOfStakeCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);
    chain.start(header(PIVOT));
    final BlockHeader proofOfWork =
        new BlockHeaderTestFixture()
            .number(PIVOT + 1)
            .parentHash(header(PIVOT).getHash())
            .difficulty(Difficulty.ONE)
            .buildHeader();

    assertThatThrownBy(() -> chain.storeHeaders(List.of(proofOfWork)))
        .isInstanceOf(WrongChainException.class);
    assertThat(blockchain.getBlockHeader(PIVOT + 1)).isEmpty();
  }

  @Test
  void failsWhenTheHeadersCannotBeStored() {
    // the pivot is stored, then the headers below it fail
    doCallRealMethod().doThrow(diskFailure()).when(blockchain).storeBlockHeaders(anyList());
    final SyncChain chain = chain(Optional.empty(), false);

    chain.start(header(PIVOT));
    executor.runAll();

    assertThat(chain.headersDone().exceptionNow()).isInstanceOf(StorageException.class);
  }

  @Test
  void failsWhenTheSchedulerStops() {
    sender.fail(
        message -> true, new RequestFailedException(Reason.STOPPED, null, "scheduler stopped"));
    final SyncChain chain = chain(Optional.empty(), false);

    chain.start(header(PIVOT));
    executor.runAll();

    // rather than resuming a download nothing can serve
    assertThat(chain.headersDone().exceptionNow()).isInstanceOf(RequestFailedException.class);
  }

  @Test
  void startsOverWhenPeersDoNotServeTheCheckpoint() {
    final SyncChain chain = chain(Optional.of(checkpoint(MERGE, Hash.ZERO, Difficulty.ONE)), true);

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(RestartException.class);
  }

  @Test
  void rejectsACheckpointAtAnotherHeight() {
    final SyncChain chain =
        chain(
            Optional.of(checkpoint(MERGE + 1, header(MERGE).getHash(), totalDifficulty(MERGE))),
            true);

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(IllegalStateException.class);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void rejectsAPivotThatIsNotAboveTheCheckpoint(final int below) {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);

    // at the checkpoint, the window would store no body for the chain head
    assertThatThrownBy(() -> chain.start(header(MERGE - below)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void startsOnce() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void resumesTheHeaderDownloadBelowTheLowestHeaderStored() {
    final SyncChain chain = chain(Optional.empty(), false);
    // stored down to block 12, as the requests below it, and those by hash, fail
    sender.fail(message -> startOf(message).orElse(0) < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    assertHeadersStored(12, PIVOT);
    assertThat(blockchain.getBlockHeader(11)).isEmpty();
    final int asked = sender.requests().size();

    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(chain.headersDone()).isCompleted();
    assertHeadersStored(1, PIVOT);
    assertThat(sender.requests().subList(asked, sender.requests().size()))
        .allMatch(message -> startOf(message).orElseThrow() < 12);
  }

  @Test
  void countsTheHeadersOfAResumedDownloadFromTheFirstPivot() {
    final SyncChain chain = chain(Optional.empty(), false);
    sender.fail(message -> startOf(message).orElse(0) < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    // from 19 down to 12, 11 down to 1 left
    assertThat(chain.headerProgress().stored()).isEqualTo(PIVOT - 12);
    assertThat(chain.headerProgress().left()).isEqualTo(11);

    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(chain.headerProgress().stored()).isEqualTo(PIVOT - 1);
    assertThat(chain.headerProgress().left()).isZero();
  }

  @Test
  void countsTheBodiesOfTheWindow() {
    final SyncChain chain = chain(Optional.empty(), false);
    assertThat(chain.windowProgress()).isZero();

    synced(chain);

    assertThat(chain.windowProgress()).isOne();
  }

  @Test
  void removesTheCanonicalEntriesAboveTheFirstPivotOnceTheHeadIsOnTheFloor() {
    // left by an earlier run of the node, its chain head on one of them
    blockchain.storeBlockHeaders(headers(PIVOT + 1, PIVOT + 5));
    blockchain.unsafeSetChainHead(header(PIVOT + 3), Difficulty.ONE);

    chain(Optional.empty(), false).start(header(PIVOT));

    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
    assertThat(blockchain.getBlockHeader(PIVOT + 1)).isEmpty();
    assertThat(blockchain.getBlockHeader(PIVOT + 5)).isEmpty();
    final InOrder order = inOrder(blockchain);
    order.verify(blockchain).unsafeSetChainHead(eq(header(0)), any());
    order.verify(blockchain).unsafeRemoveCanonicalIndexRange(PIVOT, PIVOT + 5);
  }

  @Test
  void downloadsTheBodiesOfTheWindowOnceTheHeadersAreStored() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));
    windowWorkers.runAll();
    assertThat(window).isNotDone();

    executor.runAll();
    windowWorkers.runAll();

    assertThat(window).isCompleted();
    final long from = PIVOT - WINDOW + 1;
    for (long number = from; number <= PIVOT; number++) {
      assertThat(blockchain.getBlockBody(header(number).getHash())).isPresent();
    }
    assertThat(blockchain.getBlockBody(header(from - 1).getHash())).isEmpty();
    assertThat(blockchain.getBodiesFrom()).contains(from);
  }

  @Test
  void downloadsTheWindowOnce() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    chain.downloadWindow(header(PIVOT));
    executor.runAll();

    final CompletableFuture<Void> second = chain.downloadWindow(header(PIVOT));
    executor.runAll();

    assertThat(second.exceptionNow()).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void setsTheChainHeadAtTheFinalPivot() {
    final SyncChain chain = synced(chain(Optional.empty(), false));

    chain.setChainHead();

    assertThat(blockchain.getChainHeadHeader()).isEqualTo(header(PIVOT));
    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
    // above the proof-of-work genesis, the total difficulties of eight headers a commit
    final InOrder order = inOrder(blockchain);
    order.verify(blockchain).unsafeStoreSyncTotalDifficulties(eq(0L), any(), eq(8L));
    order.verify(blockchain).unsafeStoreSyncTotalDifficulties(eq(8L), any(), eq(16L));
    order.verify(blockchain).unsafeStoreSyncTotalDifficulties(eq(16L), any(), eq(20L));
  }

  @Test
  void setsTheChainHeadAboveTheTerminalTotalDifficultyWithoutReadingTheHeaders() {
    final SyncChain chain = synced(chain(Optional.of(checkpoint()), true));

    chain.setChainHead();

    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
    verify(blockchain, never()).unsafeStoreSyncTotalDifficulties(anyLong(), any(), anyLong());
  }

  @Test
  void readsTheHeadersAgainOnANetworkThatDoesNotMerge() {
    final SyncChain chain =
        synced(chain(Optional.of(checkpoint()), true, Optional.empty(), windowWorkers));

    chain.setChainHead();

    assertThat(blockchain.getChainHead().getTotalDifficulty()).isEqualTo(totalDifficulty(PIVOT));
    verify(blockchain, times(2)).unsafeStoreSyncTotalDifficulties(anyLong(), any(), anyLong());
  }

  @Test
  void setsNoChainHeadUntilTheWindowIsStored() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    executor.runAll();
    assertThatThrownBy(chain::setChainHead).isInstanceOf(IllegalStateException.class);
    chain.downloadWindow(header(PIVOT));
    executor.runAll();

    // started, not stored yet
    assertThatThrownBy(chain::setChainHead).isInstanceOf(IllegalStateException.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void downloadsNoWindowUpToAPivotOffTheCanonicalChain() {
    final SyncChain chain = chain(Optional.of(checkpoint()), true);
    chain.start(header(PIVOT));
    executor.runAll();
    // a sibling of the pivot, stored with its total difficulty
    final Block other =
        gen.block(
            new BlockOptions()
                .setBlockNumber(PIVOT)
                .setParentHash(header(PIVOT - 1).getHash())
                .setTimestamp((long) HEAD + 1)
                .setDifficulty(Difficulty.ZERO)
                .hasOmmers(false)
                .hasTransactions(false));
    blockchain.storeBlock(other, List.of());
    final int asked = sender.requests().size();

    final CompletableFuture<Void> window = chain.downloadWindow(other.getHeader());
    executor.runAll();

    assertThat(window.exceptionNow()).isInstanceOf(IllegalStateException.class);
    assertThat(sender.requests()).hasSize(asked);
  }

  @Test
  void setsNoChainHeadOnHeadersThatDoNotFollowTheChain() {
    final SyncChain chain = synced(chain(Optional.empty(), false));
    // the canonical block 10 is not the child of block 9
    blockchain.storeBlockHeaders(
        List.of(new BlockHeaderTestFixture().number(10).parentHash(Hash.ZERO).buildHeader()));

    assertThatThrownBy(chain::setChainHead).isInstanceOf(IllegalStateException.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void removesTheCanonicalEntriesAboveTheChainHead() {
    final SyncChain chain = synced(chain(Optional.empty(), false));
    // stored by a pivot move the final pivot is below
    chain.storeHeaders(headers(PIVOT + 1, PIVOT + 2));

    chain.setChainHead();

    assertThat(blockchain.getBlockHeader(PIVOT + 1)).isEmpty();
    assertThat(blockchain.getBlockHeader(PIVOT + 2)).isEmpty();
  }

  @Test
  void triesAHeadWriteAgainUntilItCommits() {
    doThrow(notCommitted()).doCallRealMethod().when(blockchain).unsafeSetChainHead(any(), any());

    chain(Optional.empty(), false).start(header(PIVOT));

    assertThat(pauses).containsExactly(20L);
    assertThat(blockchain.getBlockHeader(PIVOT)).contains(header(PIVOT));
  }

  @Test
  void startsNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    final SyncChain toCheckpoint = chain(Optional.of(checkpoint()), true);
    chain.cancel();
    toCheckpoint.cancel();

    assertThatThrownBy(() -> chain.start(header(PIVOT))).isInstanceOf(CancellationException.class);
    assertThatThrownBy(() -> toCheckpoint.start(header(PIVOT)))
        .isInstanceOf(CancellationException.class);
    // not even the checkpoint is downloaded
    assertThat(sender.requests()).isEmpty();
    assertThat(blockchain.getBlockHeader(PIVOT)).isEmpty();
  }

  @Test
  void storesNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));

    chain.cancel();
    executor.runAll();
    windowWorkers.runAll();

    // only the pivot
    assertThat(blockchain.getBlockHeader(PIVOT)).isPresent();
    assertThat(blockchain.getBlockHeader(PIVOT - 1)).isEmpty();
    assertThat(chain.headersDone()).isCancelled();
    assertThat(window).isCompletedExceptionally();
    assertThatThrownBy(() -> chain.storeHeaders(headers(PIVOT + 1, PIVOT + 2)))
        .isInstanceOf(CancellationException.class);
    assertThatThrownBy(chain::setChainHead).isInstanceOf(CancellationException.class);
  }

  @Test
  void stopsTheWindowOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    chain.start(header(PIVOT));
    final CompletableFuture<Void> window = chain.downloadWindow(header(PIVOT));
    executor.runAll();

    chain.cancel();
    windowWorkers.runAll();

    assertThat(window).isCompletedExceptionally();
    assertThat(blockchain.getBlockBody(header(PIVOT).getHash())).isEmpty();
  }

  @Test
  void stopsThePassOverTheHeadersOnceCancelled() {
    final SyncChain chain = synced(chain(Optional.empty(), false));
    // cancelled while the first commit is written
    doAnswer(
            invocation -> {
              chain.cancel();
              return invocation.callRealMethod();
            })
        .when(blockchain)
        .unsafeStoreSyncTotalDifficulties(anyLong(), any(), anyLong());

    assertThatThrownBy(chain::setChainHead).isInstanceOf(CancellationException.class);
    verify(blockchain, times(1)).unsafeStoreSyncTotalDifficulties(anyLong(), any(), anyLong());
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void setsNoChainHeadOnceCancelledDuringTheLastCommit() {
    final SyncChain chain = synced(chain(Optional.empty(), false));
    doAnswer(
            invocation -> {
              chain.cancel();
              return invocation.callRealMethod();
            })
        .when(blockchain)
        .unsafeStoreSyncTotalDifficulties(anyLong(), any(), eq((long) PIVOT));

    assertThatThrownBy(chain::setChainHead).isInstanceOf(CancellationException.class);
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void resumesNothingOnceCancelled() {
    final SyncChain chain = chain(Optional.empty(), false);
    sender.fail(message -> startOf(message).orElse(0) < 12);
    chain.start(header(PIVOT));
    executor.runAll();
    final int asked = sender.requests().size();

    chain.cancel();
    sender.fail(message -> false);
    resumes.runAll();
    executor.runAll();

    assertThat(sender.requests()).hasSize(asked);
    assertThat(chain.headersDone()).isCancelled();
  }

  @Test
  void waitsForTheWindowWorkersOnceCancelled() throws InterruptedException {
    final ExecutorService workers = mock(ExecutorService.class);
    final SyncChain chain =
        chain(Optional.empty(), false, Optional.of(totalDifficulty(MERGE - 1)), workers);

    chain.cancel();
    chain.awaitStop();

    // interrupted, then awaited before the storage they write to closes
    final InOrder order = inOrder(workers);
    order.verify(workers).shutdownNow();
    order.verify(workers).awaitTermination(anyLong(), any());
  }

  // proof of stake from the merge
  private SyncChain chain(final Optional<Checkpoint> checkpoint, final boolean toCheckpointOnly) {
    return chain(
        checkpoint, toCheckpointOnly, Optional.of(totalDifficulty(MERGE - 1)), windowWorkers);
  }

  private SyncChain chain(
      final Optional<Checkpoint> checkpoint,
      final boolean toCheckpointOnly,
      final Optional<Difficulty> terminalTotalDifficulty,
      final ExecutorService workers) {
    return new SyncChain(
        blockchain,
        ProtocolScheduleFixture.MAINNET,
        sender,
        new HeaderFetcher(sender, ProtocolScheduleFixture.MAINNET, Optional::empty, 4),
        checkpoint,
        new SyncChain.Settings(
            4, HEADER_PARALLELISM, WINDOW, toCheckpointOnly, false, terminalTotalDifficulty),
        new NoOpMetricsSystem(),
        executor,
        workers,
        resumes,
        pauses::add,
        TOTAL_DIFFICULTIES_PER_COMMIT);
  }

  // started at the pivot, with the headers and the window up to it stored
  private SyncChain synced(final SyncChain chain) {
    chain.start(header(PIVOT));
    chain.downloadWindow(header(PIVOT));
    executor.runAll();
    windowWorkers.runAll();
    return chain;
  }

  // genesis, then empty blocks of proof of work below the merge and of proof of stake from it
  private List<Block> chain() {
    final List<Block> chain =
        new ArrayList<>(List.of(gen.genesisBlock(new BlockOptions().setTimestamp(0L))));
    for (int number = 1; number <= HEAD; number++) {
      chain.add(
          gen.block(
              new BlockOptions()
                  .setBlockNumber(number)
                  .setParentHash(chain.getLast().getHash())
                  .setTimestamp((long) number)
                  .setDifficulty(number < MERGE ? Difficulty.ONE : Difficulty.ZERO)
                  .hasOmmers(false)
                  .hasTransactions(false)
                  .setReceiptsRoot(Hash.EMPTY_TRIE_HASH)));
    }
    return chain;
  }

  private BlockHeader header(final long number) {
    return blocks.get((int) number).getHeader();
  }

  private List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (long number = from; number <= to; number++) {
      headers.add(header(number));
    }
    return headers;
  }

  private Difficulty totalDifficulty(final long number) {
    return source.getTotalDifficultyByHash(header(number).getHash()).orElseThrow();
  }

  private Checkpoint checkpoint() {
    return checkpoint(MERGE, header(MERGE).getHash(), totalDifficulty(MERGE));
  }

  private static Checkpoint checkpoint(
      final long number, final Hash hash, final Difficulty totalDifficulty) {
    return ImmutableCheckpoint.builder()
        .blockNumber(number)
        .blockHash(hash)
        .totalDifficulty(totalDifficulty)
        .build();
  }

  private void assertHeadersStored(final long from, final long to) {
    for (long number = from; number <= to; number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(header(number));
    }
  }

  // the highest block a header request by number asks for, none for a request by hash
  private static OptionalLong startOf(final MessageData message) {
    return GetBlockHeadersMessage.readFrom(message).blockNumber();
  }
}
