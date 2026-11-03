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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.StorageTrieUpdater;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.trie.RangeManager;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs the download on the test thread, in rounds: the network answers the requests sent, then the
 * loop and the writers of the download run what they queued. Its periodic dispatch only runs when a
 * test ticks it.
 */
class StateSyncTest {

  // 16 account ranges, 4 KiB responses, 8 storages a request, 8 ranges of a large storage
  private static final StateSync.Settings SETTINGS = new StateSync.Settings(16, 4096, 8, 8, 16);
  // far more than a download of these worlds takes
  private static final int MAX_ROUNDS = 10_000;

  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final QueuedExecutor tasks = new QueuedExecutor();
  // with a clock that stands still, a peer left out stays out
  private LongSupplier nanoTime = () -> 0L;
  private UnaryOperator<StorageTrieUpdater> trieUpdaters = UnaryOperator.identity();
  private StateSync stateSync;

  @AfterEach
  void stopTheDownload() {
    if (stateSync != null) {
      stateSync.stop();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void downloadsAStaticState(final Format format) {
    final TestWorld world = TestWorld.random(new Random(3), 400, 3, 600);
    network.serve(world);
    // small responses split the large storages, and leave out some of the 16 bytecodes asked
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(world.codes().keySet()).allMatch(storages.sync()::hasCode);
    // every storage gets its trie as its slots arrive
    final int withStorage = world.storageLeaves().size();
    assertThat(stateSync.storageTriesBuilt()).isEqualTo(withStorage);
    assertThat(stateSync.storageTriesLeft()).isZero();
    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(built.root()).isEqualTo(world.root());
    assertThat(built.keptStorageTries()).isEqualTo(withStorage);
  }

  // its ranges come at two roots, so the final build makes its trie from its slots
  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesAStorageThatChangesWhileDownloadingToTheFinalBuild(final Format format) {
    final Random random = new Random(19);
    final TestWorld world = TestWorld.random(random, 1, 1, 20_000);
    network.serve(world);
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runUntil(() -> stateSync.downloadedSlots() >= 500);
    final TestWorld next = move(world, random, 2, 1, 3);
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(stateSync.storageTriesLeft()).isOne();
    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(built.root()).isEqualTo(next.root());
    assertThat(built.keptStorageTries()).isEqualTo(next.storageLeaves().size() - 1);
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void updatesTheTrieOfAStorageThatChangesOnceDownloaded(final Format format) {
    final Random random = new Random(29);
    final TestWorld world = TestWorld.random(random, 1, 1, 2_000);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));
    runToEnd();
    assertThat(completion).isCompleted();
    assertThat(stateSync.storageTriesBuilt()).isOne();

    final TestWorld next = move(world, random, 2, 1, 3);

    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(stateSync.storageTriesUpdated()).isPositive();
    assertThat(built.root()).isEqualTo(next.root());
    assertThat(built.keptStorageTries()).isEqualTo(next.storageLeaves().size());
  }

  // its account fetched again at the new pivot tells that its storage is now empty
  @ParameterizedTest
  @EnumSource(Format.class)
  void dropsAStorageThatAMoveEmptiesWhileDownloading(final Format format) {
    final TestWorld world = TestWorld.random(new Random(67), 1, 1, 2_000);
    network.serve(world);
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));
    runUntil(() -> stateSync.downloadedSlots() > 0);

    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    diff.add(next.clearStorages());
    network.serve(next);
    assertThat(stateSync.moveTo(header(2, next.root()), diff)).isCompleted();
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(build(storages).root()).isEqualTo(next.root());
  }

  // a write that fails loses the root node of the storage it completes, not the ranges of the other
  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesAStorageWhoseRootNodeWasLostToTheFinalBuild(final Format format) {
    // two storages of 60 slots in one request: the answer completes the first and cuts the second
    final TestWorld world = TestWorld.random(new Random(43), 2, 2, 60);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final AtomicBoolean failed = new AtomicBoolean();
    // the first commit holding a root node fails
    final SyncStateStorage storage =
        FailingTrieWriter.failingOnce(storages.sync(), Bytes::isEmpty, failed);
    final CompletableFuture<Void> completion =
        start(storage, accountRanges(1), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(failed).isTrue();
    assertThat(stateSync.storageTriesLeft()).isOne();
    assertThat(build(storages).root()).isEqualTo(world.root());
    // every storage trie reads back whole, the lost one rebuilt
    world
        .accountLeaves()
        .forEach(
            (hash, leaf) -> {
              final Bytes32 root = Bytes32.wrap(Accounts.decode(leaf).getStorageRoot().getBytes());
              assertThat(
                      storages.storageTrie(hash, root).entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE))
                  .isEqualTo(world.storageLeaves().getOrDefault(hash, new TreeMap<>()));
            });
  }

  @Test
  void requestsTheAccountRangesLowestFirst() {
    final TestWorld world = TestWorld.random(new Random(7), 2000, 0, 0);
    network.serve(world);
    final int ranges = 1024;
    final List<Bytes32> starts =
        List.copyOf(
            RangeManager.generateRanges(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, ranges)
                .keySet());

    final CompletableFuture<Void> completion =
        start(
            SyncStorageFixture.create(Format.BONSAI).sync(),
            accountRanges(ranges),
            header(1, world.root()));

    // before any answer, the requests sent are those of the lowest ranges
    final List<Bytes32> first = List.copyOf(network.accountRangeOrigins());
    assertThat(first)
        .isNotEmpty()
        .hasSizeLessThan(ranges)
        .isEqualTo(starts.subList(0, first.size()));
    runToEnd();
    assertThat(completion).isCompleted();
    // each range is started once, in key order
    assertThat(network.accountRangeOrigins().stream().filter(Set.copyOf(starts)::contains))
        .isSorted()
        .hasSize(ranges);
  }

  static Stream<Arguments> formatsAndSeeds() {
    return Stream.of(Format.values())
        .flatMap(
            format ->
                LongStream.of(11, 23, 37, 41, 59).mapToObj(seed -> Arguments.of(format, seed)));
  }

  // the moves change storages still downloading and storages whose trie is written
  @ParameterizedTest
  @MethodSource("formatsAndSeeds")
  void followsAMovingPivot(final Format format, final long seed) {
    final Random random = new Random(seed);
    TestWorld world = TestWorld.random(random, 1500, 6, 800);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(100, world.root()));

    long block = 100;
    for (int move = 0; move < 4; move++) {
      final long progress = progress() + 50;
      runUntil(() -> progress() >= progress);
      block += 3;
      world = move(world, random, block, 3, 25);
    }
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(stateSync.pivot().header().getNumber()).isEqualTo(block);
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesOutAPeerThatServesNoState(final Format format) {
    final TestWorld world = TestWorld.random(new Random(13), 800, 3, 400);
    network.serve(world);
    network.serveNoState(1);
    // small responses make many requests, among which the few it got before being left out
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    // its empty answers neither stalled the pivot nor kept it busy; it still gets bytecodes
    assertThat(stateSync.isPivotStale()).isFalse();
    final EthPeer leftOut = network.peers().get(1);
    assertThat(network.stateRequestsTo(leftOut)).isLessThan(network.stateRequestCount() / 10);
    assertThat(network.codeRequestsTo(leftOut) * 4).isGreaterThan(network.codeRequestCount());
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void detectsAStalePivot(final Format format) {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(7, world.root()));
    // every peer serves some of the state, then none serves its root, as when it gets too old
    round();
    network.forget(world.root());

    runToEnd();

    assertThat(stateSync.isPivotStale()).isTrue();
    assertThat(completion).isNotDone();
    // a move resumes the download, the empty answers of the stale pivot forgotten
    network.serve(world);
    assertThat(stateSync.moveTo(header(8, world.root()), new StateDiff())).isCompleted();
    tasks.runAll();
    network.forget(world.root());
    network.answer("account_range", 1);
    tasks.runAll();
    assertThat(stateSync.isPivotStale()).isFalse();
    network.serve(world);
    runToEnd();
    assertThat(completion).isCompleted();
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  // the empty answers to the requests of an older pivot say nothing about the new one
  @Test
  void ignoresTheEmptyAnswersOfAnOlderPivot() {
    final Random random = new Random(47);
    final TestWorld world = TestWorld.random(random, 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));
    round();
    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    diff.add(next.mutate(random, 10));
    network.serve(next);
    assertThat(stateSync.moveTo(header(2, next.root()), diff)).isCompleted();

    // the requests sent at the old pivot come back empty, as it is no longer served
    network.forget(world.root());
    round();

    assertThat(stateSync.isPivotStale()).isFalse();
    // nor do they count with the empty answers of the new pivot
    network.forget(next.root());
    network.answer("account_range", 3);
    tasks.runAll();
    assertThat(stateSync.isPivotStale()).isFalse();
  }

  // a peer left out gets no state request, so it cannot answer empty either
  @Test
  void detectsAStalePivotWhileAPeerIsLeftOut() {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(7, world.root()));
    // every peer serves some of the state, then one serves none of the next pivot
    round();
    network.serveNoState(2);
    assertThat(stateSync.moveTo(header(8, world.root()), new StateDiff())).isCompleted();
    runUntil(() -> stateSync.peersWithoutState() == 1);
    assertThat(stateSync.peersWithoutState()).isOne();
    network.forget(world.root());

    for (int round = 0; round < 5; round++) {
      round();
    }

    assertThat(stateSync.isPivotStale()).isTrue();
  }

  // only empty answers in a row tell that the pivot is too old
  @Test
  void startsTheCountOfEmptyAnswersOverAfterAnAnswerWithState() {
    final TestWorld world = TestWorld.random(new Random(31), 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));
    round();

    // ten empty answers, one with state, then two empty answers again
    network.forget(world.root());
    network.answer("account_range", 5);
    network.answer("storage_ranges", 5);
    tasks.runAll();
    network.serve(world);
    network.answer("storage_ranges", 1);
    tasks.runAll();
    network.forget(world.root());
    network.answer("storage_ranges", 2);
    tasks.runAll();

    assertThat(stateSync.isPivotStale()).isFalse();
  }

  // only left out peers serve the state: a request fails at once, as the next ones would
  @Test
  void waitsForThePeriodicDispatchWhileEveryPeerIsLeftOut() {
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, root(1)));
    // no peer serves either pivot: answering nothing for the first says nothing about its age
    for (int round = 0; round < 3; round++) {
      round();
    }
    assertThat(stateSync.isPivotStale()).isFalse();
    // and leaves each peer out at the second
    assertThat(stateSync.moveTo(header(2, root(2)), new StateDiff())).isCompleted();
    runToEnd();
    assertThat(stateSync.peersWithoutState()).isEqualTo(network.peers().size());

    final int sent = network.requestCount();
    tasks.tick();
    assertThat(network.requestCount()).isGreaterThan(sent);
    final int retried = network.requestCount();
    round();
    assertThat(network.requestCount()).isEqualTo(retried);
    assertThat(completion).isNotDone();
  }

  // a diff that failed is partly applied, so the download does not go on from it
  @ParameterizedTest
  @MethodSource("lastingWriteFailures")
  void stopsAndFailsWhenAMoveFailsToApply(final Throwable failure) {
    final TestWorld world = TestWorld.random(new Random(41), 300, 2, 200);
    network.serve(world);
    final SyncStateStorage storage = spy(SyncStorageFixture.create(Format.BONSAI).sync());
    final CompletableFuture<Void> completion = start(storage, header(1, world.root()));
    round();
    doThrow(failure).when(storage).batch();

    final CompletableFuture<StateDiffApplier.Outcome> move =
        stateSync.moveTo(header(2, world.root()), new StateDiff());

    assertThat(move).isCompletedExceptionally();
    assertThatThrownBy(move::join).hasCause(failure);
    assertThat(completion).isCompletedExceptionally();
    assertThatThrownBy(completion::join).hasCause(failure);
    // the answers in flight write nothing, and nothing more is sent
    final long written = progress();
    final int sent = network.requestCount();
    round();
    tasks.tick();
    assertThat(progress()).isEqualTo(written);
    assertThat(network.requestCount()).isEqualTo(sent);
  }

  @Test
  void refusesAMoveBeforeTheStart() {
    stateSync = stateSync(SyncStorageFixture.create(Format.BONSAI).sync(), SETTINGS, tasks);

    assertThatThrownBy(() -> stateSync.moveTo(header(2, Bytes32.ZERO), new StateDiff()))
        .isInstanceOf(IllegalStateException.class);
  }

  // a move between the check of an empty answer and its count leaves the new pivot fresh
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void marksStaleOnlyThePivotOfTheEmptyAnswers(final boolean moveDuringTheLastAnswer) {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    // the clock, which an empty answer reads once it has checked its pivot
    final AtomicReference<Runnable> onTheNextReading = new AtomicReference<>(() -> {});
    nanoTime =
        () -> {
          onTheNextReading.getAndSet(() -> {}).run();
          return 0L;
        };
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(7, world.root()));
    // every peer serves some of the state, then none serves its root
    round();
    network.forget(world.root());
    // eleven empty answers in a row, one short of a stale pivot
    for (int answer = 0; answer < 11; answer++) {
      network.answer("account_range", 1);
      tasks.runAll();
    }
    assertThat(stateSync.isPivotStale()).isFalse();
    if (moveDuringTheLastAnswer) {
      onTheNextReading.set(() -> stateSync.moveTo(header(8, world.root()), new StateDiff()));
    }

    network.answer("account_range", 1);
    tasks.runAll();

    assertThat(stateSync.pivot().epoch()).isEqualTo(moveDuringTheLastAnswer ? 1 : 0);
    assertThat(stateSync.isPivotStale()).isEqualTo(!moveDuringTheLastAnswer);
  }

  // a peer that only answers invalid responses never answers empty, so it does not count
  @Test
  void detectsAStalePivotWhileAPeerOnlyAnswersInvalid() {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    network.answerInvalid(2);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(7, world.root()));
    // the two other peers serve some of the state, then none serves its root
    round();
    network.forget(world.root());

    for (int round = 0; round < 5; round++) {
      round();
    }

    assertThat(stateSync.isPivotStale()).isTrue();
  }

  // bytecodes do not depend on the pivot, so they go on while it waits for a move
  @Test
  void keepsAskingBytecodesWhileThePivotIsStale() {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(7, world.root()));
    round();
    network.forget(world.root());
    // the account ranges come back empty until the pivot is stale; the storage requests left in
    // flight keep most bytecodes waiting
    for (int answers = 0; answers < 100 && !stateSync.isPivotStale(); answers++) {
      network.answer("account_range", 1);
      tasks.runAll();
    }
    assertThat(stateSync.isPivotStale()).isTrue();
    assertThat(network.unansweredRequests()).contains("bytecodes");
    final int codeRequests = network.codeRequestCount();
    final int stateRequests = network.stateRequestCount();

    round();

    assertThat(network.codeRequestCount()).isGreaterThan(codeRequests);
    assertThat(network.stateRequestCount()).isEqualTo(stateRequests);
  }

  // the requests go first, as the updates may take a minute to stop
  @Test
  void withdrawsTheRequestsBeforeItWaitsForTheTrieUpdates() {
    final TestWorld world = TestWorld.random(new Random(101), 300, 1, 100);
    network.serve(world);
    final AtomicLong withdrawn = new AtomicLong(-1);
    trieUpdaters =
        updater -> {
          final StorageTrieUpdater spied = spy(updater);
          doAnswer(
                  invocation -> {
                    withdrawn.set(network.withdrawnRequestCount());
                    return invocation.callRealMethod();
                  })
              .when(spied)
              .stop();
          return spied;
        };
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    stateSync.stop();

    assertThat(withdrawn.get()).isPositive().isEqualTo(network.unansweredRequests().size());
  }

  // a quarter more than the peers take, so that a peer gets its next request at once
  @Test
  void keepsAQuarterMoreRequestsInFlightThanThePeersTake() {
    final TestWorld world = TestWorld.random(new Random(31), 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    round();

    // the peers take 12 requests, a third of the 15 sent for account ranges
    assertThat(network.unansweredRequests())
        .hasSize(15)
        .filteredOn("account_range"::equals)
        .hasSize(5);
  }

  static Stream<Arguments> backlogs() {
    return Stream.of(
        Arguments.of("bytecodes", TestWorld.random(new Random(59), 80_000, 80_000, 0)),
        Arguments.of("storages", TestWorld.storagesWithoutCode(new Random(61), 80_000)));
  }

  // the accounts wait while more than 50,000 storages or bytecodes do, to bound their memory
  @ParameterizedTest(name = "{0}")
  @MethodSource("backlogs")
  void asksNoAccountRangeWhileTheirStoragesOrBytecodesLag(
      final String backlog, final TestWorld world) {
    network.serve(world);
    // the bound is passed once 13 of the 20 ranges are in
    start(
        SyncStorageFixture.create(Format.BONSAI).sync(),
        new StateSync.Settings(20, 64 * 1024 * 1024, 8, 8, 16),
        header(1, world.root()));

    // each round asks for as many ranges as it answered, all of them by the third
    for (int round = 0; round < 4; round++) {
      round();
    }

    assertThat(network.accountRangeOrigins()).hasSizeLessThan(20);
  }

  // as many ranges as the density of its first slots suggests, up to the most a storage gets
  @Test
  void asksALargeStorageInParallelSlotRanges() {
    final TestWorld world = TestWorld.random(new Random(37), 1, 1, 20_000);
    network.serve(world);
    start(
        SyncStorageFixture.create(Format.BONSAI).sync(), accountRanges(1), header(1, world.root()));

    // the account, then the first slots of its storage
    round();
    round();

    assertThat(network.unansweredRequests()).containsOnly("storage_chunk").hasSize(8);
  }

  // the last account range brings a bytecode, which the download waits for
  @Test
  void completesOnceTheLastBytecodeIsStored() {
    // one account, with a bytecode and no storage
    final TestWorld world = TestWorld.random(new Random(53), 1, 1, 0);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final CompletableFuture<Void> completion =
        start(storages.sync(), accountRanges(1), header(1, world.root()));

    round();
    assertThat(completion).isNotDone();
    round();

    assertThat(completion).isCompleted();
    assertThat(world.codes().keySet()).hasSize(1).allMatch(storages.sync()::hasCode);
  }

  // bytecodes do not depend on the pivot, so only the state waits for the moves
  @Test
  void asksOnlyBytecodesWhilePaused() {
    final TestWorld world = TestWorld.random(new Random(17), 300, 1, 100);
    network.serve(world);
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    stateSync.pause();
    round();
    tasks.tick();
    assertThat(network.unansweredRequests()).isNotEmpty().containsOnly("bytecodes");
    runToEnd();

    // with nothing in flight, the resume asks for the state at once
    stateSync.resume();
    tasks.runAll();
    assertThat(network.unansweredRequests()).contains("account_range");
    runToEnd();
    assertThat(completion).isCompleted();
  }

  // a request no peer takes fails, as the next ones would: asked again at once, they would loop
  // until the bound of the test executor; they wait for the periodic dispatch
  @Test
  void waitsForThePeriodicDispatchWhileNoPeerTakesItsRequests() {
    final TestWorld world = TestWorld.random(new Random(71), 300, 1, 100);
    network.serve(world);
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    network.disconnect();
    round();
    final int sent = network.requestCount();
    tasks.tick();
    assertThat(network.requestCount()).isGreaterThan(sent);

    // the periodic dispatch resumes the download once peers are back
    network.reconnect();
    tasks.tick();
    runToEnd();
    assertThat(completion).isCompleted();
  }

  // bytecodes ignore the exclusions: they go on while only peers left out serve the state
  @Test
  void keepsAskingBytecodesWhileNoPeerTakesTheStateRequests() {
    final TestWorld world = TestWorld.random(new Random(83), 600, 600, 0);
    network.serve(world);
    network.serveNoState(0);
    network.serveNoState(1);
    // answers with a few bytecodes, so that they still wait when the state requests stop
    network.maxResponseBytes(1024);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));
    runUntil(() -> stateSync.peersWithoutState() == 2);
    assertThat(stateSync.peersWithoutState()).isEqualTo(2);

    // the one peer that serves the state leaves
    network.disconnect(2);
    final int codeRequestsBefore = network.codeRequestCount();
    runToEnd();
    final int codeRequests = network.codeRequestCount();
    tasks.tick();

    // the peers left out took the bytecodes, every one known before the periodic dispatch
    assertThat(codeRequests).isGreaterThan(codeRequestsBefore);
    assertThat(network.codeRequestCount()).isEqualTo(codeRequests);
  }

  static Stream<Throwable> finalFailures() {
    return Stream.of(
        new RequestFailedException(Reason.STOPPED, null, "the scheduler stopped"),
        new IllegalStateException("the request cannot be sent"));
  }

  // a stopped scheduler, or a request that fails on its own, would fail any request again
  @ParameterizedTest
  @MethodSource("finalFailures")
  void failsWhenNoRequestCanSucceed(final Throwable failure) {
    final TestWorld world = TestWorld.random(new Random(79), 300, 1, 100);
    network.serve(world);
    network.failEveryRequest(failure);

    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    assertThatThrownBy(completion::join).hasCause(failure);
    // the five account ranges of the first dispatch, none asked again
    assertThat(network.requestCount()).isEqualTo(5);
  }

  static Stream<Throwable> lastingWriteFailures() {
    return Stream.of(StorageFailures.diskFailure(), new StackOverflowError("a bug"));
  }

  // a write that no retry fixes would fail with every answer
  @ParameterizedTest
  @MethodSource("lastingWriteFailures")
  void failsOnAWriteThatNoRetryFixes(final Throwable failure) {
    final TestWorld world = TestWorld.random(new Random(89), 300, 1, 100);
    network.serve(world);
    final SyncStateStorage storage = spy(SyncStorageFixture.create(Format.BONSAI).sync());
    doThrow(failure).when(storage).batch();
    final CompletableFuture<Void> completion = start(storage, header(1, world.root()));

    runToEnd();

    assertThatThrownBy(completion::join).hasCause(failure);
    // the five account ranges of the first dispatch, none asked again
    assertThat(network.requestCount()).isEqualTo(5);
  }

  // a failed loop task leaves the bookkeeping unsure, which a hang would hide
  @Test
  void failsWhenALoopTaskFails() {
    final TestWorld world = TestWorld.random(new Random(97), 300, 300, 0);
    network.serve(world);
    final SyncStateStorage storage = spy(SyncStorageFixture.create(Format.BONSAI).sync());
    final StackOverflowError failure = new StackOverflowError("a bug");
    // the loop looks the bytecodes up before it asks them
    doThrow(failure).when(storage).hasCode(any());
    final CompletableFuture<Void> completion = start(storage, header(1, world.root()));

    runToEnd();

    assertThatThrownBy(completion::join).hasCause(failure);
  }

  // a bytecode already stored, as after a restart, leaves nothing to ask
  @Test
  void completesWhenTheLastBytecodeIsAlreadyStored() {
    // one account, with a bytecode and no storage
    final TestWorld world = TestWorld.random(new Random(53), 1, 1, 0);
    network.serve(world);
    final SyncStateStorage storage = SyncStorageFixture.create(Format.BONSAI).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    world.codes().forEach(batch::putCode);
    batch.commit();
    final CompletableFuture<Void> completion =
        start(storage, accountRanges(1), header(1, world.root()));

    round();

    assertThat(completion).isCompleted();
    assertThat(network.codeRequestCount()).isZero();
  }

  // the scheduler still holds requests, which must not go out once the download stopped
  @Test
  void withdrawsItsRequestsOnStop() {
    final TestWorld world = TestWorld.random(new Random(101), 300, 1, 100);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    stateSync.stop();

    assertThat(network.unansweredRequests()).isNotEmpty();
    assertThat(network.withdrawnRequestCount()).isEqualTo(network.unansweredRequests().size());
  }

  // a zero would leave nothing to ask, or a queue that never drains
  @ParameterizedTest
  @CsvSource({
    "0, 4096, 8, 8, 16",
    "16, 0, 8, 8, 16",
    "16, 4096, 0, 8, 16",
    "16, 4096, 8, 0, 16",
    "16, 4096, 8, 8, 0"
  })
  void refusesSettingsWithAZero(
      final int accountChunks,
      final int responseBytes,
      final int storageBatchSize,
      final int maxStorageChunks,
      final int codeBatchSize) {
    assertThatThrownBy(
            () ->
                new StateSync.Settings(
                    accountChunks,
                    responseBytes,
                    storageBatchSize,
                    maxStorageChunks,
                    codeBatchSize))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void startsOnce() {
    final TestWorld world = TestWorld.random(new Random(73), 10, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    assertThatThrownBy(() -> stateSync.start(header(1, world.root())))
        .isInstanceOf(IllegalStateException.class);
  }

  // its loop shut down, as a real one refuses the periodic dispatch
  @Test
  void startsNothingOnceStopped() {
    final ScheduledExecutorService loop = Executors.newSingleThreadScheduledExecutor();
    stateSync = stateSync(SyncStorageFixture.create(Format.BONSAI).sync(), SETTINGS, loop);
    stateSync.stop();

    assertThat(stateSync.start(header(1, Bytes32.ZERO))).isCompletedExceptionally();
    assertThat(network.requestCount()).isZero();
  }

  // a stop on another thread can land between the check of the start and its periodic dispatch
  @Test
  void returnsTheFailedCompletionOfAStopDuringTheStart() {
    final QueuedExecutor loop = spy(new QueuedExecutor());
    stateSync = stateSync(SyncStorageFixture.create(Format.BONSAI).sync(), SETTINGS, loop);
    doAnswer(
            invocation -> {
              stateSync.stop();
              throw new RejectedExecutionException("stopped");
            })
        .when(loop)
        .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());

    assertThat(stateSync.start(header(1, Bytes32.ZERO))).isCompletedExceptionally();
  }

  // the settings, the account key space split into that many ranges
  private static StateSync.Settings accountRanges(final int ranges) {
    return new StateSync.Settings(
        ranges,
        SETTINGS.responseBytes(),
        SETTINGS.storageBatchSize(),
        SETTINGS.maxStorageChunks(),
        SETTINGS.codeBatchSize());
  }

  // a download on the test thread, its first requests sent
  private CompletableFuture<Void> start(final SyncStateStorage storage, final BlockHeader pivot) {
    return start(storage, SETTINGS, pivot);
  }

  private CompletableFuture<Void> start(
      final SyncStateStorage storage, final StateSync.Settings settings, final BlockHeader pivot) {
    stateSync = stateSync(storage, settings, tasks);
    final CompletableFuture<Void> completion = stateSync.start(pivot);
    tasks.runAll();
    return completion;
  }

  // a download whose writes and storage trie updates run on the test thread
  private StateSync stateSync(
      final SyncStateStorage storage,
      final StateSync.Settings settings,
      final ScheduledExecutorService loop) {
    return new StateSync(
        storage,
        network,
        settings,
        loop,
        tasks,
        nanoTime,
        (withoutMoves, trieLeft) ->
            trieUpdaters.apply(TestTries.updaterWithoutWorkers(storage, withoutMoves, trieLeft)));
  }

  // answers the requests sent, then runs what the download queued
  private void round() {
    network.answerAll();
    tasks.runAll();
  }

  // runs rounds until the condition holds or no request waits for an answer
  private void runUntil(final BooleanSupplier condition) {
    tasks.runAll();
    for (int rounds = 0; network.hasUnanswered() && !condition.getAsBoolean(); rounds++) {
      assertThat(rounds).as("rounds").isLessThan(MAX_ROUNDS);
      round();
    }
  }

  private void runToEnd() {
    runUntil(() -> false);
  }

  // moves the pivot over blocks that change the world; the answers at the old pivot are dropped
  private TestWorld move(
      final TestWorld world,
      final Random random,
      final long toBlock,
      final int blocks,
      final int changes) {
    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    for (int b = 0; b < blocks; b++) {
      diff.add(next.mutate(random, changes));
    }
    network.serve(next);
    assertThat(stateSync.moveTo(header(toBlock, next.root()), diff)).isCompleted();
    round();
    network.forget(world.root());
    return next;
  }

  private long progress() {
    return stateSync.downloadedAccounts() + stateSync.downloadedSlots();
  }

  // the download applies its last changes to the tries and stops first, as before the final build
  private WorldStateTrieBuilder.Result build(final Storages storages) {
    stateSync.finishStorageTries();
    stateSync.stop();
    return TestTries.build(storages.sync(), stateSync::storageTrieLeft);
  }

  // a state root no peer serves
  private static Bytes32 root(final int value) {
    return Bytes32.leftPad(Bytes.of(value));
  }

  private static BlockHeader header(final long number, final Bytes32 root) {
    return new BlockHeaderTestFixture().number(number).stateRoot(Hash.wrap(root)).buildHeader();
  }
}
