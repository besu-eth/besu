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
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.ImmutableCheckpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.TestStateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * A node that syncs a test chain on the test thread, in rounds: the network answers the state
 * requests, then the state download, the header download, the window and the attempts run what they
 * queued. The chain answers at once, and the waits run when a test wakes them up.
 */
final class SyncFixture {

  // a move once the head is six blocks ahead, to four blocks behind it
  static final PivotMoves.Settings MOVES = new PivotMoves.Settings(4, 2);
  static final int WINDOW = 4;
  // 16 account ranges, 4 KiB answers, 8 storages a request, 8 ranges of a storage, 16 bytecodes
  private static final StateSync.Settings STATE = new StateSync.Settings(16, 4096, 8, 8, 16);
  // far more than a sync of these worlds takes
  private static final int MAX_ROUNDS = 10_000;

  final FakeSnapNetwork network = spy(new FakeSnapNetwork());
  final TestChain chain;
  final MutableBlockchain blockchain;
  final Storages storages = SyncStorageFixture.create(Format.BONSAI);
  final SyncStateStorage storage = spy(storages.sync());
  // where the sync records the world state root and its block
  final BonsaiWorldStateKeyValueStorage bonsai =
      storages.worldState().getStrategy(BonsaiWorldStateKeyValueStorage.class);
  final HeaderFetcher fetcher;
  // a round without the lists fails the fetch
  final BalFetcher balFetcher = new BalFetcher(network, 32, 1 << 20, 1);
  // the loop and the writes of the state download
  final QueuedExecutor tasks = new QueuedExecutor();
  final QueuedExecutor headers = new QueuedExecutor();
  final QueuedExecutor windowWorkers = new QueuedExecutor();
  final QueuedExecutor resumes = new QueuedExecutor();
  // the attempts and their final builds
  final QueuedExecutor executor = new QueuedExecutor();
  // the tasks that wait for a delay, run when the test wakes them up
  final List<Runnable> waiting = new ArrayList<>();
  final Function<Duration, Executor> delays = delay -> waiting::add;

  SyncFixture(final long seed) {
    this(seed, TestWorld.random(new Random(seed), 60, 2, 50));
  }

  SyncFixture(final long seed, final TestWorld genesisWorld) {
    chain = new TestChain(new Random(seed), genesisWorld, network);
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    fetcher = new HeaderFetcher(network, ProtocolScheduleFixture.MAINNET, chain, 8);
  }

  /** Returns a checkpoint of proof of stake. */
  static Checkpoint checkpoint(final long number, final Hash hash) {
    return ImmutableCheckpoint.builder()
        .blockNumber(number)
        .blockHash(hash)
        .totalDifficulty(Difficulty.ZERO)
        .build();
  }

  /** Returns a state download whose loop and writes run on {@link #tasks}. */
  StateSync stateSync() {
    return TestStateSync.create(storage, network, STATE, tasks);
  }

  /** Returns what the attempts share, the checkpoint as their floor. */
  SyncAttempt.Context context(final Optional<Checkpoint> checkpoint) {
    return new SyncAttempt.Context(
        blockchain,
        storage,
        chain,
        fetcher,
        balFetcher,
        MOVES,
        this::stateSync,
        () ->
            new SyncChain(
                blockchain,
                ProtocolScheduleFixture.MAINNET,
                network,
                fetcher,
                checkpoint,
                new SyncChain.Settings(8, 4, WINDOW, false, false, Optional.of(Difficulty.ZERO)),
                new NoOpMetricsSystem(),
                headers,
                windowWorkers,
                resumes,
                millis -> {},
                10_000),
        trieLeft -> TestTries.builder(storage, trieLeft),
        executor,
        delays);
  }

  /** Answers the state requests, then runs what the state download queued. */
  void stateRound() {
    network.answerAll();
    tasks.runAll();
  }

  void round() {
    stateRound();
    headers.runAll();
    windowWorkers.runAll();
    executor.runAll();
  }

  void runUntil(final BooleanSupplier condition) {
    runUntil(condition, this::round);
  }

  void runUntil(final BooleanSupplier condition, final Runnable round) {
    for (int rounds = 0; !condition.getAsBoolean(); rounds++) {
      assertThat(rounds).as("rounds").isLessThan(MAX_ROUNDS);
      round.run();
    }
  }

  /** Runs the state rounds until no state request waits for an answer. */
  void downloadState() {
    stateRound();
    runUntil(() -> !network.hasUnanswered(), this::stateRound);
  }

  /** Runs the waits due so far, not those they start. */
  void wakeUp() {
    final List<Runnable> due = List.copyOf(waiting);
    waiting.clear();
    due.forEach(Runnable::run);
  }
}
