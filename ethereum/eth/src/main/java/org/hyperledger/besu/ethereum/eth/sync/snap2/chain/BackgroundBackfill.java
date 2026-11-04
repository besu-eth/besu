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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.ethereum.chain.ChainDataPruner;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores in the background the bodies and receipts below the lowest block whose body a sync stored,
 * from the end of the initial sync. The back-fill sends through a SNAP2 scheduler that runs until
 * it ends.
 */
public final class BackgroundBackfill implements InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(BackgroundBackfill.class);
  // decoding an answer is short next to waiting for it
  private static final int DECODE_THREADS = 2;
  // a stopped back-fill writes nothing, its workers only end the reads they started
  private static final Duration WORKERS_TIMEOUT = Duration.ofSeconds(30);

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final SyncState syncState;
  private final Optional<ChainDataPruner> chainDataPruner;
  private final boolean enabled;
  private final BodyBackfill.Settings settings;
  private final MetricsSystem metricsSystem;
  private final Supplier<RequestSender> senders;
  private final Supplier<ExecutorService> workerPools;

  // guarded by this: the last back-fill started, the pools its workers and earlier ones ran on,
  // whether the node stopped, and the scheduler a SNAP2 sync handed over for the next back-fill
  private BodyBackfill current;
  private final List<ExecutorService> startedPools = new ArrayList<>();
  private boolean closed;
  private Optional<RequestSender> syncScheduler = Optional.empty();

  /** Creates the back-fill of a node, idle until the initial sync completes. */
  public static BackgroundBackfill create(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final EthPeers ethPeers,
      final SyncState syncState,
      final Optional<ChainDataPruner> chainDataPruner,
      final SnapSyncConfiguration config,
      final MetricsSystem metricsSystem) {
    final BodyBackfill.Settings settings =
        BodyBackfill.Settings.forBackground(config.isSnapSyncTransactionIndexingEnabled());
    return new BackgroundBackfill(
        blockchain,
        protocolSchedule,
        syncState,
        chainDataPruner,
        config.isSnap2BackfillEnabled(),
        settings,
        metricsSystem,
        // a scheduler of its own, when no SNAP2 sync handed over the one it ran on
        () -> RequestScheduler.forSync(ethPeers, metricsSystem, DECODE_THREADS),
        () ->
            Executors.newFixedThreadPool(
                settings.parallelism(),
                Thread.ofPlatform().name("snap2-backfill-", 0).daemon().factory()));
  }

  /**
   * Creates a back-fill that sends through the schedulers {@code senders} creates, and runs on the
   * pools {@code workerPools} creates.
   */
  @VisibleForTesting
  BackgroundBackfill(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final SyncState syncState,
      final Optional<ChainDataPruner> chainDataPruner,
      final boolean enabled,
      final BodyBackfill.Settings settings,
      final MetricsSystem metricsSystem,
      final Supplier<RequestSender> senders,
      final Supplier<ExecutorService> workerPools) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.syncState = syncState;
    this.chainDataPruner = chainDataPruner;
    this.enabled = enabled;
    this.settings = settings;
    this.metricsSystem = metricsSystem;
    this.senders = senders;
    this.workerPools = workerPools;
  }

  /**
   * Starts the back-fill when a sync left bodies and receipts to store and none runs, nothing once
   * {@link #awaitStop} closed it.
   */
  @Override
  public synchronized void onInitialSyncCompleted() {
    final Optional<RequestSender> handedOver = syncScheduler;
    syncScheduler = Optional.empty();
    boolean started = false;
    if (!closed && !isRunning()) {
      try {
        started = maybeStart(handedOver);
      } catch (final RuntimeException e) {
        // the sync and the other listeners of the event go on without the back-fill
        LOG.warn("SNAP2 back-fill not started", e);
      }
    }
    // the scheduler of the sync serves the back-fill that starts now, or none
    if (!started) {
      handedOver.ifPresent(RequestSender::stop);
    }
  }

  /**
   * Takes the scheduler a SNAP2 sync ran on: the back-fill that starts once the sync completes
   * sends through it, and stops it once it ends, as when none starts.
   */
  public synchronized void handOver(final RequestSender scheduler) {
    if (closed) {
      // nothing would stop it once closed
      scheduler.stop();
      return;
    }
    syncScheduler.ifPresent(RequestSender::stop);
    syncScheduler = Optional.of(scheduler);
  }

  /** Stops the back-fill, which starts again with the next initial sync. */
  @Override
  public void onInitialSyncRestart() {
    stop();
  }

  /** Stops the back-fill; nothing is written afterwards. */
  public synchronized void stop() {
    if (current != null) {
      current.stop();
    }
  }

  /**
   * Stops the back-fill for good and waits for the workers of every back-fill started, which read
   * the storage, so that the storage can close.
   */
  public void awaitStop() throws InterruptedException {
    final List<ExecutorService> pools;
    synchronized (this) {
      closed = true;
      // a sync that just completed may have started one after the stop
      stop();
      syncScheduler.ifPresent(RequestSender::stop);
      syncScheduler = Optional.empty();
      pools = List.copyOf(startedPools);
    }
    // one deadline for every pool, so that the wait stays bounded
    final long deadline = System.nanoTime() + WORKERS_TIMEOUT.toNanos();
    boolean terminated = true;
    for (final ExecutorService pool : pools) {
      terminated &= pool.awaitTermination(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
    }
    if (!terminated) {
      LOG.warn("SNAP2 back-fill workers still running after {}", WORKERS_TIMEOUT);
    }
  }

  // ended rather than done, which completes later: a completion in between would be lost
  private boolean isRunning() {
    return current != null && !current.hasEnded();
  }

  // whether a back-fill started, which stops its scheduler once it ends
  private boolean maybeStart(final Optional<RequestSender> handedOver) {
    // recorded by a sync that stored the bodies of the latest blocks only
    final Optional<Long> bodiesFrom = blockchain.getBodiesFrom();
    final LongUnaryOperator lowestNeeded = lowestNeeded();
    final boolean bodiesMissing =
        bodiesFrom.isPresent()
            && bodiesFrom.get() > lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber());
    if (bodiesMissing && !enabled) {
      LOG.info(
          "SNAP2 back-fill disabled, the bodies and receipts below block {} are not downloaded",
          bodiesFrom.get());
    }
    if (!bodiesMissing || !enabled) {
      return false;
    }
    final BodyBackfill backfill =
        build(bodiesFrom.get(), lowestNeeded, handedOver.orElseGet(senders));
    try {
      final ExecutorService workers = workerPools.get();
      startedPools.removeIf(ExecutorService::isTerminated);
      startedPools.add(workers);
      current = backfill;
      backfill.start(workers);
    } catch (final RuntimeException e) {
      // ended, it stops its scheduler and lets the next initial sync start another
      backfill.stop();
      throw e;
    }
    return true;
  }

  // the scheduler runs until the back-fill ends, however it ends
  private BodyBackfill build(
      final long lowest, final LongUnaryOperator lowestNeeded, final RequestSender sender) {
    final BodyBackfill backfill;
    try {
      backfill =
          new BodyBackfill(
              blockchain,
              protocolSchedule,
              sender,
              followsTheHead(syncState),
              lowest,
              lowestNeeded,
              settings,
              metricsSystem);
    } catch (final RuntimeException e) {
      // no back-fill is there to stop the scheduler
      sender.stop();
      throw e;
    }
    backfill.done().whenComplete((unused, error) -> sender.stop());
    return backfill;
  }

  // in sync once a peer told its head: with no peer known, the node counts as in sync
  @VisibleForTesting
  static BooleanSupplier followsTheHead(final SyncState syncState) {
    return () -> syncState.getBestPeerChainHead().isPresent() && syncState.isInSync();
  }

  // above the floor of the sync, and a pruning run away from the blocks a pruner removes
  private LongUnaryOperator lowestNeeded() {
    // recorded with the bodies from, whatever the checkpoint of this run
    final long floor = blockchain.getSyncFloor().orElse(0L);
    return head ->
        Math.max(
            floor + 1, chainDataPruner.map(pruner -> pruner.lowestToBackfill(head)).orElse(0L));
  }
}
