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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.Subscribers;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point of backward sync: turns the heads and payloads the consensus client sends into
 * events of the {@link BackwardSync} loop, and reports its outcome as sessions. A session starts
 * with the first target after an idle period and ends when the target is imported or the sync
 * fails; every call during a session gets the same future.
 */
public class BackwardSyncContext implements BackwardSync.Listener {
  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncContext.class);
  private static final long STOP_TIMEOUT_SECONDS = 60;

  private final SyncState syncState;
  private final EthPeers ethPeers;
  private final ScheduledExecutorService loop;
  private final PendingChain pendingChain;
  private final RequestSender sender;
  private final ExecutorService headerExecutor;
  private final BackwardSync engine;
  private final Subscribers<BadChainListener> badChainListeners = Subscribers.create();
  private final long ttdSubscription;
  private final long completionSubscription;
  private final long connectSubscription;

  // guarded by this
  private CompletableFuture<Void> session;
  private volatile boolean stopped;

  public BackwardSyncContext(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration syncConfig,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final SyncState syncState,
      final StorageProvider storageProvider) {
    final BackwardSyncConfiguration config =
        BackwardSyncConfiguration.create(syncConfig, protocolContext.getWorldStateArchive());
    final Blockchain blockchain = protocolContext.getBlockchain();
    this.syncState = syncState;
    this.ethPeers = ethContext.getEthPeers();
    this.loop =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("backward-sync").daemon().factory());
    this.sender = ethContext.getRequestScheduler().scope();
    this.pendingChain =
        PendingChain.create(
            storageProvider, ScheduleBasedBlockHeaderFunctions.create(protocolSchedule));
    this.headerExecutor =
        Executors.newCachedThreadPool(
            Thread.ofPlatform().name("backward-sync-headers-", 0).daemon().factory());
    final TrustedPayloads payloads = new TrustedPayloads(config.trustedPayloadBytes());
    final HeaderFetcher headerFetcher =
        new HeaderFetcher(
            sender,
            protocolSchedule,
            new ChainHeadSource() {
              @Override
              public Optional<Hash> headHash() {
                return Optional.empty();
              }

              @Override
              public Optional<BlockHeader> knownHeader(final Hash hash) {
                return payloads.block(hash).map(Block::getHeader);
              }
            },
            config.headersPerRequest());
    final HeaderSync headerSync =
        new HeaderSync(
            sender,
            protocolSchedule,
            pendingChain,
            blockchain,
            protocolContext.getBadBlockManager(),
            headerFetcher,
            headerExecutor,
            config,
            metricsSystem);
    final AtomicReference<BackwardSync> engineRef = new AtomicReference<>();
    final BlockDataFetcher fetcher =
        new BlockDataFetcher(
            sender,
            ethPeers,
            protocolSchedule,
            pendingChain,
            payloads,
            loop,
            System::nanoTime,
            config,
            () -> engineRef.get().scheduleImport());
    final BackwardSyncMetrics metrics =
        new BackwardSyncMetrics(
            metricsSystem,
            () -> engineRef.get() == null ? 0 : engineRef.get().pendingHeaders(),
            () -> engineRef.get() == null ? 0 : engineRef.get().targetBlock(),
            fetcher::blocks,
            fetcher::bytes);
    this.engine =
        new BackwardSync(
            protocolContext,
            pendingChain,
            payloads,
            headerSync,
            headerFetcher,
            fetcher,
            new ChainImporter(protocolContext, protocolSchedule, metrics),
            loop,
            System::nanoTime,
            config,
            metrics,
            this);
    engineRef.set(engine);
    this.ttdSubscription = subscribeTTD();
    this.completionSubscription = subscribeCompletion();
    this.connectSubscription = subscribeConnect();
    loop.execute(engine::initialize);
    postReadiness();
  }

  @VisibleForTesting
  BackwardSyncContext(
      final SyncState syncState,
      final EthPeers ethPeers,
      final ScheduledExecutorService loop,
      final PendingChain pendingChain,
      final RequestSender sender,
      final ExecutorService headerExecutor,
      final Function<BackwardSync.Listener, BackwardSync> engineFactory) {
    this.syncState = syncState;
    this.ethPeers = ethPeers;
    this.loop = loop;
    this.pendingChain = pendingChain;
    this.sender = sender;
    this.headerExecutor = headerExecutor;
    this.engine = engineFactory.apply(this);
    this.ttdSubscription = subscribeTTD();
    this.completionSubscription = subscribeCompletion();
    this.connectSubscription = subscribeConnect();
    loop.execute(engine::initialize);
    postReadiness();
  }

  private long subscribeTTD() {
    return syncState.subscribeTTDReached(reached -> postReadiness());
  }

  private long subscribeCompletion() {
    return syncState.subscribeCompletionReached(
        new InitialSyncCompletionListener() {
          @Override
          public void onInitialSyncCompleted() {
            postReadiness();
          }

          @Override
          public void onInitialSyncRestart() {
            postReadiness();
          }
        });
  }

  private long subscribeConnect() {
    return ethPeers.subscribeConnect(peer -> postReadiness());
  }

  private void postReadiness() {
    post(() -> engine.onReadiness(isReady()));
  }

  private boolean isReady() {
    return syncState.hasReachedTerminalDifficulty().orElse(false)
        && syncState.isInitialSyncPhaseDone()
        && ethPeers.peerCount() > 0;
  }

  private void post(final Runnable event) {
    try {
      loop.execute(event);
    } catch (final RejectedExecutionException e) {
      LOG.debug("Backward sync is stopped, event dropped");
    }
  }

  /**
   * Makes the head the CL named the target of backward sync.
   *
   * @param headHash the hash of the new head
   * @param finalizedHash the hash of the finalized block, zero when unknown
   * @return the future of the session, completed when the head is imported or the sync failed
   */
  public CompletableFuture<Void> syncBackwardsUntil(final Hash headHash, final Hash finalizedHash) {
    return startSession(() -> engine.onNewHead(headHash, finalizedHash));
  }

  /**
   * Makes a block received with a newPayload the target of backward sync.
   *
   * @param newHead the block
   * @param blockAccessList its block access list, when it came with it
   * @return the future of the session, completed when the block is imported or the sync failed
   */
  public CompletableFuture<Void> syncBackwardsUntil(
      final Block newHead, final Optional<BlockAccessList> blockAccessList) {
    return startSession(() -> engine.onNewPayload(newHead, blockAccessList));
  }

  private CompletableFuture<Void> startSession(final Runnable event) {
    final CompletableFuture<Void> current;
    synchronized (this) {
      if (stopped) {
        return CompletableFuture.failedFuture(new BackwardSyncException("backward sync stopped"));
      }
      current = currentSession();
    }
    post(event);
    return current;
  }

  // callers hold the lock
  private CompletableFuture<Void> currentSession() {
    if (session == null || session.isDone()) {
      LOG.info("Starting a new backward sync session");
      session = new CompletableFuture<>();
    }
    return session;
  }

  /**
   * Whether a backward sync session is currently running.
   *
   * @return true while a backward sync session is running
   */
  public synchronized boolean isSyncing() {
    return session != null && !session.isDone();
  }

  /**
   * Gets a header downloaded by backward sync that is not imported yet.
   *
   * @param hash the hash of the header
   * @return the header, when the pending chain holds it
   */
  public Optional<BlockHeader> getPendingHeader(final Hash hash) {
    return pendingChain.header(hash);
  }

  public void subscribeBadChainListener(final BadChainListener badChainListener) {
    badChainListeners.subscribe(badChainListener);
  }

  /** Stops the sync after the block being imported. Use {@link #awaitStop()} to wait for it. */
  public void stop() {
    synchronized (this) {
      stopped = true;
    }
    syncState.unsubscribeTTDReached(ttdSubscription);
    syncState.unsubscribeInitialConditionReached(completionSubscription);
    ethPeers.unsubscribeConnect(connectSubscription);
    post(engine::stop);
    loop.shutdown();
  }

  /**
   * Waits for the loop to finish its import, then releases the requests, the header download and
   * the open session.
   *
   * @throws InterruptedException when interrupted while waiting
   */
  public void awaitStop() throws InterruptedException {
    if (!loop.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      LOG.warn("Backward sync did not stop within {} seconds", STOP_TIMEOUT_SECONDS);
    }
    sender.stop();
    headerExecutor.shutdownNow();
    final CompletableFuture<Void> open;
    synchronized (this) {
      open = session;
      session = null;
    }
    if (open != null) {
      open.completeExceptionally(new BackwardSyncException("backward sync stopped"));
    }
  }

  @Override
  public void syncing() {
    synchronized (this) {
      currentSession();
    }
  }

  @Override
  public void completed() {
    endSession(null);
  }

  @Override
  public void failed(final BackwardSyncException e) {
    endSession(e);
  }

  private void endSession(final BackwardSyncException failure) {
    final CompletableFuture<Void> ended;
    synchronized (this) {
      ended = session;
      session = null;
    }
    if (ended == null) {
      return;
    }
    if (failure == null) {
      ended.complete(null);
    } else {
      ended.completeExceptionally(failure);
    }
  }

  @Override
  public void badChain(
      final BlockHeader bad, final List<Block> blocks, final List<BlockHeader> headers) {
    badChainListeners.forEach(listener -> listener.onBadChain(bad, blocks, headers));
  }
}
