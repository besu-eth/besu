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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;
import org.hyperledger.besu.plugin.services.BesuEvents.TTDReachedListener;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class BackwardSyncContextTest {

  private static final Hash HEAD = Hash.fromHexStringLenient("0x01");
  private static final Hash FINALIZED = Hash.fromHexStringLenient("0x02");

  private final SyncState syncState = mock(SyncState.class);
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final PendingChain pendingChain = mock(PendingChain.class);
  private final RequestSender sender = mock(RequestSender.class);
  private final BackwardSync engine = mock(BackwardSync.class);
  private final AtomicReference<BackwardSync.Listener> listener = new AtomicReference<>();

  private BackwardSyncContext create(
      final ScheduledExecutorService loop, final ExecutorService headerExecutor) {
    return new BackwardSyncContext(
        syncState,
        ethPeers,
        loop,
        pendingChain,
        sender,
        headerExecutor,
        backwardSyncListener -> {
          listener.set(backwardSyncListener);
          return engine;
        });
  }

  private BackwardSyncContext create(final ManualScheduledExecutor loop) {
    return create(loop, mock(ExecutorService.class));
  }

  @Test
  void isSyncingIsTrueFromTheFirstTargetUntilTheSessionCompletes() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);
    assertThat(context.isSyncing()).isFalse();

    final CompletableFuture<Void> session = context.syncBackwardsUntil(HEAD, FINALIZED);
    loop.runPending();

    assertThat(context.isSyncing()).isTrue();
    assertThat(session).isNotDone();
    verify(engine).onNewHead(HEAD, FINALIZED);

    listener.get().completed();

    assertThat(session).isCompleted();
    assertThat(context.isSyncing()).isFalse();
  }

  @Test
  void callsWithinOneSessionShareItsFutureAndALaterCallStartsANewOne() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);
    final Block block = new BlockDataGenerator(1).block();

    final CompletableFuture<Void> first = context.syncBackwardsUntil(HEAD, FINALIZED);
    final CompletableFuture<Void> second = context.syncBackwardsUntil(FINALIZED, HEAD);
    final CompletableFuture<Void> third = context.syncBackwardsUntil(block, Optional.empty());
    loop.runPending();

    assertThat(second).isSameAs(first);
    assertThat(third).isSameAs(first);
    verify(engine).onNewHead(FINALIZED, HEAD);
    verify(engine).onNewPayload(block, Optional.empty());

    listener.get().completed();
    final CompletableFuture<Void> next = context.syncBackwardsUntil(HEAD, FINALIZED);

    assertThat(next).isNotSameAs(first);
    assertThat(next).isNotDone();
    assertThat(context.isSyncing()).isTrue();
  }

  @Test
  void aFailedSessionFailsItsFutureAndIsNoLongerSyncing() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);
    final CompletableFuture<Void> session = context.syncBackwardsUntil(HEAD, FINALIZED);

    listener.get().failed(new BackwardSyncException("invalid block"));

    assertThat(session)
        .failsWithin(0, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(BackwardSyncException.class)
        .withMessageContaining("invalid block");
    assertThat(context.isSyncing()).isFalse();
  }

  @Test
  void aSessionTheEngineStartsItselfIsSyncing() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);

    listener.get().syncing();

    assertThat(context.isSyncing()).isTrue();
    listener.get().completed();
    assertThat(context.isSyncing()).isFalse();
  }

  @Test
  void badChainEventsReachTheSubscribers() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);
    final BadChainListener badChainListener = mock(BadChainListener.class);
    context.subscribeBadChainListener(badChainListener);
    final BlockDataGenerator generator = new BlockDataGenerator(2);
    final BlockHeader bad = generator.header();
    final List<Block> blocks = List.of(generator.block());
    final List<BlockHeader> headers = List.of(generator.header());

    listener.get().badChain(bad, blocks, headers);

    verify(badChainListener).onBadChain(bad, blocks, headers);
  }

  @Test
  void aPendingHeaderComesFromThePendingChain() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final BackwardSyncContext context = create(loop);
    final BlockHeader header = new BlockDataGenerator(3).header();
    when(pendingChain.header(header.getHash())).thenReturn(Optional.of(header));

    assertThat(context.getPendingHeader(header.getHash())).contains(header);
    assertThat(context.getPendingHeader(HEAD)).isEmpty();
  }

  @Test
  void theEngineIsNotReadyUntilTtdInitialSyncAndAPeerAreAllThere() {
    final ManualScheduledExecutor loop = new ManualScheduledExecutor();
    final ArgumentCaptor<TTDReachedListener> ttd =
        ArgumentCaptor.forClass(TTDReachedListener.class);
    final ArgumentCaptor<InitialSyncCompletionListener> completion =
        ArgumentCaptor.forClass(InitialSyncCompletionListener.class);
    final ArgumentCaptor<EthPeers.ConnectCallback> connect =
        ArgumentCaptor.forClass(EthPeers.ConnectCallback.class);
    when(syncState.hasReachedTerminalDifficulty()).thenReturn(Optional.empty());
    when(syncState.isInitialSyncPhaseDone()).thenReturn(false);
    when(ethPeers.peerCount()).thenReturn(0);
    create(loop);
    verify(syncState).subscribeTTDReached(ttd.capture());
    verify(syncState).subscribeCompletionReached(completion.capture());
    verify(ethPeers).subscribeConnect(connect.capture());
    loop.runPending();

    verify(engine).initialize();
    verify(engine).onReadiness(false);
    verify(engine, never()).onReadiness(true);

    when(syncState.hasReachedTerminalDifficulty()).thenReturn(Optional.of(true));
    ttd.getValue().onTTDReached(true);
    loop.runPending();
    verify(engine, never()).onReadiness(true);

    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    completion.getValue().onInitialSyncCompleted();
    loop.runPending();
    verify(engine, never()).onReadiness(true);

    when(ethPeers.peerCount()).thenReturn(1);
    connect.getValue().onPeerConnected(null);
    loop.runPending();
    verify(engine).onReadiness(true);
  }

  @Test
  void stopEndsTheLoopThreadAndAwaitStopReleasesTheRequestsAndTheOpenSession()
      throws InterruptedException {
    final ScheduledExecutorService loop = Executors.newSingleThreadScheduledExecutor();
    final ExecutorService headerExecutor = Executors.newCachedThreadPool();
    final BackwardSyncContext context = create(loop, headerExecutor);
    final CompletableFuture<Void> session = context.syncBackwardsUntil(HEAD, FINALIZED);
    verify(engine, timeout(5_000)).onNewHead(HEAD, FINALIZED);

    context.stop();
    context.awaitStop();

    verify(engine, timeout(5_000)).stop();
    verify(syncState).unsubscribeTTDReached(0L);
    verify(syncState).unsubscribeInitialConditionReached(0L);
    verify(ethPeers).unsubscribeConnect(0L);
    verify(sender).stop();
    assertThat(loop.isTerminated()).isTrue();
    assertThat(headerExecutor.isShutdown()).isTrue();
    assertThat(session)
        .failsWithin(0, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withCauseInstanceOf(BackwardSyncException.class)
        .withMessageContaining("backward sync stopped");
    assertThat(context.isSyncing()).isFalse();
    assertThat(context.syncBackwardsUntil(HEAD, FINALIZED)).isCompletedExceptionally();
    verify(engine, never()).onNewPayload(any(), any());
  }
}
