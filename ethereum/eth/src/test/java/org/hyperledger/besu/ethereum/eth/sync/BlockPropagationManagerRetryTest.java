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
package org.hyperledger.besu.ethereum.eth.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.PeerReputation;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetHeadersFromPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.NewBlockHashesMessage.BlockAnnouncement;
import org.hyperledger.besu.ethereum.eth.sync.BlockPropagationManager.ProcessingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.PendingBlocksManager;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.collect.Range;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a failed block retrieval is retried with a delay, that the number of retries is
 * bounded, and that a retry stops when the request is no longer current (the entry was removed or
 * superseded, or the manager was stopped).
 */
public class BlockPropagationManagerRetryTest {

  private static final long NUMBER = 42L;
  private static final long CHAIN_HEAD = 40L;
  private static final long BEST_CHAIN_HEIGHT = 50L;

  private static final int MAX_BLOCK_RETRIES = 10;

  private static final PeerTaskExecutorResult<Object> NO_PEER_RESULT =
      new PeerTaskExecutorResult<>(
          Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, List.of());

  private ProcessingBlocksManager processingBlocksManager;
  private BlockPropagationManager blockPropagationManager;
  private EthScheduler scheduler;
  private EthPeer peer1;
  private EthPeer peer2;
  private Hash hash1;
  private Hash hash2;

  private final AtomicInteger attemptsHash1 = new AtomicInteger();
  private final AtomicInteger attemptsHash2 = new AtomicInteger();
  private final AtomicInteger attemptsNonAnnounced = new AtomicInteger();
  private final AtomicInteger failedReleases = new AtomicInteger();
  private final List<Duration> retryDelays = new ArrayList<>();
  private final Deque<Runnable> pendingRetries = new ArrayDeque<>();
  private Runnable onFirstFailedRelease = () -> {};

  @BeforeEach
  public void setUp() {
    processingBlocksManager =
        new ProcessingBlocksManager() {
          @Override
          synchronized void registerFailedGetBlock(
              final long blockNumber, final Optional<Hash> maybeBlockHash) {
            super.registerFailedGetBlock(blockNumber, maybeBlockHash);
            if (failedReleases.incrementAndGet() == 1) {
              onFirstFailedRelease.run();
            }
          }
        };
    hash1 = hashOf(1);
    hash2 = hashOf(2);
    peer1 = peerWithFreshReputation(1);
    peer2 = peerWithFreshReputation(2);

    final SynchronizerConfiguration config = mock(SynchronizerConfiguration.class);
    when(config.getBlockPropagationRange()).thenReturn(Range.closed(-10L, 30L));

    final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    when(protocolSchedule.anyMatch(any())).thenReturn(false);

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(CHAIN_HEAD);
    when(blockchain.getBlockByHash(any())).thenReturn(Optional.empty());
    when(blockchain.getBlockByNumber(anyLong())).thenReturn(Optional.empty());

    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getBadBlockManager()).thenReturn(mock(BadBlockManager.class));

    final PeerTaskExecutor peerTaskExecutor = mock(PeerTaskExecutor.class);
    doAnswer(
            invocation -> {
              countAttempt(invocation.getArgument(0));
              return NO_PEER_RESULT;
            })
        .when(peerTaskExecutor)
        .execute(any());
    // A failed preferred-peer request falls back to execute(), which is where attempts are
    // counted; do not count the same attempt twice.
    doAnswer(invocation -> NO_PEER_RESULT).when(peerTaskExecutor).executeAgainstPeer(any(), any());

    final EthPeers ethPeers = mock(EthPeers.class);
    when(ethPeers.peerCount()).thenReturn(0);

    scheduler = buildSchedulerMock();

    final EthContext ethContext = mock(EthContext.class);
    when(ethContext.getScheduler()).thenReturn(scheduler);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(ethContext.getPeerTaskExecutor()).thenReturn(peerTaskExecutor);

    final SyncState syncState = mock(SyncState.class);
    when(syncState.bestChainHeight(CHAIN_HEAD)).thenReturn(BEST_CHAIN_HEIGHT);

    final PendingBlocksManager pendingBlocksManager = mock(PendingBlocksManager.class);
    when(pendingBlocksManager.contains(any())).thenReturn(false);

    blockPropagationManager =
        new BlockPropagationManager(
            config,
            protocolSchedule,
            protocolContext,
            ethContext,
            syncState,
            pendingBlocksManager,
            mock(MetricsSystem.class),
            mock(BlockBroadcaster.class),
            processingBlocksManager);
  }

  @Test
  public void retriesAreDelayedAndBounded() {
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer1))
        .isTrue();

    final CompletableFuture<Block> retrieval =
        blockPropagationManager.processAnnouncedBlock(peer1, new BlockAnnouncement(hash1, NUMBER));

    runAllPendingRetries();

    assertThat(attemptsHash1)
        .as("one initial attempt plus %d retries", MAX_BLOCK_RETRIES)
        .hasValue(MAX_BLOCK_RETRIES + 1);
    assertThat(retryDelays)
        .as("retries are spaced out and the delay is capped")
        .containsExactly(
            Duration.ofMillis(250),
            Duration.ofMillis(500),
            Duration.ofSeconds(1),
            Duration.ofSeconds(2),
            Duration.ofSeconds(4),
            Duration.ofSeconds(8),
            Duration.ofSeconds(16),
            Duration.ofSeconds(30),
            Duration.ofSeconds(30),
            Duration.ofSeconds(30));
    assertThat(retrieval).isCompletedWithValue(null);
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.of(hash1)))
        .as("the block number is released after the last attempt")
        .isFalse();
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer2))
        .as("a later announcement can request the number again")
        .isTrue();
  }

  @Test
  public void supersededRequestIsNotRetried() {
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer1))
        .isTrue();

    final CompletableFuture<Block> supersededRetrieval =
        blockPropagationManager.processAnnouncedBlock(peer1, new BlockAnnouncement(hash1, NUMBER));

    assertThat(attemptsHash1).hasValue(1);
    assertThat(pendingRetries).hasSize(1);

    // The first peer loses reputation, so an announcement for another hash can supersede it.
    peer1.getReputation().recordRequestTimeout("eth", 5, peer1);
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash2, NUMBER), peer2))
        .isTrue();

    runAllPendingRetries();

    assertThat(attemptsHash1).as("the superseded request is not retried").hasValue(1);
    assertThat(attemptsHash2).hasValue(0);
    assertThat(supersededRetrieval).isCompletedWithValue(null);
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.of(hash1))).isFalse();
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.of(hash2))).isTrue();
  }

  @Test
  public void stoppedManagerDoesNotRetry() {
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer1))
        .isTrue();

    final CompletableFuture<Block> retrieval =
        blockPropagationManager.processAnnouncedBlock(peer1, new BlockAnnouncement(hash1, NUMBER));
    assertThat(pendingRetries).hasSize(1);

    blockPropagationManager.stop();
    runAllPendingRetries();

    assertThat(attemptsHash1).as("no attempt is made after stop()").hasValue(1);
    assertThat(retrieval).isCompletedWithValue(null);
  }

  @Test
  public void requestRemovedByAnotherPathIsNotRetried() {
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer1))
        .isTrue();

    final CompletableFuture<Block> retrieval =
        blockPropagationManager.processAnnouncedBlock(peer1, new BlockAnnouncement(hash1, NUMBER));
    assertThat(pendingRetries).hasSize(1);

    // The block arrives by another path, so its request entry is removed while the retry is
    // pending.
    processingBlocksManager.registerReceivedBlock(blockWith(hash1, NUMBER));

    runAllPendingRetries();

    assertThat(attemptsHash1).as("the removed request is not retried").hasValue(1);
    assertThat(retrieval).isCompletedWithValue(null);
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.of(hash1))).isFalse();
  }

  @Test
  public void nonAnnouncedRequestIsRetriedAndBounded() {
    assertThat(processingBlocksManager.addNonAnnouncedBlocks(NUMBER)).isTrue();

    final CompletableFuture<Block> retrieval =
        blockPropagationManager.retrieveNonAnnouncedBlock(NUMBER);

    runAllPendingRetries();

    assertThat(attemptsNonAnnounced)
        .as("one initial attempt plus %d retries", MAX_BLOCK_RETRIES)
        .hasValue(MAX_BLOCK_RETRIES + 1);
    assertThat(retrieval).isCompletedWithValue(null);
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.empty()))
        .as("the non-announced number is released after the last attempt")
        .isFalse();
  }

  @Test
  public void announcementAcceptedWhileGivingUpIsNotDropped() {
    assertThat(
            processingBlocksManager.addRequestedBlock(new BlockAnnouncement(hash1, NUMBER), peer1))
        .isTrue();

    final CompletableFuture<Block> retrieval =
        blockPropagationManager.processAnnouncedBlock(peer1, new BlockAnnouncement(hash1, NUMBER));

    // Simulate another thread announcing the same block in the window between giving up and
    // releasing the number: the announcement has to survive the release.
    onFirstFailedRelease =
        () ->
            assertThat(
                    processingBlocksManager.addRequestedBlock(
                        new BlockAnnouncement(hash1, NUMBER), peer2))
                .isTrue();

    runAllPendingRetries();

    assertThat(failedReleases).as("the request is released exactly once").hasValue(1);
    assertThat(processingBlocksManager.isStillRequested(NUMBER, Optional.of(hash1)))
        .as("the announcement accepted while giving up is kept")
        .isTrue();
    assertThat(retrieval).isCompletedWithValue(null);
  }

  private void countAttempt(final Object task) {
    if (task instanceof GetHeadersFromPeerTask headersTask) {
      final Hash requestedHash = headersTask.getBlockHash();
      if (requestedHash != null && requestedHash.equals(hash1)) {
        attemptsHash1.incrementAndGet();
      } else if (requestedHash != null && requestedHash.equals(hash2)) {
        attemptsHash2.incrementAndGet();
      } else if (requestedHash == null) {
        attemptsNonAnnounced.incrementAndGet();
      }
    }
  }

  private static Block blockWith(final Hash hash, final long number) {
    final Block block = mock(Block.class);
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getNumber()).thenReturn(number);
    when(block.getHeader()).thenReturn(header);
    when(block.getHash()).thenReturn(hash);
    return block;
  }

  private EthPeer peerWithFreshReputation(final int id) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getReputation()).thenReturn(new PeerReputation());
    when(peer.getLoggableId()).thenReturn("retry-test-peer-" + id);
    return peer;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private EthScheduler buildSchedulerMock() {
    final EthScheduler schedulerMock = mock(EthScheduler.class);
    doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
        .when(schedulerMock)
        .scheduleServiceTask(any(Supplier.class));
    doAnswer(invocation -> ((Supplier) invocation.getArgument(0)).get())
        .when(schedulerMock)
        .scheduleSyncWorkerTask(any(Supplier.class));
    doAnswer(
            invocation -> {
              final Supplier<CompletableFuture<Block>> retry = invocation.getArgument(0);
              final Duration delay = invocation.getArgument(1);
              retryDelays.add(delay);
              final CompletableFuture<Block> promise = new CompletableFuture<>();
              pendingRetries.add(
                  () ->
                      retry
                          .get()
                          .whenComplete(
                              (block, throwable) -> {
                                if (throwable != null) {
                                  promise.completeExceptionally(throwable);
                                } else {
                                  promise.complete(block);
                                }
                              }));
              return promise;
            })
        .when(schedulerMock)
        .scheduleFutureTask(any(Supplier.class), any(Duration.class));
    return schedulerMock;
  }

  private void runAllPendingRetries() {
    int guard = 0;
    while (!pendingRetries.isEmpty()) {
      assertThat(guard++).as("retry loop did not terminate").isLessThan(2 * MAX_BLOCK_RETRIES + 2);
      pendingRetries.poll().run();
    }
  }

  private static Hash hashOf(final int seed) {
    return Hash.hash(Bytes.ofUnsignedInt(seed));
  }
}
