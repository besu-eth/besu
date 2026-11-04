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
import static org.hyperledger.besu.ethereum.eth.sync.snap2.SyncFixture.checkpoint;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.metrics.StubMetricsSystem;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the sync on the test thread: the attempts in rounds, the pivot checks when the test ticks
 * them, and the waits when the test wakes them up.
 */
class Snap2SyncControllerTest {

  @TempDir Path dataDirectory;

  private final SyncFixture node = new SyncFixture(31);
  private final TestChain chain = node.chain;
  private final QueuedExecutor control = new QueuedExecutor();
  private final List<RequestSender> handedOver = new ArrayList<>();
  private final StubMetricsSystem metricsSystem = new StubMetricsSystem();

  @Test
  void syncsTheChainAndTheStateWhileTheHeadMoves() {
    chain.extend(12);
    final Snap2SyncController controller = controller(Optional.empty());
    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    node.round();
    chain.extend(4);

    control.tick();
    node.runUntil(result::isDone);

    assertThat(pivot(result)).isEqualTo(chain.header(12));
    assertThat(node.blockchain.getChainHeadHeader()).isEqualTo(chain.header(12));
    assertThat(node.bonsai.getWorldStateRootHash()).contains(chain.world(12).root());
    assertThat(controller.pivotMoves()).isOne();
    assertThat(controller.restarts()).isZero();
    assertThat(metricsSystem.getGaugeValue("snap2_pivot_block")).isEqualTo(12);
    // the attempt released, its figures kept for the metrics
    assertThat(controller.current()).isEmpty();
    assertThat(controller.figures().orElseThrow().stage()).isEqualTo(SyncAttempt.Stage.DONE);
    // the back-fill goes on with the scheduler
    assertThat(handedOver).containsExactly(node.network);
    verify(node.network, never()).stop();
    final Path marker = Snap2SyncController.markerFile(dataDirectory);
    assertThat(marker).exists();
    controller.deletePivotSyncState();
    assertThat(marker).doesNotExist();
  }

  @Test
  void startsOverOnAReorgBelowThePivot() {
    chain.extend(12);
    final Snap2SyncController controller = controller(Optional.empty());
    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    node.round();
    chain.reorg(6, 10);

    control.tick();
    node.runUntil(result::isDone);

    // the new attempt at the new chain, a move away from its head
    assertThat(pivot(result)).isEqualTo(chain.header(12));
    assertThat(node.bonsai.getWorldStateRootHash()).contains(chain.world(12).root());
    assertThat(controller.restarts()).isOne();
  }

  @Test
  void failsOnceItStartedOverThreeTimes() {
    chain.extend(12);
    // a checkpoint no peer serves starts each attempt over
    final Snap2SyncController controller = controller(Optional.of(checkpoint(2, Hash.ZERO)));
    final CompletableFuture<SnapSyncProcessState> result = controller.start();

    node.runUntil(result::isDone);

    assertThat(result).isCompletedExceptionally();
    assertThat(controller.restarts()).isEqualTo(3);
    assertThat(handedOver).isEmpty();
    verify(node.network).stop();
  }

  @Test
  void waitsForASnapPeer() {
    chain.extend(12);
    node.network.disconnect();
    final CompletableFuture<SnapSyncProcessState> result = controller(Optional.empty()).start();
    node.round();
    assertThat(node.waiting).hasSize(1);

    node.network.reconnect();
    node.wakeUp();
    node.runUntil(result::isDone);

    assertThat(pivot(result)).isEqualTo(chain.header(8));
  }

  @Test
  void waitsForTheHeadToPassTheCheckpoint() {
    chain.extend(10);
    final CompletableFuture<SnapSyncProcessState> result =
        controller(Optional.of(checkpoint(10, chain.header(10).getHash()))).start();
    node.round();
    assertThat(node.waiting).hasSize(1);

    chain.extend(2);
    node.wakeUp();
    node.runUntil(result::isDone);

    // the first block above the checkpoint, nearer the head than the pivot distance
    assertThat(pivot(result)).isEqualTo(chain.header(11));
  }

  @Test
  void stopsTheAttemptAndTheScheduler() {
    chain.extend(12);
    final Snap2SyncController controller = controller(Optional.empty());
    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    node.round();

    controller.stop();
    node.runUntil(() -> control.isShutdown());

    assertThat(result).isCancelled();
    assertThat(handedOver).isEmpty();
    verify(node.network).stop();
    // a node restarted now syncs again
    assertThat(Snap2SyncController.markerFile(dataDirectory)).exists();
    assertThat(node.blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void wipesNothingOnceStopped() {
    final Snap2SyncController controller = controller(Optional.empty());
    final CompletableFuture<SnapSyncProcessState> result = controller.start();

    // before the sync thread gets to the wipe
    controller.stop();
    node.executor.runAll();

    assertThat(result).isCancelled();
    verify(node.storage, never()).prepareForSync();
  }

  @Test
  void startsNoAttemptOnceStopped() {
    chain.extend(12);
    node.network.disconnect();
    final Snap2SyncController controller = controller(Optional.empty());
    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    node.round();

    controller.stop();
    node.network.reconnect();
    node.wakeUp();
    node.round();

    assertThat(result).isCancelled();
    verify(node.network).stop();
    assertThat(node.network.requestCount()).isZero();
  }

  private Snap2SyncController controller(final Optional<Checkpoint> checkpoint) {
    return new Snap2SyncController(
        node.context(checkpoint),
        node.network,
        checkpoint.map(Checkpoint::blockNumber).orElse(0L),
        Duration.ofSeconds(2),
        Snap2SyncController.markerFile(dataDirectory),
        handedOver::add,
        node.executor,
        control,
        metricsSystem);
  }

  private static BlockHeader pivot(final CompletableFuture<SnapSyncProcessState> result) {
    assertThat(result).isCompleted();
    return result.join().getPivotBlockHeader().orElseThrow();
  }
}
