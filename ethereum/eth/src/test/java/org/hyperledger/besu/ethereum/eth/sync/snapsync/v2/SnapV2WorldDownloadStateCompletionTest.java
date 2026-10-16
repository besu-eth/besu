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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.common.WorldStateHealFinishedListener;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;

import java.time.Clock;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapV2WorldDownloadStateCompletionTest {

  private final WorldStateStorageCoordinator worldStateStorageCoordinator =
      mock(WorldStateStorageCoordinator.class);
  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final BlockHeader worldStatePivot = pivot(2000);
  private SnapV2WorldDownloadState downloadState;

  @BeforeEach
  void setUp() {
    final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);
    when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(worldStatePivot));
    downloadState =
        new SnapV2WorldDownloadState(
            worldStateStorageCoordinator,
            mock(SnapSyncStatePersistenceManager.class),
            snapSyncState,
            new InMemoryTasksPriorityQueues<>(),
            100,
            1000,
            metricsManager,
            Clock.systemUTC(),
            mock(SyncDurationMetrics.class),
            mock(WorldStateHealFinishedListener.class),
            mock(SnapV2PivotCatchupListener.class),
            mock(SnapV2BlockAccessListApplier.class),
            mock(SnapV2ReorgHealer.class),
            mock(Blockchain.class),
            mock(EthContext.class),
            1000);
    // Every account range is downloaded and no request is pending.
    downloadState.getAccountRangeTracker().registerPending(Bytes32.ZERO, Bytes32.ZERO, 1);
    downloadState.getAccountRangeTracker().onChildCompleted(Bytes32.ZERO);
  }

  @Test
  void completionIsNotCheckedAgainstAPivotOtherThanTheWorldStatePivot() {
    // The chain downloader still holds the pivot before a completed catch-up.
    assertThat(downloadState.checkCompletion(pivot(1000))).isFalse();

    verify(worldStateStorageCoordinator, never()).getAccountStateTrieNode(any(), any());
    assertThat(downloadState.getDownloadFuture()).isNotDone();
  }

  @Test
  void completionIsCheckedAgainstTheWorldStatePivot() {
    when(worldStateStorageCoordinator.getAccountStateTrieNode(any(), any()))
        .thenReturn(Optional.empty());

    assertThat(downloadState.checkCompletion(worldStatePivot)).isFalse();

    // The root is looked up, so the download is otherwise complete.
    verify(worldStateStorageCoordinator)
        .getAccountStateTrieNode(
            Bytes.EMPTY, Bytes32.wrap(worldStatePivot.getStateRoot().getBytes()));
    assertThat(downloadState.getDownloadFuture()).isCompletedExceptionally();
  }

  private static BlockHeader pivot(final long number) {
    return new BlockHeaderTestFixture()
        .number(number)
        .stateRoot(Hash.hash(Bytes.ofUnsignedLong(number)))
        .buildHeader();
  }
}
