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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.common.PivotSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamicPivotBlockManagerTest {

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final PivotSyncActions fastSyncActions = mock(PivotSyncActions.class);
  private final SyncState syncState = mock(SyncState.class);
  private final EthContext ethContext = mock(EthContext.class);
  private DynamicPivotBlockSelector dynamicPivotBlockManager;

  @BeforeEach
  public void setup() {
    when(fastSyncActions.getSyncState()).thenReturn(syncState);
    when(ethContext.getScheduler()).thenReturn(new DeterministicEthScheduler());
    dynamicPivotBlockManager =
        new DynamicPivotBlockSelector(ethContext, fastSyncActions, snapSyncState, null);
  }

  @Test
  public void shouldSwitchPivotWhenDifferentFromCurrent() {
    final BlockHeader newHeader = new BlockHeaderTestFixture().number(1060).buildHeader();
    final SnapSyncProcessState selectedState = new SnapSyncProcessState(newHeader.getHash());
    final SnapSyncProcessState downloadedState = new SnapSyncProcessState(newHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectedState));
    when(fastSyncActions.resolvePivotBlockHeader(selectedState))
        .thenReturn(completedFuture(downloadedState));
    // current pivot is different (empty → no current pivot header)
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.empty());
    when(snapSyncState.getPivotBlockNumber()).thenReturn(java.util.OptionalLong.of(900));

    dynamicPivotBlockManager.check(
        (blockHeader, newBlockFound) -> {
          assertThat(newBlockFound).isTrue();
          assertThat(blockHeader.getNumber()).isEqualTo(1060);
        });

    verify(snapSyncState).setCurrentHeader(newHeader);
  }

  @Test
  public void shouldNotSwitchPivotWhenSameAsCurrent() {
    final BlockHeader currentHeader = new BlockHeaderTestFixture().number(1060).buildHeader();
    final SnapSyncProcessState selectedState = new SnapSyncProcessState(currentHeader.getHash());

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectedState));
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.of(currentHeader));
    when(snapSyncState.getPivotBlockNumber()).thenReturn(java.util.OptionalLong.of(1060));

    dynamicPivotBlockManager.check(
        (blockHeader, newBlockFound) -> assertThat(newBlockFound).isFalse());

    verify(fastSyncActions, never()).resolvePivotBlockHeader(selectedState);
    verify(snapSyncState, never()).setCurrentHeader(currentHeader);
  }

  @Test
  public void shouldThrottleChecks() {
    // Use a scheduler whose timer task never fires so isTimeToCheckAgain stays false after first
    // check
    final EthScheduler nonFiringScheduler = mock(EthScheduler.class);
    when(nonFiringScheduler.scheduleFutureTask(any(Runnable.class), any()))
        .thenReturn(new CompletableFuture<>()); // never completes → timer never fires
    final EthContext nonFiringContext = mock(EthContext.class);
    when(nonFiringContext.getScheduler()).thenReturn(nonFiringScheduler);

    final DynamicPivotBlockSelector throttled =
        new DynamicPivotBlockSelector(nonFiringContext, fastSyncActions, snapSyncState, null);

    final BlockHeader newHeader = new BlockHeaderTestFixture().number(1060).buildHeader();
    final SnapSyncProcessState selectedState = new SnapSyncProcessState(newHeader.getHash());
    final SnapSyncProcessState downloadedState = new SnapSyncProcessState(newHeader);

    when(fastSyncActions.selectPivotBlock(new SnapSyncProcessState()))
        .thenReturn(completedFuture(selectedState));
    when(fastSyncActions.resolvePivotBlockHeader(selectedState))
        .thenReturn(completedFuture(downloadedState));
    when(snapSyncState.getPivotBlockHeader()).thenReturn(Optional.empty());
    when(snapSyncState.getPivotBlockNumber()).thenReturn(java.util.OptionalLong.of(900));

    // first check triggers select + switch
    throttled.check(doNothingOnPivotChange());
    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState());

    // second immediate check is a no-op — timer has not fired
    throttled.check(doNothingOnPivotChange());
    verify(fastSyncActions).selectPivotBlock(new SnapSyncProcessState()); // still just once
  }

  private static java.util.function.BiConsumer<BlockHeader, Boolean> doNothingOnPivotChange() {
    return DynamicPivotBlockSelector.doNothingOnPivotChange;
  }
}
