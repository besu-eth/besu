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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO2;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO3;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ForkChangeTracker;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Snap2AdvertiserTest {

  private static final long SYNC_SUBSCRIPTION = 7L;
  private static final long CHAIN_OBSERVER = 5L;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final SnapProtocolManager snapProtocolManager = mock(SnapProtocolManager.class);
  private final SyncState syncState = mock(SyncState.class);
  private ForkChangeTracker forks;
  private InitialSyncCompletionListener syncCompletion;
  private long nextBlock = 1;

  @BeforeEach
  void setUp() {
    setHead(OSAKA);
    when(blockchain.observeBlockAdded(any())).thenReturn(CHAIN_OBSERVER);
    forks = ForkChangeTracker.create(blockchain, protocolSchedule);
    when(snapProtocolManager.advertiseSnap2()).thenReturn(true);
    when(syncState.subscribeCompletionReached(any()))
        .thenAnswer(
            invocation -> {
              syncCompletion = invocation.getArgument(0);
              return SYNC_SUBSCRIPTION;
            });
  }

  @Test
  void waitsForTheSyncOnceTheHeadReachedTheFork() {
    final Snap2Advertiser advertiser = Snap2Advertiser.start(snapProtocolManager, syncState, forks);
    moveHead(AMSTERDAM);
    verify(snapProtocolManager, never()).advertiseSnap2();

    syncDone();

    verify(snapProtocolManager).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
    verify(syncState).unsubscribeInitialConditionReached(SYNC_SUBSCRIPTION);
  }

  @Test
  void advertisesWhenTheSyncedHeadReachesTheFork() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    final Snap2Advertiser advertiser = Snap2Advertiser.start(snapProtocolManager, syncState, forks);
    moveHead(BPO2);
    verify(snapProtocolManager, never()).advertiseSnap2();

    moveHead(AMSTERDAM);
    moveHead(BPO3);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
  }

  @Test
  void advertisesWhenTheSyncEndsPastTheFork() {
    Snap2Advertiser.start(snapProtocolManager, syncState, forks);

    // the sync sets the head at its pivot without a block added event
    setHead(AMSTERDAM);
    syncDone();

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void advertisesAtOnceWhenSyncedPastTheFork() {
    setHead(BOGOTA);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    final Snap2Advertiser advertiser = Snap2Advertiser.start(snapProtocolManager, syncState, forks);

    verify(snapProtocolManager).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
  }

  @Test
  void waitsWhileTheSyncedHeadIsBeforeTheFork() {
    Snap2Advertiser.start(snapProtocolManager, syncState, forks);

    syncDone();

    verify(snapProtocolManager, never()).advertiseSnap2();
  }

  @Test
  void advertisesWhenAReorgReachedTheForkAsTheSyncEnded() {
    Snap2Advertiser.start(snapProtocolManager, syncState, forks);

    // the reorg tells the new head before the chain head reads it
    forks.onBlockAdded(headAdvancement(header(AMSTERDAM)));
    syncDone();

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void stopsFollowingTheChainOnceAdvertised() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    Snap2Advertiser.start(snapProtocolManager, syncState, forks);
    verify(blockchain, never()).removeObserver(CHAIN_OBSERVER);

    moveHead(AMSTERDAM);

    verify(blockchain).removeObserver(CHAIN_OBSERVER);
  }

  @Test
  void survivesAFailedAdvertisement() {
    when(snapProtocolManager.advertiseSnap2()).thenThrow(new IllegalStateException("no network"));
    final Snap2Advertiser advertiser = Snap2Advertiser.start(snapProtocolManager, syncState, forks);
    moveHead(AMSTERDAM);

    syncDone();
    moveHead(BPO3);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
  }

  @Test
  void blockAccessListsStartWithAmsterdam() {
    assertThat(Snap2Advertiser.hasBlockAccessLists(OSAKA)).isFalse();
    assertThat(Snap2Advertiser.hasBlockAccessLists(BPO2)).isFalse();
    assertThat(Snap2Advertiser.hasBlockAccessLists(AMSTERDAM)).isTrue();
    assertThat(Snap2Advertiser.hasBlockAccessLists(BPO3)).isTrue();
    assertThat(Snap2Advertiser.hasBlockAccessLists(BOGOTA)).isTrue();
    assertThat(Snap2Advertiser.hasBlockAccessLists(mock(HardforkId.class))).isFalse();
  }

  private void syncDone() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    syncCompletion.onInitialSyncCompleted();
  }

  private void moveHead(final HardforkId fork) {
    forks.onBlockAdded(headAdvancement(setHead(fork)));
  }

  private static BlockAddedEvent headAdvancement(final BlockHeader head) {
    return BlockAddedEvent.createForHeadAdvancement(
        new Block(head, BlockBody.empty()), List.of(), List.of());
  }

  private BlockHeader setHead(final HardforkId fork) {
    final BlockHeader header = header(fork);
    when(blockchain.getChainHeadHeader()).thenReturn(header);
    return header;
  }

  private BlockHeader header(final HardforkId fork) {
    final BlockHeader header = new BlockHeaderTestFixture().number(nextBlock++).buildHeader();
    final ProtocolSpec spec = mock(ProtocolSpec.class);
    when(spec.getHardforkId()).thenReturn(fork);
    when(protocolSchedule.getByBlockHeader(header)).thenReturn(spec);
    return header;
  }
}
