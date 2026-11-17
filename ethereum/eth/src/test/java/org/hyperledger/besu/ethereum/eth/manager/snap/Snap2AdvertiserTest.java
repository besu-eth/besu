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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.plugin.services.BesuEvents.InitialSyncCompletionListener;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Snap2AdvertiserTest {

  private static final long SYNC_SUBSCRIPTION = 7L;
  private static final long CHAIN_SUBSCRIPTION = 5L;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final SnapProtocolManager snapProtocolManager = mock(SnapProtocolManager.class);
  private final SyncState syncState = mock(SyncState.class);
  private BlockAddedObserver chain;
  private InitialSyncCompletionListener syncCompletion;
  private long nextBlock = 1;

  @BeforeEach
  void setUp() {
    setHead(header(false));
    when(snapProtocolManager.advertiseSnap2()).thenReturn(true);
    when(blockchain.observeBlockAdded(any()))
        .thenAnswer(
            invocation -> {
              chain = invocation.getArgument(0);
              return CHAIN_SUBSCRIPTION;
            });
    when(syncState.subscribeCompletionReached(any()))
        .thenAnswer(
            invocation -> {
              syncCompletion = invocation.getArgument(0);
              return SYNC_SUBSCRIPTION;
            });
  }

  @Test
  void waitsForTheSyncOnceTheHeadHasABlockAccessList() {
    final Snap2Advertiser advertiser = start();
    moveHead(true);
    verify(snapProtocolManager, never()).advertiseSnap2();

    syncDone();

    verify(snapProtocolManager).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
    verify(syncState).unsubscribeInitialConditionReached(SYNC_SUBSCRIPTION);
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void advertisesWhenTheSyncedHeadGetsABlockAccessList() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    final Snap2Advertiser advertiser = start();
    moveHead(false);
    verify(snapProtocolManager, never()).advertiseSnap2();

    moveHead(true);
    moveHead(true);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void advertisesWhenTheSyncEndsPastTheFork() {
    start();

    // the sync sets the head at its pivot without a block added event
    setHead(header(true));
    syncDone();

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void advertisesAtOnceWhenSyncedPastTheFork() {
    setHead(header(true));
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    final Snap2Advertiser advertiser = start();

    verify(snapProtocolManager).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
  }

  @Test
  void waitsWhileTheSyncedHeadHasNoBlockAccessList() {
    start();

    syncDone();

    verify(snapProtocolManager, never()).advertiseSnap2();
  }

  @Test
  void ignoresABlockOffTheCanonicalChain() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    start();

    chain.onBlockAdded(BlockAddedEvent.createForFork(new Block(header(true), BlockBody.empty())));

    verify(snapProtocolManager, never()).advertiseSnap2();
  }

  @Test
  void catchesUpOnTheNextHeadAfterAReorgReachedTheForkAsTheSyncEnded() {
    start();
    // a reorg tells the new head before the chain head reads it
    chain.onBlockAdded(headAdvancement(header(true)));
    syncDone();
    verify(snapProtocolManager, never()).advertiseSnap2();

    moveHead(true);

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void survivesAFailedAdvertisement() {
    when(snapProtocolManager.advertiseSnap2()).thenThrow(new IllegalStateException("no network"));
    final Snap2Advertiser advertiser = start();
    moveHead(true);

    syncDone();
    moveHead(true);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    assertThat(advertiser.isDone()).isTrue();
  }

  private Snap2Advertiser start() {
    return Snap2Advertiser.start(snapProtocolManager, syncState, blockchain);
  }

  private void syncDone() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    syncCompletion.onInitialSyncCompleted();
  }

  private void moveHead(final boolean withBlockAccessList) {
    final BlockHeader head = header(withBlockAccessList);
    setHead(head);
    chain.onBlockAdded(headAdvancement(head));
  }

  private void setHead(final BlockHeader head) {
    when(blockchain.getChainHeadHeader()).thenReturn(head);
  }

  private BlockHeader header(final boolean withBlockAccessList) {
    return new BlockHeaderTestFixture()
        .number(nextBlock++)
        .balHash(withBlockAccessList ? Hash.EMPTY : null)
        .buildHeader();
  }

  private static BlockAddedEvent headAdvancement(final BlockHeader head) {
    return BlockAddedEvent.createForHeadAdvancement(
        new Block(head, BlockBody.empty()), List.of(), List.of());
  }
}
