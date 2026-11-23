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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Snap2AdvertiserTest {

  private static final int SERVED_BLOCKS = 128;
  private static final long CHAIN_SUBSCRIPTION = 5L;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final SnapProtocolManager snapProtocolManager = mock(SnapProtocolManager.class);
  private final SyncState syncState = mock(SyncState.class);
  private final Map<Long, BlockHeader> canonical = new HashMap<>();
  private final Set<Hash> storedAccessLists = new HashSet<>();
  private BlockHeader head;
  private BlockAddedObserver chain;

  @BeforeEach
  void setUp() {
    final BlockHeader genesis = new BlockHeaderTestFixture().number(0).buildHeader();
    canonical.put(0L, genesis);
    setHead(genesis);
    when(blockchain.getBlockHeader(anyLong()))
        .thenAnswer(
            invocation -> Optional.ofNullable(canonical.get(invocation.<Long>getArgument(0))));
    when(blockchain.getBlockAccessListRlp(any()))
        .thenAnswer(
            invocation ->
                storedAccessLists.contains(invocation.<Hash>getArgument(0))
                    ? Optional.of(Bytes.EMPTY)
                    : Optional.empty());
    when(snapProtocolManager.advertiseSnap2()).thenReturn(true);
    when(snapProtocolManager.isServing()).thenReturn(true);
    when(blockchain.observeBlockAdded(any()))
        .thenAnswer(
            invocation -> {
              chain = invocation.getArgument(0);
              return CHAIN_SUBSCRIPTION;
            });
  }

  @Test
  void advertisesOnTheFirstBlockAfterTheSync() {
    start();
    execute(SERVED_BLOCKS);
    syncDone();
    verify(snapProtocolManager, never()).advertiseSnap2();

    execute(1);

    verify(snapProtocolManager).advertiseSnap2();
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void advertisesOnceItHoldsTheBlockAccessListsOfTheLast128Blocks() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    start();
    executeBeforeTheFork(10);
    execute(SERVED_BLOCKS - 1);
    verify(snapProtocolManager, never()).advertiseSnap2();

    execute(1);
    execute(1);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void waitsForBlocksExecutedPastThoseTheSyncDownloaded() {
    start();
    download(100);
    syncDone();
    execute(SERVED_BLOCKS - 1);
    verify(snapProtocolManager, never()).advertiseSnap2();

    execute(1);

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void waitsForTheSnapServer() {
    when(snapProtocolManager.isServing()).thenReturn(false);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    start();
    execute(SERVED_BLOCKS);
    verify(snapProtocolManager, never()).advertiseSnap2();

    when(snapProtocolManager.isServing()).thenReturn(true);
    execute(1);

    verify(snapProtocolManager).advertiseSnap2();
  }

  @Test
  void advertisesAtOnceWhenSyncedWithTheBlockAccessLists() {
    execute(SERVED_BLOCKS);
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);

    start();

    verify(snapProtocolManager).advertiseSnap2();
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void waitsWithoutReadingWhileTheSyncedChainIsBeforeTheFork() {
    syncDone();
    start();

    executeBeforeTheFork(200);

    verify(snapProtocolManager, never()).advertiseSnap2();
    verify(blockchain, never()).getBlockHeader(anyLong());
  }

  @Test
  void ignoresABlockOffTheCanonicalChain() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    start();
    execute(SERVED_BLOCKS - 1);

    final BlockHeader forkBlock = block(true);
    storedAccessLists.add(forkBlock.getHash());
    chain.onBlockAdded(BlockAddedEvent.createForFork(new Block(forkBlock, BlockBody.empty())));

    verify(snapProtocolManager, never()).advertiseSnap2();
  }

  @Test
  void survivesAFailedAdvertisement() {
    when(snapProtocolManager.advertiseSnap2()).thenThrow(new IllegalStateException("no network"));
    start();
    execute(SERVED_BLOCKS);

    syncDone();
    execute(1);
    execute(1);

    verify(snapProtocolManager, times(1)).advertiseSnap2();
    verify(blockchain).removeObserver(CHAIN_SUBSCRIPTION);
  }

  @Test
  void triesTheNextHeadWhenReadingTheBlockAccessListsFails() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
    start();
    execute(SERVED_BLOCKS - 1);
    doThrow(new IllegalStateException("closed")).when(blockchain).getBlockAccessListRlp(any());

    assertThatCode(() -> execute(1)).doesNotThrowAnyException();
    verify(snapProtocolManager, never()).advertiseSnap2();

    doReturn(Optional.of(Bytes.EMPTY)).when(blockchain).getBlockAccessListRlp(any());
    execute(1);

    verify(snapProtocolManager).advertiseSnap2();
  }

  private Snap2Advertiser start() {
    return Snap2Advertiser.start(snapProtocolManager, syncState, blockchain);
  }

  private void syncDone() {
    when(syncState.isInitialSyncPhaseDone()).thenReturn(true);
  }

  // blocks the node executed past the fork, which stores their block access lists
  private void execute(final int count) {
    for (int i = 0; i < count; i++) {
      final BlockHeader block = block(true);
      storedAccessLists.add(block.getHash());
      moveHead(block);
    }
  }

  private void executeBeforeTheFork(final int count) {
    for (int i = 0; i < count; i++) {
      moveHead(block(false));
    }
  }

  // blocks the SNAP sync downloaded past the fork, without their block access lists or an event
  private void download(final int count) {
    for (int i = 0; i < count; i++) {
      final BlockHeader block = block(true);
      canonical.put(block.getNumber(), block);
      setHead(block);
    }
  }

  private void moveHead(final BlockHeader block) {
    canonical.put(block.getNumber(), block);
    setHead(block);
    if (chain != null) {
      chain.onBlockAdded(headAdvancement(block));
    }
  }

  private void setHead(final BlockHeader block) {
    head = block;
    when(blockchain.getChainHeadHeader()).thenReturn(block);
  }

  private BlockHeader block(final boolean pastTheFork) {
    return new BlockHeaderTestFixture()
        .number(head.getNumber() + 1)
        .parentHash(head.getHash())
        .balHash(pastTheFork ? Hash.EMPTY : null)
        .buildHeader();
  }

  private static BlockAddedEvent headAdvancement(final BlockHeader head) {
    return BlockAddedEvent.createForHeadAdvancement(
        new Block(head, BlockBody.empty()), List.of(), List.of());
  }
}
