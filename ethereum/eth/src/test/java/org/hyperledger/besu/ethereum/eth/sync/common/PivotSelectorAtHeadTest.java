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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PivotSelectorAtHeadTest {

  private static final long FCU_TIME_MILLIS = 1_000_000L;
  private static final long SLOT_MILLIS = 12_000L;
  private static final int PIVOT_WINDOW_VALIDITY = 10;
  private static final int MIN_PEERS = 2;

  private final StubGenesisConfigOptions genesisConfig = new StubGenesisConfigOptions();
  private final SingleBlockHeaderDownloader headerDownloader =
      mock(SingleBlockHeaderDownloader.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final EthContext ethContext = mock(EthContext.class);
  private final EthPeers ethPeers = mock(EthPeers.class);
  private final TestClock clock = new TestClock(Instant.ofEpochMilli(FCU_TIME_MILLIS));

  private PivotSelectorAtHead selector;

  @BeforeEach
  void setUp() {
    final ProtocolSpec spec = mock(ProtocolSpec.class);
    when(spec.getSlotDuration()).thenReturn(Duration.ofSeconds(12));
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(spec);

    // Default: no peers available, so only the FCU-anchored branch is reachable.
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    when(ethPeers.streamAvailablePeers()).thenAnswer(inv -> Stream.empty());

    selector = newSelector(PivotSelectorAtHead.DEFAULT_BLOCKS_BEHIND_HEAD);
  }

  private PivotSelectorAtHead newSelector(final long blocksBehindHead) {
    return new PivotSelectorAtHead(
        protocolContext,
        genesisConfig,
        headerDownloader,
        protocolSchedule,
        ethContext,
        MIN_PEERS,
        clock,
        PIVOT_WINDOW_VALIDITY,
        () -> {},
        blocksBehindHead);
  }

  // --- constructor validation ---

  @Test
  void constructorRejectsOutOfRangeBlocksBehindHead() {
    assertThatThrownBy(() -> newSelector(-1L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> newSelector((long) Integer.MAX_VALUE + 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- selectNewPivotBlock: FCU-anchored branch ---

  @Test
  void selectNewPivotBlockFailsWhenNoFcuReceived() {
    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockReturnsPivotOneBlockBehindHeadByDefault() {
    final List<BlockHeader> chain = chain(100, 1); // blocks 1–100
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(99); // block 100
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // head(100) - DEFAULT_BLOCKS_BEHIND_HEAD(1) = block 99
    assertThat(result.join().getPivotBlockHeader().map(BlockHeader::getNumber)).contains(99L);
  }

  @Test
  void selectNewPivotBlockReusesExistingPivotWhenHeadHasNotAdvanced() {
    final List<BlockHeader> chain = chain(100, 1); // blocks 1–100
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(99); // block 100
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final SnapSyncProcessState first = selector.selectNewPivotBlock().join();
    assertThat(first.getPivotBlockHeader().map(BlockHeader::getNumber)).contains(99L);

    // FCU again with the same head — pivot must be reused
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final SnapSyncProcessState second = selector.selectNewPivotBlock().join();
    assertThat(second.getPivotBlockHeader().map(BlockHeader::getNumber)).contains(99L);
    verify(headerDownloader, never()).downloadBlockHeader(any());
  }

  @Test
  void selectNewPivotBlockReusesExistingPivotWhenHeadHasAdvancedSmallAmount() {
    final List<BlockHeader> chain = chain(200, 1);
    chain.forEach(selector::onNewPayload);

    final BlockHeader head100 = chain.get(99); // block 100
    selector.onNewUnverifiedForkchoice(fcu(head100.getHash(), Hash.ZERO, Hash.ZERO));
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(99L);

    // Head advances 5 blocks — still within the reuse window (< PIVOT_WINDOW_VALIDITY).
    final BlockHeader head105 = chain.get(104); // block 105
    selector.onNewUnverifiedForkchoice(fcu(head105.getHash(), Hash.ZERO, Hash.ZERO));

    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(99L);
    verify(headerDownloader, never()).downloadBlockHeader(any());
  }

  @Test
  void selectNewPivotBlockAdvancesPivotWhenHeadMovesBeyondReuseWindow() {
    final List<BlockHeader> chain = chain(200, 1); // blocks 1–200
    chain.forEach(selector::onNewPayload);

    final BlockHeader head100 = chain.get(99); // block 100
    selector.onNewUnverifiedForkchoice(fcu(head100.getHash(), Hash.ZERO, Hash.ZERO));

    final SnapSyncProcessState first = selector.selectNewPivotBlock().join();
    assertThat(first.getPivotBlockHeader().map(BlockHeader::getNumber)).contains(99L);

    // Advance head past the reuse window (< PIVOT_WINDOW_VALIDITY) → pivot refreshes to head−1.
    final BlockHeader head115 = chain.get(114); // block 115
    selector.onNewUnverifiedForkchoice(fcu(head115.getHash(), Hash.ZERO, Hash.ZERO));

    final SnapSyncProcessState second = selector.selectNewPivotBlock().join();
    assertThat(second.getPivotBlockHeader().map(BlockHeader::getNumber)).contains(114L);
  }

  @Test
  void selectNewPivotBlockUsesHeadAsPivotWhenHeadIsNearGenesis() {
    final List<BlockHeader> chain = chain(10, 0); // blocks 0–9
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(0); // block 0
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // head(0) - 1 would be negative — fall back to the head itself
    assertThat(result.join().getPivotBlockHeader()).contains(head);
  }

  @Test
  void selectNewPivotBlockFailsWhenConsensusClientAppearsOffline() {
    final BlockHeader head = header(1000, Hash.ZERO);
    selector.onNewPayload(head);

    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    // >pivotBlockWindowValidity slots have elapsed since the last FCU — CL appears offline
    clock.stepMillis(PIVOT_WINDOW_VALIDITY * SLOT_MILLIS + 1);

    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockDownloadsHeadFromPeersWhenNotCached() {
    final BlockHeader head = header(500, Hash.ZERO);
    when(headerDownloader.downloadBlockHeader(head.getHash()))
        .thenReturn(CompletableFuture.completedFuture(head));
    // Do NOT call onNewPayload — the head header must come from peers. Use blocksBehindHead=0 so
    // the pivot is the head itself and no parent walk-back is needed.
    final PivotSelectorAtHead headSelector = newSelector(0L);

    headSelector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final CompletableFuture<SnapSyncProcessState> result = headSelector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    assertThat(result.join().getPivotBlockHeader()).contains(head);
    verify(headerDownloader).downloadBlockHeader(head.getHash());
  }

  @Test
  void selectNewPivotBlockOfflineCheckTakesPrecedenceOverReuse() {
    final List<BlockHeader> chain = chain(100, 1); // blocks 1–100
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(99); // block 100
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));
    // First selection succeeds and pins lastReturnedPivot at block 99.
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(99L);

    // The head has NOT advanced, so the reuse branch would normally short-circuit. But enough
    // time passes with no new FCU that the CL-offline guard must fail first.
    clock.stepMillis(PIVOT_WINDOW_VALIDITY * SLOT_MILLIS + 1);

    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  // --- selection when the pivot lags far behind head ---

  @Test
  void selectNewPivotBlockRefreshesToHeadWhenPivotLagsBeyondReuseWindow() {
    final List<BlockHeader> chain = chain(50, 1); // blocks 1–50
    chain.forEach(selector::onNewPayload);

    final BlockHeader earlyHead = chain.get(4); // block 5
    selector.onNewUnverifiedForkchoice(fcu(earlyHead.getHash(), Hash.ZERO, Hash.ZERO));
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(4L);

    // Head jumps to block 50 — well past the reuse window. The pivot refreshes to head−1.
    final BlockHeader newHead = chain.get(49); // block 50
    selector.onNewUnverifiedForkchoice(fcu(newHead.getHash(), Hash.ZERO, Hash.ZERO));

    final SnapSyncProcessState result = selector.selectNewPivotBlock().join();
    assertThat(result.getPivotBlockHeader().map(BlockHeader::getNumber)).contains(49L);
  }

  // --- selectNewPivotBlock: peer-anchored branch ---

  @Test
  void selectNewPivotBlockUsesPeerHeightWhenNoFcuReceived() {
    withPeers(500, 500);

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // peerBest(500) - DEFAULT_BLOCKS_BEHIND_HEAD(1) = 499, header resolved downstream
    assertThat(result.join().getPivotBlockNumber()).hasValue(499L);
    assertThat(result.join().getPivotBlockHeader()).isEmpty();
  }

  @Test
  void selectNewPivotBlockFailsWhenNoFcuAndInsufficientPeers() {
    withPeers(500); // 1 < MIN_PEERS(2)

    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockFailsWhenNoFcuAndPeerPivotWouldBeAtGenesis() {
    withPeers(1, 1); // pivot would be 1 - 1 = 0

    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockUsesPeerHeightWhenFcuHeadLagsBeyondThreshold() {
    final List<BlockHeader> chain = chain(200, 1); // blocks 1–200
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(199); // block 200
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    // Peers are 300 ahead of the FCU head — beyond PEER_HEAD_LAG_THRESHOLD(100)
    withPeers(500, 500);

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    assertThat(result.join().getPivotBlockNumber()).hasValue(499L);
    assertThat(result.join().getPivotBlockHeader()).isEmpty();
  }

  @Test
  void selectNewPivotBlockUsesFcuHeadWhenPeerLagIsWithinThreshold() {
    final List<BlockHeader> chain = chain(500, 1); // blocks 1–500
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(449); // block 450
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    // Peers are only 50 ahead of the FCU head — within PEER_HEAD_LAG_THRESHOLD(100)
    withPeers(500, 500);

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // FCU-anchored: head(450) - 1 = block 449, header-based
    assertThat(result.join().getPivotBlockHeader().map(BlockHeader::getNumber)).contains(449L);
  }

  @Test
  void selectNewPivotBlockPeerBranchReusesPivotWhenPeersHaveNotAdvancedFar() {
    withPeers(500, 500);
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(499L);

    // Peers advance by 5 blocks — still within the reuse window (< PIVOT_WINDOW_VALIDITY).
    withPeers(505, 505);
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(499L);
  }

  @Test
  void selectNewPivotBlockPeerBranchDoesNotMovePivotBackwards() {
    withPeers(500, 500);
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(499L);

    // Peer estimates drop below the last returned pivot — the pivot must not regress.
    withPeers(400, 400);
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(499L);
  }

  @Test
  void selectNewPivotBlockDoesNotMoveBackwardsWhenSwitchingFromPeerToFcuBranch() {
    withPeers(500, 500);
    assertThat(selector.selectNewPivotBlock().join().getPivotBlockNumber()).hasValue(499L);

    // FCU arrives with a head that lags the peer-derived pivot; peers are within the threshold,
    // so the FCU branch runs — but it must reuse the higher pivot instead of selecting 449.
    final List<BlockHeader> chain = chain(450, 1); // blocks 1–450
    chain.forEach(selector::onNewPayload);
    final BlockHeader head = chain.get(449); // block 450
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));
    withPeers(490, 490); // lag = 490 - 450 = 40 <= threshold

    final SnapSyncProcessState result = selector.selectNewPivotBlock().join();
    assertThat(result.getPivotBlockNumber()).hasValue(499L);
  }

  @Test
  void selectNewPivotBlockOfflineGuardDoesNotFailWhenPeersAreAhead() {
    final BlockHeader head = header(1000, Hash.ZERO);
    selector.onNewPayload(head);

    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    // Enough time passes that the CL-offline guard would fail the FCU branch, but the FCU head
    // lags the peers far beyond the threshold, so the peer branch takes over instead.
    clock.stepMillis(PIVOT_WINDOW_VALIDITY * SLOT_MILLIS + 1);
    withPeers(2000, 2000);

    final CompletableFuture<SnapSyncProcessState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    assertThat(result.join().getPivotBlockNumber()).hasValue(1999L);
  }

  // --- getBestChainHeight ---

  @Test
  void getBestChainHeightReturnsMaxOfLocalCachedAndPeerHeight() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(10L);

    final BlockHeader head = header(500, Hash.ZERO);
    selector.onNewPayload(head);
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));
    // Cached FCU head beats the local chain height.
    assertThat(selector.getBestChainHeight()).isEqualTo(500L);

    // A higher peer estimate beats both.
    withPeers(800, 800);
    assertThat(selector.getBestChainHeight()).isEqualTo(800L);

    // A higher local chain head beats both.
    when(blockchain.getChainHeadBlockNumber()).thenReturn(1000L);
    assertThat(selector.getBestChainHeight()).isEqualTo(1000L);
  }

  // --- helpers ---

  private void withPeers(final long... heights) {
    final List<EthPeerImmutableAttributes> peers =
        Arrays.stream(heights).mapToObj(PivotSelectorAtHeadTest::peerAtHeight).toList();
    when(ethPeers.streamAvailablePeers()).thenAnswer(inv -> peers.stream());
  }

  private static EthPeerImmutableAttributes peerAtHeight(final long chainHeight) {
    return new EthPeerImmutableAttributes(
        UInt256.ZERO,
        true,
        chainHeight,
        0,
        0,
        0L,
        false,
        true,
        false,
        true,
        false,
        mock(EthPeer.class));
  }

  private static BlockHeader header(final long number, final Hash parentHash) {
    return new BlockHeaderTestFixture().number(number).parentHash(parentHash).buildHeader();
  }

  /** Builds a linked chain of {@code length} headers starting at block {@code startNumber}. */
  private static List<BlockHeader> chain(final int length, final long startNumber) {
    final List<BlockHeader> result = new ArrayList<>();
    Hash parentHash = Hash.ZERO;
    for (int i = 0; i < length; i++) {
      final BlockHeader h = header(startNumber + i, parentHash);
      result.add(h);
      parentHash = h.getHash();
    }
    return result;
  }

  private static ForkchoiceEvent fcu(final Hash head, final Hash safe, final Hash finalized) {
    return new ForkchoiceEvent(head, safe, finalized);
  }
}
