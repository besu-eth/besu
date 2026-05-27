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
package org.hyperledger.besu.ethereum.eth.sync.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PivotSelectorFromSafeBlockTest {

  private static final long FCU_TIME_MILLIS = 1_000_000L;
  private static final long SLOT_MILLIS = 12_000L;

  private final GenesisConfigOptions genesisConfig = mock(GenesisConfigOptions.class);
  private final SingleBlockHeaderDownloader headerDownloader =
      mock(SingleBlockHeaderDownloader.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final Clock clock = mock(Clock.class);

  private PivotSelectorFromSafeBlock selector;

  @BeforeEach
  void setUp() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockchain.getChainHeadBlockNumber()).thenReturn(0L);
    when(genesisConfig.getTerminalBlockNumber()).thenReturn(OptionalLong.of(0L));
    when(clock.millis()).thenReturn(FCU_TIME_MILLIS);

    final ProtocolSpec spec = mock(ProtocolSpec.class);
    when(spec.getSlotDuration()).thenReturn(Duration.ofSeconds(12));
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(spec);

    selector =
        new PivotSelectorFromSafeBlock(
            protocolContext, genesisConfig, headerDownloader, protocolSchedule, clock, () -> {});
  }

  // --- selectNewPivotBlock ---

  @Test
  void selectNewPivotBlockFailsWhenNoFcuReceived() {
    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockFailsWhenConsensusClientAppearsOffline() {
    final BlockHeader head = header(1000, Hash.ZERO);
    selector.onNewPayload(head);

    when(clock.millis()).thenReturn(0L);
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    // >93 slots have elapsed since the last FCU — CL appears offline
    when(clock.millis()).thenReturn(93 * SLOT_MILLIS + 1);

    assertThat(selector.selectNewPivotBlock()).isCompletedExceptionally();
  }

  @Test
  void selectNewPivotBlockReturnsSafeBlockWhenFreshEnough() {
    final List<BlockHeader> chain = chain(100, 1); // blocks 1–100
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(99); // block 100
    final BlockHeader safe = chain.get(49); // block 50 — 50 behind head, within 93 threshold
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), safe.getHash(), Hash.ZERO));

    final CompletableFuture<PivotSyncState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    assertThat(result.join().getPivotBlockHeader()).contains(safe);
  }

  @Test
  void selectNewPivotBlockWalksBackFromHeadWhenNoSafeBlock() {
    final List<BlockHeader> chain = chain(50, 1); // blocks 1–50
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(49); // block 50
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final CompletableFuture<PivotSyncState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // head(50) - REORG_SAFETY_DISTANCE(32) = block 18
    assertThat(result.join().getPivotBlockHeader().map(BlockHeader::getNumber)).contains(18L);
  }

  @Test
  void selectNewPivotBlockWalksBackFromHeadWhenSafeBlockTooFarFromHead() {
    final List<BlockHeader> chain = chain(200, 1); // blocks 1–200
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(199); // block 200
    final BlockHeader safe = chain.get(0); // block 1 — 199 blocks behind head, exceeds 93
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), safe.getHash(), Hash.ZERO));

    final CompletableFuture<PivotSyncState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // head(200) - REORG_SAFETY_DISTANCE(32) = block 168
    assertThat(result.join().getPivotBlockHeader().map(BlockHeader::getNumber)).contains(168L);
  }

  @Test
  void selectNewPivotBlockCapsWalkbackAtGenesisWhenHeadIsCloseToGenesis() {
    final List<BlockHeader> chain = chain(10, 0); // blocks 0–9
    chain.forEach(selector::onNewPayload);

    final BlockHeader head = chain.get(9); // block 9 — less than REORG_SAFETY_DISTANCE(32)
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    final CompletableFuture<PivotSyncState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompleted();
    // walks back min(32, 9) = 9 steps → block 0
    assertThat(result.join().getPivotBlockHeader().map(BlockHeader::getNumber)).contains(0L);
  }

  // --- header cache pruning ---

  @Test
  void prunesHeadersBelowFinalizedBlockNumberWhenFinalizedIsInCache() {
    final List<BlockHeader> chain = chain(100, 1); // blocks 1–100
    chain.forEach(selector::onNewPayload);

    final BlockHeader finalized = chain.get(49); // block 50
    final BlockHeader head = chain.get(99); // block 100

    // FCU with known finalized — triggers pruning of blocks < 50
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), head.getHash(), finalized.getHash()));

    // Block 30 (pruned from cache) used as safe: cache miss → headerDownloader must be called
    final BlockHeader block30 = chain.get(29);
    when(headerDownloader.downloadBlockHeader(block30.getHash()))
        .thenReturn(CompletableFuture.completedFuture(block30));

    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), block30.getHash(), finalized.getHash()));
    selector.selectNewPivotBlock().join();

    verify(headerDownloader).downloadBlockHeader(block30.getHash());
  }

  @Test
  void doesNotPruneWhenFinalizedHeaderIsNotInCache() {
    final List<BlockHeader> chain = chain(50, 1); // blocks 1–50
    chain.forEach(selector::onNewPayload);

    // Finalized hash unknown to the cache — pruning must be skipped
    final Hash unknownFinalized = header(999, Hash.ZERO).getHash();
    final BlockHeader head = chain.get(49); // block 50
    final BlockHeader safe = chain.get(24); // block 25

    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), safe.getHash(), unknownFinalized));

    // safe (block 25) must still be in cache — no download should occur
    selector.selectNewPivotBlock().join();

    verify(headerDownloader, never()).downloadBlockHeader(safe.getHash());
  }

  @Test
  void doesNotRepeatPruningForSameFinalizedHash() {
    final List<BlockHeader> chain = chain(100, 1);
    chain.forEach(selector::onNewPayload);

    final BlockHeader finalized = chain.get(49); // block 50
    final BlockHeader head = chain.get(99); // block 100

    // First FCU — pruning fires
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), head.getHash(), finalized.getHash()));

    // Second FCU with the same finalized hash — pruning must NOT fire again
    // (block 60, above finalized, should still be in cache without re-pruning)
    final BlockHeader block60 = chain.get(59);
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), block60.getHash(), finalized.getHash()));

    selector.selectNewPivotBlock().join();

    // block60 is above finalized and was not touched — served from cache, no download
    verify(headerDownloader, never()).downloadBlockHeader(block60.getHash());
  }

  // --- getBestChainHeight ---

  @Test
  void getBestChainHeightReturnsCachedHeadWhenHigherThanLocalChain() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(10L);

    final BlockHeader head = header(500, Hash.ZERO);
    selector.onNewPayload(head);
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    assertThat(selector.getBestChainHeight()).isEqualTo(500L);
  }

  @Test
  void getBestChainHeightReturnsLocalChainHeightWhenHigherThanCachedHead() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(1000L);

    final BlockHeader head = header(500, Hash.ZERO);
    selector.onNewPayload(head);
    selector.onNewUnverifiedForkchoice(fcu(head.getHash(), Hash.ZERO, Hash.ZERO));

    assertThat(selector.getBestChainHeight()).isEqualTo(1000L);
  }

  // --- helpers ---

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
