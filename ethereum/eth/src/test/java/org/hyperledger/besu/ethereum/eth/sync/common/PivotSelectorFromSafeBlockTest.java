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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PivotSelectorFromSafeBlockTest {

  private static final Hash SAFE_HASH_1 = Hash.fromHexStringLenient("0x1111");
  private static final Hash SAFE_HASH_2 = Hash.fromHexStringLenient("0x2222");
  private static final Hash HEAD_HASH_1 = Hash.fromHexStringLenient("0xaaaa");
  private static final Hash HEAD_HASH_2 = Hash.fromHexStringLenient("0xbbbb");

  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final EthContext ethContext = mock(EthContext.class);
  private final GenesisConfigOptions genesisConfig = mock(GenesisConfigOptions.class);
  private final SingleBlockHeaderDownloader headerDownloader =
      mock(SingleBlockHeaderDownloader.class);
  private final Runnable cleanupAction = mock(Runnable.class);

  private TestClock clock;
  private Optional<ForkchoiceEvent> currentForkchoice;
  private PivotSelectorFromSafeBlock selector;

  @BeforeEach
  void setUp() {
    clock = new TestClock(Instant.parse("2026-01-01T00:00:00Z"));
    currentForkchoice = Optional.empty();
    final Supplier<Optional<ForkchoiceEvent>> supplier = () -> currentForkchoice;
    selector =
        new PivotSelectorFromSafeBlock(
            protocolContext,
            protocolSchedule,
            ethContext,
            genesisConfig,
            supplier,
            cleanupAction,
            headerDownloader,
            clock);
  }

  @Test
  void returnsSafeBlockAsPivotWhenSafeIsFresh() throws Exception {
    final BlockHeader safeHeader = headerWithHash(SAFE_HASH_1, 100L);
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_1)))
        .thenReturn(CompletableFuture.completedFuture(safeHeader));
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_1, SAFE_HASH_1, Hash.ZERO));

    final PivotSyncState state = selector.selectNewPivotBlock().get();

    assertThat(state.getPivotBlockHash()).contains(SAFE_HASH_1);
  }

  @Test
  void returnsHeadBlockAsUntrustedPivotWhenSafeIsStaleButHeadIsFresh() throws Exception {
    // Prime with a fresh forkchoice (safe S1, head H1)
    final BlockHeader safeHeader = headerWithHash(SAFE_HASH_1, 100L);
    final BlockHeader head2Header = headerWithHash(HEAD_HASH_2, 150L);
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_1)))
        .thenReturn(CompletableFuture.completedFuture(safeHeader));
    when(headerDownloader.downloadBlockHeader(eq(HEAD_HASH_2)))
        .thenReturn(CompletableFuture.completedFuture(head2Header));

    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_1, SAFE_HASH_1, Hash.ZERO));
    selector.selectNewPivotBlock().get(); // initial call selects safe

    // 25 minutes pass with no new safe block (> SAFE_PIVOT_FRESHNESS_LIMIT), but head advances to
    // H2
    clock.stepMillis(Duration.ofMinutes(25).toMillis());
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_2, SAFE_HASH_1, Hash.ZERO));

    final PivotSyncState state = selector.selectNewPivotBlock().get();

    assertThat(state.getPivotBlockHash()).contains(HEAD_HASH_2);
  }

  @Test
  void failsWhenBothSafeAndHeadAreStale() throws Exception {
    final BlockHeader safeHeader = headerWithHash(SAFE_HASH_1, 100L);
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_1)))
        .thenReturn(CompletableFuture.completedFuture(safeHeader));
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_1, SAFE_HASH_1, Hash.ZERO));
    selector.selectNewPivotBlock().get(); // priming

    // 25 minutes pass with no FCU change at all (> both SAFE_PIVOT_FRESHNESS_LIMIT and ONE_EPOCH)
    clock.stepMillis(Duration.ofMinutes(25).toMillis());

    final CompletableFuture<PivotSyncState> result = selector.selectNewPivotBlock();
    assertThat(result).isCompletedExceptionally();
  }

  @Test
  void returnsCachedHeadInFallbackUntilHeadAdvances() throws Exception {
    final BlockHeader safeHeader = headerWithHash(SAFE_HASH_1, 100L);
    final BlockHeader head2Header = headerWithHash(HEAD_HASH_2, 150L);
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_1)))
        .thenReturn(CompletableFuture.completedFuture(safeHeader));
    when(headerDownloader.downloadBlockHeader(eq(HEAD_HASH_2)))
        .thenReturn(CompletableFuture.completedFuture(head2Header));

    // priming
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_1, SAFE_HASH_1, Hash.ZERO));
    selector.selectNewPivotBlock().get();

    // safe stale (> SAFE_PIVOT_FRESHNESS_LIMIT), head advances → fallback fires, returns H2
    clock.stepMillis(Duration.ofMinutes(25).toMillis());
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_2, SAFE_HASH_1, Hash.ZERO));
    PivotSyncState first = selector.selectNewPivotBlock().get();
    assertThat(first.getPivotBlockHash()).contains(HEAD_HASH_2);

    // 30s later head still H2, safe still S1 → fallback returns same H2
    clock.stepMillis(Duration.ofSeconds(30).toMillis());
    PivotSyncState second = selector.selectNewPivotBlock().get();
    assertThat(second.getPivotBlockHash()).contains(HEAD_HASH_2);
  }

  @Test
  void returnsToSafeWhenSafeBlockResumes() throws Exception {
    final BlockHeader safe1Header = headerWithHash(SAFE_HASH_1, 100L);
    final BlockHeader safe2Header = headerWithHash(SAFE_HASH_2, 200L);
    final BlockHeader head2Header = headerWithHash(HEAD_HASH_2, 150L);
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_1)))
        .thenReturn(CompletableFuture.completedFuture(safe1Header));
    when(headerDownloader.downloadBlockHeader(eq(SAFE_HASH_2)))
        .thenReturn(CompletableFuture.completedFuture(safe2Header));
    when(headerDownloader.downloadBlockHeader(eq(HEAD_HASH_2)))
        .thenReturn(CompletableFuture.completedFuture(head2Header));

    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_1, SAFE_HASH_1, Hash.ZERO));
    selector.selectNewPivotBlock().get();
    clock.stepMillis(Duration.ofMinutes(25).toMillis());
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_2, SAFE_HASH_1, Hash.ZERO));
    selector
        .selectNewPivotBlock()
        .get(); // enters fallback (safe stale > SAFE_PIVOT_FRESHNESS_LIMIT)

    // A new safe block S2 arrives
    currentForkchoice = Optional.of(new ForkchoiceEvent(HEAD_HASH_2, SAFE_HASH_2, Hash.ZERO));
    PivotSyncState state = selector.selectNewPivotBlock().get();

    assertThat(state.getPivotBlockHash()).contains(SAFE_HASH_2);
  }

  private static BlockHeader headerWithHash(final Hash hash, final long number) {
    // Mock the BlockHeader so getHash() returns the requested hash. The selector wraps the
    // downloaded header in a PivotSyncState, whose pivotBlockHash is derived from the header's
    // own hash — so the header must report SAFE_HASH_1 for the assertion to hold.
    final BlockHeader header = mock(BlockHeader.class);
    when(header.getHash()).thenReturn(hash);
    when(header.getNumber()).thenReturn(number);
    return header;
  }
}
