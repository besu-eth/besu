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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BPO3;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ForkChangeTrackerTest {

  private static final long OBSERVER = 3L;
  private final Blockchain blockchain = mock(Blockchain.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final List<HardforkId> changes = new ArrayList<>();
  private ForkChangeTracker tracker;

  @BeforeEach
  void setUp() {
    final BlockHeader genesis = header(1, OSAKA);
    when(blockchain.getChainHeadHeader()).thenReturn(genesis);
    when(blockchain.observeBlockAdded(any())).thenReturn(OBSERVER);
    tracker = ForkChangeTracker.create(blockchain, protocolSchedule);
    tracker.subscribe((fork, head) -> changes.add(fork));
  }

  @Test
  void observesTheBlockchain() {
    verify(blockchain).observeBlockAdded(tracker);
  }

  @Test
  void stopsObservingTheBlockchain() {
    tracker.stop();

    verify(blockchain).removeObserver(OBSERVER);
  }

  @Test
  void tellsWhenTheHeadMovesToAnotherFork() {
    moveHead(header(2, OSAKA));
    moveHead(header(3, AMSTERDAM));
    moveHead(header(4, AMSTERDAM));
    moveHead(header(5, BPO3));

    assertThat(changes).containsExactly(AMSTERDAM, BPO3);
  }

  @Test
  void passesTheNewHead() {
    final List<BlockHeader> heads = new ArrayList<>();
    tracker.subscribe((fork, head) -> heads.add(head));
    final BlockHeader head = header(2, AMSTERDAM);

    moveHead(head);

    assertThat(heads).containsExactly(head);
  }

  @Test
  void ignoresBlocksOffTheCanonicalChain() {
    final Block block = new Block(header(2, AMSTERDAM), BlockBody.empty());

    tracker.onBlockAdded(BlockAddedEvent.createForFork(block));
    tracker.onBlockAdded(BlockAddedEvent.createForStoredOnly(block));

    assertThat(changes).isEmpty();
  }

  @Test
  void tellsTheForkOfTheHeadSetWithoutAnEvent() {
    final BlockHeader pivot = header(100, AMSTERDAM);
    when(blockchain.getChainHeadHeader()).thenReturn(pivot);

    assertThat(tracker.currentFork()).isEqualTo(AMSTERDAM);
  }

  @Test
  void stopsTellingOnceUnsubscribed() {
    final List<HardforkId> others = new ArrayList<>();
    final long subscription = tracker.subscribe((fork, head) -> others.add(fork));

    assertThat(tracker.unsubscribe(subscription)).isTrue();
    moveHead(header(2, AMSTERDAM));

    assertThat(others).isEmpty();
    assertThat(changes).containsExactly(AMSTERDAM);
  }

  @Test
  void aFailingListenerDoesNotFailTheImport() {
    final ForkChangeTracker failing = ForkChangeTracker.create(blockchain, protocolSchedule);
    failing.subscribe(
        (fork, head) -> {
          throw new IllegalStateException("listener failed");
        });
    final List<HardforkId> after = new ArrayList<>();
    failing.subscribe((fork, head) -> after.add(fork));

    failing.onBlockAdded(headAdvanced(header(2, AMSTERDAM)));

    assertThat(after).containsExactly(AMSTERDAM);
  }

  private void moveHead(final BlockHeader head) {
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    tracker.onBlockAdded(headAdvanced(head));
  }

  private static BlockAddedEvent headAdvanced(final BlockHeader head) {
    return BlockAddedEvent.createForHeadAdvancement(
        new Block(head, BlockBody.empty()), List.of(), List.of());
  }

  private BlockHeader header(final long number, final HardforkId fork) {
    final BlockHeader header = new BlockHeaderTestFixture().number(number).buildHeader();
    final ProtocolSpec spec = mock(ProtocolSpec.class);
    when(spec.getHardforkId()).thenReturn(fork);
    when(protocolSchedule.getByBlockHeader(header)).thenReturn(spec);
    return header;
  }
}
