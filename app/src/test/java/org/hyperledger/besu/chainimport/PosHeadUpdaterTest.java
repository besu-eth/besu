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
package org.hyperledger.besu.chainimport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The fork choice update {@link PosHeadUpdater} builds. Blocks: genesis, finalized (child of
 * genesis), head1 and head2 (children of finalized, then head1), and oldSafe, a child of genesis on
 * another branch.
 */
public class PosHeadUpdaterTest {
  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final MergeMiningCoordinator coordinator = mock(MergeMiningCoordinator.class);
  private final PosHeadUpdater posHead = new PosHeadUpdater(blockchain, coordinator);

  private final BlockHeader genesis = header(0, Hash.ZERO, "genesis");
  private final BlockHeader finalized = header(1, genesis.getHash(), "finalized");
  private final BlockHeader head1 = header(2, finalized.getHash(), "head1");
  private final BlockHeader head2 = header(3, head1.getHash(), "head2");
  private final BlockHeader oldSafe = header(1, genesis.getHash(), "oldSafe");

  @BeforeEach
  public void setUp() {
    lenient().when(blockchain.getGenesisBlock()).thenReturn(new Block(genesis, BlockBody.empty()));
    for (final BlockHeader header : new BlockHeader[] {genesis, finalized, head1, head2, oldSafe}) {
      lenient().when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.of(header));
    }
    lenient().when(blockchain.getFinalized()).thenReturn(Optional.empty());
    lenient().when(blockchain.getSafeBlock()).thenReturn(Optional.empty());
    lenient()
        .when(coordinator.updateForkChoice(any(), any(), any()))
        .thenAnswer(
            invocation ->
                ForkchoiceResult.withResult(
                    Optional.empty(), Optional.of(invocation.<BlockHeader>getArgument(0))));
  }

  @Test
  public void safeBlockMovesBackToFinalizedWhenHeadDoesNotDescendFromIt() {
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalized.getHash()));
    when(blockchain.getSafeBlock()).thenReturn(Optional.of(oldSafe.getHash()));
    when(coordinator.isDescendantOf(finalized, head1)).thenReturn(true);
    when(coordinator.isDescendantOf(oldSafe, head1)).thenReturn(false);

    assertThat(posHead.makeHead(head1)).isTrue();
    verify(coordinator).updateForkChoice(head1, Hash.ZERO, finalized.getHash());
  }

  @Test
  public void safeBlockMovesBackToGenesisWithoutFinalizedBlock() {
    when(blockchain.getSafeBlock()).thenReturn(Optional.of(oldSafe.getHash()));
    when(coordinator.isDescendantOf(oldSafe, head1)).thenReturn(false);

    assertThat(posHead.makeHead(head1)).isTrue();
    verify(coordinator).updateForkChoice(head1, Hash.ZERO, genesis.getHash());
  }

  @Test
  public void safeBlockStaysWhenHeadDescendsFromIt() {
    when(blockchain.getSafeBlock()).thenReturn(Optional.of(finalized.getHash()));
    when(coordinator.isDescendantOf(finalized, head1)).thenReturn(true);

    assertThat(posHead.makeHead(head1)).isTrue();
    verify(coordinator).updateForkChoice(head1, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void blockNotDescendingFromFinalizedStaysSideBlock() {
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalized.getHash()));
    when(coordinator.isDescendantOf(finalized, oldSafe)).thenReturn(false);

    assertThat(posHead.makeHead(oldSafe)).isFalse();
    verify(coordinator, never()).updateForkChoice(any(), any(), any());
  }

  @Test
  public void childIsAnsweredFromParentCheck() {
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalized.getHash()));
    when(coordinator.isDescendantOf(finalized, head1)).thenReturn(true);

    assertThat(posHead.makeHead(head1)).isTrue();
    assertThat(posHead.makeHead(head2)).isTrue();
    verify(coordinator, times(1)).isDescendantOf(any(), any());
    verify(coordinator).updateForkChoice(head2, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void blockWhoseParentWasNotLastCheckedWalksBack() {
    when(blockchain.getFinalized()).thenReturn(Optional.of(genesis.getHash()));
    when(coordinator.isDescendantOf(genesis, head1)).thenReturn(true);
    when(coordinator.isDescendantOf(genesis, oldSafe)).thenReturn(true);

    assertThat(posHead.makeHead(head1)).isTrue();
    assertThat(posHead.makeHead(oldSafe)).isTrue();
    verify(coordinator).isDescendantOf(genesis, oldSafe);
  }

  @Test
  public void finalizedBlockDescendsFromItself() {
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalized.getHash()));

    assertThat(posHead.makeHead(finalized)).isTrue();
    verify(coordinator, never()).isDescendantOf(any(), any());
    verify(coordinator).updateForkChoice(finalized, Hash.ZERO, Hash.ZERO);
  }

  @Test
  public void failedForkChoiceUpdateThrows() {
    when(coordinator.updateForkChoice(head1, Hash.ZERO, Hash.ZERO))
        .thenReturn(
            ForkchoiceResult.withFailure(
                ForkchoiceResult.Status.INTERNAL_ERROR,
                "Failed to set new head",
                Optional.empty()));

    assertThatThrownBy(() -> posHead.makeHead(head1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Unable to make block 2 the chain head: Failed to set new head");
  }

  private static BlockHeader header(final long number, final Hash parent, final String name) {
    return new BlockHeaderTestFixture()
        .number(number)
        .parentHash(parent)
        .extraData(Bytes.wrap(name.getBytes(UTF_8)))
        .buildHeader();
  }
}
