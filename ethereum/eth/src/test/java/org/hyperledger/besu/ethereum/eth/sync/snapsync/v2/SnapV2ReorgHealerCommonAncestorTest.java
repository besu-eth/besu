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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SnapV2ReorgHealer#findCommonAncestor} using a real reorging {@link
 * org.hyperledger.besu.ethereum.chain.DefaultBlockchain} over in-memory storage.
 */
class SnapV2ReorgHealerCommonAncestorTest {

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
  private final SnapV2ReorgHealer healer =
      new SnapV2ReorgHealer(
          b.blockchain(), unusedStorageCoordinator(), ReorgBlockchainBuilder.balEnabledSchedule());

  private static WorldStateStorageCoordinator unusedStorageCoordinator() {
    return null;
  }

  @Test
  void findsCommonAncestorAtEqualHeightReorg() {
    // Chain: genesis(0) -> block1 -> block2o  [original head]
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2o = b.appendStale(block1.getHeader(), b.emptyBal(), 2L);

    final BlockHeader oldPivot = block2o.getHeader();
    assertThat(b.blockchain().blockIsOnCanonicalChain(oldPivot.getHash())).isTrue();

    // Fork block2c at the same height -> wins the reorg
    final Block block2c = b.appendCanonical(block1.getHeader(), b.emptyBal(), 2L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(oldPivot.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(block2c.getHash())).isTrue();

    final BlockHeader newPivot = block2c.getHeader();
    final BlockHeader ancestor = healer.findCommonAncestor(oldPivot, newPivot);

    assertThat(ancestor.getHash()).isEqualTo(block1.getHash());
    assertThat(ancestor.getNumber()).isEqualTo(1L);
  }

  @Test
  void findsCommonAncestorForDeepFork() {
    // Build a 3-block canonical chain: genesis -> 1 -> 2 -> 3
    final BlockHeader oldPivot = b.appendStaleChain(b.header(0), 1L, 3);
    assertThat(b.blockchain().blockIsOnCanonicalChain(oldPivot.getHash())).isTrue();

    // Fork from block 1, two blocks, out-pacing the original chain.
    final BlockHeader forkBase = b.header(1);
    final Block forkA = b.appendCanonical(forkBase, b.emptyBal(), 2L);
    final Block forkB = b.appendCanonical(forkA.getHeader(), b.emptyBal(), 3L);

    assertThat(b.blockchain().blockIsOnCanonicalChain(oldPivot.getHash())).isFalse();
    assertThat(b.blockchain().blockIsOnCanonicalChain(forkB.getHash())).isTrue();

    final BlockHeader ancestor = healer.findCommonAncestor(oldPivot, forkB.getHeader());
    assertThat(ancestor.getNumber()).isEqualTo(1L);
    assertThat(ancestor.getHash()).isEqualTo(forkBase.getHash());
  }

  @Test
  void returnsOldPivotWhenOldPivotIsAlreadyCanonical() {
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 = b.appendBlockWithBal(block1.getHeader(), b.emptyBal(), 2L);
    final Block block3 = b.appendBlockWithBal(block2.getHeader(), b.emptyBal(), 3L);

    final BlockHeader ancestor = healer.findCommonAncestor(block2.getHeader(), block3.getHeader());

    assertThat(ancestor.getHash()).isEqualTo(block2.getHash());
    assertThat(ancestor.getNumber()).isEqualTo(2L);
  }

  @Test
  void throwsWhenNewPivotIsNotCanonical() {
    // Build chain 0..2, then reorg so block2 becomes orphaned.
    final BlockHeader oldPivot = b.appendStaleChain(b.header(0), 1L, 2);
    final Block fork = b.appendCanonical(b.header(1), b.emptyBal(), 2L);
    // oldPivot is now orphaned; passing it as the *new* pivot must be unrecoverable.
    assertThatThrownBy(() -> healer.findCommonAncestor(fork.getHeader(), oldPivot))
        .isInstanceOf(ReorgUnrecoverableException.class);
  }
}
