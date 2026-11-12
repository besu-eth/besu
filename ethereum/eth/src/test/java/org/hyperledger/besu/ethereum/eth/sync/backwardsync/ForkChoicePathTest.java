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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ForkChoicePathTest {

  private final BlockDataGenerator blockDataGenerator = new BlockDataGenerator();
  private final Map<Hash, BlockHeader> knownHeaders = new HashMap<>();
  private final List<Hash> lookups = new ArrayList<>();
  private ForkChoicePath forkChoicePath;

  private List<BlockHeader> mainBranch;
  private List<BlockHeader> sideBranch;

  @BeforeEach
  void setUp() {
    forkChoicePath =
        new ForkChoicePath(
            hash -> {
              lookups.add(hash);
              return Optional.ofNullable(knownHeaders.get(hash));
            });
    final BlockHeader genesis = blockDataGenerator.header(0);
    mainBranch = branch(genesis, 10);
    sideBranch = branch(mainBranch.get(3), 10);
    knownHeaders.put(genesis.getHash(), genesis);
    mainBranch.forEach(header -> knownHeaders.put(header.getHash(), header));
    sideBranch.forEach(header -> knownHeaders.put(header.getHash(), header));
  }

  @Test
  void containsNothingWithoutForkChoice() {
    assertThat(forkChoicePath.contains(mainBranch.get(5))).isFalse();
  }

  @Test
  void containsTheForkChoiceHeadAndItsAncestors() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(9))).isTrue();
    assertThat(forkChoicePath.contains(mainBranch.get(0))).isTrue();
    assertThat(forkChoicePath.contains(mainBranch.get(5))).isTrue();
  }

  @Test
  void doesNotContainBlocksOfAnotherBranch() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());

    assertThat(forkChoicePath.contains(sideBranch.get(0))).isFalse();
    assertThat(forkChoicePath.contains(sideBranch.get(9))).isFalse();
  }

  @Test
  void doesNotContainDescendantsOfTheForkChoiceHead() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(5).getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(6))).isFalse();
  }

  @Test
  void followsTheForkChoiceToAnotherBranch() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());
    assertThat(forkChoicePath.contains(mainBranch.get(5))).isTrue();

    forkChoicePath.setForkChoiceHead(sideBranch.get(9).getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(5))).isFalse();
    assertThat(forkChoicePath.contains(sideBranch.get(5))).isTrue();
    // the common ancestors stay selected
    assertThat(forkChoicePath.contains(mainBranch.get(2))).isTrue();
  }

  @Test
  void dropsTheBlocksAboveAnOlderForkChoiceHead() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());
    assertThat(forkChoicePath.contains(mainBranch.get(0))).isTrue();

    forkChoicePath.setForkChoiceHead(mainBranch.get(5).getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(7))).isFalse();
    assertThat(forkChoicePath.contains(mainBranch.get(4))).isTrue();
  }

  @Test
  void keepsTheWalkedAncestorsWhenTheForkChoiceAdvances() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(5).getHash());
    assertThat(forkChoicePath.contains(mainBranch.get(0))).isTrue();

    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());
    lookups.clear();

    assertThat(forkChoicePath.contains(mainBranch.get(0))).isTrue();
    // only the new head and the blocks between it and the previous head are looked up
    assertThat(lookups).hasSize(5);
  }

  @Test
  void resumesTheWalkWhenMissingHeadersArrive() {
    final BlockHeader missing = mainBranch.get(6);
    knownHeaders.remove(missing.getHash());
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(2))).isFalse();

    knownHeaders.put(missing.getHash(), missing);

    assertThat(forkChoicePath.contains(mainBranch.get(2))).isTrue();
  }

  @Test
  void containsNothingUntilTheForkChoiceHeadIsKnown() {
    final BlockHeader head = mainBranch.get(9);
    knownHeaders.remove(head.getHash());
    forkChoicePath.setForkChoiceHead(head.getHash());

    assertThat(forkChoicePath.contains(mainBranch.get(5))).isFalse();

    knownHeaders.put(head.getHash(), head);

    assertThat(forkChoicePath.contains(mainBranch.get(5))).isTrue();
  }

  @Test
  void keepsTheForkChoiceHeadOnReset() {
    forkChoicePath.setForkChoiceHead(mainBranch.get(9).getHash());
    assertThat(forkChoicePath.contains(mainBranch.get(5))).isTrue();

    forkChoicePath.reset();

    assertThat(forkChoicePath.contains(mainBranch.get(5))).isTrue();
  }

  private List<BlockHeader> branch(final BlockHeader parent, final int length) {
    final List<BlockHeader> headers = new ArrayList<>(length);
    BlockHeader current = parent;
    for (int i = 0; i < length; i++) {
      current =
          blockDataGenerator
              .block(
                  new BlockDataGenerator.BlockOptions()
                      .setBlockNumber(current.getNumber() + 1)
                      .setParentHash(current.getHash()))
              .getHeader();
      headers.add(current);
    }
    return headers;
  }
}
