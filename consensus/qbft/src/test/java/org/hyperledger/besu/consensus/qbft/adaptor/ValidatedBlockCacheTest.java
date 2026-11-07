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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class ValidatedBlockCacheTest {
  private final BlockDataGenerator generator = new BlockDataGenerator();
  private final Block block = generator.block();
  private final long blockNumber = block.getHeader().getNumber();
  private final Hash proposal = generator.hash();
  private final ValidatedBlockCache cache = new ValidatedBlockCache();

  @Test
  void ownsClaimedBlockUntilReleased() {
    assertThat(cache.isOwnedByConsensus(block.getHash())).isFalse();

    cache.claim(block.getHash(), proposal, blockNumber);
    assertThat(cache.isOwnedByConsensus(block.getHash())).isTrue();

    cache.release(block.getHash(), proposal);
    assertThat(cache.isOwnedByConsensus(block.getHash())).isFalse();
  }

  @Test
  void keepsBlockClaimedWhileAProposalOfAnotherRoundClaimsIt() {
    // the proposals of rounds 0 and 2 of the same block share its on-chain hash
    final Hash round0 = generator.hash();
    final Hash round2 = generator.hash();
    final List<Hash> released = new ArrayList<>();
    cache.setReleaseListener(released::add);
    cache.claim(block.getHash(), round0, blockNumber);
    cache.claim(block.getHash(), round2, blockNumber);

    cache.release(block.getHash(), round0);
    assertThat(cache.isOwnedByConsensus(block.getHash())).isTrue();
    assertThat(released).isEmpty();

    cache.release(block.getHash(), round2);
    assertThat(cache.isOwnedByConsensus(block.getHash())).isFalse();
    assertThat(released).containsExactly(block.getHash());
  }

  @Test
  void outputsAloneDoNotMakeConsensusOwnTheBlock() {
    // outputs outlive the round that produced them, ownership must not
    cache.put(block.getHash(), validatedBlock());

    assertThat(cache.isOwnedByConsensus(block.getHash())).isFalse();
  }

  @Test
  void keepsClaimWhenOutputsAreRecordedOrTaken() {
    cache.claim(block.getHash(), proposal, blockNumber);

    cache.put(block.getHash(), validatedBlock());
    assertThat(cache.isOwnedByConsensus(block.getHash())).isTrue();

    // the round releases its claim once the import attempt is over, not when it starts
    assertThat(cache.take(block.getHash(), blockNumber)).isPresent();
    assertThat(cache.isOwnedByConsensus(block.getHash())).isTrue();
  }

  @Test
  void claimDropsClaimsOfLowerHeights() {
    final Block nextBlock = generator.block();
    cache.claim(block.getHash(), proposal, blockNumber);

    cache.claim(nextBlock.getHash(), generator.hash(), blockNumber + 1);

    assertThat(cache.isOwnedByConsensus(block.getHash())).isFalse();
    assertThat(cache.isOwnedByConsensus(nextBlock.getHash())).isTrue();
  }

  @Test
  void tellsReleaseListenerAboutReleasedClaimsOnly() {
    final List<Hash> released = new ArrayList<>();
    cache.setReleaseListener(released::add);

    cache.release(block.getHash(), proposal);
    cache.claim(block.getHash(), proposal, blockNumber);
    cache.release(block.getHash(), proposal);
    cache.release(block.getHash(), proposal);

    assertThat(released).containsExactly(block.getHash());
  }

  private ValidatedBlockCache.ValidatedBlock validatedBlock() {
    return new ValidatedBlockCache.ValidatedBlock(
        blockNumber, block.getBody(), List.of(), Optional.empty());
  }
}
