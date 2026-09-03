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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * Covers the ancestry walk in {@code buildHeaders}. The execution-spec reference tests cannot reach
 * this: their fixtures build a single linear chain, so canonical-by-height and true ancestry return
 * the same headers and a regression here would pass unnoticed.
 */
class BonsaiExecutionWitnessBuilderTest {

  private final Blockchain blockchain = mock(Blockchain.class);
  private final BonsaiExecutionWitnessBuilder builder =
      new BonsaiExecutionWitnessBuilder(mock(PathBasedWorldStateProvider.class), blockchain);

  /** Chains headers 0..count-1, each pointing at the previous, tagged so forks differ by hash. */
  private List<BlockHeader> chain(final int count, final long extraData) {
    final List<BlockHeader> headers = new ArrayList<>();
    Hash parent = Hash.ZERO;
    for (int i = 0; i < count; i++) {
      final BlockHeader h =
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(parent)
              .gasLimit(30_000_000L + extraData)
              .buildHeader();
      headers.add(h);
      parent = h.getHash();
    }
    return headers;
  }

  private void registerByHash(final List<BlockHeader> headers) {
    headers.forEach(h -> when(blockchain.getBlockHeader(h.getHash())).thenReturn(Optional.of(h)));
  }

  private static String rlp(final BlockHeader header) {
    return RLP.encode(header::writeTo).toHexString();
  }

  @Test
  void headersFollowTheBlockOwnAncestryNotTheCanonicalChain() {
    // Canonical chain 0..4, and a fork that diverges after block 2 with different hashes at 3 and
    // 4.
    final List<BlockHeader> canonical = chain(5, 0);
    final List<BlockHeader> fork = new ArrayList<>(canonical.subList(0, 3));
    for (int i = 3; i < 5; i++) {
      fork.add(
          new BlockHeaderTestFixture()
              .number(i)
              .parentHash(fork.get(i - 1).getHash())
              .gasLimit(31_000_000L) // differs from canonical, so the hash differs too
              .buildHeader());
    }
    registerByHash(canonical);
    registerByHash(fork);
    // Height lookups resolve on the canonical chain - what the buggy implementation used.
    canonical.forEach(
        h -> when(blockchain.getBlockHeader(h.getNumber())).thenReturn(Optional.of(h)));

    assertThat(fork.get(3).getHash()).isNotEqualTo(canonical.get(3).getHash());

    // Witness for the fork's block 4: ancestors 1..3 must come from the fork, not the canonical
    // chain.
    final List<String> headers = builder.buildHeaders(1L, fork.get(4));

    assertThat(headers)
        .as("ancestors must be the block's own, ascending by number")
        .containsExactly(rlp(fork.get(1)), rlp(fork.get(2)), rlp(fork.get(3)));
    assertThat(headers).doesNotContain(rlp(canonical.get(3)));
    verify(blockchain, never()).getBlockHeader(anyLong());
  }

  @Test
  void headersAreAscendingAndInclusiveOfTheOldestAncestor() {
    final List<BlockHeader> headers = chain(6, 0);
    registerByHash(headers);

    final List<String> result = builder.buildHeaders(2L, headers.get(5));

    assertThat(result)
        .containsExactly(rlp(headers.get(2)), rlp(headers.get(3)), rlp(headers.get(4)));
  }

  @Test
  void walkStopsAtGenesisWhenTheOldestAncestorReachesBelowIt() {
    final List<BlockHeader> headers = chain(3, 0);
    registerByHash(headers);

    // oldestAncestor below genesis must terminate rather than walk past block 0.
    final List<String> result = builder.buildHeaders(0L, headers.get(2));

    assertThat(result).containsExactly(rlp(headers.get(0)), rlp(headers.get(1)));
  }

  @Test
  void parentOnlyWitnessReturnsASingleHeader() {
    final List<BlockHeader> headers = chain(4, 0);
    registerByHash(headers);

    final List<String> result = builder.buildHeaders(2L, headers.get(3));

    assertThat(result).containsExactly(rlp(headers.get(2)));
  }
}
