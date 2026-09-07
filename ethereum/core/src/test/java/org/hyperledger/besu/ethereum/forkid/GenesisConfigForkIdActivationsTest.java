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
package org.hyperledger.besu.ethereum.forkid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.mockBlockchain;

import org.hyperledger.besu.config.ForkIdActivations;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.GenesisHash;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Proves the fork activations carried by {@link GenesisConfig#withAdditionalForkIdActivations}
 * reach the EIP-2124 fork ID a {@link ForkIdManager} computes, exactly as the genesis-declared
 * forks do.
 */
public class GenesisConfigForkIdActivationsTest {

  // beyond mainnet's last block fork (Gray Glacier, 15_050_000) but ordered before its
  // timestamp forks, as EIP-6122 requires of any block-number activation
  private static final long EXTRA_BLOCK_FORK = 20_000_000L;
  // beyond mainnet's last genesis-declared timestamp fork
  private static final long EXTRA_TIMESTAMP_FORK = 1_900_000_000L;

  @Test
  public void additionalActivationsBecomeForkIdBoundaries() {
    final List<ForkId> stock = allForkIds(GenesisConfig.mainnet().getConfigOptions());
    final List<ForkId> extended =
        allForkIds(
            GenesisConfig.mainnet()
                .withAdditionalForkIdActivations(
                    new ForkIdActivations(List.of(EXTRA_BLOCK_FORK), List.of(EXTRA_TIMESTAMP_FORK)))
                .getConfigOptions());

    assertThat(stock).noneMatch(forkId -> forkId.getNext() == EXTRA_BLOCK_FORK);
    assertThat(stock).noneMatch(forkId -> forkId.getNext() == EXTRA_TIMESTAMP_FORK);
    assertThat(extended).anyMatch(forkId -> forkId.getNext() == EXTRA_BLOCK_FORK);
    assertThat(extended).anyMatch(forkId -> forkId.getNext() == EXTRA_TIMESTAMP_FORK);
    assertThat(extended).hasSize(stock.size() + 2);
  }

  @Test
  public void additionalActivationChangesTheAdvertisedForkNext() {
    // a chain head past every mainnet block fork, before every timestamp fork
    final long head = 16_000_000L;
    final long headTimestamp = 0L;

    final ForkId stock =
        forkIdManager(GenesisConfig.mainnet().getConfigOptions(), head, headTimestamp)
            .getForkIdForChainHead();
    final ForkId extended =
        forkIdManager(
                GenesisConfig.mainnet()
                    .withAdditionalForkIdActivations(
                        new ForkIdActivations(List.of(EXTRA_BLOCK_FORK), List.of()))
                    .getConfigOptions(),
                head,
                headTimestamp)
            .getForkIdForChainHead();

    // EIP-2124 FORK_NEXT: the first fork not yet crossed becomes the out-of-band activation
    assertThat(extended.getNext()).isEqualTo(EXTRA_BLOCK_FORK);
    assertThat(stock.getNext()).isNotEqualTo(EXTRA_BLOCK_FORK);
    // the crossed-forks hash is unchanged: the extra activation lies ahead of the head
    assertThat(extended.getHash()).isEqualTo(stock.getHash());
  }

  private static List<ForkId> allForkIds(final GenesisConfigOptions options) {
    return forkIdManager(options, 0L, 0L).getAllForkIds();
  }

  private static ForkIdManager forkIdManager(
      final GenesisConfigOptions options, final long chainHeight, final long timestamp) {
    return new ForkIdManager(
        mockBlockchain(GenesisHash.MAINNET, chainHeight, timestamp),
        options.getForkIdBlockNumbers(),
        options.getForkIdBlockTimestamps());
  }
}
