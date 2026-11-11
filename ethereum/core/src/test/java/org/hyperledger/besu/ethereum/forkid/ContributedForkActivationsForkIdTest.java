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

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.ethereum.forkid.ForkIdTestUtil.GenesisHash;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleActivation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecModification;

import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

/**
 * Proves the activations a {@link ProtocolScheduleCustomization} contributes reach the EIP-2124
 * fork ID of the node's {@link ForkIdManager}, exactly as the genesis-declared forks do.
 */
public class ContributedForkActivationsForkIdTest {

  // beyond mainnet's last block fork (Gray Glacier, 15_050_000) but ordered before its
  // timestamp forks, as EIP-6122 requires of any block-number activation
  private static final long EXTRA_BLOCK_FORK = 20_000_000L;
  // beyond mainnet's last genesis-declared timestamp fork
  private static final long EXTRA_TIMESTAMP_FORK = 1_900_000_000L;

  @Test
  public void additionalActivationsBecomeForkIdBoundaries() {
    final List<ForkId> stock = allForkIds(ProtocolScheduleCustomization.none());
    final List<ForkId> extended =
        allForkIds(
            customization(
                ProtocolScheduleActivation.blockNumber(EXTRA_BLOCK_FORK),
                ProtocolScheduleActivation.timestamp(EXTRA_TIMESTAMP_FORK)));

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
        forkIdManager(ProtocolScheduleCustomization.none(), head, headTimestamp)
            .getForkIdForChainHead();
    final ForkId extended =
        forkIdManager(
                customization(ProtocolScheduleActivation.blockNumber(EXTRA_BLOCK_FORK)),
                head,
                headTimestamp)
            .getForkIdForChainHead();

    // EIP-2124 FORK_NEXT: the first fork not yet crossed becomes the out-of-band activation
    assertThat(extended.getNext()).isEqualTo(EXTRA_BLOCK_FORK);
    assertThat(stock.getNext()).isNotEqualTo(EXTRA_BLOCK_FORK);
    // the crossed-forks hash is unchanged: the extra activation lies ahead of the head
    assertThat(extended.getHash()).isEqualTo(stock.getHash());
  }

  @Test
  public void rejectsAPeerWhoseNextForkIsAlreadyPassedLocally() {
    // the genesis declares a block fork at 100; the customization contributes an earlier one
    final GenesisConfig genesis =
        GenesisConfig.fromConfig("{\"config\":{\"chainId\":1,\"homesteadBlock\":100}}");
    final ForkIdManager manager =
        customization(ProtocolScheduleActivation.blockNumber(50L))
            .forkIdManager(
                mockBlockchain(GenesisHash.MAINNET, 200L, 0L), genesis.getConfigOptions());
    final ForkId latest = manager.getForkIdForChainHead();
    assertThat(latest.getNext()).isZero();

    // EIP-2124 rule 1a: same FORK_HASH, but FORK_NEXT (75) is already passed locally
    assertThat(manager.peerCheck(new ForkId(latest.getHash(), 75L))).isFalse();
  }

  private static List<ForkId> allForkIds(final ProtocolScheduleCustomization customization) {
    return forkIdManager(customization, 0L, 0L).getAllForkIds();
  }

  private static ForkIdManager forkIdManager(
      final ProtocolScheduleCustomization customization,
      final long chainHeight,
      final long timestamp) {
    return customization.forkIdManager(
        mockBlockchain(GenesisHash.MAINNET, chainHeight, timestamp),
        GenesisConfig.mainnet().getConfigOptions());
  }

  private static ProtocolScheduleCustomization customization(
      final ProtocolScheduleActivation... activations) {
    return new ProtocolScheduleCustomization(
        "example-chain",
        Arrays.stream(activations)
            .map(activation -> new ProtocolSpecModification(activation, UnaryOperator.identity()))
            .toList());
  }
}
