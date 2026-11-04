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
package org.hyperledger.besu.ethereum.eth.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainDataPruner;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BlockRangeProviderTest {
  private static final long HEAD = 2000L;

  @Mock private Blockchain blockchain;
  @Mock private ChainDataPruner chainDataPruner;

  @BeforeEach
  public void setup() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(HEAD);
  }

  @Test
  public void usesChainEarliestWhenNoPruner() {
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.of(10L));
    final BlockRangeProvider provider = new BlockRangeProvider(blockchain, Optional.empty());

    assertThat(provider.getEarliestBlockNumber()).contains(10L);
  }

  @Test
  public void usesChainEarliestWhenPrunerHasNoBound() {
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.of(10L));
    when(chainDataPruner.getEarliestRetainedBlock(HEAD)).thenReturn(Optional.empty());
    final BlockRangeProvider provider =
        new BlockRangeProvider(blockchain, Optional.of(chainDataPruner));

    assertThat(provider.getEarliestBlockNumber()).contains(10L);
  }

  @Test
  public void usesPrunerBoundWhenHigherThanChainEarliest() {
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.of(10L));
    when(chainDataPruner.getEarliestRetainedBlock(HEAD)).thenReturn(Optional.of(1500L));
    final BlockRangeProvider provider =
        new BlockRangeProvider(blockchain, Optional.of(chainDataPruner));

    assertThat(provider.getEarliestBlockNumber()).contains(1500L);
  }

  @Test
  public void usesChainEarliestWhenHigherThanPrunerBound() {
    // Snap-synced node: bodies only from the checkpoint, retention larger than head - checkpoint
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.of(1800L));
    when(chainDataPruner.getEarliestRetainedBlock(HEAD)).thenReturn(Optional.of(1500L));
    final BlockRangeProvider provider =
        new BlockRangeProvider(blockchain, Optional.of(chainDataPruner));

    assertThat(provider.getEarliestBlockNumber()).contains(1800L);
  }

  @Test
  public void neverAdvertisesEarliestAboveHead() {
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.of(10L));
    when(chainDataPruner.getEarliestRetainedBlock(HEAD)).thenReturn(Optional.of(HEAD + 300));
    final BlockRangeProvider provider =
        new BlockRangeProvider(blockchain, Optional.of(chainDataPruner));

    assertThat(provider.getEarliestBlockNumber()).contains(HEAD);
  }

  @Test
  public void isEmptyWhenChainHasNoEarliestBlock() {
    when(blockchain.getEarliestBlockNumber(HEAD)).thenReturn(Optional.empty());
    final BlockRangeProvider provider =
        new BlockRangeProvider(blockchain, Optional.of(chainDataPruner));

    assertThat(provider.getEarliestBlockNumber()).isEmpty();
  }
}
