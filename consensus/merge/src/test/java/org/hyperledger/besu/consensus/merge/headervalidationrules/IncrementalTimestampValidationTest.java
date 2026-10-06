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
package org.hyperledger.besu.consensus.merge.headervalidationrules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IncrementalTimestampValidationTest {

  @Mock private ProtocolContext protocolContext;
  @Mock private MutableBlockchain blockchain;
  @Mock private MergeContext mergeContext;

  @BeforeEach
  public void setUp() {
    when(blockchain.getTotalDifficultyByHash(any())).thenReturn(Optional.of(Difficulty.ONE));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(mergeContext.getTerminalTotalDifficulty()).thenReturn(Difficulty.ZERO);
    when(protocolContext.getConsensusContext(MergeContext.class)).thenReturn(mergeContext);
  }

  @Test
  public void validWhenTimestampMoreRecentThanParent() {
    final IncrementalTimestampRule rule = new IncrementalTimestampRule();
    final long now = System.currentTimeMillis();
    final BlockHeader parentHeader = mock(BlockHeader.class);
    when(parentHeader.getTimestamp()).thenReturn(now);
    final BlockHeader validHeader = mock(BlockHeader.class);
    when(validHeader.getNumber()).thenReturn(1337L);
    when(validHeader.getTimestamp()).thenReturn(now + 1);

    assertThat(rule.validate(validHeader, parentHeader, protocolContext)).isTrue();
  }

  @Test
  public void invalidWhenTimestampNotMoreRecentThanParent() {
    final IncrementalTimestampRule rule = new IncrementalTimestampRule();
    final long now = System.currentTimeMillis();
    final BlockHeader parentHeader = mock(BlockHeader.class);
    when(parentHeader.getTimestamp()).thenReturn(now);
    final BlockHeader validHeader = mock(BlockHeader.class);
    when(validHeader.getNumber()).thenReturn(1337L);
    when(validHeader.getTimestamp()).thenReturn(now);

    assertThat(rule.validate(validHeader, parentHeader, protocolContext)).isFalse();
  }

  @Test
  public void validWhenTimestampAboveLongMaxValueIsMoreRecentThanParent() {
    final IncrementalTimestampRule rule = new IncrementalTimestampRule();
    final BlockHeader parentHeader = mock(BlockHeader.class);
    when(parentHeader.getTimestamp()).thenReturn(System.currentTimeMillis());
    final BlockHeader validHeader = mock(BlockHeader.class);
    when(validHeader.getNumber()).thenReturn(1337L);
    // uint64 0xffffffffffffffff, carried as -1
    when(validHeader.getTimestamp()).thenReturn(-1L);

    assertThat(rule.validate(validHeader, parentHeader, protocolContext)).isTrue();
  }

  @Test
  public void alwaysValidWhenTTDNotReached() {
    when(mergeContext.getTerminalTotalDifficulty()).thenReturn(Difficulty.of(2L));

    final IncrementalTimestampRule rule = new IncrementalTimestampRule();
    final BlockHeader parentHeader = mock(BlockHeader.class);
    final BlockHeader validHeader = mock(BlockHeader.class);
    when(validHeader.getNumber()).thenReturn(1337L);

    assertThat(rule.validate(validHeader, parentHeader, protocolContext)).isTrue();
  }
}
