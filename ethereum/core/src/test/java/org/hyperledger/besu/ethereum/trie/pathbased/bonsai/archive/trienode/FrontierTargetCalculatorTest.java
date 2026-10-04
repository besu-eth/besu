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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FrontierTargetCalculatorTest {

  @Mock Blockchain blockchain;
  @Mock BlockHeader finalizedHeader;

  @Test
  void usesFinalizedNumberWhenPresent() {
    final Hash finalizedHash = Hash.fromHexStringLenient("0x01");
    when(blockchain.getFinalized()).thenReturn(Optional.of(finalizedHash));
    when(blockchain.getBlockHeader(finalizedHash)).thenReturn(Optional.of(finalizedHeader));
    when(finalizedHeader.getNumber()).thenReturn(1000L);

    assertThat(FrontierTargetCalculator.computeFrontierTarget(blockchain, 512)).isEqualTo(1000L);
  }

  @Test
  void fallsBackToHeadMinusMaxLayersWhenNoFinality() {
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadBlockNumber()).thenReturn(2000L);

    assertThat(FrontierTargetCalculator.computeFrontierTarget(blockchain, 512)).isEqualTo(1488L);
  }

  @Test
  void fallbackClampsToZeroOnShortChain() {
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    when(blockchain.getChainHeadBlockNumber()).thenReturn(100L);

    assertThat(FrontierTargetCalculator.computeFrontierTarget(blockchain, 512)).isEqualTo(0L);
  }
}
