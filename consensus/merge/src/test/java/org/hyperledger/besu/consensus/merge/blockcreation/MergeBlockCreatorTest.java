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
package org.hyperledger.besu.consensus.merge.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.merge.blockcreation.MergeBlockCreator.FIRST_BLOCK_TXS_SELECTION_MAX_TIME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ImmutableMiningConfiguration;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.time.Duration;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class MergeBlockCreatorTest {

  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);

  @Test
  public void onlyTheFirstBlockFromThePoolHasAShorterSelection() {
    final Duration configuredMaxTime = Duration.ofSeconds(5);
    final MergeBlockCreator blockCreator = blockCreator(configuredMaxTime);

    assertThat(blockCreator.txsSelectionMaxTime(protocolSpec))
        .isEqualTo(FIRST_BLOCK_TXS_SELECTION_MAX_TIME);
    assertThat(blockCreator.txsSelectionMaxTime(protocolSpec)).isEqualTo(configuredMaxTime);
    assertThat(blockCreator.txsSelectionMaxTime(protocolSpec)).isEqualTo(configuredMaxTime);
  }

  @Test
  public void firstBlockFromThePoolKeepsAShorterConfiguredSelection() {
    final Duration configuredMaxTime = FIRST_BLOCK_TXS_SELECTION_MAX_TIME.dividedBy(2);
    final MergeBlockCreator blockCreator = blockCreator(configuredMaxTime);

    assertThat(blockCreator.txsSelectionMaxTime(protocolSpec)).isEqualTo(configuredMaxTime);
    assertThat(blockCreator.txsSelectionMaxTime(protocolSpec)).isEqualTo(configuredMaxTime);
  }

  private MergeBlockCreator blockCreator(final Duration txsSelectionMaxTime) {
    when(protocolSpec.isPoS()).thenReturn(true);
    final MiningConfiguration miningConfiguration =
        ImmutableMiningConfiguration.builder()
            .posBlockTxsSelectionMaxTime(
                PositiveNumber.fromInt((int) txsSelectionMaxTime.toMillis()))
            .build();
    return new MergeBlockCreator(
        miningConfiguration,
        parent -> Bytes.EMPTY,
        mock(TransactionPool.class),
        mock(ProtocolContext.class),
        mock(ProtocolSchedule.class),
        mock(BlockHeader.class),
        mock(EthScheduler.class));
  }
}
