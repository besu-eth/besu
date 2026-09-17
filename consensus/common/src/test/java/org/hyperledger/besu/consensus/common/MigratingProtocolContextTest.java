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
package org.hyperledger.besu.consensus.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.ConsensusContext;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.ServiceManager;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MigratingProtocolContextTest {

  @Test
  public void returnsContextForSpecificChainHeight() {
    final MutableBlockchain blockchain = Mockito.mock(MutableBlockchain.class);
    final BlockHeader header = Mockito.mock(BlockHeader.class);
    final WorldStateArchive worldStateArchive = Mockito.mock(WorldStateArchive.class);

    when(blockchain.getChainHeadHeader()).thenReturn(header);

    final ConsensusContext context1 = Mockito.mock(ConsensusContext.class);
    when(context1.as(any())).thenReturn(context1);

    final ConsensusContext context2 = Mockito.mock(ConsensusContext.class);
    when(context2.as(any())).thenReturn(context2);

    final ForksSchedule<ConsensusContext> contextSchedule =
        new ForksSchedule<>(List.of(new ForkSpec<>(0L, context1), new ForkSpec<>(10L, context2)));

    final MigratingProtocolContext migratingProtocolContext =
        new MigratingProtocolContext(
            blockchain,
            worldStateArchive,
            new MigratingConsensusContext(contextSchedule),
            new BadBlockManager(),
            new ServiceManager.SimpleServiceManager());

    assertThat(migratingProtocolContext.getConsensusContext(ConsensusContext.class))
        .isSameAs(context1);

    when(header.getNumber()).thenReturn(2L);
    assertThat(migratingProtocolContext.getConsensusContext(ConsensusContext.class))
        .isSameAs(context1);

    when(header.getNumber()).thenReturn(10L);
    assertThat(migratingProtocolContext.getConsensusContext(ConsensusContext.class))
        .isSameAs(context2);
  }
}
