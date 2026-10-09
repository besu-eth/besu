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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class BlockExecutionContextTest {

  private final ProtocolContext protocolContext = mock(ProtocolContext.class);
  private final MutableWorldState worldState = mock(MutableWorldState.class);
  private final Block block = mock(Block.class);

  @Test
  void blockAccessListMustBeSet() {
    final BlockExecutionContext.Builder builder =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block);

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("blockAccessList is required");
  }

  @Test
  void blockAccessListCanBeSetToEmpty() {
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build();

    assertThat(context.getBlockAccessList()).isEmpty();
  }

  @Test
  void blockAccessListIsKept() {
    final BlockAccessList blockAccessList = mock(BlockAccessList.class);
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.of(blockAccessList))
            .build();

    assertThat(context.getBlockAccessList()).contains(blockAccessList);
  }
}
