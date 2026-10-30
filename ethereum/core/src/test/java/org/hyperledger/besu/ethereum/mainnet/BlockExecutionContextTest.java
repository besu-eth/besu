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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
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

  @Test
  void sequentialExecutionIsNotRequestedByDefault() {
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build();

    assertThat(context.isSequentialExecutionRequested()).isFalse();
  }

  @Test
  void blockTracerRequiresSequentialExecution() {
    final BlockExecutionContext.Builder builder =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .blockTracer(mock(BlockAwareOperationTracer.class));

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("blockTracer requires sequential execution");
  }

  @Test
  void blockTracerIsKeptWithSequentialExecution() {
    final BlockAwareOperationTracer blockTracer = mock(BlockAwareOperationTracer.class);
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .sequentialExecutionRequested(true)
            .blockTracer(blockTracer)
            .build();

    assertThat(context.getBlockTracer()).containsSame(blockTracer);
    assertThat(context.toBuilder().build().getBlockTracer()).containsSame(blockTracer);
  }

  @Test
  void blockTracerIsEmptyByDefault() {
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build();

    assertThat(context.getBlockTracer()).isEmpty();
  }

  @Test
  void toBuilderCopiesAllFields() {
    final BlockAccessList blockAccessList = mock(BlockAccessList.class);
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.of(blockAccessList))
            .build();

    final BlockExecutionContext sequential =
        context.toBuilder().sequentialExecutionRequested(true).build();

    assertThat(sequential.getProtocolContext()).isSameAs(protocolContext);
    assertThat(sequential.getWorldState()).isSameAs(worldState);
    assertThat(sequential.getBlock()).isSameAs(block);
    assertThat(sequential.getBlockAccessList()).contains(blockAccessList);
    assertThat(sequential.isSequentialExecutionRequested()).isTrue();
    assertThat(sequential.toBuilder().build().isSequentialExecutionRequested()).isTrue();
    assertThat(context.isSequentialExecutionRequested()).isFalse();
  }

  @Test
  void blockAccessListFactoryDefaultsToProtocolSpec() {
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build();

    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    final BlockAccessListFactory blockAccessListFactory = new BlockAccessListFactory();
    when(protocolSpec.getBlockAccessListFactory()).thenReturn(Optional.of(blockAccessListFactory));

    assertThat(context.getBlockAccessListFactory(protocolSpec))
        .containsSame(blockAccessListFactory);
  }

  @Test
  void blockAccessListFactoryProviderIsKeptAndCopiedByToBuilder() {
    // The fork defines no block access list factory (the mocked spec returns empty).
    final ProtocolSpec protocolSpec = mock(ProtocolSpec.class);
    final BlockAccessListFactory blockAccessListFactory = new BlockAccessListFactory();
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .blockAccessListFactoryProvider(spec -> Optional.of(blockAccessListFactory))
            .build();

    assertThat(context.getBlockAccessListFactory(protocolSpec))
        .containsSame(blockAccessListFactory);
    assertThat(context.toBuilder().build().getBlockAccessListFactory(protocolSpec))
        .containsSame(blockAccessListFactory);
  }

  @Test
  void inclusionListIsEmptyByDefault() {
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .build();

    assertThat(context.getInclusionListTransactions()).isSameAs(InclusionListTransactions.EMPTY);
  }

  @Test
  void inclusionListIsKeptAndCopiedByToBuilder() {
    final InclusionListTransactions inclusionListTransactions =
        new InclusionListTransactions(
            List.of(
                new TransactionTestFixture()
                    .createTransaction(SignatureAlgorithmFactory.getInstance().generateKeyPair())));
    final BlockExecutionContext context =
        BlockExecutionContext.builder()
            .protocolContext(protocolContext)
            .worldState(worldState)
            .block(block)
            .blockAccessList(Optional.empty())
            .inclusionListTransactions(inclusionListTransactions)
            .build();

    assertThat(context.getInclusionListTransactions()).isSameAs(inclusionListTransactions);
    assertThat(context.toBuilder().build().getInclusionListTransactions())
        .isSameAs(inclusionListTransactions);
  }

  @Test
  void inclusionListCannotBeNull() {
    assertThatNullPointerException()
        .isThrownBy(() -> BlockExecutionContext.builder().inclusionListTransactions(null))
        .withMessageContaining("inclusionListTransactions");
  }
}
