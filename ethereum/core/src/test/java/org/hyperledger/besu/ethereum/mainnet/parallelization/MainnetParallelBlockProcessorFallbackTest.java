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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionPathListener;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class MainnetParallelBlockProcessorFallbackTest {

  private final BlockExecutionPathListener listener = mock(BlockExecutionPathListener.class);

  @Test
  void reportsTheSequentialFallbackOnceWithBothResults() {
    final ExecutionContextTestFixture ctx =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(parent);
    final MainnetParallelBlockProcessor processor =
        new MainnetParallelBlockProcessor(
            spec.getTransactionProcessor(),
            spec.getTransactionReceiptFactory(),
            BlockHeader::getCoinbase,
            ctx.getProtocolSchedule(),
            ImmutableBalConfiguration.builder().executionPathListener(listener).build(),
            new NoOpMetricsSystem());
    final MutableWorldState worldState =
        ctx.getStateArchive()
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parent))
            .orElseThrow();
    // A zero state root fails the parallel run and the sequential re-run alike.
    final Block block = emptyBlockWithZeroStateRoot(parent);

    final BlockProcessingResult result =
        processor.processBlock(
            BlockExecutionContext.builder()
                .protocolContext(ctx.getProtocolContext())
                .worldState(worldState)
                .block(block)
                .blockAccessList(Optional.empty())
                .build());

    assertThat(result.isFailed()).isTrue();
    verify(listener).onParallel(block.getHeader(), "optimistic");
    verify(listener)
        .onSequentialFallback(
            eq(block.getHeader()),
            argThat(BlockProcessingResult::isFailed),
            argThat(sequential -> sequential == result));
    verify(listener, never()).onSequential(any(), any());
  }

  private static Block emptyBlockWithZeroStateRoot(final BlockHeader parent) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(parent.getNumber() + 1L)
            .parentHash(parent.getHash())
            .coinbase(MINING_BENEFICIARY)
            .stateRoot(Hash.ZERO)
            .gasLimit(30_000_000L)
            .baseFeePerGas(Wei.of(5))
            // Prague is active at genesis here, so the requests hash must be present. This is the
            // one the system-contract predeploys yield for a block that enqueues no requests.
            .requestsHash(
                Hash.fromHexString(
                    "0x5f7606bf4b9eb2a8414aaa53f4c84062ec8789d24c604453563dc26e4ae65837"))
            .buildHeader();
    return new Block(
        header, new BlockBody(Collections.emptyList(), Collections.emptyList(), Optional.empty()));
  }
}
