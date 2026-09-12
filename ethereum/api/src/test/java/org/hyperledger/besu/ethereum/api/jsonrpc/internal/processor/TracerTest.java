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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.blockhash.PreExecutionProcessor;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TracerTest {

  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private Blockchain blockchain;
  @Mock private WorldStateArchive worldStateArchive;
  @Mock private MiningConfiguration miningConfiguration;
  @Mock private BlockHeader blockHeader;
  @Mock private BlockHeader parentBlockHeader;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private PreExecutionProcessor preExecutionProcessor;
  @Mock private BlockHashLookup blockHashLookup;

  @Test
  void appliesPreExecutionStateChangesBeforeTracing() {
    final Hash parentHash = Hash.fromHexStringLenient("0x01");
    final Hash parentStateRoot = Hash.fromHexStringLenient("0x02");
    final Address preExecutionAddress = Address.fromHexString("0x1234");
    final long preExecutionNonce = 42L;
    final MutableWorldState worldState = createInMemoryWorldState();
    final BlockchainQueries blockchainQueries =
        new BlockchainQueries(protocolSchedule, blockchain, worldStateArchive, miningConfiguration);

    when(blockHeader.getParentHash()).thenReturn(parentHash);
    when(blockchain.getBlockHeader(parentHash)).thenReturn(Optional.of(parentBlockHeader));
    when(parentBlockHeader.getBlockHash()).thenReturn(parentHash);
    when(parentBlockHeader.getStateRoot()).thenReturn(parentStateRoot);
    when(worldStateArchive.getWorldState(any(WorldStateQueryParams.class)))
        .thenReturn(Optional.of(worldState));
    when(protocolSchedule.getByBlockHeader(blockHeader)).thenReturn(protocolSpec);
    when(protocolSpec.getPreExecutionProcessor()).thenReturn(preExecutionProcessor);
    when(preExecutionProcessor.createBlockHashLookup(blockchain, blockHeader))
        .thenReturn(blockHashLookup);

    doAnswer(
            invocation -> {
              final BlockProcessingContext context = invocation.getArgument(0);
              final var updater = context.getWorldState().updater();
              updater.getOrCreate(preExecutionAddress).setNonce(preExecutionNonce);
              updater.commit();
              return null;
            })
        .when(preExecutionProcessor)
        .process(any(), eq(Optional.empty()));

    final Optional<Long> result =
        Tracer.processTracing(
            blockchainQueries,
            Optional.of(blockHeader),
            traceableState -> Optional.of(traceableState.get(preExecutionAddress).getNonce()));

    assertThat(result).contains(preExecutionNonce);

    final ArgumentCaptor<BlockProcessingContext> contextCaptor =
        ArgumentCaptor.forClass(BlockProcessingContext.class);
    verify(preExecutionProcessor).process(contextCaptor.capture(), eq(Optional.empty()));

    final BlockProcessingContext context = contextCaptor.getValue();
    assertThat(context.getBlockHeader()).isSameAs(blockHeader);
    assertThat(context.getWorldState()).isSameAs(worldState);
    assertThat(context.getProtocolSpec()).isSameAs(protocolSpec);
    assertThat(context.getBlockHashLookup()).isSameAs(blockHashLookup);
    assertThat(context.getOperationTracer()).isSameAs(OperationTracer.NO_TRACING);
    assertThat(context.getBlockAccessListBuilder()).isEmpty();
  }
}
