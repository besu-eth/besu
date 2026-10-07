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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers what differs from {@code engine_newPayloadV5} and doesn't need a real world state: the
 * method name, and answering an imported payload with an internal error rather than a VALID status
 * without a witness when the witness can't be built. The VALID path needs a real Bonsai world
 * state; the witness builder it relies on is covered by the zkEVM execution-spec reference tests.
 */
@ExtendWith(MockitoExtension.class)
class EngineNewPayloadWithWitnessV5Test {

  private static final Vertx vertx = Vertx.vertx();
  private static final Object REQUEST_ID = 1;

  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolContext protocolContext;
  @Mock private MergeMiningCoordinator mergeCoordinator;
  @Mock private MutableBlockchain blockchain;
  @Mock private EthPeers ethPeers;
  @Mock private EngineCallListener engineCallListener;
  @Mock private TransactionPool transactionPool;
  @Mock private ExecutionPayloadV1 param;

  private final BlockHeader newBlockHeader = new BlockHeaderTestFixture().buildHeader();
  private EngineNewPayloadWithWitnessV5<?, ?> method;

  @BeforeEach
  void setUp() {
    method = newMethod();
  }

  private EngineNewPayloadWithWitnessV5<?, ?> newMethod() {
    return new EngineNewPayloadWithWitnessV5<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .ethPeers(ethPeers)
            .metricsSystem(new NoOpMetricsSystem())
            .transactionPool(transactionPool)
            .maxRequestBlocks(0)
            .build(),
        AMSTERDAM,
        null);
  }

  @Test
  void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadWithWitnessV5");
  }

  @Test
  void shouldReturnInternalErrorWhenImportProducedNoBlockAccessList() {
    final JsonRpcResponse resp =
        method.respondWithValid(
            REQUEST_ID, param, newBlockHeader, new BlockProcessingResult(Optional.empty()));

    assertInternalError(resp);
  }

  @Test
  void shouldReturnInternalErrorWhenWitnessCannotBeBuilt() {
    // a non path-based world state makes the witness builder throw
    when(protocolContext.getWorldStateArchive()).thenReturn(mock(ForestWorldStateArchive.class));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    final BlockProcessingOutputs outputs = mock(BlockProcessingOutputs.class);
    when(outputs.getBlockAccessList()).thenReturn(Optional.of(new BlockAccessList(List.of())));
    when(outputs.getAccessedAncestors()).thenReturn(Map.of());

    final JsonRpcResponse resp =
        method.respondWithValid(
            REQUEST_ID, param, newBlockHeader, new BlockProcessingResult(Optional.of(outputs)));

    assertInternalError(resp);
  }

  private static void assertInternalError(final JsonRpcResponse resp) {
    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType()).isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }
}
