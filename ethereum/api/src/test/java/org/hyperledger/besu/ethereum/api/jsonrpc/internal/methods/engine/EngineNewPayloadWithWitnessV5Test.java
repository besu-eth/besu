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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Map;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EngineNewPayloadWithWitnessV5Test {

  @Test
  void nameIsEngineNewPayloadWithWitnessV5() {
    final EngineNewPayloadWithWitnessV5 method = newMethod(mock(ProtocolContext.class));
    assertThat(method.getName())
        .isEqualTo(RpcMethod.ENGINE_NEW_PAYLOAD_WITH_WITNESS_V5.getMethodName());
  }

  @Test
  void buildValidResponseReturnsErrorWhenWitnessCannotBeBuilt() {
    // World state archive is a plain WorldStateArchive (not path-based) so
    // BonsaiExecutionWitnessBuilder returns empty — must surface as INTERNAL_ERROR.
    final BlockHeader parent = new BlockHeaderTestFixture().number(0).buildHeader();
    final BlockHeader header =
        new BlockHeaderTestFixture().number(1).parentHash(parent.getHash()).buildHeader();

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.of(header));
    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));

    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(mock(WorldStateArchive.class));

    final EngineNewPayloadWithWitnessV5 method = newMethod(protocolContext);
    final BlockProcessingResult result =
        new BlockProcessingResult(
            Optional.of(
                new BlockProcessingOutputs(
                    null, java.util.List.of(), Optional.empty(), Optional.empty(), 0L, Map.of())));

    final JsonRpcResponse response = method.buildValidResponse("1", header.getHash(), result);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  void buildValidResponseReturnsErrorWhenParentHeaderMissing() {
    final BlockHeader header = new BlockHeaderTestFixture().number(1).buildHeader();

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.of(header));
    when(blockchain.getBlockHeader(header.getParentHash())).thenReturn(Optional.empty());

    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    final EngineNewPayloadWithWitnessV5 method = newMethod(protocolContext);
    final BlockProcessingResult result =
        new BlockProcessingResult(
            Optional.of(
                new BlockProcessingOutputs(
                    null, java.util.List.of(), Optional.empty(), Optional.empty(), 0L, Map.of())));

    final JsonRpcResponse response = method.buildValidResponse("1", header.getHash(), result);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }

  @Test
  void buildValidResponseReturnsErrorWhenBlockHeaderNotPersisted() {
    final BlockHeader header = new BlockHeaderTestFixture().number(1).buildHeader();

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.empty());

    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);

    final EngineNewPayloadWithWitnessV5 method = newMethod(protocolContext);
    final BlockProcessingResult result =
        new BlockProcessingResult(
            Optional.of(
                new BlockProcessingOutputs(
                    null, java.util.List.of(), Optional.empty(), Optional.empty(), 0L, Map.of())));

    final JsonRpcResponse response = method.buildValidResponse("1", header.getHash(), result);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.INTERNAL_ERROR);
  }

  private static EngineNewPayloadWithWitnessV5 newMethod(final ProtocolContext protocolContext) {
    final ProtocolSchedule schedule = mock(ProtocolSchedule.class);
    when(schedule.milestoneFor(org.mockito.ArgumentMatchers.any())).thenReturn(Optional.empty());
    return new EngineNewPayloadWithWitnessV5(
        mock(Vertx.class),
        schedule,
        protocolContext,
        mock(MergeMiningCoordinator.class),
        mock(EthPeers.class),
        mock(EngineCallListener.class),
        new NoOpMetricsSystem());
  }
}
