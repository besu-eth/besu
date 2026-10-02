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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

class ExecutionEngineJsonRpcMethodsTest {
  private static final List<String> ALL_ENGINE_METHOD_NAMES =
      Arrays.stream(RpcMethod.values())
          .map(RpcMethod::getMethodName)
          .filter(name -> name.startsWith("engine_"))
          .toList();

  /**
   * Ensures that all methods returned by create() are valid and declared "engine_" methods. This
   * protects against accidental omissions from the RpcMethod enum.
   */
  @Test
  void testGetSupportedMethods() {
    final Map<String, JsonRpcMethod> engineMethods = createEngineMethods(Optional.of(0L));

    assertThat(engineMethods.keySet()).containsExactlyInAnyOrderElementsOf(ALL_ENGINE_METHOD_NAMES);
  }

  @Test
  void registersEveryMethodWhenNoForkIsScheduled() {
    final Map<String, JsonRpcMethod> engineMethods = createEngineMethods(Optional.empty());

    assertThat(engineMethods.keySet()).containsExactlyInAnyOrderElementsOf(ALL_ENGINE_METHOD_NAMES);
  }

  @Test
  void doesNotRegisterTheWitnessMethodWithoutPathBasedWorldState() {
    final String witnessMethod = RpcMethod.ENGINE_NEW_PAYLOAD_WITH_WITNESS_V5.getMethodName();

    final Map<String, JsonRpcMethod> engineMethods =
        createEngineMethods(Optional.of(0L), mock(ForestWorldStateArchive.class));

    assertThat(engineMethods.keySet())
        .containsExactlyInAnyOrderElementsOf(
            ALL_ENGINE_METHOD_NAMES.stream().filter(name -> !name.equals(witnessMethod)).toList());
    assertThat(advertisedCapabilities(engineMethods)).doesNotContain(witnessMethod);
  }

  @Test
  void advertisesExactlyTheRegisteredMethods() {
    final Map<String, JsonRpcMethod> engineMethods = createEngineMethods(Optional.empty());
    final String exchangeCapabilities = RpcMethod.ENGINE_EXCHANGE_CAPABILITIES.getMethodName();

    final List<String> registeredMethods =
        engineMethods.keySet().stream().filter(name -> !name.equals(exchangeCapabilities)).toList();
    assertThat(advertisedCapabilities(engineMethods))
        .containsExactlyInAnyOrderElementsOf(registeredMethods);
  }

  @SuppressWarnings("unchecked")
  private List<String> advertisedCapabilities(final Map<String, JsonRpcMethod> engineMethods) {
    final String exchangeCapabilities = RpcMethod.ENGINE_EXCHANGE_CAPABILITIES.getMethodName();
    final JsonRpcResponse response =
        engineMethods
            .get(exchangeCapabilities)
            .response(
                new JsonRpcRequestContext(
                    new JsonRpcRequest("2.0", exchangeCapabilities, new Object[] {List.of()})));

    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    return (List<String>) ((JsonRpcSuccessResponse) response).getResult();
  }

  private Map<String, JsonRpcMethod> createEngineMethods(final Optional<Long> forkMilestone) {
    return createEngineMethods(forkMilestone, mock(PathBasedWorldStateProvider.class));
  }

  private Map<String, JsonRpcMethod> createEngineMethods(
      final Optional<Long> forkMilestone, final WorldStateArchive worldStateArchive) {
    MiningCoordinator miningCoordinator = mock(MergeMiningCoordinator.class);
    when(miningCoordinator.isCompatibleWithEngineApi()).thenReturn(true);
    ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
    when(protocolSchedule.milestoneFor(any())).thenReturn(forkMilestone);
    ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);
    ExecutionEngineJsonRpcMethods methods =
        new ExecutionEngineJsonRpcMethods(
            miningCoordinator,
            protocolSchedule,
            protocolContext,
            mock(EthPeers.class),
            mock(Vertx.class),
            "testClient",
            "testCommit",
            mock(TransactionPool.class),
            mock(MetricsSystem.class));

    return methods.create();
  }
}
