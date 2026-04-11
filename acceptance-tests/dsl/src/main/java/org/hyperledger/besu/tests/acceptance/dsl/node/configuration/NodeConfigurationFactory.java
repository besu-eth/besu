/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.tests.acceptance.dsl.node.configuration;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.ADMIN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.IBFT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.MINER;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.QBFT;

import org.hyperledger.besu.ethereum.api.jsonrpc.ImmutableInProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.InProcessRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.node.RunnableNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class NodeConfigurationFactory {

  public Optional<String> createGenesisConfigForValidators(
      final Collection<String> validators,
      final Collection<? extends RunnableNode> besuNodes,
      final GenesisConfigurationProvider genesisConfigProvider) {
    final List<RunnableNode> nodes =
        besuNodes.stream().filter(n -> validators.contains(n.getName())).collect(toList());
    return genesisConfigProvider.create(nodes);
  }

  public JsonRpcConfiguration createJsonRpcWithIbft2EnabledConfig(final boolean minerEnabled) {
    return minerEnabled
        ? createJsonRpcWithRpcApiEnabledConfig(IBFT.name(), MINER.name())
        : createJsonRpcWithRpcApiEnabledConfig(IBFT.name());
  }

  public JsonRpcConfiguration createJsonRpcWithIbft2AdminEnabledConfig() {
    return createJsonRpcWithRpcApiEnabledConfig(IBFT.name(), ADMIN.name());
  }

  public JsonRpcConfiguration createJsonRpcWithQbftEnabledConfig(final boolean minerEnabled) {
    return minerEnabled
        ? createJsonRpcWithRpcApiEnabledConfig(QBFT.name(), MINER.name())
        : createJsonRpcWithRpcApiEnabledConfig(QBFT.name());
  }

  public JsonRpcConfiguration createJsonRpcEnabledConfig() {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    config.setHostsAllowlist(singletonList("*"));
    return config;
  }

  public WebSocketConfiguration createWebSocketEnabledConfig() {
    final WebSocketConfiguration config = WebSocketConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    return config;
  }

  public JsonRpcConfiguration jsonRpcConfigWithAdmin() {
    return createJsonRpcWithRpcApiEnabledConfig(ADMIN.name());
  }

  public JsonRpcConfiguration createJsonRpcWithRpcApiEnabledConfig(final String... rpcApi) {
    final JsonRpcConfiguration jsonRpcConfig = createJsonRpcEnabledConfig();
    final List<String> rpcApis = new ArrayList<>(jsonRpcConfig.getRpcApis());
    rpcApis.addAll(Arrays.asList(rpcApi));
    jsonRpcConfig.setRpcApis(rpcApis);
    return jsonRpcConfig;
  }

  public InProcessRpcConfiguration createInProcessRpcConfiguration(final Set<String> extraRpcApis) {
    final Set<String> rpcApis =
        new HashSet<>(ImmutableInProcessRpcConfiguration.DEFAULT_IN_PROCESS_RPC_APIS);
    rpcApis.addAll(extraRpcApis);
    return ImmutableInProcessRpcConfiguration.builder()
        .inProcessRpcApis(rpcApis)
        .isEnabled(true)
        .build();
  }
}
