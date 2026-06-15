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

import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface NodeConfiguration {

  void setBootnodes(List<String> bootnodes);

  List<String> getBootnodes();

  boolean isDiscoveryV5Enabled();

  void ensureAdminRpcEnabled();

  void useWebSocketsForJsonRpc();

  void useAuthenticationTokenInHeaderForJsonRpc(String token);

  Optional<Integer> getJsonRpcWebSocketPort();

  Optional<Integer> getJsonRpcPort();

  Optional<Integer> getEngineJsonRpcPort();

  String getHostName();

  boolean isJsonRpcEnabled();

  boolean isEngineRpcEnabled();

  GenesisConfigurationProvider getGenesisConfigProvider();

  Optional<String> getGenesisConfig();

  void setGenesisConfig(final String config);

  boolean isP2pEnabled();

  boolean isDiscoveryEnabled();

  boolean isBootnodeEligible();

  List<String> getExtraCLIOptions();

  boolean isRevertReasonEnabled();

  List<String> getStaticNodes();

  Map<String, String> getEnvironment();
}
