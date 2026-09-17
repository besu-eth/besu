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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RpcConfigurationPluginTest extends AcceptanceTestBase {
  private BesuNode node;
  private JsonRpcConfiguration jsonRpcConfiguration;

  @BeforeEach
  public void setUp() throws Exception {
    jsonRpcConfiguration = JsonRpcConfiguration.createDefault();
    jsonRpcConfiguration.setEnabled(true);
    jsonRpcConfiguration.setPort(0);
    jsonRpcConfiguration.setHostsAllowlist(List.of("*"));
    node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name("node1")
                .jsonRpcConfiguration(jsonRpcConfiguration)
                .plugins(List.of("testPlugins"))
                .build());
    cluster.start(node);
  }

  @Test
  public void rpcConfigurationServiceProvidesRpcHttpOptions() throws IOException {
    final Path rpcHttpConfigFile =
        node.homeDirectory().resolve("plugins/rpcConfiguration.rpcHttpConfig");
    waitForFile(rpcHttpConfigFile);
    final String reportedRpcHttpConfig = Files.readString(rpcHttpConfigFile).trim();
    assertThat(reportedRpcHttpConfig)
        .isEqualTo(
            jsonRpcConfiguration.getHost() + ":0:" + jsonRpcConfiguration.getHttpTimeoutSec());
  }
}
