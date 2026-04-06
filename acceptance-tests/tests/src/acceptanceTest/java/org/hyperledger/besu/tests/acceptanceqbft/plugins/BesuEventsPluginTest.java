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
package org.hyperledger.besu.tests.acceptanceqbft.plugins;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BesuEventsPluginTest extends AcceptanceTestBase {
  private BesuNode pluginNode;
  private BesuNode minerNode;

  @BeforeEach
  public void setUp() throws Exception {
    minerNode =
        besu.createQbftNode(
            "minerNode",
            b ->
                b.genesisConfigProvider(
                    GenesisConfigurationFactory::createQbftLondonGenesisConfig));
    pluginNode =
        besu.createQbftPluginsNode(
            "node1",
            Collections.singletonList("testPlugins"),
            PluginConfiguration.DEFAULT,
            Collections.emptyList());
    cluster.start(pluginNode, minerNode);

    // Ensure nodes are connected before starting test
    minerNode.awaitPeerDiscovery(net.awaitPeerCount(1));
    pluginNode.awaitPeerDiscovery(net.awaitPeerCount(1));

    // Wait for initial block production to stabilize
    waitForBlockHeight(minerNode, 1);
  }

  @Test
  public void blockIsAnnounced() {
    waitForFile(pluginNode.homeDirectory().resolve("plugins/newBlock.2"));
  }
}
