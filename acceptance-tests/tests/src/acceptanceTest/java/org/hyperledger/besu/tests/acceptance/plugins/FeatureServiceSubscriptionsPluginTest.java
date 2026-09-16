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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.genesis.GenesisConfigurationFactory;

import java.math.BigInteger;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the subscriptions added to the three feature services, delivered through the test plugins
 * that subscribe via {@code BlockchainService}, {@code TransactionPoolService} and {@code
 * SynchronizationService} instead of {@code BesuEvents}.
 */
public class FeatureServiceSubscriptionsPluginTest extends AcceptanceTestBase {
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

    minerNode.awaitPeerDiscovery(net.awaitPeerCount(1));
    pluginNode.awaitPeerDiscovery(net.awaitPeerCount(1));

    waitForBlockHeight(minerNode, 1);
  }

  @Test
  public void blockAddedIsDeliveredThroughBlockchainService() {
    waitForFile(pluginNode.homeDirectory().resolve("plugins/subscribedBlock.2"));
  }

  @Test
  public void transactionAddedIsDeliveredThroughTransactionPoolService() {
    final Account recipient = accounts.createAccount("recipient");
    pluginNode.execute(accountTransactions.createTransfer(recipient, Amount.wei(BigInteger.ONE)));

    waitForFile(pluginNode.homeDirectory().resolve("plugins/subscribedTx.1"));
  }

  @Test
  public void initialSyncCompletionIsDeliveredThroughSynchronizationService() {
    // the plugin closes this subscription from inside the callback, so the file is written once
    waitForFile(pluginNode.homeDirectory().resolve("plugins/initialSyncCompleted"));
  }
}
