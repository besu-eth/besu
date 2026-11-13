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
package org.hyperledger.besu.tests.acceptanceqbft.bootstrap;

import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNodeRunner;
import org.hyperledger.besu.tests.acceptance.dsl.node.ThreadBesuNodeRunner;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.Cluster;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfigurationBuilder;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClusterThreadNodeRunnerAcceptanceTest extends AcceptanceTestBase {

  private BesuNode miner;
  private Cluster noDiscoveryCluster;

  @BeforeEach
  public void setUp() throws Exception {
    final ClusterConfiguration clusterConfiguration =
        new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build();
    final BesuNodeRunner besuNodeRunner = new ThreadBesuNodeRunner();
    noDiscoveryCluster = new Cluster(clusterConfiguration, net, besuNodeRunner);
    miner =
        besu.createQbftNode(
            "minerNode",
            (builder) -> {
              builder.storageImplementation("rocksdb");
              return builder;
            });
    noDiscoveryCluster.start(miner);
    final BesuNode noDiscoveryNode =
        besu.createQbftNode(
            "noDiscoveryNode",
            (builder) -> {
              builder
                  .discoveryEnabled(false)
                  .engineRpcEnabled(false)
                  .staticNodes(List.of(miner.enodeUrl().toString()));
              return builder;
            });
    noDiscoveryCluster.addNode(noDiscoveryNode);
  }

  @Test
  public void shouldVerifySomething() {
    // we don't care what verifies, just that it gets to the point something can verify
    miner.verify(net.awaitPeerCount(1));
  }

  @Test
  void shouldMineTransactionsEvenAfterRestart() {
    final Account recipient = accounts.createAccount("account1");
    miner.execute(accountTransactions.createTransfer(recipient, 2));
    miner.verify(recipient.balanceEquals(2));

    noDiscoveryCluster.stop();
    noDiscoveryCluster.start(miner);
    // Checking that state is retained after restart
    miner.verify(recipient.balanceEquals(2));
  }

  @AfterEach
  @Override
  public void tearDownAcceptanceTestBase() {
    noDiscoveryCluster.stop();
    super.tearDownAcceptanceTestBase();
  }
}
