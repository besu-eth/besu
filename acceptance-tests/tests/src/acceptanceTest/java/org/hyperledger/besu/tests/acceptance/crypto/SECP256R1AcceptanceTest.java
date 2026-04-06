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
package org.hyperledger.besu.tests.acceptance.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNodeRunner;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.Cluster;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfigurationBuilder;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SECP256R1AcceptanceTest extends AcceptanceTestBase {
  private Node minerNode;
  private Node otherNode;
  private Cluster noDiscoveryCluster;

  private static final String MINER_NODE_PRIVATE_KEY =
      "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63";
  private static final String OTHER_NODE_PRIVATE_KEY =
      "c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3";

  private static final SECP256R1 SECP256R1_SIGNATURE_ALGORITHM = new SECP256R1();

  @BeforeEach
  public void setUp() throws Exception {
    KeyPair minerNodeKeyPair = createKeyPair(MINER_NODE_PRIVATE_KEY);
    KeyPair otherNodeKeyPair = createKeyPair(OTHER_NODE_PRIVATE_KEY);

    final ClusterConfiguration clusterConfiguration =
        new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build();
    noDiscoveryCluster = new Cluster(clusterConfiguration, net);

    minerNode =
        besu.createNodeWithNonDefaultSignatureAlgorithm("minerNode", minerNodeKeyPair, List.of());
    noDiscoveryCluster.start(minerNode);

    otherNode =
        besu.createNodeWithNonDefaultSignatureAlgorithm(
            "otherNode", otherNodeKeyPair, List.of(minerNode));
    noDiscoveryCluster.addNode(otherNode);

    minerNode.verify(net.awaitPeerCount(1));
    otherNode.verify(net.awaitPeerCount(1));

    final var minerChainHead = minerNode.execute(ethTransactions.block());
    otherNode.verify(blockchain.minimumHeight(minerChainHead.getNumber().longValue()));
  }

  @Test
  public void transactionShouldBeSuccessful() {
    // SignatureAlgorithmFactory.instance is static. When ThreadBesuRunner is used, we cannot change
    // the signature algorithm instance to SECP256R1 as it could influence other tests running at
    // the same time. So we only execute the test when ProcessBesuNodeRunner is used, as there is
    // not conflict because we use separate processes.
    assumeTrue(BesuNodeRunner.isProcessBesuNodeRunner());

    minerNode.verify(net.awaitPeerCount(1));
    otherNode.verify(net.awaitPeerCount(1));

    final Account recipient = accounts.createAccount("recipient");

    final Hash transactionHash =
        minerNode.execute(accountTransactions.createTransfer(recipient, 5, new SECP256R1()));
    assertThat(transactionHash).isNotNull();
    noDiscoveryCluster.verify(recipient.balanceEquals(5));
  }

  @AfterEach
  @Override
  public void tearDownAcceptanceTestBase() {
    super.tearDownAcceptanceTestBase();

    noDiscoveryCluster.stop();
  }

  private KeyPair createKeyPair(final String privateKey) {
    return SECP256R1_SIGNATURE_ALGORITHM.createKeyPair(
        SECP256R1_SIGNATURE_ALGORITHM.createPrivateKey(Bytes32.fromHexString(privateKey)));
  }
}
