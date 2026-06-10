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
package org.hyperledger.besu.tests.acceptance.config;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.Node;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.Cluster;
import org.hyperledger.besu.tests.acceptance.dsl.node.cluster.ClusterConfigurationBuilder;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.net.ServerSocket;
import java.util.List;
import java.util.Optional;

import com.google.common.net.InetAddresses;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.ethereum.beacon.discovery.schema.EnrField;
import org.ethereum.beacon.discovery.schema.IdentitySchema;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BootNodesGenesisSetupTest extends AcceptanceTestBase {
  private static final String CURVE_NAME = "secp256k1";
  private static final String ALGORITHM = SignatureAlgorithm.ALGORITHM;

  private static ECDomainParameters curve;

  private Cluster noDiscoveryCluster;

  @BeforeAll
  public static void environment() {
    final X9ECParameters params = SECNamedCurves.getByName(CURVE_NAME);
    curve = new ECDomainParameters(params.getCurve(), params.getG(), params.getN(), params.getH());
  }

  @BeforeEach
  public void setUp() {
    noDiscoveryCluster =
        new Cluster(new ClusterConfigurationBuilder().awaitPeerDiscovery(false).build(), net);
  }

  @AfterEach
  @Override
  public void tearDownAcceptanceTestBase() {
    noDiscoveryCluster.stop();
    super.tearDownAcceptanceTestBase();
  }

  @Test
  public void shouldConnectNodesViaV4EnodeBootnodesInGenesis() throws Exception {
    int nodeAPort, nodeBPort;
    try (ServerSocket nodeASocket = new ServerSocket(0);
        ServerSocket nodeBSocket = new ServerSocket(0)) {
      nodeAPort = nodeASocket.getLocalPort();
      nodeBPort = nodeBSocket.getLocalPort();
    }

    final KeyPair nodeAKeyPair =
        createKeyPair(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"));
    final KeyPair nodeBKeyPair =
        createKeyPair(
            Bytes32.fromHexString(
                "0xc87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"));

    final Node nodeA =
        besu.createNode(
            "nodeA",
            b ->
                configureV4Node(
                    b, nodeAPort, nodeAKeyPair, nodeBKeyPair.getPublicKey(), nodeBPort));
    final Node nodeB =
        besu.createNode(
            "nodeB",
            b ->
                configureV4Node(
                    b, nodeBPort, nodeBKeyPair, nodeAKeyPair.getPublicKey(), nodeAPort));

    noDiscoveryCluster.start(nodeA, nodeB);

    nodeA.verify(net.awaitPeerCount(1));
    nodeA.verify(admin.hasPeer(nodeB));
    nodeB.verify(admin.hasPeer(nodeA));
  }

  @Test
  public void shouldConnectNodesViaV5EnrBootnodesInGenesis() throws Exception {
    int nodeAPort, nodeBPort;
    try (ServerSocket nodeASocket = new ServerSocket(0);
        ServerSocket nodeBSocket = new ServerSocket(0)) {
      nodeAPort = nodeASocket.getLocalPort();
      nodeBPort = nodeBSocket.getLocalPort();
    }

    final KeyPair nodeAKeyPair =
        createKeyPair(
            Bytes32.fromHexString(
                "0x8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"));
    final KeyPair nodeBKeyPair =
        createKeyPair(
            Bytes32.fromHexString(
                "0xc87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3"));

    final String nodeAEnr = buildBootnodeEnr(nodeAKeyPair, nodeAPort, nodeAPort);

    final Node nodeA =
        besu.createNode("nodeA", b -> configureV5Node(b, nodeAPort, nodeAKeyPair, null));
    final Node nodeB =
        besu.createNode("nodeB", b -> configureV5Node(b, nodeBPort, nodeBKeyPair, nodeAEnr));

    noDiscoveryCluster.start(nodeA, nodeB);

    nodeA.verify(net.awaitPeerCount(1));
    nodeA.verify(admin.hasPeer(nodeB));
    nodeB.verify(admin.hasPeer(nodeA));
  }

  private KeyPair createKeyPair(final Bytes32 privateKey) {
    return KeyPair.create(SECPPrivateKey.create(privateKey, ALGORITHM), curve, ALGORITHM);
  }

  private BesuNodeConfigurationBuilder configureV4Node(
      final BesuNodeConfigurationBuilder b,
      final int p2pPort,
      final KeyPair keyPair,
      final SECPPublicKey peerPublicKey,
      final int peerP2pPort) {
    return b.devMode(false)
        .keyPair(keyPair)
        .p2pPort(p2pPort)
        .genesisConfigProvider(
            nodes ->
                Optional.of(
                    String.format(
                        "{\"config\":{\"ethash\":{},\"discovery\":{\"bootnodes\":[\"enode://%s@127.0.0.1:%d\"]}},\"gasLimit\":\"0x1\",\"difficulty\":\"0x1\"}",
                        peerPublicKey.toString().substring(2), peerP2pPort)))
        .bootnodeEligible(false)
        .discoveryV5Enabled(false)
        .jsonRpcEnabled()
        .jsonRpcAdmin();
  }

  private BesuNodeConfigurationBuilder configureV5Node(
      final BesuNodeConfigurationBuilder b,
      final int p2pPort,
      final KeyPair keyPair,
      final String bootEnr) {
    final String discoverySection =
        bootEnr != null
            ? String.format("\"discovery\":{\"v5bootnodes\":[\"%s\"]}", bootEnr)
            : "\"discovery\":{}";
    return b.devMode(false)
        .keyPair(keyPair)
        .p2pPort(p2pPort)
        .genesisConfigProvider(
            nodes ->
                Optional.of(
                    String.format(
                        "{\"config\":{\"ethash\":{},%s},\"gasLimit\":\"0x1\",\"difficulty\":\"0x1\"}",
                        discoverySection)))
        .bootnodeEligible(false)
        .jsonRpcEnabled()
        .jsonRpcAdmin();
  }

  private String buildBootnodeEnr(final KeyPair keyPair, final int udpPort, final int tcpPort) {
    final SignatureAlgorithm algo = SignatureAlgorithmFactory.getInstance();
    final List<EnrField> fields =
        List.of(
            new EnrField(EnrField.ID, IdentitySchema.V4),
            new EnrField(algo.getCurveName(), algo.compressPublicKey(keyPair.getPublicKey())),
            new EnrField(
                EnrField.IP_V4, Bytes.of(InetAddresses.forString("127.0.0.1").getAddress())),
            new EnrField(EnrField.TCP, tcpPort),
            new EnrField(EnrField.UDP, udpPort));
    final NodeRecord record = NodeRecordFactory.DEFAULT.createFromValues(UInt64.ONE, fields);
    record.setSignature(
        algo.sign(Hash.keccak256(record.serializeNoSignature()), keyPair)
            .encodedBytes()
            .slice(0, 64));
    return record.asEnr();
  }
}
