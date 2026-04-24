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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.testutil.TestClock;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminJsonRpcHttpServiceTest extends JsonRpcHttpServiceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AdminJsonRpcHttpServiceTest.class);

  @BeforeAll
  public static void setup() throws Exception {
    initServerAndClient();
  }

  @Test
  public void getPeers() throws Exception {
    final List<Capability> caps = new ArrayList<>();
    caps.add(EthProtocol.LATEST);
    final List<EthPeer> peerList = new ArrayList<>();
    final PeerInfo info1 =
        new PeerInfo(
            4, CLIENT_NODE_NAME, caps, 30302, Bytes.fromHexString(String.format("%0128x", 1)));
    final PeerInfo info2 =
        new PeerInfo(
            4, CLIENT_NODE_NAME, caps, 60302, Bytes.fromHexString(String.format("%0128x", 2)));
    final PeerInfo info3 =
        new PeerInfo(
            4, CLIENT_NODE_NAME, caps, 60303, Bytes.fromHexString(String.format("%0128x", 3)));
    final InetSocketAddress addr30301 = new InetSocketAddress("localhost", 30301);
    final InetSocketAddress addr30302 = new InetSocketAddress("localhost", 30302);
    final InetSocketAddress addr60301 = new InetSocketAddress("localhost", 60301);
    final InetSocketAddress addr60302 = new InetSocketAddress("localhost", 60302);
    final InetSocketAddress addr60303 = new InetSocketAddress("localhost", 60303);

    peerList.add(
        new EthPeer(
            MockPeerConnection.create(info1, addr60301, addr30302),
            c -> {},
            List.of(),
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            TestClock.fixed(),
            Collections.emptyList(),
            Bytes.random(64)));
    peerList.add(
        new EthPeer(
            MockPeerConnection.create(info2, addr30301, addr60302),
            c -> {},
            List.of(),
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            TestClock.fixed(),
            Collections.emptyList(),
            Bytes.random(64)));
    peerList.add(
        new EthPeer(
            MockPeerConnection.create(info3, addr30301, addr60303),
            c -> {},
            List.of(),
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            TestClock.fixed(),
            Collections.emptyList(),
            Bytes.random(64)));

    when(ethPeersMock.streamAllPeers())
        .thenReturn(peerList.stream().map(EthPeerImmutableAttributes::from));
    when(peerDiscoveryMock.getPeerCount()).thenReturn(peerList.size());

    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"admin_peers\"}",
            JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl).build();
    LOG.info("Request: " + request);
    try (final Response resp = client.newCall(request).execute()) {
      LOG.info("Response: " + resp);

      assertThat(resp.code()).isEqualTo(200);
      // Check general format of result
      final JsonObject json = new JsonObject(resp.body().string());
      LOG.info("Response Body: " + json.encodePrettily());
      testHelper.assertValidJsonRpcResult(json, id);
      // Check result
      final JsonArray result = json.getJsonArray("result");

      assertPeerResultMatchesPeer(result, peerList);
    }
  }

  private void assertPeerResultMatchesPeer(
      final JsonArray result, final Collection<EthPeer> peerList) {
    int i = -1;
    for (final EthPeer ethPeer : peerList) {
      final JsonObject peerJson = result.getJsonObject(++i);
      final int jsonVersion = Integer.decode(peerJson.getString("version"));
      final String jsonClient = peerJson.getString("name");
      final List<Capability> caps = getCapabilities(peerJson.getJsonArray("caps"));
      final int jsonPort = Integer.decode(peerJson.getString("port"));
      final Bytes jsonNodeId = Bytes.fromHexString(peerJson.getString("id"));

      final PeerInfo jsonPeer = new PeerInfo(jsonVersion, jsonClient, caps, jsonPort, jsonNodeId);
      assertThat(ethPeer.getConnection().getPeerInfo()).isEqualTo(jsonPeer);
    }
  }

  protected List<Capability> getCapabilities(final JsonArray jsonCaps) {
    final List<Capability> caps = new ArrayList<>();
    for (final Object jsonCap : jsonCaps) {
      final StringTokenizer st = new StringTokenizer(jsonCap.toString(), "/");
      caps.add(Capability.create(st.nextToken(), Integer.parseInt(st.nextToken())));
    }
    return caps;
  }
}
