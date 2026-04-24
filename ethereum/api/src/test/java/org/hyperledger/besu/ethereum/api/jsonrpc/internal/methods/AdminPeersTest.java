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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.MockPeerConnection;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PeerResult;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.p2p.network.exceptions.P2PDisabledException;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.testutil.TestClock;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import org.apache.tuweni.bytes.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdminPeersTest {

  private AdminPeers adminPeers;

  private final String VALID_NODE_ID =
      "6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0";

  @Mock private EthPeers ethPeers;

  @BeforeEach
  public void before() {
    adminPeers = new AdminPeers(ethPeers);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(adminPeers.getName()).isEqualTo("admin_peers");
  }

  @Test
  public void shouldReturnEmptyPeersListWhenP2PNetworkDoesNotHavePeers() {
    final JsonRpcResponse expectedResponse =
        new JsonRpcSuccessResponse(null, Collections.emptyList());
    final JsonRpcRequestContext request = adminPeers();

    final JsonRpcResponse response = adminPeers.response(request);

    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void shouldReturnExpectedPeerListWhenP2PNetworkHavePeers() {
    final Collection<EthPeer> peerList = peerList();
    final List<PeerResult> expectedPeerResults =
        peerList.stream().map(PeerResult::fromEthPeer).collect(Collectors.toList());

    final JsonRpcRequestContext request = adminPeers();
    final JsonRpcResponse expectedResponse = new JsonRpcSuccessResponse(null, expectedPeerResults);

    when(ethPeers.streamAllPeers())
        .thenReturn(peerList.stream().map(EthPeerImmutableAttributes::from));

    final JsonRpcResponse response = adminPeers.response(request);

    assertThat(Json.encode(response)).isEqualTo(Json.encode(expectedResponse));
  }

  @Test
  public void shouldFailIfP2pDisabled() {
    when(ethPeers.streamAllPeers()).thenThrow(new P2PDisabledException("P2P disabled."));

    final JsonRpcRequestContext request = adminPeers();
    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.P2P_DISABLED);

    Assertions.assertThat(adminPeers.response(request))
        .usingRecursiveComparison()
        .isEqualTo(expectedResponse);
  }

  private Collection<EthPeer> peerList() {
    final PeerInfo peerInfo =
        new PeerInfo(5, "0x0", Collections.emptyList(), 30303, Bytes.fromHexString(VALID_NODE_ID));
    final PeerConnection p =
        MockPeerConnection.create(
            peerInfo,
            new InetSocketAddress("1.2.3.4", 9876),
            new InetSocketAddress("4.3.2.1", 6789));
    final EthPeer ethPeer =
        new EthPeer(
            p,
            c -> {},
            List.of(),
            EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE,
            TestClock.fixed(),
            Collections.emptyList(),
            Bytes.random(64));
    return Lists.newArrayList(ethPeer);
  }

  private JsonRpcRequestContext adminPeers() {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "admin_peers", new Object[] {}));
  }
}
