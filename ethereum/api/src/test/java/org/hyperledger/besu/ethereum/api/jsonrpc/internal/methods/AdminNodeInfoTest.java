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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.LONDON;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.DefaultPeer;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AdminNodeInfoTest {

  @Mock private P2PNetwork p2pNetwork;
  @Mock private Blockchain blockchain;
  @Mock private BlockchainQueries blockchainQueries;
  @Mock private NatService natService;
  @Mock private BlockHeader blockHeader;
  @Mock private ProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;

  private AdminNodeInfo method;

  private final Bytes nodeId =
      Bytes.fromHexString(
          "0x0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
  private final GenesisConfigOptions genesisConfigOptions =
      new StubGenesisConfigOptions().chainId(BigInteger.valueOf(2019));
  private final DefaultPeer defaultPeer =
      DefaultPeer.fromEnodeURL(
          EnodeURLImpl.builder()
              .nodeId(nodeId)
              .ipAddress("1.2.3.4")
              .discoveryPort(7890)
              .listeningPort(30303)
              .build());

  @BeforeEach
  public void setup() {
    when(blockHeader.getHash()).thenReturn(Hash.EMPTY);
    final ChainHead testChainHead = new ChainHead(blockHeader, Difficulty.ONE, 1L);

    when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    when(blockchainQueries.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(Hash.EMPTY));
    when(blockchain.getChainHead()).thenReturn(testChainHead);
    when(natService.queryExternalIPAddress(anyString())).thenReturn("1.2.3.4");
    when(p2pNetwork.getLocalEnr()).thenReturn(Optional.of("enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test"));
    when(protocolSpec.getHardforkId()).thenReturn(LONDON);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    method =
        new AdminNodeInfo(
            "testnet/1.0/this/that",
            BigInteger.valueOf(2018),
            genesisConfigOptions,
            p2pNetwork,
            blockchainQueries,
            natService,
            protocolSchedule);
  }

  @Test
  public void shouldReturnCorrectResult() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));
    final JsonRpcRequestContext request = adminNodeInfo();

    final Map<String, Object> expected = new HashMap<>();
    expected.put("activeFork", "London");
    expected.put(
        "enode",
        "enode://0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807@1.2.3.4:30303?discport=7890");
    expected.put("enr", "enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test");
    expected.put(
        "id",
        "0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
    expected.put("ip", "1.2.3.4");
    expected.put("listenAddr", "1.2.3.4:30303");
    expected.put("name", "testnet/1.0/this/that");
    expected.put("ports", ImmutableMap.of("discovery", 7890, "listener", 30303));
    expected.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                BigInteger.ONE,
                "genesis",
                Hash.EMPTY.toString(),
                "head",
                Hash.EMPTY.toString(),
                "network",
                BigInteger.valueOf(2018))));

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse actual = (JsonRpcSuccessResponse) response;
    assertThat(actual.getResult()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void shouldReturnCorrectResultWhenIsNatEnvironment() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));

    when(natService.queryExternalIPAddress("1.2.3.4")).thenReturn("3.4.5.6");
    when(natService.getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP))
        .thenReturn(
            Optional.of(
                new NatPortMapping(
                    NatServiceType.DISCOVERY, NetworkProtocol.UDP, "", "", 8080, 8080)));
    when(natService.getPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP))
        .thenReturn(
            Optional.of(
                new NatPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP, "", "", 8081, 8081)));

    final JsonRpcRequestContext request = adminNodeInfo();

    final Map<String, Object> expected = new HashMap<>();
    expected.put("activeFork", "London");
    expected.put(
        "enode",
        "enode://0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807@3.4.5.6:8081?discport=8080");
    expected.put("enr", "enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test");
    expected.put(
        "id",
        "0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
    expected.put("ip", "3.4.5.6");
    expected.put("listenAddr", "3.4.5.6:8081");
    expected.put("name", "testnet/1.0/this/that");
    expected.put("ports", ImmutableMap.of("discovery", 8080, "listener", 8081));
    expected.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                BigInteger.ONE,
                "genesis",
                Hash.EMPTY.toString(),
                "head",
                Hash.EMPTY.toString(),
                "network",
                BigInteger.valueOf(2018))));

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse actual = (JsonRpcSuccessResponse) response;
    assertThat(actual.getResult()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void handlesLocalEnodeWithListeningAndDiscoveryDisabled() {
    final EnodeURLImpl localEnode =
        EnodeURLImpl.builder()
            .nodeId(nodeId)
            .ipAddress("1.2.3.4")
            .discoveryAndListeningPorts(0)
            .build();

    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(localEnode));
    final JsonRpcRequestContext request = adminNodeInfo();

    final Map<String, Object> expected = new HashMap<>();
    expected.put("activeFork", "London");
    expected.put(
        "enode",
        "enode://0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807@1.2.3.4:0");
    expected.put("enr", "enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test");
    expected.put(
        "id",
        "0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
    expected.put("ip", "1.2.3.4");
    expected.put("name", "testnet/1.0/this/that");
    expected.put("ports", Collections.emptyMap());
    expected.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                BigInteger.ONE,
                "genesis",
                Hash.EMPTY.toString(),
                "head",
                Hash.EMPTY.toString(),
                "network",
                BigInteger.valueOf(2018))));

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse actual = (JsonRpcSuccessResponse) response;
    assertThat(actual.getResult()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void handlesLocalEnodeWithListeningDisabled() {
    final EnodeURLImpl localEnode =
        EnodeURLImpl.builder()
            .nodeId(nodeId)
            .ipAddress("1.2.3.4")
            .discoveryAndListeningPorts(0)
            .discoveryPort(7890)
            .build();

    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(localEnode));
    final JsonRpcRequestContext request = adminNodeInfo();

    final Map<String, Object> expected = new HashMap<>();
    expected.put("activeFork", "London");
    expected.put(
        "enode",
        "enode://0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807@1.2.3.4:0?discport=7890");
    expected.put("enr", "enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test");
    expected.put(
        "id",
        "0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
    expected.put("ip", "1.2.3.4");
    expected.put("name", "testnet/1.0/this/that");
    expected.put("ports", ImmutableMap.of("discovery", 7890));
    expected.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                BigInteger.ONE,
                "genesis",
                Hash.EMPTY.toString(),
                "head",
                Hash.EMPTY.toString(),
                "network",
                BigInteger.valueOf(2018))));

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse actual = (JsonRpcSuccessResponse) response;
    assertThat(actual.getResult()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void handlesLocalEnodeWithDiscoveryDisabled() {
    final EnodeURLImpl localEnode =
        EnodeURLImpl.builder()
            .nodeId(nodeId)
            .ipAddress("1.2.3.4")
            .discoveryAndListeningPorts(0)
            .listeningPort(7890)
            .build();

    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(localEnode));
    final JsonRpcRequestContext request = adminNodeInfo();

    final Map<String, Object> expected = new HashMap<>();
    expected.put("activeFork", "London");
    expected.put(
        "enode",
        "enode://0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807@1.2.3.4:7890?discport=0");
    expected.put("enr", "enr:-JG4QF0FFhEXDu_G-1LD5lkWh5-test");
    expected.put(
        "id",
        "0f1b319e32017c3fcb221841f0f978701b4e9513fe6a567a2db43d43381a9c7e3dfe7cae13cbc2f56943400bacaf9082576ab087cd51983b17d729ae796f6807");
    expected.put("ip", "1.2.3.4");
    expected.put("listenAddr", "1.2.3.4:7890");
    expected.put("name", "testnet/1.0/this/that");
    expected.put("ports", ImmutableMap.of("listener", 7890));
    expected.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                BigInteger.ONE,
                "genesis",
                Hash.EMPTY.toString(),
                "head",
                Hash.EMPTY.toString(),
                "network",
                BigInteger.valueOf(2018))));

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final JsonRpcSuccessResponse actual = (JsonRpcSuccessResponse) response;
    assertThat(actual.getResult()).usingRecursiveComparison().isEqualTo(expected);
  }

  @Test
  public void returnsErrorWhenP2PDisabled() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(false);
    final JsonRpcRequestContext request = adminNodeInfo();

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(request.getRequest().getId(), RpcErrorType.P2P_DISABLED);

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  public void returnsErrorWhenP2PNotReady() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.empty());
    final JsonRpcRequestContext request = adminNodeInfo();

    final JsonRpcResponse expectedResponse =
        new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.P2P_NETWORK_NOT_RUNNING);

    final JsonRpcResponse response = method.response(request);
    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(response).usingRecursiveComparison().isEqualTo(expectedResponse);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnIPv6FieldsWhenDualStack() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));
    when(p2pNetwork.getIPv6AddressInfo())
        .thenReturn(
            Optional.of(
                new P2PNetwork.IPv6AddressInfo(
                    "2001:db8::1", Optional.of(30304), Optional.of(30305))));

    final JsonRpcResponse response = method.response(adminNodeInfo());
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Map<String, Object> result =
        (Map<String, Object>) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(result).containsEntry("ipv6", "2001:db8::1");
    assertThat(result).containsEntry("listenAddrV6", "[2001:db8::1]:30304");

    final Map<String, Integer> ports = (Map<String, Integer>) result.get("ports");
    assertThat(ports).containsEntry("discoveryV6", 30305);
    assertThat(ports).containsEntry("listenerV6", 30304);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotReturnIPv6FieldsWhenIPv4Only() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));

    final JsonRpcResponse response = method.response(adminNodeInfo());
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Map<String, Object> result =
        (Map<String, Object>) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(result).doesNotContainKey("ipv6");
    assertThat(result).doesNotContainKey("listenAddrV6");

    final Map<String, Integer> ports = (Map<String, Integer>) result.get("ports");
    assertThat(ports).doesNotContainKey("discoveryV6");
    assertThat(ports).doesNotContainKey("listenerV6");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnIPv6AddressWithoutPortsWhenNoIPv6Ports() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));
    when(p2pNetwork.getIPv6AddressInfo())
        .thenReturn(
            Optional.of(
                new P2PNetwork.IPv6AddressInfo("2001:db8::1", Optional.empty(), Optional.empty())));

    final JsonRpcResponse response = method.response(adminNodeInfo());
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Map<String, Object> result =
        (Map<String, Object>) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(result).containsEntry("ipv6", "2001:db8::1");
    assertThat(result).doesNotContainKey("listenAddrV6");

    final Map<String, Integer> ports = (Map<String, Integer>) result.get("ports");
    assertThat(ports).doesNotContainKey("discoveryV6");
    assertThat(ports).doesNotContainKey("listenerV6");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnOnlyListenerV6WhenDiscoveryV6Absent() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(defaultPeer.getEnodeURL()));
    when(p2pNetwork.getIPv6AddressInfo())
        .thenReturn(
            Optional.of(
                new P2PNetwork.IPv6AddressInfo(
                    "2001:db8::1", Optional.of(30304), Optional.empty())));

    final JsonRpcResponse response = method.response(adminNodeInfo());
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Map<String, Object> result =
        (Map<String, Object>) ((JsonRpcSuccessResponse) response).getResult();

    assertThat(result).containsEntry("ipv6", "2001:db8::1");
    assertThat(result).containsEntry("listenAddrV6", "[2001:db8::1]:30304");

    final Map<String, Integer> ports = (Map<String, Integer>) result.get("ports");
    assertThat(ports).containsEntry("listenerV6", 30304);
    assertThat(ports).doesNotContainKey("discoveryV6");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBracketIPv6InListenAddr() {
    final EnodeURLImpl ipv6Enode =
        EnodeURLImpl.builder()
            .nodeId(nodeId)
            .ipAddress("2001:db8::1")
            .discoveryAndListeningPorts(30303)
            .build();

    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getLocalEnode()).thenReturn(Optional.of(ipv6Enode));
    // Override the default anyString() stub from setUp — NAT returns input unchanged for IPv6
    when(natService.queryExternalIPAddress(anyString()))
        .thenAnswer(invocation -> invocation.getArgument(0));

    final JsonRpcResponse response = method.response(adminNodeInfo());
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    final Map<String, Object> result =
        (Map<String, Object>) ((JsonRpcSuccessResponse) response).getResult();

    // InetAddress.getHostAddress() may return compressed or expanded IPv6 representation
    assertThat(result.get("listenAddr").toString()).matches("\\[.+]:30303");
    assertThat(result.get("ip").toString()).contains("2001:db8");
    assertThat(result.get("enode").toString()).matches("enode://.*@\\[.+]:30303");
  }

  private JsonRpcRequestContext adminNodeInfo() {
    return new JsonRpcRequestContext(new JsonRpcRequest("2.0", "admin_nodeInfo", new Object[] {}));
  }
}
