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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.ChainHead;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.math.BigInteger;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import org.apache.tuweni.bytes.Bytes;

public class AdminNodeInfo implements JsonRpcMethod {

  private final String clientVersion;
  private final BigInteger networkId;
  private final GenesisConfigOptions genesisConfigOptions;
  private final P2PNetwork peerNetwork;
  private final BlockchainQueries blockchainQueries;
  private final NatService natService;
  private final ProtocolSchedule protocolSchedule;

  public AdminNodeInfo(
      final String clientVersion,
      final BigInteger networkId,
      final GenesisConfigOptions genesisConfigOptions,
      final P2PNetwork peerNetwork,
      final BlockchainQueries blockchainQueries,
      final NatService natService,
      final ProtocolSchedule protocolSchedule) {
    this.peerNetwork = peerNetwork;
    this.clientVersion = clientVersion;
    this.genesisConfigOptions = genesisConfigOptions;
    this.blockchainQueries = blockchainQueries;
    this.networkId = networkId;
    this.natService = natService;
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public String getName() {
    return RpcMethod.ADMIN_NODE_INFO.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Map<String, Object> response = new HashMap<>();
    final Map<String, Integer> ports = new HashMap<>();

    if (!peerNetwork.isP2pEnabled()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.P2P_DISABLED);
    }
    final Optional<EnodeURLImpl> maybeEnode = peerNetwork.getLocalEnode();
    if (maybeEnode.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.P2P_NETWORK_NOT_RUNNING);
    }
    final EnodeURL enode = maybeEnode.get();

    response.put("enode", enode.toString());
    response.put("ip", enode.getIpAsString());
    final Bytes nodeId = enode.getNodeId();

    final String ip = getIp(enode);
    final int listeningPort = getListeningPort(enode);
    final int discoveryPort = getDiscoveryPort(enode);

    response.put("enode", getNodeAsString(enode, ip, listeningPort, discoveryPort));
    peerNetwork.getLocalEnr().ifPresent(enr -> response.put("enr", enr));
    response.put("ip", ip);

    if (enode.isListening()) {
      response.put("listenAddr", formatHostPort(ip, listeningPort));
    }
    response.put("id", nodeId.toUnprefixedHexString());
    response.put("name", clientVersion);

    final Optional<P2PNetwork.IPv6AddressInfo> maybeIpv6Info = peerNetwork.getIPv6AddressInfo();
    maybeIpv6Info.ifPresent(
        ipv6Info -> {
          response.put("ipv6", ipv6Info.address());
          ipv6Info
              .listeningPort()
              .ifPresent(
                  tcpV6 -> response.put("listenAddrV6", formatHostPort(ipv6Info.address(), tcpV6)));
        });

    if (enode.isRunningDiscovery()) {
      ports.put("discovery", discoveryPort);
    }
    if (enode.isListening()) {
      ports.put("listener", listeningPort);
    }
    maybeIpv6Info.ifPresent(
        ipv6Info -> {
          ipv6Info.discoveryPort().ifPresent(udpV6 -> ports.put("discoveryV6", udpV6));
          ipv6Info.listeningPort().ifPresent(tcpV6 -> ports.put("listenerV6", tcpV6));
        });
    response.put("ports", ports);

    final ChainHead chainHead = blockchainQueries.getBlockchain().getChainHead();
    response.put(
        "protocols",
        ImmutableMap.of(
            "eth",
            ImmutableMap.of(
                "config",
                genesisConfigOptions.asMap(),
                "difficulty",
                chainHead.getTotalDifficulty().toBigInteger(),
                "genesis",
                blockchainQueries.getBlockHashByNumber(0).get().toString(),
                "head",
                chainHead.getHash().toString(),
                "network",
                networkId)));

    response.put(
        "activeFork",
        protocolSchedule
            .getByBlockHeader(chainHead.getBlockHeader())
            .getHardforkId()
            .description());

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), response);
  }

  private String getNodeAsString(
      final EnodeURL enodeURL, final String ip, final int listeningPort, final int discoveryPort) {
    final String host = bracketIpv6(ip);
    final String uri =
        String.format(
            "enode://%s@%s:%d", enodeURL.getNodeId().toUnprefixedHexString(), host, listeningPort);
    if (listeningPort != discoveryPort) {
      return URI.create(uri + String.format("?discport=%d", discoveryPort)).toString();
    } else {
      return URI.create(uri).toString();
    }
  }

  private String getIp(final EnodeURL enode) {
    return natService.queryExternalIPAddress(enode.getIpAsString());
  }

  private int getDiscoveryPort(final EnodeURL enode) {
    return natService
        .getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP)
        .map(NatPortMapping::getExternalPort)
        .orElseGet(enode::getDiscoveryPortOrZero);
  }

  private int getListeningPort(final EnodeURL enode) {
    return natService
        .getPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP)
        .map(NatPortMapping::getExternalPort)
        .orElseGet(enode::getListeningPortOrZero);
  }

  private static String formatHostPort(final String host, final int port) {
    return String.format("%s:%d", bracketIpv6(host), port);
  }

  private static String bracketIpv6(final String host) {
    if (host.startsWith("[")) {
      return host;
    }
    return host.contains(":") ? "[" + host + "]" : host;
  }
}
