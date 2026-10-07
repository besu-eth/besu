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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.ImmutableNetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultPeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.DefaultRlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgentFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.RlpxAgentFactory;
import org.hyperledger.besu.ethereum.p2p.network.DefaultP2PNetwork;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.permissions.PeerPermissions;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JsonRpcHttpServiceRpcApisTest {

  @TempDir private Path folder;

  private final Vertx vertx = Vertx.vertx();
  private final OkHttpClient client = new OkHttpClient();
  private JsonRpcHttpService service;
  private static String baseUrl;
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final String CLIENT_NODE_NAME = "TestClientVersion/0.1.0";
  private static final String CLIENT_VERSION = "0.1.0";
  private static final String CLIENT_COMMIT = "12345678";
  private static final BigInteger NETWORK_ID = BigInteger.valueOf(123);
  private JsonRpcConfiguration configuration;
  private static final List<String> netServices =
      new ArrayList<>(Arrays.asList("jsonrpc", "ws", "p2p", "metrics"));

  @Mock protected BlockchainQueries blockchainQueries;

  private final JsonRpcTestHelper testHelper = new JsonRpcTestHelper();
  private final NatService natService = new NatService(Optional.empty());

  @BeforeEach
  public void before() {
    configuration = JsonRpcConfiguration.createDefault();
    configuration.setPort(0);
  }

  @AfterEach
  public void after() {
    service.stop().join();
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenDefaultApisEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(configuration);
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}",
            JSON);

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenNetApiIsEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.NET.name());
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}",
            JSON);

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  @Test
  public void requestWithNetMethodShouldSuccessWithCode200WhenNetApiIsNotEnabled()
      throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.WEB3.name());
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}",
            JSON);

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      // Check general format of result
      final JsonObject json = new JsonObject(resp.body().string());
      final RpcErrorType expectedError = RpcErrorType.METHOD_NOT_ENABLED;
      testHelper.assertValidJsonRpcError(
          json, id, expectedError.getCode(), expectedError.getMessage());
    }
  }

  @Test
  public void requestWithNetMethodShouldSucceedWhenNetApiAndOtherIsEnabled() throws Exception {
    service = createJsonRpcHttpServiceWithRpcApis(RpcApis.NET.name(), RpcApis.WEB3.name());
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}",
            JSON);

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {
      assertThat(resp.code()).isEqualTo(200);
    }
  }

  private JsonRpcConfiguration createJsonRpcConfigurationWithRpcApis(final String... rpcApis) {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setCorsAllowedDomains(singletonList("*"));
    config.setPort(0);
    if (rpcApis != null) {
      config.setRpcApis(Lists.newArrayList(rpcApis));
    }
    return config;
  }

  private JsonRpcHttpService createJsonRpcHttpServiceWithRpcApis(final String... rpcApis)
      throws Exception {
    return createJsonRpcHttpServiceWithRpcApis(createJsonRpcConfigurationWithRpcApis(rpcApis));
  }

  private JsonRpcHttpService createJsonRpcHttpServiceWithRpcApis(final JsonRpcConfiguration config)
      throws Exception {
    setupMocksRequiredForBlockchainGenesisHash();
    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);

    final Map<String, JsonRpcMethod> rpcMethods =
        new JsonRpcMethodsFactory()
            .methods(
                CLIENT_NODE_NAME,
                CLIENT_VERSION,
                CLIENT_COMMIT,
                NETWORK_ID,
                new StubGenesisConfigOptions(),
                mock(P2PNetwork.class),
                blockchainQueries,
                mock(Synchronizer.class),
                ProtocolScheduleFixture.TESTING_NETWORK,
                mock(ProtocolContext.class),
                mock(FilterManager.class),
                mock(TransactionPool.class),
                mock(MiningConfiguration.class),
                new NoopMiningCoordinator(),
                new NoOpMetricsSystem(),
                supportedCapabilities,
                Optional.of(mock(AccountLocalConfigPermissioningController.class)),
                Optional.of(mock(NodeLocalConfigPermissioningController.class)),
                config.getRpcApis(),
                mock(JsonRpcConfiguration.class),
                mock(WebSocketConfiguration.class),
                mock(MetricsConfiguration.class),
                mock(GraphQLConfiguration.class),
                natService,
                new HashMap<>(),
                folder,
                mock(EthPeers.class),
                vertx,
                mock(ApiConfiguration.class),
                Optional.empty(),
                mock(TransactionSimulator.class),
                new DeterministicEthScheduler());
    final JsonRpcHttpService jsonRpcHttpService =
        new JsonRpcHttpService(
            vertx,
            folder,
            config,
            new NoOpMetricsSystem(),
            natService,
            rpcMethods,
            HealthService.ALWAYS_HEALTHY,
            HealthService.ALWAYS_HEALTHY);
    jsonRpcHttpService.start().join();

    baseUrl = jsonRpcHttpService.url();
    return jsonRpcHttpService;
  }

  private Request buildRequest(final RequestBody body) {
    return new Request.Builder().post(body).url(baseUrl).build();
  }

  private JsonRpcConfiguration createJsonRpcConfiguration() {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setEnabled(true);
    return config;
  }

  private WebSocketConfiguration createWebSocketConfiguration() {
    final WebSocketConfiguration config = WebSocketConfiguration.createDefault();
    config.setEnabled(true);
    return config;
  }

  private P2PNetwork createP2pNetwork() {
    final NetworkingConfiguration config =
        ImmutableNetworkingConfiguration.builder()
            .rlpxConfiguration(RlpxConfiguration.create().setBindPort(0))
            .discoveryConfiguration(DiscoveryConfiguration.create().setBindPort(0))
            .build();

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    final Block genesisBlock = mock(Block.class);
    when(blockchain.getGenesisBlock()).thenReturn(genesisBlock);
    when(genesisBlock.getHash()).thenReturn(Hash.ZERO);

    NodeKey nodeKey = NodeKeyUtils.generate();
    final PeerDiscoveryAgentFactory peerDiscoveryAgentFactory =
        DefaultPeerDiscoveryAgentFactory.builder()
            .nodeKey(nodeKey)
            .config(config)
            .peerPermissions(PeerPermissions.noop())
            .metricsSystem(new NoOpMetricsSystem())
            .storageProvider(new InMemoryKeyValueStorageProvider())
            .blockchain(blockchain)
            .blockNumberForks(Collections.emptyList())
            .timestampForks(Collections.emptyList())
            .build();

    final RlpxAgentFactory defaultRlpxFactory =
        DefaultRlpxAgentFactory.builder()
            .nodeKey(nodeKey)
            .config(config)
            .peerPermissions(PeerPermissions.noop())
            .metricsSystem(new NoOpMetricsSystem())
            .allConnectionsSupplier(Stream::empty)
            .allActiveConnectionsSupplier(Stream::empty)
            .maxPeers(25)
            .build();

    final P2PNetwork p2pNetwork =
        DefaultP2PNetwork.builder()
            .supportedCapabilities(EthProtocol.LATEST)
            .nodeKey(nodeKey)
            .vertx(vertx)
            .config(config)
            .metricsSystem(new NoOpMetricsSystem())
            .rlpxAgentFactory(defaultRlpxFactory)
            .peerDiscoveryAgentFactory(peerDiscoveryAgentFactory)
            .build();

    p2pNetwork.start();
    return p2pNetwork;
  }

  private MetricsConfiguration createMetricsConfiguration() {
    return MetricsConfiguration.builder().enabled(true).port(0).build();
  }

  private JsonRpcHttpService createJsonRpcHttpService(
      final JsonRpcConfiguration jsonRpcConfiguration,
      final WebSocketConfiguration webSocketConfiguration,
      final P2PNetwork p2pNetwork,
      final MetricsConfiguration metricsConfiguration,
      final NatService natService) {
    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);
    jsonRpcConfiguration.setPort(0);
    webSocketConfiguration.setPort(0);

    setupMocksRequiredForBlockchainGenesisHash();

    final Map<String, JsonRpcMethod> rpcMethods =
        new JsonRpcMethodsFactory()
            .methods(
                CLIENT_NODE_NAME,
                CLIENT_VERSION,
                CLIENT_COMMIT,
                NETWORK_ID,
                new StubGenesisConfigOptions(),
                p2pNetwork,
                blockchainQueries,
                mock(Synchronizer.class),
                ProtocolScheduleFixture.TESTING_NETWORK,
                mock(ProtocolContext.class),
                mock(FilterManager.class),
                mock(TransactionPool.class),
                mock(MiningConfiguration.class),
                new NoopMiningCoordinator(),
                new NoOpMetricsSystem(),
                supportedCapabilities,
                Optional.of(mock(AccountLocalConfigPermissioningController.class)),
                Optional.of(mock(NodeLocalConfigPermissioningController.class)),
                jsonRpcConfiguration.getRpcApis(),
                jsonRpcConfiguration,
                webSocketConfiguration,
                metricsConfiguration,
                mock(GraphQLConfiguration.class),
                natService,
                new HashMap<>(),
                folder,
                mock(EthPeers.class),
                vertx,
                mock(ApiConfiguration.class),
                Optional.empty(),
                mock(TransactionSimulator.class),
                new DeterministicEthScheduler());
    final JsonRpcHttpService jsonRpcHttpService =
        new JsonRpcHttpService(
            vertx,
            folder,
            jsonRpcConfiguration,
            new NoOpMetricsSystem(),
            natService,
            rpcMethods,
            HealthService.ALWAYS_HEALTHY,
            HealthService.ALWAYS_HEALTHY);
    jsonRpcHttpService.start().join();

    baseUrl = jsonRpcHttpService.url();
    return jsonRpcHttpService;
  }

  @Test
  public void netServicesTestWhenJsonrpcWebsocketP2pNetworkAndMatricesIsEnabled() throws Exception {
    final boolean[] servicesStates = new boolean[netServices.size()];
    Arrays.fill(servicesStates, Boolean.TRUE); // All services are enabled
    service = getJsonRpcHttpService(servicesStates);

    final RequestBody body = createNetServicesRequestBody();

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {

      final JsonObject responseBody = new JsonObject(resp.body().string());
      for (int j = 0; j < netServices.size(); j++) {
        assertNetService(servicesStates, responseBody, netServices.get(j));
      }
    }
  }

  @Test
  public void netServicesTestWhenOneIsEnabled() throws Exception {

    for (int i = 0; i < netServices.size(); i++) {

      final boolean[] servicesStates = new boolean[netServices.size()];
      final int enabledServiceIndex = i % netServices.size();
      servicesStates[enabledServiceIndex] = true; // enable only one service at a time
      service = getJsonRpcHttpService(servicesStates);

      final RequestBody body = createNetServicesRequestBody();
      try (final Response resp = client.newCall(buildRequest(body)).execute()) {
        final JsonObject responseBody = new JsonObject(resp.body().string());

        for (int j = 0; j < netServices.size(); j++) {
          assertNetService(servicesStates, responseBody, netServices.get(j));
        }
      }
      service.stop().join();
    }
  }

  private void assertNetService(
      final boolean[] servicesStates, final JsonObject jsonBody, final String serviceName) {

    final boolean isAssertTrue = servicesStates[netServices.indexOf(serviceName)];

    final JsonObject result = jsonBody.getJsonObject("result");
    final JsonObject serviceElement = result.getJsonObject(serviceName);
    if (isAssertTrue) {
      assertThat(
              serviceElement != null
                  && serviceElement.containsKey("host")
                  && serviceElement.containsKey("port"))
          .isTrue();
    } else {
      assertThat(
              serviceElement != null
                  && serviceElement.containsKey("host")
                  && serviceElement.containsKey("port"))
          .isFalse();
    }
  }

  public RequestBody createNetServicesRequestBody() {
    final String id = "123";
    return RequestBody.create(
        "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_services\"}", JSON);
  }

  public JsonRpcHttpService getJsonRpcHttpService(final boolean[] enabledNetServices) {

    setupMocksRequiredForBlockchainGenesisHash();

    JsonRpcConfiguration jsonRpcConfiguration = JsonRpcConfiguration.createDefault();
    WebSocketConfiguration webSocketConfiguration = WebSocketConfiguration.createDefault();
    P2PNetwork p2pNetwork = mock(P2PNetwork.class);
    MetricsConfiguration metricsConfiguration = MetricsConfiguration.builder().build();
    final NatService natService = mock(NatService.class);

    if (enabledNetServices[netServices.indexOf("jsonrpc")]) {
      jsonRpcConfiguration = createJsonRpcConfiguration();
    }

    if (enabledNetServices[netServices.indexOf("ws")]) {
      webSocketConfiguration = createWebSocketConfiguration();
    }
    if (enabledNetServices[netServices.indexOf("p2p")]) {
      p2pNetwork = createP2pNetwork();
    }
    if (enabledNetServices[netServices.indexOf("metrics")]) {
      metricsConfiguration = createMetricsConfiguration();
    }

    return createJsonRpcHttpService(
        jsonRpcConfiguration, webSocketConfiguration, p2pNetwork, metricsConfiguration, natService);
  }

  private void setupMocksRequiredForBlockchainGenesisHash() {
    Blockchain blockchain = mock(Blockchain.class);
    Block block = mock(Block.class);
    lenient().when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlock()).thenReturn(block);
    lenient().when(block.getHash()).thenReturn(Hash.EMPTY);
  }

  @Test
  public void netServicesTestWhenJsonrpcWebsocketP2pNetworkAndMatricesIsDisabled()
      throws Exception {
    service =
        createJsonRpcHttpService(
            JsonRpcConfiguration.createDefault(),
            WebSocketConfiguration.createDefault(),
            mock(P2PNetwork.class),
            MetricsConfiguration.builder().build(),
            natService);
    final RequestBody body = createNetServicesRequestBody();

    try (final Response resp = client.newCall(buildRequest(body)).execute()) {
      final JsonObject json = new JsonObject(resp.body().string());
      final JsonObject result = json.getJsonObject("result");
      assertThat(result.isEmpty()).isTrue();
    }
  }
}
