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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.list;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcHttpService;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.DefaultAuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.AuthenticatedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(VertxExtension.class)
public class WebSocketServiceLoginTest {
  private static final int VERTX_AWAIT_TIMEOUT_MILLIS = 10000;

  @TempDir private Path folder;

  private Vertx vertx;
  private VertxTestContext testContext;
  protected static Map<String, JsonRpcMethod> rpcMethods;
  protected static JsonRpcHttpService service;
  protected static OkHttpClient client;
  protected static String baseUrl;
  protected static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  protected static final String CLIENT_NODE_NAME = "TestClientVersion/0.1.0";
  protected static final String CLIENT_VERSION = "0.1.0";
  protected static final String CLIENT_COMMIT = "12345678";
  protected static final BigInteger CHAIN_ID = BigInteger.valueOf(123);
  protected static P2PNetwork peerDiscoveryMock;
  protected static BlockchainQueries blockchainQueries;
  protected static Synchronizer synchronizer;
  protected static final Collection<String> JSON_RPC_APIS =
      Arrays.asList(
          RpcApis.ETH.name(), RpcApis.NET.name(), RpcApis.WEB3.name(), RpcApis.ADMIN.name());
  protected static final List<String> NO_AUTH_METHODS =
      Arrays.asList(RpcMethod.NET_SERVICES.getMethodName());
  protected static JWTAuth jwtAuth;
  protected static final NatService natService = new NatService(Optional.empty());
  private WebSocketConfiguration websocketConfiguration;
  private WebSocketMessageHandler webSocketMessageHandlerSpy;
  private WebSocketService websocketService;
  private HttpClient httpClient;

  @BeforeEach
  public void before() throws URISyntaxException {
    vertx = Vertx.vertx();
    testContext = new VertxTestContext();

    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource("JsonRpcHttpService/auth.toml").toURI())
            .toAbsolutePath()
            .toString();

    websocketConfiguration = WebSocketConfiguration.createDefault();
    websocketConfiguration.setPort(0);
    websocketConfiguration.setAuthenticationEnabled(true);
    websocketConfiguration.setAuthenticationCredentialsFile(authTomlPath);
    websocketConfiguration.setHostsAllowlist(Collections.singletonList("*"));
    websocketConfiguration.setRpcApisNoAuth(new ArrayList<>(NO_AUTH_METHODS));
    peerDiscoveryMock = mock(P2PNetwork.class);
    blockchainQueries = mock(BlockchainQueries.class);
    synchronizer = mock(Synchronizer.class);

    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);
    final StubGenesisConfigOptions genesisConfigOptions =
        new StubGenesisConfigOptions().constantinopleBlock(0).chainId(CHAIN_ID);

    final Map<String, JsonRpcMethod> websocketMethods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>())
            .methods();

    // mocks so that genesis hash is populated
    Blockchain blockchain = mock(Blockchain.class);
    Block block = mock(Block.class);
    lenient().when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlock()).thenReturn(block);
    lenient().when(block.getHash()).thenReturn(Hash.EMPTY);

    rpcMethods =
        spy(
            new JsonRpcMethodsFactory()
                .methods(
                    CLIENT_NODE_NAME,
                    CLIENT_VERSION,
                    CLIENT_COMMIT,
                    CHAIN_ID,
                    genesisConfigOptions,
                    peerDiscoveryMock,
                    blockchainQueries,
                    synchronizer,
                    MainnetProtocolSchedule.fromConfig(
                        genesisConfigOptions,
                        MiningConfiguration.MINING_DISABLED,
                        new BadBlockManager(),
                        false,
                        BalConfiguration.DEFAULT,
                        new NoOpMetricsSystem()),
                    mock(ProtocolContext.class),
                    mock(FilterManager.class),
                    mock(TransactionPool.class),
                    mock(MiningConfiguration.class),
                    new NoopMiningCoordinator(),
                    new NoOpMetricsSystem(),
                    supportedCapabilities,
                    Optional.empty(),
                    Optional.empty(),
                    JSON_RPC_APIS,
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
                    new DeterministicEthScheduler()));

    websocketMethods.putAll(rpcMethods);
    webSocketMessageHandlerSpy =
        spy(
            new WebSocketMessageHandler(
                vertx,
                new JsonRpcExecutor(
                    new AuthenticatedJsonRpcProcessor(
                        new CombinedJsonRpcProcessor(new NoOpMetricsSystem()),
                        DefaultAuthenticationService.create(vertx, websocketConfiguration).get(),
                        websocketConfiguration.getRpcApisNoAuth()),
                    websocketMethods),
                mock(EthScheduler.class),
                TimeoutOptions.defaultOptions().getTimeoutSeconds()));

    websocketService =
        new WebSocketService(
            vertx, websocketConfiguration, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    websocketService.start().join();
    jwtAuth = websocketService.authenticationService.get().getJwtAuthProvider();

    websocketConfiguration.setPort(websocketService.socketAddress().getPort());

    final HttpClientOptions httpClientOptions =
        new HttpClientOptions()
            .setDefaultHost(websocketConfiguration.getHost())
            .setDefaultPort(websocketConfiguration.getPort());

    httpClient = vertx.createHttpClient(httpClientOptions);
  }

  @AfterEach
  public void after() {
    reset(webSocketMessageHandlerSpy);
    websocketService.stop();
  }

  @Test
  public void loginWithBadCredentials() throws InterruptedException {
    httpClient.request(
        HttpMethod.POST,
        websocketConfiguration.getPort(),
        websocketConfiguration.getHost(),
        "/login",
        request -> {
          request.result().putHeader("Content-Type", "application/json; charset=utf-8");
          request.result().end("{\"username\":\"user\",\"password\":\"pass\"}");
          request
              .result()
              .send(
                  response -> {
                    assertThat(response.result().statusCode()).isEqualTo(401);
                    assertThat(response.result().statusMessage()).isEqualTo("Unauthorized");
                    testContext.completeNow();
                  });
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void loginWithGoodCredentials() throws InterruptedException {
    Handler<AsyncResult<HttpClientResponse>> responseHandler =
        response -> {
          assertThat(response.result().statusCode()).isEqualTo(200);
          assertThat(response.result().statusMessage()).isEqualTo("OK");
          assertThat(response.result().getHeader("Content-Type")).isNotNull();
          assertThat(response.result().getHeader("Content-Type")).isEqualTo("application/json");
          response
              .result()
              .bodyHandler(
                  buffer -> {
                    final String body = buffer.toString();
                    assertThat(body).isNotBlank();

                    final JsonObject respBody = new JsonObject(body);
                    final String token = respBody.getString("token");
                    assertThat(token).isNotNull();
                    assertThat(token).isNotEmpty();

                    websocketService
                        .authenticationService
                        .get()
                        .getJwtAuthProvider()
                        .authenticate(
                            new JsonObject().put("token", token),
                            (r) -> {
                              Assertions.assertThat(r.succeeded()).isTrue();
                              final User user = r.result();
                              user.isAuthorized(
                                  "noauths",
                                  (authed) -> {
                                    assertThat(authed.succeeded()).isTrue();
                                    assertThat(authed.result()).isFalse();
                                  });
                              user.isAuthorized(
                                  "fakePermission",
                                  (authed) -> {
                                    assertThat(authed.succeeded()).isTrue();
                                    assertThat(authed.result()).isTrue();
                                  });
                              user.isAuthorized(
                                  "eth:subscribe",
                                  (authed) -> {
                                    assertThat(authed.succeeded()).isTrue();
                                    assertThat(authed.result()).isTrue();
                                    testContext.completeNow();
                                  });
                            });
                  });
        };
    Handler<AsyncResult<HttpClientRequest>> requestHandler =
        request -> {
          request.result().putHeader("Content-Type", "application/json; charset=utf-8");
          request.result().end("{\"username\":\"user\",\"password\":\"pegasys\"}");
          request.result().send(responseHandler);
        };
    httpClient.request(
        HttpMethod.POST,
        websocketConfiguration.getPort(),
        websocketConfiguration.getHost(),
        "/login",
        requestHandler);

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void websocketServiceWithBadHeaderAuthenticationToken() throws InterruptedException {

    final String request = "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    final String expectedResponse =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-40100,\"message\":\"Unauthorized\"}}";

    WebSocketConnectOptions options = new WebSocketConnectOptions();
    options.setURI("/");
    options.setHost(websocketConfiguration.getHost());
    options.setPort(websocketConfiguration.getPort());
    String badtoken = "badtoken";
    options.addHeader("Authorization", "Bearer " + badtoken);
    httpClient.webSocket(
        options,
        webSocket -> {
          webSocket.result().writeTextMessage(request);

          webSocket
              .result()
              .handler(
                  buffer ->
                      testContext.verify(
                          () -> {
                            assertEquals(expectedResponse, buffer.toString());
                            testContext.completeNow();
                          }));
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void netServicesSucceedWithNoAuth() throws InterruptedException {

    final String id = "123";
    final String request =
        "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_services\"}";

    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":\"123\",\"result\":{}}";

    WebSocketConnectOptions options = new WebSocketConnectOptions();
    options.setURI("/");
    options.setHost(websocketConfiguration.getHost());
    options.setPort(websocketConfiguration.getPort());
    httpClient.webSocket(
        options,
        webSocket -> {
          webSocket.result().writeTextMessage(request);

          webSocket
              .result()
              .handler(
                  buffer ->
                      testContext.verify(
                          () -> {
                            assertEquals(expectedResponse, buffer.toString());
                            testContext.completeNow();
                          }));
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void websocketServiceWithGoodHeaderAuthenticationToken() throws InterruptedException {

    final JWTOptions jwtOptions = new JWTOptions().setExpiresInMinutes(5).setAlgorithm("RS256");

    final JsonObject jwtContents =
        new JsonObject().put("permissions", Lists.newArrayList("eth:*")).put("username", "user");
    final String goodToken = jwtAuth.generateToken(jwtContents, jwtOptions);

    final String requestSub =
        "{\"id\": 1, \"method\": \"eth_subscribe\", \"params\": [\"syncing\"]}";
    final String expectedResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x1\"}";

    WebSocketConnectOptions options = new WebSocketConnectOptions();
    options.setURI("/");
    options.setHost(websocketConfiguration.getHost());
    options.setPort(websocketConfiguration.getPort());
    if (goodToken != null) {
      options.addHeader("Authorization", "Bearer " + goodToken);
    }
    httpClient.webSocket(
        options,
        webSocket -> {
          webSocket.result().writeTextMessage(requestSub);

          webSocket
              .result()
              .handler(
                  buffer ->
                      testContext.verify(
                          () -> {
                            assertEquals(expectedResponse, buffer.toString());
                            testContext.completeNow();
                          }));
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void loginPopulatesJWTPayloadWithRequiredValues() throws InterruptedException {
    httpClient.request(
        HttpMethod.POST,
        websocketConfiguration.getPort(),
        websocketConfiguration.getHost(),
        "/login",
        request -> {
          request.result().putHeader("Content-Type", "application/json; charset=utf-8");
          request.result().end("{\"username\":\"user\",\"password\":\"pegasys\"}");
          request
              .result()
              .send(
                  response -> {
                    response
                        .result()
                        .bodyHandler(
                            buffer -> {
                              final String body = buffer.toString();
                              assertThat(body).isNotBlank();

                              final JsonObject respBody = new JsonObject(body);
                              final String token = respBody.getString("token");

                              final JsonObject jwtPayload = decodeJwtPayload(token);
                              assertThat(jwtPayload.getString("username")).isEqualTo("user");
                              assertThat(jwtPayload.getJsonArray("permissions"))
                                  .isEqualTo(
                                      new JsonArray(
                                          list(
                                              "fakePermission",
                                              "eth:blockNumber",
                                              "eth:subscribe",
                                              "web3:*")));

                              assertThat(jwtPayload.getString("privacyPublicKey"))
                                  .isEqualTo("A1aVtMxLCUHmBVHXoZzzBgPbW/wj5axDpW9X8l91SGo=");

                              assertThat(jwtPayload.containsKey("iat")).isTrue();
                              assertThat(jwtPayload.containsKey("exp")).isTrue();
                              final long tokenExpiry =
                                  jwtPayload.getLong("exp") - jwtPayload.getLong("iat");
                              assertThat(tokenExpiry).isEqualTo(MINUTES.toSeconds(5));

                              testContext.completeNow();
                            });
                  });
        });

    testContext.awaitCompletion(VERTX_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  private JsonObject decodeJwtPayload(final String token) {
    final List<String> tokenParts = Splitter.on('.').splitToList(token);
    final String payload = tokenParts.get(1);
    return new JsonObject(new String(Base64.getUrlDecoder().decode(payload), UTF_8));
  }
}
