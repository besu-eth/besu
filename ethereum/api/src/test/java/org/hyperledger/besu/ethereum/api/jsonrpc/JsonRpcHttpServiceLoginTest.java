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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.list;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.AuthenticationService;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthAccounts;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthBlockNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.NetVersion;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.Web3ClientVersion;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.Web3Sha3;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.NoopMiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Splitter;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.jwt.JWTAuth;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JsonRpcHttpServiceLoginTest {

  // this tempDir is deliberately static
  @TempDir private static Path folder;

  private static final Vertx vertx = Vertx.vertx();

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
  protected static String authPermissionsConfigFilePath = "JsonRpcHttpService/auth.toml";
  protected final JsonRpcTestHelper testHelper = new JsonRpcTestHelper();
  protected static final NatService natService = new NatService(Optional.empty());

  @BeforeAll
  public static void initServerAndClient() throws Exception {
    peerDiscoveryMock = mock(P2PNetwork.class);
    blockchainQueries = mock(BlockchainQueries.class);
    synchronizer = mock(Synchronizer.class);

    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);

    final StubGenesisConfigOptions genesisConfigOptions =
        new StubGenesisConfigOptions().constantinopleBlock(0).chainId(CHAIN_ID);

    // mocks so that genesis hash is populated
    Blockchain blockchain = mock(Blockchain.class);
    Block block = mock(Block.class);
    lenient().when(blockchainQueries.getBlockchain()).thenReturn(blockchain);
    lenient().when(blockchain.getGenesisBlock()).thenReturn(block);
    lenient().when(block.getHash()).thenReturn(Hash.EMPTY);

    rpcMethods =
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
                new DeterministicEthScheduler());
    service = createJsonRpcHttpService();
    jwtAuth = service.authenticationService.get().getJwtAuthProvider();
    service.start().join();

    // Build an OkHttp client.
    client = new OkHttpClient();
    baseUrl = service.url();
  }

  private static JsonRpcHttpService createJsonRpcHttpService() throws Exception {
    final String authTomlPath =
        Paths.get(ClassLoader.getSystemResource(authPermissionsConfigFilePath).toURI())
            .toAbsolutePath()
            .toString();

    final JsonRpcConfiguration config = createJsonRpcConfig();
    config.setAuthenticationEnabled(true);
    config.setAuthenticationCredentialsFile(authTomlPath);
    config.setNoAuthRpcApis(NO_AUTH_METHODS);

    return new JsonRpcHttpService(
        vertx,
        folder,
        config,
        new NoOpMetricsSystem(),
        natService,
        rpcMethods,
        HealthService.ALWAYS_HEALTHY,
        HealthService.ALWAYS_HEALTHY);
  }

  private static JsonRpcConfiguration createJsonRpcConfig() {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setPort(0);
    config.setHostsAllowlist(Collections.singletonList("*"));
    return config;
  }

  /** Tears down the HTTP server. */
  @AfterAll
  public static void shutdownServer() {
    service.stop().join();
  }

  @Test
  public void loginWithEmptyCredentials() throws IOException {
    final RequestBody body = RequestBody.create("{}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(400);
      assertThat(resp.message()).isEqualTo("Bad Request");
      final String bodyString = resp.body().string();
      assertThat(bodyString).containsIgnoringCase("username and password are required");
    }
  }

  @Test
  public void loginWithBadCredentials() throws IOException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"badpass\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(401);
      assertThat(resp.message()).isEqualTo("Unauthorized");
      final String bodyString = resp.body().string();
      assertThat(bodyString).containsIgnoringCase("the username or password is incorrect");
    }
  }

  @Test
  public void loginWithGoodCredentials() throws IOException, InterruptedException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final User user = authenticateAndAwait(token);
      assertThat(user).isNotNull();
      final boolean authed = PermissionBasedAuthorization.create("noauths").match(user);
      assertThat(authed).isFalse();
    }
  }

  @Test
  public void loginWithGoodCredentialsAndPermissions() throws IOException, InterruptedException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final User user = authenticateAndAwait(token);
      assertThat(user).isNotNull();
      final boolean noauths = PermissionBasedAuthorization.create("noauths").match(user);
      assertThat(noauths).isFalse();
      final boolean fakePermission =
          PermissionBasedAuthorization.create("fakePermission").match(user);
      assertThat(fakePermission).isTrue();
    }
  }

  @Test
  public void loginDoesntPopulateJWTPayloadWithPassword()
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final JsonObject jwtPayload = decodeJwtPayload(token);
      final String jwtPayloadString = jwtPayload.encode();
      assertThat(jwtPayloadString.contains("password")).isFalse();
      assertThat(jwtPayloadString.contains("pegasys")).isFalse();
    }
  }

  @Test
  public void loginPopulatesJWTPayloadWithRequiredValues()
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final JsonObject jwtPayload = decodeJwtPayload(token);
      assertThat(jwtPayload.getString("username")).isEqualTo("user");
      assertThat(jwtPayload.getJsonArray("permissions"))
          .isEqualTo(
              new JsonArray(list("fakePermission", "eth:blockNumber", "eth:subscribe", "web3:*")));
      assertThat(jwtPayload.getString("privacyPublicKey"))
          .isEqualTo("A1aVtMxLCUHmBVHXoZzzBgPbW/wj5axDpW9X8l91SGo=");
      assertThat(jwtPayload.containsKey("iat")).isTrue();
      assertThat(jwtPayload.containsKey("exp")).isTrue();
      final long tokenExpiry = jwtPayload.getLong("exp") - jwtPayload.getLong("iat");
      assertThat(tokenExpiry).isEqualTo(MINUTES.toSeconds(5));
    }
  }

  private String login(final String username, final String password) throws IOException {
    final RequestBody loginBody =
        RequestBody.create(
            "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}", JSON);
    final Request loginRequest =
        new Request.Builder().post(loginBody).url(baseUrl + "/login").build();
    final String token;
    try (final Response loginResp = client.newCall(loginRequest).execute()) {
      assertThat(loginResp.code()).isEqualTo(200);
      assertThat(loginResp.message()).isEqualTo("OK");
      assertThat(loginResp.body().contentType()).isNotNull();
      assertThat(loginResp.body().contentType().type()).isEqualTo("application");
      assertThat(loginResp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = loginResp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      token = respBody.getString("token");
      assertThat(token).isNotNull();
    }
    return token;
  }

  /**
   * Authenticates via {@link AuthenticationService#authenticate}, the same path production JSON-RPC
   * requests go through, rather than the raw {@link #jwtAuth} provider -- only
   * DefaultAuthenticationService#authenticate populates {@code user.authorizations()}, which
   * PermissionBasedAuthorization#match (and so isPermitted) depends on. Blocks the calling thread
   * until the (Vert.x-callback-based) authentication completes, so callers can assert on the result
   * from the test's own thread instead of from inside the callback, where a thrown AssertionError
   * would otherwise be silently swallowed rather than failing the test.
   */
  private User authenticateAndAwait(final String token) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<User> userRef = new AtomicReference<>();
    service
        .authenticationService
        .get()
        .authenticate(
            token,
            user -> {
              userRef.set(user.orElse(null));
              latch.countDown();
            });
    assertThat(latch.await(10, TimeUnit.SECONDS))
        .as("authentication callback did not complete in time")
        .isTrue();
    return userRef.get();
  }

  @Test
  public void checkJsonRpcMethodsAvailableWithGoodCredentialsAndPermissions()
      throws IOException, InterruptedException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"user\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final JsonRpcMethod ethAccounts = new EthAccounts();
      final JsonRpcMethod netVersion = new NetVersion(Optional.of(BigInteger.valueOf(123)));
      final JsonRpcMethod ethBlockNumber = new EthBlockNumber(blockchainQueries);
      final JsonRpcMethod web3Sha3 = new Web3Sha3();
      final JsonRpcMethod web3ClientVersion = new Web3ClientVersion("777");

      final User user = authenticateAndAwait(token);
      assertThat(user).isNotNull();
      // single eth/blockNumber method permitted
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), ethBlockNumber, Collections.emptyList()))
          .isTrue();
      // eth/accounts NOT permitted
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), ethAccounts, Collections.emptyList()))
          .isFalse();
      // allowed by web3/*
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), web3ClientVersion, Collections.emptyList()))
          .isTrue();
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), web3Sha3, Collections.emptyList()))
          .isTrue();
      // NO net permissions
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), netVersion, Collections.emptyList()))
          .isFalse();
    }
  }

  @Test
  public void checkJsonRpcMethodsAvailableWithGoodCredentialsAndAllPermissions()
      throws IOException, InterruptedException {
    final RequestBody body =
        RequestBody.create("{\"username\":\"adminuser\",\"password\":\"pegasys\"}", JSON);
    final Request request = new Request.Builder().post(body).url(baseUrl + "/login").build();
    try (final Response resp = client.newCall(request).execute()) {
      assertThat(resp.code()).isEqualTo(200);
      assertThat(resp.message()).isEqualTo("OK");
      assertThat(resp.body().contentType()).isNotNull();
      assertThat(resp.body().contentType().type()).isEqualTo("application");
      assertThat(resp.body().contentType().subtype()).isEqualTo("json");
      final String bodyString = resp.body().string();
      assertThat(bodyString).isNotNull();
      assertThat(bodyString).isNotBlank();

      final JsonObject respBody = new JsonObject(bodyString);
      final String token = respBody.getString("token");
      assertThat(token).isNotNull();

      final JsonRpcMethod ethAccounts = new EthAccounts();
      final JsonRpcMethod netVersion = new NetVersion(Optional.of(BigInteger.valueOf(123)));
      final JsonRpcMethod ethBlockNumber = new EthBlockNumber(blockchainQueries);
      final JsonRpcMethod web3Sha3 = new Web3Sha3();
      final JsonRpcMethod web3ClientVersion = new Web3ClientVersion("777");

      // adminuser has *:* permissions so everything should be allowed
      final User user = authenticateAndAwait(token);
      assertThat(user).isNotNull();
      // single eth/blockNumber method permitted
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), ethBlockNumber, Collections.emptyList()))
          .isTrue();
      // eth/accounts IS permitted
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), ethAccounts, Collections.emptyList()))
          .isTrue();
      // allowed by *:*
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), web3ClientVersion, Collections.emptyList()))
          .isTrue();
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), web3Sha3, Collections.emptyList()))
          .isTrue();
      // YES net permissions
      assertThat(
              service
                  .authenticationService
                  .get()
                  .isPermitted(Optional.of(user), netVersion, Collections.emptyList()))
          .isTrue();
    }
  }

  @Test
  public void checkPermissionsWithEmptyUser() {
    final JsonRpcMethod ethAccounts = new EthAccounts();

    assertThat(
            service
                .authenticationService
                .get()
                .isPermitted(Optional.empty(), ethAccounts, Collections.emptyList()))
        .isFalse();
  }

  @Test
  public void web3ClientVersionUnsuccessfulBeforeLogin() throws Exception {
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":"
                + Json.encode(id)
                + ",\"method\":\"web3_clientVersion\"}",
            JSON);

    try (final Response resp = client.newCall(buildPostRequest(body)).execute()) {
      assertThat(resp.code()).isEqualTo(401);
      assertThat(resp.message()).isEqualTo("Unauthorized");
    }
  }

  @Test
  public void web3ClientVersionUnsuccessfulWithBadBearer() throws Exception {
    final String id = "123";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":"
                + Json.encode(id)
                + ",\"method\":\"web3_clientVersion\"}",
            JSON);

    try (final Response resp = client.newCall(buildPostRequest(body, "badtoken")).execute()) {
      assertThat(resp.code()).isEqualTo(401);
      assertThat(resp.message()).isEqualTo("Unauthorized");
    }
  }

  @Test
  public void web3ClientVersionSuccessfulAfterLogin() throws Exception {
    final String token = login("user", "pegasys");

    final String id = "123";
    final RequestBody web3ClientVersionBody =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":"
                + Json.encode(id)
                + ",\"method\":\"web3_clientVersion\"}",
            JSON);

    try (final Response web3ClientVersionResp =
        client.newCall(buildPostRequest(web3ClientVersionBody, token)).execute()) {
      assertThat(web3ClientVersionResp.code()).isEqualTo(200);
      // Check general format of result
      final JsonObject json = new JsonObject(web3ClientVersionResp.body().string());
      testHelper.assertValidJsonRpcResult(json, id);
      // Check result
      final String result = json.getString("result");
      assertThat(result).isEqualTo("TestClientVersion/0.1.0");
    }
  }

  @Test
  public void noAuthMethodSuccessfulAfterLogin() throws Exception {
    final String token = login("user", "pegasys");

    final String id = "123";
    final RequestBody requestBody =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_services\"}",
            JSON);

    try (final Response response = client.newCall(buildPostRequest(requestBody, token)).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonObject json = new JsonObject(response.body().string());
      testHelper.assertValidJsonRpcResult(json, id);
    }
  }

  @Test
  public void noAuthMethodSuccessfulWithNoToken() throws Exception {
    final String id = "123";
    final RequestBody requestBody =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_services\"}",
            JSON);

    try (final Response response = client.newCall(buildPostRequest(requestBody)).execute()) {
      assertThat(response.code()).isEqualTo(200);
      final JsonObject json = new JsonObject(response.body().string());
      testHelper.assertValidJsonRpcResult(json, id);
    }
  }

  @Test
  public void ethSyncingUnauthorisedWithoutPermission() throws Exception {
    final String token = login("user", "pegasys");

    final String id = "007";
    final RequestBody body =
        RequestBody.create(
            "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"eth_syncing\"}",
            JSON);

    try (final Response resp = client.newCall(buildPostRequest(body, token)).execute()) {
      assertThat(resp.code()).isEqualTo(401);
      assertThat(resp.message()).isEqualTo("Unauthorized");
    }
  }

  private Request buildPostRequest(final RequestBody body) {
    return buildPostRequest(body, Optional.empty());
  }

  private Request buildPostRequest(final RequestBody body, final String token) {
    return buildPostRequest(body, Optional.of(token));
  }

  private Request buildPostRequest(final RequestBody body, final Optional<String> token) {
    final Request.Builder request = new Request.Builder().post(body).url(baseUrl);
    token.ifPresent(t -> request.addHeader("Authorization", "Bearer " + t));
    return request.build();
  }

  private JsonObject decodeJwtPayload(final String token) {
    final List<String> tokenParts = Splitter.on('.').splitToList(token);
    final String payload = tokenParts.get(1);
    return new JsonObject(new String(Base64.getUrlDecoder().decode(payload), UTF_8));
  }
}
