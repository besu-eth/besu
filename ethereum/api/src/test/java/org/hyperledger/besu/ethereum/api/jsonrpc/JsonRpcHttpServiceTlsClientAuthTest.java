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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.DEFAULT_RPC_APIS;
import static org.hyperledger.besu.ethereum.api.tls.KnownClientFileUtil.writeToKnownClientsFile;
import static org.hyperledger.besu.ethereum.api.tls.TlsClientAuthConfiguration.Builder.aTlsClientAuthConfiguration;
import static org.hyperledger.besu.ethereum.api.tls.TlsConfiguration.Builder.aTlsConfiguration;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.StubGenesisConfigOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.health.HealthService;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.JsonRpcMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.ethereum.api.pluginadapter.NetworkEndpointServiceImpl;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.tls.FileBasedPasswordProvider;
import org.hyperledger.besu.ethereum.api.tls.SelfSignedP12Certificate;
import org.hyperledger.besu.ethereum.api.tls.TlsConfiguration;
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
import org.hyperledger.besu.ethereum.permissioning.AccountLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.permissioning.NodeLocalConfigPermissioningController;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class JsonRpcHttpServiceTlsClientAuthTest {
  // this tempDir is deliberately static
  @TempDir private static Path folder;

  protected static final Vertx vertx = Vertx.vertx();

  private static final String JSON_HEADER = "application/json; charset=utf-8";
  private static final String CLIENT_NODE_NAME = "TestClientVersion/0.1.0";
  private static final String CLIENT_VERSION = "0.1.0";
  private static final String CLIENT_COMMIT = "12345678";
  private static final BigInteger CHAIN_ID = BigInteger.valueOf(123);

  private static final NatService natService = new NatService(Optional.empty());
  private static final SelfSignedP12Certificate CLIENT_AS_CA_CERT =
      SelfSignedP12Certificate.create();
  private JsonRpcHttpService service;
  private String baseUrl;
  private Map<String, JsonRpcMethod> rpcMethods;
  private final JsonRpcTestHelper testHelper = new JsonRpcTestHelper();
  private final SelfSignedP12Certificate besuCertificate = SelfSignedP12Certificate.create();
  private final SelfSignedP12Certificate okHttpClientCertificate =
      SelfSignedP12Certificate.create();
  private final FileBasedPasswordProvider fileBasedPasswordProvider =
      new FileBasedPasswordProvider(createPasswordFile(besuCertificate));

  @BeforeEach
  public void initServer() throws Exception {
    final P2PNetwork peerDiscoveryMock = mock(P2PNetwork.class);
    final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);
    final Synchronizer synchronizer = mock(Synchronizer.class);

    final Set<Capability> supportedCapabilities = new HashSet<>();
    supportedCapabilities.add(EthProtocol.LATEST);

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
                new StubGenesisConfigOptions(),
                peerDiscoveryMock,
                blockchainQueries,
                synchronizer,
                MainnetProtocolSchedule.fromConfig(
                    new StubGenesisConfigOptions().constantinopleBlock(0).chainId(CHAIN_ID),
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
                Optional.of(mock(AccountLocalConfigPermissioningController.class)),
                Optional.of(mock(NodeLocalConfigPermissioningController.class)),
                DEFAULT_RPC_APIS,
                mock(JsonRpcConfiguration.class),
                mock(WebSocketConfiguration.class),
                mock(MetricsConfiguration.class),
                mock(GraphQLConfiguration.class),
                new NetworkEndpointServiceImpl(),
                natService,
                Collections.emptyMap(),
                folder,
                mock(EthPeers.class),
                vertx,
                mock(ApiConfiguration.class),
                Optional.empty(),
                mock(TransactionSimulator.class),
                new DeterministicEthScheduler());

    System.setProperty("javax.net.ssl.trustStore", CLIENT_AS_CA_CERT.getKeyStoreFile().toString());
    System.setProperty(
        "javax.net.ssl.trustStorePassword", new String(CLIENT_AS_CA_CERT.getPassword()));

    service = createJsonRpcHttpService(createJsonRpcConfig(this::getRpcHttpTlsConfiguration));
    service.start().join();
    baseUrl = service.url();
  }

  private JsonRpcHttpService createJsonRpcHttpService(final JsonRpcConfiguration jsonRpcConfig)
      throws Exception {
    return new JsonRpcHttpService(
        vertx,
        folder,
        jsonRpcConfig,
        new NoOpMetricsSystem(),
        natService,
        rpcMethods,
        HealthService.ALWAYS_HEALTHY,
        HealthService.ALWAYS_HEALTHY);
  }

  private JsonRpcConfiguration createJsonRpcConfig(
      final Supplier<Optional<TlsConfiguration>> tlsConfigurationSupplier) {
    final JsonRpcConfiguration config = JsonRpcConfiguration.createDefault();
    config.setPort(0);
    config.setHostsAllowlist(Collections.singletonList("*"));
    config.setTlsConfiguration(tlsConfigurationSupplier.get());
    return config;
  }

  private Optional<TlsConfiguration> getRpcHttpTlsConfigurationOnlyWithTruststore() {
    final Path truststorePath = createTempFile();

    // Create a new truststore and add the okHttpClientCertificate to it
    try (FileOutputStream truststoreOutputStream = new FileOutputStream(truststorePath.toFile())) {
      KeyStore truststore = KeyStore.getInstance("PKCS12");
      truststore.load(null, null);
      truststore.setCertificateEntry(
          "okHttpClientCertificate", okHttpClientCertificate.getCertificate());
      truststore.store(truststoreOutputStream, okHttpClientCertificate.getPassword());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create truststore", e);
    }

    final FileBasedPasswordProvider trustStorePasswordProvider =
        new FileBasedPasswordProvider(createPasswordFile(okHttpClientCertificate));

    final TlsConfiguration tlsConfiguration =
        aTlsConfiguration()
            .withKeyStorePath(besuCertificate.getKeyStoreFile())
            .withKeyStorePasswordSupplier(fileBasedPasswordProvider)
            .withClientAuthConfiguration(
                aTlsClientAuthConfiguration()
                    .withTruststorePath(truststorePath)
                    .withTruststorePasswordSupplier(trustStorePasswordProvider)
                    .build())
            .build();

    return Optional.of(tlsConfiguration);
  }

  private Optional<TlsConfiguration> getRpcHttpTlsConfiguration() {
    final Path knownClientsFile = createTempFile();
    writeToKnownClientsFile(
        okHttpClientCertificate.getCommonName(),
        okHttpClientCertificate.getCertificateHexFingerprint(),
        knownClientsFile);

    final TlsConfiguration tlsConfiguration =
        aTlsConfiguration()
            .withKeyStorePath(besuCertificate.getKeyStoreFile())
            .withKeyStorePasswordSupplier(fileBasedPasswordProvider)
            .withClientAuthConfiguration(
                aTlsClientAuthConfiguration()
                    .withKnownClientsFile(knownClientsFile)
                    .withCaClientsEnabled(true)
                    .build())
            .build();

    return Optional.of(tlsConfiguration);
  }

  private Optional<TlsConfiguration> getRpcHttpTlsConfWithoutKnownClients() {
    final TlsConfiguration tlsConfiguration =
        aTlsConfiguration()
            .withKeyStorePath(besuCertificate.getKeyStoreFile())
            .withKeyStorePasswordSupplier(fileBasedPasswordProvider)
            .withClientAuthConfiguration(
                aTlsClientAuthConfiguration().withCaClientsEnabled(true).build())
            .build();

    return Optional.of(tlsConfiguration);
  }

  private Path createPasswordFile(final SelfSignedP12Certificate selfSignedP12Certificate) {
    try {
      return Files.writeString(
          createTempFile(), new String(selfSignedP12Certificate.getPassword()));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path createTempFile() {
    try {
      return Files.createTempFile(folder, "newFile", "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @AfterEach
  public void shutdownServer() {
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");

    service.stop().join();
  }

  @Test
  public void connectionFailsWhenClientCertIsNotProvided() {
    netVersionFailed(this::getTlsHttpClientWithoutCert, baseUrl);
  }

  @Test
  public void connectionFailsWhenClientIsNotWhitelisted() {
    netVersionFailed(this::getTlsHttpClientNotKnownToServer, baseUrl);
  }

  @Test
  public void netVersionSuccessfulOnTlsWithClientCertInKnownClientsFile() throws Exception {
    netVersionSuccessful(this::getTlsHttpClient, baseUrl);
  }

  @Test
  public void netVersionSuccessfulOnTlsWithClientCertInTruststore() throws Exception {

    JsonRpcHttpService jsonRpcHttpService = null;
    try {
      jsonRpcHttpService =
          createJsonRpcHttpService(
              createJsonRpcConfig(this::getRpcHttpTlsConfigurationOnlyWithTruststore));
      jsonRpcHttpService.start().join();
      netVersionSuccessful(this::getTlsHttpClient, jsonRpcHttpService.url());
    } finally {
      if (jsonRpcHttpService != null) {
        jsonRpcHttpService.stop().join();
      }
    }
  }

  @Test
  public void netVersionSuccessfulOnTlsWithClientCertAddedAsCA() throws Exception {
    netVersionSuccessful(this::getTlsHttpClientAddedAsCA, baseUrl);
  }

  @Test
  public void netVersionSuccessfulOnTlsWithClientCertAsCAWithoutKnownFiles() throws Exception {
    JsonRpcHttpService jsonRpcHttpService = null;
    try {
      jsonRpcHttpService =
          createJsonRpcHttpService(createJsonRpcConfig(this::getRpcHttpTlsConfWithoutKnownClients));
      jsonRpcHttpService.start().join();
      netVersionSuccessful(this::getTlsHttpClientAddedAsCA, jsonRpcHttpService.url());
    } finally {
      if (jsonRpcHttpService != null) {
        jsonRpcHttpService.stop().join();
      }
    }
  }

  private void netVersionFailed(
      final Supplier<OkHttpClient> okHttpClientSupplier, final String baseUrl) {
    final String id = "123";
    final String json =
        "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}";

    final OkHttpClient httpClient = okHttpClientSupplier.get();
    assertThatIOException()
        .isThrownBy(
            () -> {
              try (final Response response =
                  httpClient.newCall(buildPostRequest(json, baseUrl)).execute()) {
                Assertions.fail("Call should have failed. Got: " + response);
              } catch (final Exception e) {
                e.printStackTrace();
                throw e;
              }
            });
  }

  private void netVersionSuccessful(
      final Supplier<OkHttpClient> okHttpClientSupplier, final String baseUrl) throws Exception {
    final String id = "123";
    final String json =
        "{\"jsonrpc\":\"2.0\",\"id\":" + Json.encode(id) + ",\"method\":\"net_version\"}";

    final OkHttpClient httpClient = okHttpClientSupplier.get();
    try (final Response response = httpClient.newCall(buildPostRequest(json, baseUrl)).execute()) {

      assertThat(response.code()).isEqualTo(200);
      // Check general format of result
      final ResponseBody body = response.body();
      assertThat(body).isNotNull();
      final JsonObject jsonObject = new JsonObject(body.string());
      testHelper.assertValidJsonRpcResult(jsonObject, id);
      // Check result
      final String result = jsonObject.getString("result");
      assertThat(result).isEqualTo(String.valueOf(CHAIN_ID));
    } catch (final Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private OkHttpClient getTlsHttpClient() {
    return TlsOkHttpClientBuilder.anOkHttpClient()
        .withBesuCertificate(besuCertificate)
        .withOkHttpCertificate(okHttpClientCertificate)
        .build();
  }

  private OkHttpClient getTlsHttpClientAddedAsCA() {
    return TlsOkHttpClientBuilder.anOkHttpClient()
        .withBesuCertificate(besuCertificate)
        .withOkHttpCertificate(CLIENT_AS_CA_CERT)
        .build();
  }

  private OkHttpClient getTlsHttpClientNotKnownToServer() {
    return TlsOkHttpClientBuilder.anOkHttpClient()
        .withBesuCertificate(besuCertificate)
        .withOkHttpCertificate(SelfSignedP12Certificate.create())
        .build();
  }

  private OkHttpClient getTlsHttpClientWithoutCert() {
    return TlsOkHttpClientBuilder.anOkHttpClient().withBesuCertificate(besuCertificate).build();
  }

  private Request buildPostRequest(final String json, final String baseUrl) {
    final RequestBody body = RequestBody.create(json, MediaType.parse(JSON_HEADER));
    return new Request.Builder().post(body).url(baseUrl).build();
  }
}
