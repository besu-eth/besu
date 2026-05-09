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
package org.hyperledger.besu.tests.acceptance.jsonrpc;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.net.JksOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class JsonRpcWebsocketTlsAcceptanceTest extends AcceptanceTestBase {

  private static final String PASSWORD = "secret123";
  private static final String NET_VERSION_REQUEST =
      "{\"jsonrpc\":\"2.0\",\"method\":\"net_version\",\"params\":[],\"id\":1}";

  @TempDir private Path tempDir;

  @Test
  public void shouldUseJksKeystoreWithInlinePasswordOverDslWebSocket() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final WebSocketConfiguration config = baseTlsConfig();
    config.setKeyStorePath(writeKeyStore("server-inline.jks", serverCertificate).toString());
    config.setKeyStorePassword(PASSWORD);
    config.setKeyStoreType("JKS");

    assertDslNetVersion("ws-tls-jks-inline", config);
  }

  @Test
  public void shouldUseJksKeystoreWithPasswordFileOverDslWebSocket() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final WebSocketConfiguration config = baseTlsConfig();
    config.setKeyStorePath(writeKeyStore("server-password-file.jks", serverCertificate).toString());
    config.setKeyStorePasswordFile(writePasswordFile("server-password.txt").toString());
    config.setKeyStoreType("JKS");

    assertDslNetVersion("ws-tls-jks-password-file", config);
  }

  @Test
  public void shouldUsePemKeyAndCertificateOverDslWebSocket() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final WebSocketConfiguration config = baseTlsConfig();
    config.setKeyStoreType("PEM");
    config.setKeyPath(serverCertificate.privateKey().getAbsolutePath());
    config.setCertPath(serverCertificate.certificate().getAbsolutePath());

    assertDslNetVersion("ws-tls-pem", config);
  }

  @Test
  public void shouldAuthenticateClientWithJksTruststoreInlinePassword() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final SelfSignedCertificate clientCertificate = new SelfSignedCertificate("client");
    final WebSocketConfiguration config =
        jksServerTlsConfig("client-auth-inline", serverCertificate);
    config.setClientAuthEnabled(true);
    config.setTrustStorePath(
        writeTrustStore("trusted-client-inline.jks", clientCertificate).toString());
    config.setTrustStorePassword(PASSWORD);
    config.setTrustStoreType("JKS");

    assertClientAuthNetVersion("ws-tls-client-auth-jks-inline", config, clientCertificate);
  }

  @Test
  public void shouldAuthenticateClientWithJksTruststorePasswordFile() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final SelfSignedCertificate clientCertificate = new SelfSignedCertificate("client");
    final WebSocketConfiguration config =
        jksServerTlsConfig("client-auth-password-file", serverCertificate);
    config.setClientAuthEnabled(true);
    config.setTrustStorePath(
        writeTrustStore("trusted-client-password-file.jks", clientCertificate).toString());
    config.setTrustStorePasswordFile(writePasswordFile("trusted-client-password.txt").toString());
    config.setTrustStoreType("JKS");

    assertClientAuthNetVersion("ws-tls-client-auth-jks-password-file", config, clientCertificate);
  }

  @Test
  public void shouldAuthenticateClientWithPemTrustCertificate() throws Exception {
    final SelfSignedCertificate serverCertificate = new SelfSignedCertificate("localhost");
    final SelfSignedCertificate clientCertificate = new SelfSignedCertificate("client");
    final WebSocketConfiguration config = jksServerTlsConfig("client-auth-pem", serverCertificate);
    config.setClientAuthEnabled(true);
    config.setTrustStoreType("PEM");
    config.setTrustCertPath(clientCertificate.certificate().getAbsolutePath());

    assertClientAuthNetVersion("ws-tls-client-auth-pem", config, clientCertificate);
  }

  private void assertDslNetVersion(final String nodeName, final WebSocketConfiguration config)
      throws Exception {
    final BesuNode node = startNode(nodeName, config);
    node.useWebSocketsForJsonRpc();
    node.verify(net.netVersion());
  }

  private void assertClientAuthNetVersion(
      final String nodeName,
      final WebSocketConfiguration config,
      final SelfSignedCertificate clientCertificate)
      throws Exception {
    final BesuNode node = startNode(nodeName, config);
    final Path clientKeyStore = writeKeyStore(nodeName + "-client.jks", clientCertificate);

    assertThat(sendNetVersionRequest(node, clientKeyStore))
        .contains("\"jsonrpc\":\"2.0\"")
        .contains("\"result\"")
        .contains("\"id\":1");
  }

  private BesuNode startNode(final String nodeName, final WebSocketConfiguration config)
      throws Exception {
    final BesuNode node =
        besu.create(
            new BesuNodeConfigurationBuilder()
                .name(nodeName)
                .webSocketConfiguration(config)
                .engineRpcEnabled(false)
                .p2pEnabled(false)
                .discoveryEnabled(false)
                .build());
    cluster.start(node);
    return node;
  }

  private String sendNetVersionRequest(final BesuNode node, final Path clientKeyStore)
      throws Exception {
    final Vertx vertx = Vertx.vertx();
    try {
      final CompletableFuture<String> response = new CompletableFuture<>();
      final WebSocketClientOptions clientOptions =
          new WebSocketClientOptions()
              .setSsl(true)
              .setTrustAll(true)
              .setVerifyHost(false)
              .setKeyStoreOptions(
                  new JksOptions().setPath(clientKeyStore.toString()).setPassword(PASSWORD));
      final WebSocketClient webSocketClient = vertx.createWebSocketClient(clientOptions);

      webSocketClient
          .connect(node.getJsonRpcWebSocketPort().orElseThrow(), "localhost", "/")
          .onSuccess(
              webSocket -> {
                webSocket.textMessageHandler(
                    message -> {
                      response.complete(message);
                      webSocket.close();
                    });
                webSocket.exceptionHandler(response::completeExceptionally);
                webSocket.writeTextMessage(NET_VERSION_REQUEST);
              })
          .onFailure(response::completeExceptionally);

      return response.get(10, TimeUnit.SECONDS);
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  private WebSocketConfiguration jksServerTlsConfig(
      final String filePrefix, final SelfSignedCertificate serverCertificate) throws Exception {
    final WebSocketConfiguration config = baseTlsConfig();
    config.setKeyStorePath(writeKeyStore(filePrefix + "-server.jks", serverCertificate).toString());
    config.setKeyStorePassword(PASSWORD);
    config.setKeyStoreType("JKS");
    return config;
  }

  private WebSocketConfiguration baseTlsConfig() {
    final WebSocketConfiguration config = WebSocketConfiguration.createDefault();
    config.setEnabled(true);
    config.setPort(0);
    config.setHostsAllowlist(singletonList("*"));
    config.setSslEnabled(true);
    return config;
  }

  private Path writeKeyStore(final String fileName, final SelfSignedCertificate certificate)
      throws Exception {
    final KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "key", certificate.key(), PASSWORD.toCharArray(), new Certificate[] {certificate.cert()});

    return writeKeyStoreFile(fileName, keyStore);
  }

  private Path writeTrustStore(final String fileName, final SelfSignedCertificate certificate)
      throws Exception {
    final KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("cert", certificate.cert());

    return writeKeyStoreFile(fileName, trustStore);
  }

  private Path writeKeyStoreFile(final String fileName, final KeyStore keyStore) throws Exception {
    final Path keyStoreFile = tempDir.resolve(fileName);
    try (final FileOutputStream out = new FileOutputStream(keyStoreFile.toFile())) {
      keyStore.store(out, PASSWORD.toCharArray());
    }
    return keyStoreFile;
  }

  private Path writePasswordFile(final String fileName) throws Exception {
    return Files.writeString(tempDir.resolve(fileName), PASSWORD);
  }
}
