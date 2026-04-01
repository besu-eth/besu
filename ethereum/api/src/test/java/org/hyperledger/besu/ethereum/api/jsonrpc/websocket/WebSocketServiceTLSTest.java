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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.CombinedJsonRpcProcessor;
import org.hyperledger.besu.ethereum.api.jsonrpc.execution.JsonRpcExecutor;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods.WebSocketMethodsFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class WebSocketServiceTLSTest {

  private Vertx vertx;
  private WebSocketConfiguration config;
  private WebSocketMessageHandler webSocketMessageHandlerSpy;

  @BeforeEach
  public void setUp() {
    vertx = Vertx.vertx();
    config = WebSocketConfiguration.createDefault();
    Map<String, JsonRpcMethod> websocketMethods;
    config.setPort(0); // Use ephemeral port
    config.setHost("localhost");
    websocketMethods =
        new WebSocketMethodsFactory(
                new SubscriptionManager(new NoOpMetricsSystem()), new HashMap<>())
            .methods();
    webSocketMessageHandlerSpy =
        spy(
            new WebSocketMessageHandler(
                vertx,
                new JsonRpcExecutor(new CombinedJsonRpcProcessor(new NoOpMetricsSystem()), websocketMethods),
                mock(EthScheduler.class),
                TimeoutOptions.defaultOptions().getTimeoutSeconds()));
  }

  @Test
  public void shouldAcceptSecureWebSocketConnection(final VertxTestContext testContext)
      throws Throwable {
    // Generate a self-signed certificate
    SelfSignedCertificate ssc = new SelfSignedCertificate();

    // Create a temporary keystore file
    File keystoreFile = File.createTempFile("keystore", ".jks");
    keystoreFile.deleteOnExit();

    // Create a PKCS12 keystore and load the self-signed certificate
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "alias",
        ssc.key(),
        "password".toCharArray(),
        new java.security.cert.Certificate[] {ssc.cert()});

    // Save the keystore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(keystoreFile)) {
      keyStore.store(fos, "password".toCharArray());
    }

    // Configure WebSocket with SSL enabled
    config.setSslEnabled(true);
    config.setKeyStorePath(keystoreFile.getAbsolutePath());
    config.setKeyStorePassword("password");
    config.setKeyStoreType("JKS");

    // Create and start WebSocketService
    WebSocketService webSocketService =
        new WebSocketService(vertx, config, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    webSocketService.start().join();

    // Get the actual port
    int port = webSocketService.socketAddress().getPort();

    // Create a temporary truststore file
    File truststoreFile = File.createTempFile("truststore", ".jks");
    truststoreFile.deleteOnExit();

    // Create a PKCS12 truststore and load the server's certificate
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("alias", ssc.cert());

    // Save the truststore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(truststoreFile)) {
      trustStore.store(fos, "password".toCharArray());
    }

    // Configure the HTTP client with the truststore
    WebSocketClientOptions clientOptions =
        new WebSocketClientOptions()
            .setSsl(true)
            .setTrustOptions(
                new JksOptions().setPath(truststoreFile.getAbsolutePath()).setPassword("password"))
            .setVerifyHost(true);

    WebSocketClient webSocketClient = vertx.createWebSocketClient(clientOptions);
    webSocketClient
        .connect(port, "localhost", "/")
        .onSuccess(
            ws -> {
              assertThat(ws.isSsl()).isTrue();
              ws.close();
              testContext.completeNow();
            })
        .onFailure(testContext::failNow);

    assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    // Stop the WebSocketService after the test
    webSocketService.stop().join();
  }

  @Test
  public void shouldAcceptSecureWebSocketConnectionPEM(final VertxTestContext testContext)
      throws Throwable {
    // Generate a self-signed certificate
    SelfSignedCertificate ssc = new SelfSignedCertificate();

    // Create temporary PEM files for the certificate and key
    File certFile = File.createTempFile("cert", ".pem");
    certFile.deleteOnExit();
    File keyFile = File.createTempFile("key", ".pem");
    keyFile.deleteOnExit();

    // Write the certificate and key to the PEM files
    try (FileOutputStream certOut = new FileOutputStream(certFile);
        FileOutputStream keyOut = new FileOutputStream(keyFile)) {
      certOut.write("-----BEGIN CERTIFICATE-----\n".getBytes(UTF_8));
      certOut.write(
          Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encode(ssc.cert().getEncoded()));
      certOut.write("\n-----END CERTIFICATE-----\n".getBytes(UTF_8));

      keyOut.write("-----BEGIN PRIVATE KEY-----\n".getBytes(UTF_8));
      keyOut.write(Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encode(ssc.key().getEncoded()));
      keyOut.write("\n-----END PRIVATE KEY-----\n".getBytes(UTF_8));
    }

    // Configure WebSocket with SSL enabled using PEM files
    config.setSslEnabled(true);
    config.setKeyPath(keyFile.getAbsolutePath());
    config.setCertPath(certFile.getAbsolutePath());
    config.setKeyStoreType("PEM");

    // Create and start WebSocketService
    WebSocketService webSocketService =
        new WebSocketService(vertx, config, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    webSocketService.start().join();

    // Get the actual port
    int port = webSocketService.socketAddress().getPort();

    // Create a temporary PEM file for the trust store
    File trustCertFile = File.createTempFile("trust-cert", ".pem");
    trustCertFile.deleteOnExit();

    // Write the server's certificate to the PEM file
    try (FileOutputStream trustCertOut = new FileOutputStream(trustCertFile)) {
      trustCertOut.write("-----BEGIN CERTIFICATE-----\n".getBytes(UTF_8));
      trustCertOut.write(
          Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encode(ssc.cert().getEncoded()));
      trustCertOut.write("\n-----END CERTIFICATE-----\n".getBytes(UTF_8));
    }

    // Configure the HTTP client with the trust store using PEM files
    WebSocketClientOptions clientOptions =
        new WebSocketClientOptions()
            .setSsl(true)
            .setTrustOptions(new PemTrustOptions().addCertPath(trustCertFile.getAbsolutePath()))
            .setVerifyHost(true);

    WebSocketClient webSocketClient = vertx.createWebSocketClient(clientOptions);
    webSocketClient
        .connect(port, "localhost", "/")
        .onSuccess(
            ws -> {
              assertThat(ws.isSsl()).isTrue();
              ws.close();
              testContext.completeNow();
            })
        .onFailure(testContext::failNow);

    assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    // Stop the WebSocketService after the test
    webSocketService.stop().join();
  }

  @Test
  public void shouldFailConnectionWithWrongCertificateInTrustStore(
      final VertxTestContext testContext) throws Throwable {
    // Generate a self-signed certificate for the server
    SelfSignedCertificate serverCert = new SelfSignedCertificate();

    // Create a temporary keystore file for the server
    File keystoreFile = File.createTempFile("keystore", ".p12");
    keystoreFile.deleteOnExit();

    // Create a PKCS12 keystore and load the server's self-signed certificate
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "alias",
        serverCert.key(),
        "password".toCharArray(),
        new java.security.cert.Certificate[] {serverCert.cert()});

    // Save the keystore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(keystoreFile)) {
      keyStore.store(fos, "password".toCharArray());
    }

    // Configure WebSocket with SSL enabled
    config.setSslEnabled(true);
    config.setKeyStorePath(keystoreFile.getAbsolutePath());
    config.setKeyStorePassword("password");
    config.setKeyStoreType("PKCS12");

    // Create and start WebSocketService
    WebSocketService webSocketService =
        new WebSocketService(vertx, config, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    webSocketService.start().join();

    // Get the actual port
    int port = webSocketService.socketAddress().getPort();

    // Generate a different self-signed certificate for the trust store
    SelfSignedCertificate wrongCert = new SelfSignedCertificate();

    // Create a temporary truststore file
    File truststoreFile = File.createTempFile("truststore", ".p12");
    truststoreFile.deleteOnExit();

    // Create a PKCS12 truststore and load the wrong certificate
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("alias", wrongCert.cert());

    // Save the truststore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(truststoreFile)) {
      trustStore.store(fos, "password".toCharArray());
    }

    // Configure the HTTP client with the truststore containing the wrong certificate
    WebSocketClientOptions clientOptions =
        new WebSocketClientOptions()
            .setSsl(true)
            .setTrustOptions(
                new JksOptions().setPath(truststoreFile.getAbsolutePath()).setPassword("password"))
            .setVerifyHost(true);

    WebSocketClient webSocketClient = vertx.createWebSocketClient(clientOptions);
    webSocketClient
        .connect(port, "localhost", "/")
        .onSuccess(
            ws -> {
              testContext.failNow(new AssertionError("Connection should have been rejected"));
            })
        .onFailure(
            throwable -> {
              assertThat(throwable).isInstanceOf(Exception.class);
              testContext.completeNow();
            });

    assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    // Stop the WebSocketService after the test
    webSocketService.stop().join();
  }

  @Test
  public void shouldAuthenticateClient(final VertxTestContext testContext) throws Throwable {
    // Generate a self-signed certificate for the server
    SelfSignedCertificate serverCert = new SelfSignedCertificate();

    // Generate a self-signed certificate for the client
    SelfSignedCertificate clientCert = new SelfSignedCertificate();

    // Create a temporary keystore file for the server
    File serverKeystoreFile = File.createTempFile("keystore", ".p12");
    serverKeystoreFile.deleteOnExit();

    // Create a temporary truststore file for the server
    File serverTruststoreFile = File.createTempFile("truststore", ".p12");
    serverTruststoreFile.deleteOnExit();

    // Create a temporary keystore file for the client
    File clientKeystoreFile = File.createTempFile("client-keystore", ".p12");
    clientKeystoreFile.deleteOnExit();

    // Create a temporary truststore file for the client
    File clientTruststoreFile = File.createTempFile("truststore", ".p12");
    clientTruststoreFile.deleteOnExit();

    // Create a PKCS12 keystore and load the server's self-signed certificate
    KeyStore serverKeyStore = KeyStore.getInstance("PKCS12");
    serverKeyStore.load(null, null);
    serverKeyStore.setKeyEntry(
        "alias",
        serverCert.key(),
        "password".toCharArray(),
        new java.security.cert.Certificate[] {serverCert.cert()});

    // Save the keystore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(serverKeystoreFile)) {
      serverKeyStore.store(fos, "password".toCharArray());
    }

    // Create a PKCS12 truststore and load the client's self-signed certificate
    KeyStore serverTrustStore = KeyStore.getInstance("PKCS12");
    serverTrustStore.load(null, null);
    serverTrustStore.setCertificateEntry("alias", clientCert.cert());

    // Save the truststore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(serverTruststoreFile)) {
      serverTrustStore.store(fos, "password".toCharArray());
    }

    // Create a PKCS12 keystore and load the client's self-signed certificate
    KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
    clientKeyStore.load(null, null);
    clientKeyStore.setKeyEntry(
        "alias",
        clientCert.key(),
        "password".toCharArray(),
        new java.security.cert.Certificate[] {clientCert.cert()});

    // Save the client keystore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(clientKeystoreFile)) {
      clientKeyStore.store(fos, "password".toCharArray());
    }

    // Create a PKCS12 truststore and load the server's self-signed certificate
    KeyStore clientTrustStore = KeyStore.getInstance("PKCS12");
    clientTrustStore.load(null, null);
    clientTrustStore.setCertificateEntry("alias", serverCert.cert());

    // Save the truststore to the temporary file
    try (FileOutputStream fos = new FileOutputStream(clientTruststoreFile)) {
      clientTrustStore.store(fos, "password".toCharArray());
    }

    File tempFile = File.createTempFile("pwdfile", ".txt");
    tempFile.deleteOnExit();
    try (Writer writer = Files.newBufferedWriter(tempFile.toPath(), Charset.defaultCharset())) {
      writer.write("password");
    }

    // Configure WebSocket with SSL and client authentication enabled
    config.setSslEnabled(true);
    config.setKeyStorePath(serverKeystoreFile.getAbsolutePath());
    config.setKeyStorePasswordFile(tempFile.getAbsolutePath());
    config.setKeyStoreType("PKCS12");
    config.setClientAuthEnabled(true);
    config.setTrustStorePath(serverTruststoreFile.getAbsolutePath());
    config.setTrustStorePasswordFile(tempFile.getAbsolutePath());
    config.setTrustStoreType("PKCS12");

    // Create and start WebSocketService
    WebSocketService webSocketService =
        new WebSocketService(vertx, config, webSocketMessageHandlerSpy, new NoOpMetricsSystem());
    webSocketService.start().join();

    // Get the actual port
    int port = webSocketService.socketAddress().getPort();

    // Configure the HTTP client with the client certificate
    WebSocketClientOptions clientOptions =
        new WebSocketClientOptions()
            .setSsl(true)
            .setKeyStoreOptions(
                new JksOptions()
                    .setPath(clientKeystoreFile.getAbsolutePath())
                    .setPassword("password"))
            .setTrustOptions(
                new JksOptions()
                    .setPath(clientTruststoreFile.getAbsolutePath())
                    .setPassword("password"))
            .setVerifyHost(true);

    WebSocketClient webSocketClient = vertx.createWebSocketClient(clientOptions);
    webSocketClient
        .connect(port, "localhost", "/")
        .onSuccess(
            ws -> {
              assertThat(ws.isSsl()).isTrue();
              ws.close();
              testContext.completeNow();
            })
        .onFailure(testContext::failNow);

    assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }

    // Stop the WebSocketService after the test
    webSocketService.stop().join();
  }
}
