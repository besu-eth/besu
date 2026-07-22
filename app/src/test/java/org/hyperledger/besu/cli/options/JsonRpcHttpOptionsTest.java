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
package org.hyperledger.besu.cli.options;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.ETH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.NET;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.PERM;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.tls.TlsConfiguration;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JsonRpcHttpOptionsTest extends CommandTestAbstract {

  @Test
  public void rpcHttpEnabledPropertyMustBeUsed() {
    parseCommand("--rpc-http-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().isEnabled()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcApisPropertyMustBeUsed() {
    parseCommand("--rpc-http-api", "ETH,NET,PERM", "--rpc-http-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    verify(mockLogger)
        .warn("Permissions are disabled. Cannot enable PERM APIs when not using Permissions.");

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getRpcApis())
        .containsExactlyInAnyOrder(ETH.name(), NET.name(), PERM.name());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcApisPropertyIgnoresDuplicatesAndMustBeUsed() {
    parseCommand("--rpc-http-api", "ETH,NET,NET", "--rpc-http-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getRpcApis())
        .containsExactlyInAnyOrder(ETH.name(), NET.name());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcApiNoAuthMethodsIgnoresDuplicatesAndMustBeUsed() {
    parseCommand(
        "--rpc-http-api-methods-no-auth",
        "admin_peers, admin_peers, eth_getCode",
        "--rpc-http-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getNoAuthRpcApis())
        .containsExactlyInAnyOrder(
            RpcMethod.ADMIN_PEERS.getMethodName(), RpcMethod.ETH_GET_CODE.getMethodName());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpNoAuthApiMethodsCannotBeInvalid() {
    parseCommand("--rpc-http-enabled", "--rpc-http-api-method-no-auth", "invalid");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--rpc-http-api-methods-no-auth', options must be valid RPC methods");
  }

  @Test
  public void rpcHttpOptionsRequiresServiceToBeEnabled() {
    parseCommand(
        "--rpc-http-api",
        "ETH,NET",
        "--rpc-http-host",
        "0.0.0.0",
        "--rpc-http-port",
        "1234",
        "--rpc-http-cors-origins",
        "all",
        "--rpc-http-max-active-connections",
        "88");

    verifyOptionsConstraintLoggerCall(
        "--rpc-http-enabled",
        "--rpc-http-host",
        "--rpc-http-port",
        "--rpc-http-cors-origins",
        "--rpc-http-api",
        "--rpc-http-max-active-connections");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpOptionsRequiresServiceToBeEnabledToml() throws IOException {
    final Path toml =
        createTempFile(
            "toml",
            """
            rpc-http-api=["ETH","NET"]
            rpc-http-host="0.0.0.0"
            rpc-http-port=1234
            rpc-http-cors-origins=["all"]
            rpc-http-max-active-connections=88""");

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall(
        "--rpc-http-enabled",
        "--rpc-http-host",
        "--rpc-http-port",
        "--rpc-http-cors-origins",
        "--rpc-http-api",
        "--rpc-http-max-active-connections");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpHostAndPortOptionsMustBeUsed() {

    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand(
        "--rpc-http-enabled", "--rpc-http-host", host, "--rpc-http-port", String.valueOf(port));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpHostMayBeLocalhost() {

    final String host = "localhost";
    parseCommand("--rpc-http-enabled", "--rpc-http-host", host);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpHostMayBeIPv6() {

    final String host = "2600:DB8::8545";
    parseCommand("--rpc-http-enabled", "--rpc-http-host", host);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpMaxActiveConnectionsPropertyMustBeUsed() {
    final int maxConnections = 99;
    parseCommand("--rpc-http-max-active-connections", String.valueOf(maxConnections));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getMaxActiveConnections())
        .isEqualTo(maxConnections);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsRequiresRpcHttpEnabled() {
    parseCommand("--rpc-http-tls-enabled");

    verifyOptionsConstraintLoggerCall("--rpc-http-enabled", "--rpc-http-tls-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsRequiresRpcHttpEnabledToml() throws IOException {
    final Path toml = createTempFile("toml", "rpc-http-tls-enabled=true\n");

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall("--rpc-http-enabled", "--rpc-http-tls-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsWithoutKeystoreReportsError() {
    parseCommand("--rpc-http-enabled", "--rpc-http-tls-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Keystore file is required when TLS is enabled for JSON-RPC HTTP endpoint");
  }

  @Test
  public void rpcHttpTlsWithoutPasswordfileReportsError() {
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        "/tmp/test.p12");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "File containing password to unlock keystore is required when TLS is enabled for JSON-RPC HTTP endpoint");
  }

  @Test
  public void rpcHttpTlsKeystoreAndPasswordMustBeUsed() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration.isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().isEmpty()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsClientAuthWithoutKnownFileReportsError() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Configuration error: TLS client authentication is enabled, but none of the following options are provided: "
                + "1. Specify a known-clients file (--rpc-http-tls-known-clients-file) and/or  Enable CA clients (--rpc-http-tls-ca-clients-enabled). "
                + "2. Specify a truststore file and its password file (--rpc-http-tls-truststore-file and --rpc-http-tls-truststore-password-file). "
                + "Only one of these options must be configured");
  }

  @Test
  public void rpcHttpTlsClientAuthWithKnownClientFile() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String knownClientFile = "/tmp/knownClientFile";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled",
        "--rpc-http-tls-known-clients-file",
        knownClientFile);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration.isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().isPresent()).isTrue();
    assertThat(
            tlsConfiguration.get().getClientAuthConfiguration().get().getKnownClientsFile().get())
        .isEqualTo(Path.of(knownClientFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().get().isCaClientsEnabled())
        .isFalse();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsClientAuthWithCAClient() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled",
        "--rpc-http-tls-ca-clients-enabled");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration.isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().isPresent()).isTrue();
    assertThat(
            tlsConfiguration
                .get()
                .getClientAuthConfiguration()
                .get()
                .getKnownClientsFile()
                .isEmpty())
        .isTrue();
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().get().isCaClientsEnabled())
        .isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsClientAuthWithTrustStore() throws IOException {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String truststoreFile = "/tmp/truststore.p12";
    final String truststorePasswordFile = "/tmp/truststore.txt";

    Files.writeString(Path.of(truststorePasswordFile), "password");
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled",
        "--rpc-http-tls-truststore-file",
        truststoreFile,
        "--rpc-http-tls-truststore-password-file",
        truststorePasswordFile);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration.isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().get().getTruststorePath())
        .isEqualTo(Optional.of(Path.of(truststoreFile)));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().get().getTrustStorePassword())
        .isEqualTo(Files.readString(Path.of(truststorePasswordFile)));

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsClientAuthWithTrustStoreAndKnownClientsFileReportsError()
      throws IOException {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String truststoreFile = "/tmp/truststore.p12";
    final String truststorePasswordFile = "/tmp/truststore.txt";
    final String knownClientFile = "/tmp/knownClientFile";

    Files.writeString(Path.of(truststorePasswordFile), "password");
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled",
        "--rpc-http-tls-truststore-file",
        truststoreFile,
        "--rpc-http-tls-truststore-password-file",
        truststorePasswordFile,
        "--rpc-http-tls-known-clients-file",
        knownClientFile);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Configuration error: Truststore file (--rpc-http-tls-truststore-file) cannot be used together with CA clients (--rpc-http-tls-ca-clients-enabled) or a known-clients (--rpc-http-tls-known-clients-file) option. "
                + "These options are mutually exclusive. Choose either truststore-based authentication or known-clients/CA clients configuration.");
  }

  @Test
  public void rpcHttpTlsClientAuthWithCAClientAndKnownClientFile() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String knownClientFile = "/tmp/knownClientFile";
    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-client-auth-enabled",
        "--rpc-http-tls-ca-clients-enabled",
        "--rpc-http-tls-known-clients-file",
        knownClientFile);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration.isPresent()).isTrue();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().isPresent()).isTrue();
    assertThat(
            tlsConfiguration.get().getClientAuthConfiguration().get().getKnownClientsFile().get())
        .isEqualTo(Path.of(knownClientFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration().get().isCaClientsEnabled())
        .isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsCheckDefaultProtocolsAndCipherSuites() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration).isPresent();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration()).isEmpty();
    assertThat(tlsConfiguration.get().getCipherSuites().get()).isEmpty();
    assertThat(tlsConfiguration.get().getSecureTransportProtocols().get())
        .containsExactly("TLSv1.3", "TLSv1.2");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsCheckInvalidProtocols() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String protocol = "TLsv1.4";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-protocols",
        protocol);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).contains("No valid TLS protocols specified");
  }

  @Test
  public void rpcHttpTlsCheckInvalidCipherSuites() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String cipherSuites = "Invalid";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-cipher-suites",
        cipherSuites);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid TLS cipher suite specified " + cipherSuites);
  }

  @Test
  public void rpcHttpTlsCheckValidProtocolsAndCipherSuites() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String keystoreFile = "/tmp/test.p12";
    final String keystorePasswordFile = "/tmp/test.txt";
    final String protocols = "TLSv1.3,TLSv1.2";
    final String cipherSuites =
        "TLS_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

    parseCommand(
        "--rpc-http-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-enabled",
        "--rpc-http-tls-keystore-file",
        keystoreFile,
        "--rpc-http-tls-keystore-password-file",
        keystorePasswordFile,
        "--rpc-http-tls-protocols",
        protocols,
        "--rpc-http-tls-cipher-suites",
        cipherSuites);

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    final Optional<TlsConfiguration> tlsConfiguration =
        jsonRpcConfigArgumentCaptor.getValue().getTlsConfiguration();
    assertThat(tlsConfiguration).isPresent();
    assertThat(tlsConfiguration.get().getKeyStorePath()).isEqualTo(Path.of(keystoreFile));
    assertThat(tlsConfiguration.get().getClientAuthConfiguration()).isEmpty();
    assertThat(tlsConfiguration.get().getCipherSuites().get())
        .containsExactlyInAnyOrder(
            "TLS_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    assertThat(tlsConfiguration.get().getSecureTransportProtocols().get())
        .containsExactlyInAnyOrder("TLSv1.2", "TLSv1.3");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpTlsWarnIfCipherSuitesSpecifiedWithoutTls() {
    final String host = "1.2.3.4";
    final int port = 1234;
    final String cipherSuites = "Invalid";

    parseCommand(
        "--rpc-http-enabled",
        "--engine-rpc-enabled",
        "--rpc-http-host",
        host,
        "--rpc-http-port",
        String.valueOf(port),
        "--rpc-http-tls-cipher-suite",
        cipherSuites);
    verify(
            mockLogger,
            times(2)) // this is verified for both the full suite of apis, and the engine group.
        .warn(
            "{} has been ignored because {} was not defined on the command line.",
            "--rpc-http-tls-cipher-suite",
            "--rpc-http-tls-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsTwoDomainsMustBuildListWithBothDomains() {
    final String[] origins = {"http://domain1.com", "https://domain2.com"};
    parseCommand("--rpc-http-enabled", "--rpc-http-cors-origins", String.join(",", origins));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getCorsAllowedDomains().toArray())
        .isEqualTo(origins);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsDoubleCommaFilteredOut() {
    final String[] origins = {"http://domain1.com", "https://domain2.com"};
    parseCommand("--rpc-http-enabled", "--rpc-http-cors-origins", String.join(",,", origins));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getCorsAllowedDomains().toArray())
        .isEqualTo(origins);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsWithWildcardMustBuildListWithWildcard() {
    final String[] origins = {"*"};
    parseCommand("--rpc-http-enabled", "--rpc-http-cors-origins", String.join(",", origins));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getCorsAllowedDomains().toArray())
        .isEqualTo(origins);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsWithAllMustBuildListWithWildcard() {
    parseCommand("--rpc-http-enabled", "--rpc-http-cors-origins", "all");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getCorsAllowedDomains()).containsExactly("*");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsWithNoneMustBuildEmptyList() {
    final String[] origins = {"none"};
    parseCommand("--rpc-http-enabled", "--rpc-http-cors-origins", String.join(",", origins));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getCorsAllowedDomains()).isEmpty();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpCorsOriginsNoneWithAnotherDomainMustFail() {
    final String[] origins = {"http://domain1.com", "none"};
    parseCommand("--rpc-http-cors-origins", String.join(",", origins));

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Value 'none' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsNoneWithAnotherDomainMustFailNoneFirst() {
    final String[] origins = {"none", "http://domain1.com"};
    parseCommand("--rpc-http-cors-origins", String.join(",", origins));

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Value 'none' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsAllWithAnotherDomainMustFail() {
    parseCommand("--rpc-http-cors-origins=http://domain1.com,all");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Values '*' or 'all' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsAllWithAnotherDomainMustFailAsFlags() {
    parseCommand("--rpc-http-cors-origins=http://domain1.com", "--rpc-http-cors-origins=all");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Values '*' or 'all' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsWildcardWithAnotherDomainMustFail() {
    parseCommand("--rpc-http-cors-origins=http://domain1.com,*");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Values '*' or 'all' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsWildcardWithAnotherDomainMustFailAsFlags() {
    parseCommand("--rpc-http-cors-origins=http://domain1.com", "--rpc-http-cors-origins=*");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Values '*' or 'all' can't be used with other domains");
  }

  @Test
  public void rpcHttpCorsOriginsInvalidRegexShouldFail() {
    final String[] origins = {"**"};
    parseCommand("--rpc-http-cors-origins", String.join(",", origins));

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Domain values result in invalid regex pattern");
  }

  @Test
  public void rpcHttpCorsOriginsEmptyValueFails() {
    parseCommand("--rpc-http-cors-origins=");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Domain cannot be empty string or null string.");
  }

  @Test
  public void rpcApisPropertyWithInvalidEntryMustDisplayError() {
    parseCommand("--rpc-http-api", "BOB");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    // PicoCLI uses longest option name for message when option has multiple names, so here plural.
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Invalid value for option '--rpc-http-api': invalid entries found [BOB]");
  }

  @Test
  public void rpcApisPropertyWithPluginNamespaceAreValid() {

    rpcEndpointServiceImpl.registerRPCEndpoint(
        "bob", "method", (Function<PluginRpcRequest, Object>) request -> "nothing");

    parseCommand("--rpc-http-api", "BOB");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getRpcApis())
        .containsExactlyInAnyOrder("BOB");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpMaxRequestContentLengthOptionMustBeUsed() {
    final int rpcHttpMaxRequestContentLength = 1;
    parseCommand(
        "--rpc-http-max-request-content-length", Long.toString(rpcHttpMaxRequestContentLength));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getMaxRequestContentLength())
        .isEqualTo(rpcHttpMaxRequestContentLength);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcHttpMaxBatchSizeOptionMustBeUsed() {
    final int rpcHttpMaxBatchSize = 1;
    parseCommand("--rpc-http-max-batch-size", Integer.toString(rpcHttpMaxBatchSize));

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getMaxBatchSize())
        .isEqualTo(rpcHttpMaxBatchSize);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void portInUseReportsError() throws IOException {
    final ServerSocket serverSocket = new ServerSocket(8545);

    parseCommandWithPortCheck("--rpc-http-enabled");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Port(s) '[8545/TCP]' already in use. Check for other processes using the port(s).");

    serverSocket.close();
  }

  @Test
  public void assertThatCheckPortClashRejectsAsExpected() throws Exception {
    // use WS port for HTTP
    final int port = 8546;
    parseCommand("--rpc-http-enabled", "--rpc-http-port", String.valueOf(port), "--rpc-ws-enabled");
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Port number '8546' has been specified multiple times. Please review the supplied configuration.");
  }

  @Test
  public void assertThatCheckPortClashAcceptsAsExpected() throws Exception {
    // use WS port for HTTP
    final int port = 8546;
    parseCommand("--rpc-http-enabled", "--rpc-http-port", String.valueOf(port));
    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void httpAuthenticationWithoutRequiredConfiguredOptionsMustFail() {
    parseCommand("--rpc-http-enabled", "--rpc-http-authentication-enabled");

    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Unable to authenticate JSON-RPC HTTP endpoint without a supplied credentials file or authentication public key file");
  }

  @Test
  public void httpAuthenticationAlgorithIsConfigured() {
    parseCommand("--rpc-http-authentication-jwt-algorithm", "ES256");

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getAuthenticationAlgorithm())
        .isEqualTo(JwtAlgorithm.ES256);
  }

  @Test
  public void httpAuthenticationPublicKeyIsConfigured() throws IOException {
    final Path publicKey = Files.createTempFile("public_key", "");
    parseCommand("--rpc-http-authentication-jwt-public-key-file", publicKey.toString());

    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(jsonRpcConfigArgumentCaptor.getValue().getAuthenticationPublicKeyFile().getPath())
        .isEqualTo(publicKey.toString());
  }
}
