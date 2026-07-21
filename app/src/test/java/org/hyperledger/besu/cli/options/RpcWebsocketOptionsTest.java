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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RpcWebsocketOptionsTest extends CommandTestAbstract {

  @Test
  public void rpcWsApiPropertyMustBeUsed() {
    final CommandTestAbstract.TestBesuCommand command =
        parseCommand("--rpc-ws-enabled", "--rpc-ws-api", "ETH, NET");

    assertThat(command).isNotNull();
    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getRpcApis())
        .containsExactlyInAnyOrder(ETH.name(), NET.name());

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsHostAndPortOptionMustBeUsed() {
    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand("--rpc-ws-enabled", "--rpc-ws-host", host, "--rpc-ws-port", String.valueOf(port));

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(wsRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsMaxFrameSizePropertyMustBeUsed() {
    final int maxFrameSize = 65535;
    parseCommand("--rpc-ws-max-frame-size", String.valueOf(maxFrameSize));

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getMaxFrameSize()).isEqualTo(maxFrameSize);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsMaxActiveConnectionsPropertyMustBeUsed() {
    final int maxConnections = 99;
    parseCommand("--rpc-ws-max-active-connections", String.valueOf(maxConnections));

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getMaxActiveConnections())
        .isEqualTo(maxConnections);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsRpcEnabledPropertyMustBeUsed() {
    parseCommand("--rpc-ws-enabled");

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().isEnabled()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void webSocketAuthenticationAlgorithmIsConfigured() {
    parseCommand("--rpc-ws-authentication-jwt-algorithm", "ES256");

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getAuthenticationAlgorithm())
        .isEqualTo(JwtAlgorithm.ES256);
  }

  @Test
  public void wsAuthenticationPublicKeyIsConfigured() throws IOException {
    final Path publicKey = Files.createTempFile("public_key", "");
    parseCommand("--rpc-ws-authentication-jwt-public-key-file", publicKey.toString());

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getAuthenticationPublicKeyFile().getPath())
        .isEqualTo(publicKey.toString());
  }

  @Test
  public void rpcWsHostAndMayBeLocalhost() {
    final String host = "localhost";
    parseCommand("--rpc-ws-enabled", "--rpc-ws-host", host);

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsHostAndMayBeIPv6() {
    final String host = "2600:DB8::8545";
    parseCommand("--rpc-ws-enabled", "--rpc-ws-host", host);

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsNoAuthApiMethodsCannotBeInvalid() {
    parseCommand("--rpc-ws-enabled", "--rpc-ws-api-methods-no-auth", "invalid");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--rpc-ws-api-methods-no-auth', options must be valid RPC methods");
  }

  @Test
  public void rpcWsApisPropertyWithInvalidEntryMustDisplayError() {
    parseCommand("--rpc-ws-api", "ETH,BOB,TEST");

    Mockito.verifyNoInteractions(mockRunnerBuilder);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();

    assertThat(commandErrorOutput.toString(UTF_8).trim())
        .contains("Invalid value for option '--rpc-ws-api': invalid entries found [BOB, TEST]");
  }

  @Test
  public void rpcWsOptionsRequiresServiceToBeEnabled() {
    parseCommand(
        "--rpc-ws-api",
        "ETH,NET",
        "--rpc-ws-host",
        "0.0.0.0",
        "--rpc-ws-port",
        "1234",
        "--rpc-ws-max-active-connections",
        "77",
        "--rpc-ws-max-frame-size",
        "65535");

    verifyOptionsConstraintLoggerCall(
        "--rpc-ws-enabled",
        "--rpc-ws-host",
        "--rpc-ws-port",
        "--rpc-ws-api",
        "--rpc-ws-max-active-connections",
        "--rpc-ws-max-frame-size");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void rpcWsOptionsRequiresServiceToBeEnabledToml() throws IOException {
    final Path toml =
        createTempFile(
            "toml",
            """
            rpc-ws-api=["ETH", "NET"]
            rpc-ws-host="0.0.0.0"
            rpc-ws-port=1234
            rpc-ws-max-active-connections=77
            rpc-ws-max-frame-size=65535
            """);

    parseCommand("--config-file", toml.toString());

    verifyOptionsConstraintLoggerCall(
        "--rpc-ws-enabled",
        "--rpc-ws-host",
        "--rpc-ws-port",
        "--rpc-ws-api",
        "--rpc-ws-max-active-connections",
        "--rpc-ws-max-frame-size");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void wsAuthenticationWithoutRequiredConfiguredOptionsMustFail() {
    parseCommand("--rpc-ws-enabled", "--rpc-ws-authentication-enabled");

    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Unable to authenticate JSON-RPC WebSocket endpoint without a supplied credentials file or authentication public key file");
  }

  @Test
  public void rpcWsRpcEnabledPropertyDefaultIsFalse() {
    parseCommand();

    verify(mockRunnerBuilder).webSocketConfiguration(wsRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(wsRpcConfigArgumentCaptor.getValue().isEnabled()).isFalse();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
