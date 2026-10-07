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

import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.DEFAULT_RPC_APIS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.VALID_APIS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration.DEFAULT_WEBSOCKET_PORT;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.custom.RpcAuthFileValidator;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.WebSocketConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import picocli.CommandLine;

/** This class represents the WebSocket options for the RPC. */
public class RpcWebsocketOptions {

  static class KeystorePasswordOptions {
    @CommandLine.Option(
        names = {"--rpc-ws-ssl-keystore-password"},
        paramLabel = "<PASSWORD>",
        description = "Password for the WebSocket RPC keystore file")
    private String rpcWsKeyStorePassword;

    @CommandLine.Option(
        names = {"--rpc-ws-ssl-keystore-password-file"},
        paramLabel = "<FILE>",
        description = "File containing the password for WebSocket keystore.")
    private String rpcWsKeystorePasswordFile;
  }

  static class TruststorePasswordOptions {
    @CommandLine.Option(
        names = {"--rpc-ws-ssl-truststore-password"},
        paramLabel = "<PASSWORD>",
        description = "Password for the WebSocket RPC truststore file")
    private String rpcWsTrustStorePassword;

    @CommandLine.Option(
        names = {"--rpc-ws-ssl-truststore-password-file"},
        paramLabel = "<FILE>",
        description = "File containing the password for WebSocket truststore.")
    private String rpcWsTruststorePasswordFile;
  }

  @CommandLine.Option(
      names = {"--rpc-ws-authentication-jwt-algorithm"},
      description =
          "Encryption algorithm used for Websockets JWT public key. Possible values are ${COMPLETION-CANDIDATES}"
              + " (default: ${DEFAULT-VALUE})")
  private final JwtAlgorithm rpcWebsocketsAuthenticationAlgorithm =
      DefaultCommandValues.DEFAULT_JWT_ALGORITHM;

  @CommandLine.Option(
      names = {"--rpc-ws-enabled"},
      description = "Set to start the JSON-RPC WebSocket service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcWsEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--rpc-ws-host"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "Host for JSON-RPC WebSocket service to listen on (default: ${DEFAULT-VALUE})")
  private String rpcWsHost;

  @CommandLine.Option(
      names = {"--rpc-ws-port"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description = "Port for JSON-RPC WebSocket service to listen on (default: ${DEFAULT-VALUE})")
  private final Integer rpcWsPort = DEFAULT_WEBSOCKET_PORT;

  @CommandLine.Option(
      names = {"--rpc-ws-max-frame-size"},
      description =
          "Maximum size in bytes for JSON-RPC WebSocket frames (default: ${DEFAULT-VALUE}). If this limit is exceeded, the websocket will be disconnected.")
  private final Integer rpcWsMaxFrameSize = DefaultCommandValues.DEFAULT_WS_MAX_FRAME_SIZE;

  @CommandLine.Option(
      names = {"--rpc-ws-max-active-connections"},
      description =
          "Maximum number of WebSocket connections allowed for JSON-RPC (default: ${DEFAULT-VALUE}). Once this limit is reached, incoming connections will be rejected.")
  private final Integer rpcWsMaxConnections = DefaultCommandValues.DEFAULT_WS_MAX_CONNECTIONS;

  @CommandLine.Option(
      names = {"--rpc-ws-max-active-subscriptions"},
      description =
          "Maximum number of global active WebSocket subscriptions allowed for JSON-RPC (eth_subscribe). Must be >= 0. 0 specifies no limit (default: ${DEFAULT-VALUE}). Once this limit is reached, incoming subscriptions will return an error.")
  private final Integer rpcWsMaxActiveSubscriptions =
      DefaultCommandValues.DEFAULT_WS_MAX_ACTIVE_SUBSCRIPTIONS;

  @CommandLine.Option(
      names = {"--rpc-ws-api", "--rpc-ws-apis"},
      paramLabel = "<api name>",
      split = " {0,1}, {0,1}",
      arity = "1..*",
      description =
          "Comma separated list of APIs to enable on JSON-RPC WebSocket service (default: ${DEFAULT-VALUE})")
  private final List<String> rpcWsApis = DEFAULT_RPC_APIS;

  @CommandLine.Option(
      names = {"--rpc-ws-api-methods-no-auth", "--rpc-ws-api-method-no-auth"},
      paramLabel = "<api name>",
      split = " {0,1}, {0,1}",
      arity = "1..*",
      description =
          "Comma separated list of RPC methods to exclude from RPC authentication services, RPC WebSocket authentication must be enabled")
  private final List<String> rpcWsApiMethodsNoAuth = new ArrayList<String>();

  @CommandLine.Option(
      names = {"--rpc-ws-authentication-enabled"},
      description =
          "Require authentication for the JSON-RPC WebSocket service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcWsAuthenticationEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--rpc-ws-authentication-credentials-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description =
          "Storage file for JSON-RPC WebSocket authentication credentials (default: ${DEFAULT-VALUE})")
  private String rpcWsAuthenticationCredentialsFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-authentication-jwt-public-key-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "JWT public key file for JSON-RPC WebSocket authentication")
  private final File rpcWsAuthenticationPublicKeyFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-enabled"},
      description = "Enable SSL/TLS for the WebSocket RPC service")
  private final Boolean isRpcWsSslEnabled = false;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-keystore-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the keystore file for the WebSocket RPC service")
  private String rpcWsKeyStoreFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-key-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the PEM key file for the WebSocket RPC service")
  private String rpcWsKeyFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-cert-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the PEM cert file for the WebSocket RPC service")
  private String rpcWsCertFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-keystore-type"},
      paramLabel = "<TYPE>",
      description = "Type of the WebSocket RPC keystore (JKS, PKCS12, PEM)")
  private String rpcWsKeyStoreType = null;

  // For client authentication (mTLS)
  @CommandLine.Option(
      names = {"--rpc-ws-ssl-client-auth-enabled"},
      description = "Enable client authentication for the WebSocket RPC service")
  private final Boolean isRpcWsClientAuthEnabled = false;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-truststore-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the truststore file for the WebSocket RPC service")
  private String rpcWsTrustStoreFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-trustcert-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the PEM trustcert file for the WebSocket RPC service")
  private String rpcWsTrustCertFile = null;

  @CommandLine.Option(
      names = {"--rpc-ws-ssl-truststore-type"},
      paramLabel = "<TYPE>",
      description = "Type of the truststore (JKS, PKCS12, PEM)")
  private String rpcWsTrustStoreType = null;

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
  private KeystorePasswordOptions keystorePasswordOptions;

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
  private TruststorePasswordOptions truststorePasswordOptions;

  /** Default Constructor. */
  public RpcWebsocketOptions() {}

  /**
   * Validates the WebSocket options.
   *
   * @param logger Logger instance
   * @param commandLine CommandLine instance
   * @param configuredApis Predicate for configured APIs
   */
  public void validate(
      final Logger logger, final CommandLine commandLine, final Predicate<String> configuredApis) {
    checkOptionDependencies(logger, commandLine);

    if (!rpcWsApis.stream().allMatch(configuredApis)) {
      final List<String> invalidWsApis = new ArrayList<>(rpcWsApis);
      invalidWsApis.removeAll(VALID_APIS);
      throw new CommandLine.ParameterException(
          commandLine,
          "Invalid value for option '--rpc-ws-api': invalid entries found " + invalidWsApis);
    }

    final boolean validWsApiMethods =
        rpcWsApiMethodsNoAuth.stream().allMatch(RpcMethod::rpcMethodExists);

    if (!validWsApiMethods) {
      throw new CommandLine.ParameterException(
          commandLine,
          "Invalid value for option '--rpc-ws-api-methods-no-auth', options must be valid RPC methods");
    }

    if (isRpcWsAuthenticationEnabled
        && rpcWsAuthenticationCredentialsFile(commandLine) == null
        && rpcWsAuthenticationPublicKeyFile == null) {
      throw new CommandLine.ParameterException(
          commandLine,
          "Unable to authenticate JSON-RPC WebSocket endpoint without a supplied credentials file or authentication public key file");
    }

    if (rpcWsMaxActiveSubscriptions < 0) {
      throw new CommandLine.ParameterException(
          commandLine, "--rpc-ws-max-active-subscriptions must be >= 0 (0 specifies no limit)");
    }
  }

  /**
   * Checks the dependencies of the WebSocket options.
   *
   * @param logger Logger instance
   * @param commandLine CommandLine instance
   */
  private void checkOptionDependencies(final Logger logger, final CommandLine commandLine) {
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-ws-enabled",
        !isRpcWsEnabled,
        List.of(
            "--rpc-ws-api",
            "--rpc-ws-apis",
            "--rpc-ws-api-method-no-auth",
            "--rpc-ws-api-methods-no-auth",
            "--rpc-ws-host",
            "--rpc-ws-port",
            "--rpc-ws-max-frame-size",
            "--rpc-ws-max-active-connections",
            "--rpc-ws-authentication-enabled",
            "--rpc-ws-authentication-credentials-file",
            "--rpc-ws-authentication-public-key-file",
            "--rpc-ws-authentication-jwt-algorithm",
            "--rpc-ws-ssl-enabled"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-ws-ssl-enabled",
        !isRpcWsSslEnabled,
        List.of(
            "--rpc-ws-ssl-keystore-file",
            "--rpc-ws-ssl-keystore-type",
            "--rpc-ws-ssl-client-auth-enabled"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-ws-ssl-client-auth-enabled",
        !isRpcWsClientAuthEnabled,
        List.of(
            "--rpc-ws-ssl-truststore-file",
            "--rpc-ws-ssl-truststore-type",
            "--rpc-ws-ssl-trustcert-file"));

    if (isRpcWsSslEnabled) {
      if ("PEM".equalsIgnoreCase(rpcWsKeyStoreType)) {
        CommandLineUtils.checkOptionDependencies(
            logger,
            commandLine,
            "--rpc-ws-ssl-key-file",
            rpcWsKeyFile == null,
            List.of("--rpc-ws-ssl-cert-file"));
        CommandLineUtils.checkOptionDependencies(
            logger,
            commandLine,
            "--rpc-ws-ssl-cert-file",
            rpcWsCertFile == null,
            List.of("--rpc-ws-ssl-key-file"));
      } else {
        CommandLineUtils.checkOptionDependencies(
            logger,
            commandLine,
            "--rpc-ws-ssl-keystore-file",
            rpcWsKeyStoreFile == null,
            List.of("--rpc-ws-ssl-keystore-password", "--rpc-ws-ssl-keystore-password-file"));
      }
    }

    if (isRpcWsClientAuthEnabled && !"PEM".equalsIgnoreCase(rpcWsTrustStoreType)) {
      CommandLineUtils.checkOptionDependencies(
          logger,
          commandLine,
          "--rpc-ws-ssl-truststore-file",
          rpcWsTrustStoreFile == null,
          List.of("--rpc-ws-ssl-truststore-password", "--rpc-ws-ssl-truststore-password-file"));
    }

    if (isRpcWsAuthenticationEnabled) {
      CommandLineUtils.checkOptionDependencies(
          logger,
          commandLine,
          "--rpc-ws-authentication-public-key-file",
          rpcWsAuthenticationPublicKeyFile == null,
          List.of("--rpc-ws-authentication-jwt-algorithm"));
    }
  }

  /**
   * Creates a WebSocket configuration based on the WebSocket options.
   *
   * @param hostsAllowlist List of allowed hosts
   * @param defaultHostAddress Default host address
   * @param wsTimeoutSec WebSocket timeout in seconds
   * @return WebSocketConfiguration instance
   */
  public WebSocketConfiguration webSocketConfiguration(
      final List<String> hostsAllowlist, final String defaultHostAddress, final Long wsTimeoutSec) {
    final WebSocketConfiguration webSocketConfiguration = WebSocketConfiguration.createDefault();
    webSocketConfiguration.setEnabled(isRpcWsEnabled);
    webSocketConfiguration.setHost(
        Strings.isNullOrEmpty(rpcWsHost) ? defaultHostAddress : rpcWsHost);
    webSocketConfiguration.setPort(rpcWsPort);
    webSocketConfiguration.setMaxFrameSize(rpcWsMaxFrameSize);
    webSocketConfiguration.setMaxActiveConnections(rpcWsMaxConnections);
    webSocketConfiguration.setMaxActiveSubscriptions(rpcWsMaxActiveSubscriptions);
    webSocketConfiguration.setRpcApis(rpcWsApis);
    webSocketConfiguration.setRpcApisNoAuth(
        rpcWsApiMethodsNoAuth.stream().distinct().collect(Collectors.toList()));
    webSocketConfiguration.setAuthenticationEnabled(isRpcWsAuthenticationEnabled);
    webSocketConfiguration.setAuthenticationCredentialsFile(rpcWsAuthenticationCredentialsFile);
    webSocketConfiguration.setHostsAllowlist(hostsAllowlist);
    webSocketConfiguration.setAuthenticationPublicKeyFile(rpcWsAuthenticationPublicKeyFile);
    webSocketConfiguration.setAuthenticationAlgorithm(rpcWebsocketsAuthenticationAlgorithm);
    webSocketConfiguration.setTimeoutSec(wsTimeoutSec);
    webSocketConfiguration.setSslEnabled(isRpcWsSslEnabled);
    webSocketConfiguration.setKeyStorePath(rpcWsKeyStoreFile);
    webSocketConfiguration.setKeyStoreType(rpcWsKeyStoreType);
    webSocketConfiguration.setClientAuthEnabled(isRpcWsClientAuthEnabled);
    webSocketConfiguration.setTrustStorePath(rpcWsTrustStoreFile);
    webSocketConfiguration.setTrustStoreType(rpcWsTrustStoreType);
    webSocketConfiguration.setKeyPath(rpcWsKeyFile);
    webSocketConfiguration.setCertPath(rpcWsCertFile);
    webSocketConfiguration.setTrustCertPath(rpcWsTrustCertFile);

    if (keystorePasswordOptions != null) {
      webSocketConfiguration.setKeyStorePassword(keystorePasswordOptions.rpcWsKeyStorePassword);
      webSocketConfiguration.setKeyStorePasswordFile(
          keystorePasswordOptions.rpcWsKeystorePasswordFile);
    }

    if (truststorePasswordOptions != null) {
      webSocketConfiguration.setTrustStorePassword(
          truststorePasswordOptions.rpcWsTrustStorePassword);
      webSocketConfiguration.setTrustStorePasswordFile(
          truststorePasswordOptions.rpcWsTruststorePasswordFile);
    }

    return webSocketConfiguration;
  }

  /**
   * Validates the authentication credentials file for the WebSocket.
   *
   * @param commandLine CommandLine instance
   * @return Filename of the authentication credentials file
   */
  private String rpcWsAuthenticationCredentialsFile(final CommandLine commandLine) {
    final String filename = rpcWsAuthenticationCredentialsFile;

    if (filename != null) {
      RpcAuthFileValidator.validate(commandLine, filename, "WS");
    }
    return filename;
  }

  /**
   * Returns the list of APIs for the WebSocket.
   *
   * @return List of APIs
   */
  public List<String> getRpcWsApis() {
    return rpcWsApis;
  }

  /**
   * Checks if the WebSocket service is enabled.
   *
   * @return Boolean indicating if the WebSocket service is enabled
   */
  public Boolean isRpcWsEnabled() {
    return isRpcWsEnabled;
  }

  /**
   * Returns the port for the WebSocket service.
   *
   * @return Port number
   */
  public Integer getRpcWsPort() {
    return rpcWsPort;
  }
}
