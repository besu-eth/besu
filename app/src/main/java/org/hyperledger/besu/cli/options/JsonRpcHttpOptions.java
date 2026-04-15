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

import static java.util.Arrays.asList;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration.DEFAULT_JSON_RPC_HOST;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration.DEFAULT_JSON_RPC_PORT;
import static org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration.DEFAULT_PRETTY_JSON_ENABLED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.DEFAULT_RPC_APIS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis.VALID_APIS;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.custom.CorsAllowedOriginsProperty;
import org.hyperledger.besu.cli.custom.RpcAuthFileValidator;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.authentication.JwtAlgorithm;
import org.hyperledger.besu.ethereum.api.tls.FileBasedPasswordProvider;
import org.hyperledger.besu.ethereum.api.tls.TlsClientAuthConfiguration;
import org.hyperledger.besu.ethereum.api.tls.TlsConfiguration;

import java.io.File;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import picocli.CommandLine;

/**
 * Handles configuration options for the JSON-RPC HTTP service, including validation and creation of
 * a JSON-RPC configuration.
 */
// TODO: implement CLIOption<JsonRpcConfiguration>
public class JsonRpcHttpOptions {
  @CommandLine.Option(
      names = {"--rpc-http-enabled"},
      description = "Set to start the JSON-RPC HTTP service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcHttpEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--rpc-http-host"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "Host for JSON-RPC HTTP to listen on (default: ${DEFAULT-VALUE})")
  private String rpcHttpHost = DEFAULT_JSON_RPC_HOST;

  @CommandLine.Option(
      names = {"--rpc-http-port"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description = "Port for JSON-RPC HTTP to listen on (default: ${DEFAULT-VALUE})")
  private final Integer rpcHttpPort = DEFAULT_JSON_RPC_PORT;

  @CommandLine.Option(
      names = {"--rpc-http-max-active-connections"},
      description =
          "Maximum number of HTTP connections allowed for JSON-RPC (default: ${DEFAULT-VALUE}). Once this limit is reached, incoming connections will be rejected.")
  private final Integer rpcHttpMaxConnections = DefaultCommandValues.DEFAULT_HTTP_MAX_CONNECTIONS;

  // A list of origins URLs that are accepted by the JsonRpcHttpServer (CORS)
  @CommandLine.Option(
      names = {"--rpc-http-cors-origins"},
      description = "Comma separated origin domain URLs for CORS validation (default: none)")
  private final CorsAllowedOriginsProperty rpcHttpCorsAllowedOrigins =
      new CorsAllowedOriginsProperty();

  @CommandLine.Option(
      names = {"--rpc-http-api", "--rpc-http-apis"},
      paramLabel = "<api name>",
      split = " {0,1}, {0,1}",
      arity = "1..*",
      description =
          "Comma separated list of APIs to enable on JSON-RPC HTTP service (default: ${DEFAULT-VALUE})")
  private final List<String> rpcHttpApis = DEFAULT_RPC_APIS;

  @CommandLine.Option(
      names = {"--rpc-http-api-method-no-auth", "--rpc-http-api-methods-no-auth"},
      paramLabel = "<api name>",
      split = " {0,1}, {0,1}",
      arity = "1..*",
      description =
          "Comma separated list of API methods to exclude from RPC authentication services, RPC HTTP authentication must be enabled")
  private final List<String> rpcHttpApiMethodsNoAuth = new ArrayList<String>();

  @CommandLine.Option(
      names = {"--rpc-http-authentication-enabled"},
      description =
          "Require authentication for the JSON-RPC HTTP service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcHttpAuthenticationEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--rpc-http-authentication-credentials-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description =
          "Storage file for JSON-RPC HTTP authentication credentials (default: ${DEFAULT-VALUE})")
  private String rpcHttpAuthenticationCredentialsFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-authentication-jwt-public-key-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "JWT public key file for JSON-RPC HTTP authentication")
  private final File rpcHttpAuthenticationPublicKeyFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-authentication-jwt-algorithm"},
      description =
          "Encryption algorithm used for HTTP JWT public key. Possible values are ${COMPLETION-CANDIDATES}"
              + " (default: ${DEFAULT-VALUE})")
  private final JwtAlgorithm rpcHttpAuthenticationAlgorithm =
      DefaultCommandValues.DEFAULT_JWT_ALGORITHM;

  @CommandLine.Option(
      names = {"--rpc-http-tls-enabled"},
      description = "Enable TLS for the JSON-RPC HTTP service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcHttpTlsEnabled = false;

  @CommandLine.Option(
      names = {"--rpc-http-tls-keystore-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description =
          "Keystore (PKCS#12) containing key/certificate for the JSON-RPC HTTP service. Required if TLS is enabled.")
  private final Path rpcHttpTlsKeyStoreFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-tls-keystore-password-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description =
          "File containing password to unlock keystore for the JSON-RPC HTTP service. Required if TLS is enabled.")
  private final Path rpcHttpTlsKeyStorePasswordFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-tls-client-auth-enabled"},
      description =
          "Enable TLS client authentication for the JSON-RPC HTTP service (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcHttpTlsClientAuthEnabled = false;

  @CommandLine.Option(
      names = {"--rpc-http-tls-known-clients-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description =
          "Path to file containing clients certificate common name and fingerprint for client authentication")
  private final Path rpcHttpTlsKnownClientsFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-tls-ca-clients-enabled"},
      description =
          "Enable to accept clients certificate signed by a valid CA for client authentication (default: ${DEFAULT-VALUE})")
  private final Boolean isRpcHttpTlsCAClientsEnabled = false;

  @CommandLine.Option(
      names = {"--rpc-http-tls-truststore-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the truststore file for the JSON-RPC HTTP service.")
  private final Path rpcHttpTlsTruststoreFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-tls-truststore-password-file"},
      paramLabel = DefaultCommandValues.MANDATORY_FILE_FORMAT_HELP,
      description = "Path to the file containing the password for the truststore.")
  private final Path rpcHttpTlsTruststorePasswordFile = null;

  @CommandLine.Option(
      names = {"--rpc-http-tls-protocol", "--rpc-http-tls-protocols"},
      description = "Comma separated list of TLS protocols to support (default: ${DEFAULT-VALUE})",
      split = ",",
      arity = "1..*")
  private final List<String> rpcHttpTlsProtocols =
      new ArrayList<>(DefaultCommandValues.DEFAULT_TLS_PROTOCOLS);

  @CommandLine.Option(
      names = {"--rpc-http-tls-cipher-suite", "--rpc-http-tls-cipher-suites"},
      description = "Comma separated list of TLS cipher suites to support",
      split = ",",
      arity = "1..*")
  private final List<String> rpcHttpTlsCipherSuites = new ArrayList<>();

  @CommandLine.Option(
      names = {"--rpc-http-max-batch-size"},
      paramLabel = DefaultCommandValues.MANDATORY_INTEGER_FORMAT_HELP,
      description =
          "Specifies the maximum number of requests in a single RPC batch request via RPC. -1 specifies no limit  (default: ${DEFAULT-VALUE})")
  private final Integer rpcHttpMaxBatchSize = DefaultCommandValues.DEFAULT_HTTP_MAX_BATCH_SIZE;

  @CommandLine.Option(
      names = {"--rpc-http-max-request-content-length"},
      paramLabel = DefaultCommandValues.MANDATORY_LONG_FORMAT_HELP,
      description = "Specifies the maximum request content length. (default: ${DEFAULT-VALUE})")
  private final Long rpcHttpMaxRequestContentLength =
      DefaultCommandValues.DEFAULT_MAX_REQUEST_CONTENT_LENGTH;

  @CommandLine.Option(
      names = {"--json-pretty-print-enabled"},
      description = "Enable JSON pretty print format (default: ${DEFAULT-VALUE})")
  private final Boolean prettyJsonEnabled = DEFAULT_PRETTY_JSON_ENABLED;

  /** Default constructor */
  public JsonRpcHttpOptions() {}

  /**
   * Validates the Rpc Http options.
   *
   * @param logger Logger instance
   * @param commandLine CommandLine instance
   * @param configuredApis Predicate for configured APIs
   */
  public void validate(
      final Logger logger, final CommandLine commandLine, final Predicate<String> configuredApis) {

    if (!rpcHttpApis.stream().allMatch(configuredApis)) {
      final List<String> invalidHttpApis = new ArrayList<>(rpcHttpApis);
      invalidHttpApis.removeAll(VALID_APIS);
      throw new CommandLine.ParameterException(
          commandLine,
          "Invalid value for option '--rpc-http-api': invalid entries found " + invalidHttpApis);
    }

    final boolean validHttpApiMethods =
        rpcHttpApiMethodsNoAuth.stream().allMatch(RpcMethod::rpcMethodExists);

    if (!validHttpApiMethods) {
      throw new CommandLine.ParameterException(
          commandLine,
          "Invalid value for option '--rpc-http-api-methods-no-auth', options must be valid RPC methods");
    }

    if (isRpcHttpAuthenticationEnabled) {
      CommandLineUtils.checkOptionDependencies(
          logger,
          commandLine,
          "--rpc-http-authentication-public-key-file",
          rpcHttpAuthenticationPublicKeyFile == null,
          List.of("--rpc-http-authentication-jwt-algorithm"));
    }

    if (isRpcHttpAuthenticationEnabled
        && rpcHttpAuthenticationCredentialsFile(commandLine) == null
        && rpcHttpAuthenticationPublicKeyFile == null) {
      throw new CommandLine.ParameterException(
          commandLine,
          "Unable to authenticate JSON-RPC HTTP endpoint without a supplied credentials file or authentication public key file");
    }

    checkDependencies(logger, commandLine);

    if (isRpcTlsConfigurationRequired()) {
      validateTls(commandLine);
    }
  }

  /**
   * Creates a JsonRpcConfiguration based on the provided options.
   *
   * @return configuration populated from options or defaults
   */
  public JsonRpcConfiguration jsonRpcConfiguration() {

    final JsonRpcConfiguration jsonRpcConfiguration = JsonRpcConfiguration.createDefault();
    jsonRpcConfiguration.setEnabled(isRpcHttpEnabled);
    jsonRpcConfiguration.setPort(rpcHttpPort);
    jsonRpcConfiguration.setMaxActiveConnections(rpcHttpMaxConnections);
    jsonRpcConfiguration.setCorsAllowedDomains(rpcHttpCorsAllowedOrigins);
    jsonRpcConfiguration.setRpcApis(rpcHttpApis.stream().distinct().collect(Collectors.toList()));
    jsonRpcConfiguration.setNoAuthRpcApis(
        rpcHttpApiMethodsNoAuth.stream().distinct().collect(Collectors.toList()));
    jsonRpcConfiguration.setAuthenticationEnabled(isRpcHttpAuthenticationEnabled);
    jsonRpcConfiguration.setAuthenticationCredentialsFile(rpcHttpAuthenticationCredentialsFile);
    jsonRpcConfiguration.setAuthenticationPublicKeyFile(rpcHttpAuthenticationPublicKeyFile);
    jsonRpcConfiguration.setAuthenticationAlgorithm(rpcHttpAuthenticationAlgorithm);
    jsonRpcConfiguration.setTlsConfiguration(rpcHttpTlsConfiguration());
    jsonRpcConfiguration.setMaxBatchSize(rpcHttpMaxBatchSize);
    jsonRpcConfiguration.setMaxRequestContentLength(rpcHttpMaxRequestContentLength);
    jsonRpcConfiguration.setPrettyJsonEnabled(prettyJsonEnabled);
    return jsonRpcConfiguration;
  }

  /**
   * Creates a JsonRpcConfiguration based on the provided options.
   *
   * @param hostsAllowlist List of hosts allowed
   * @param defaultHostAddress Default host address
   * @param timeoutSec timeout in seconds
   * @param streamingTimeoutSec timeout in seconds for streaming methods
   * @return A JsonRpcConfiguration instance
   */
  public JsonRpcConfiguration jsonRpcConfiguration(
      final List<String> hostsAllowlist,
      final String defaultHostAddress,
      final Long timeoutSec,
      final Long streamingTimeoutSec) {

    final JsonRpcConfiguration jsonRpcConfiguration = this.jsonRpcConfiguration();

    jsonRpcConfiguration.setHost(
        Strings.isNullOrEmpty(rpcHttpHost) ? defaultHostAddress : rpcHttpHost);
    jsonRpcConfiguration.setHostsAllowlist(hostsAllowlist);
    jsonRpcConfiguration.setHttpTimeoutSec(timeoutSec);
    jsonRpcConfiguration.setHttpStreamingTimeoutSec(streamingTimeoutSec);
    return jsonRpcConfiguration;
  }

  /**
   * Checks dependencies between options.
   *
   * @param logger Logger instance
   * @param commandLine CommandLine instance
   */
  public void checkDependencies(final Logger logger, final CommandLine commandLine) {
    checkRpcTlsClientAuthOptionsDependencies(logger, commandLine);
    checkRpcTlsOptionsDependencies(logger, commandLine);
    checkRpcHttpOptionsDependencies(logger, commandLine);
  }

  private void checkRpcTlsClientAuthOptionsDependencies(
      final Logger logger, final CommandLine commandLine) {
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-http-tls-client-auth-enabled",
        !isRpcHttpTlsClientAuthEnabled,
        asList(
            "--rpc-http-tls-known-clients-file",
            "--rpc-http-tls-ca-clients-enabled",
            "--rpc-http-tls-truststore-file",
            "--rpc-http-tls-truststore-password-file"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-http-tls-truststore-file",
        rpcHttpTlsTruststoreFile == null,
        asList("--rpc-http-tls-truststore-password-file"));
  }

  private void checkRpcTlsOptionsDependencies(final Logger logger, final CommandLine commandLine) {
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-http-tls-enabled",
        !isRpcHttpTlsEnabled,
        asList(
            "--rpc-http-tls-keystore-file",
            "--rpc-http-tls-keystore-password-file",
            "--rpc-http-tls-client-auth-enabled",
            "--rpc-http-tls-known-clients-file",
            "--rpc-http-tls-ca-clients-enabled",
            "--rpc-http-tls-protocols",
            "--rpc-http-tls-cipher-suite",
            "--rpc-http-tls-cipher-suites"));
  }

  private void checkRpcHttpOptionsDependencies(final Logger logger, final CommandLine commandLine) {
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--rpc-http-enabled",
        !isRpcHttpEnabled,
        asList(
            "--rpc-http-api",
            "--rpc-http-apis",
            "--rpc-http-api-method-no-auth",
            "--rpc-http-api-methods-no-auth",
            "--rpc-http-cors-origins",
            "--rpc-http-host",
            "--rpc-http-port",
            "--rpc-http-max-active-connections",
            "--rpc-http-authentication-enabled",
            "--rpc-http-authentication-credentials-file",
            "--rpc-http-authentication-public-key-file",
            "--rpc-http-tls-enabled",
            "--rpc-http-tls-keystore-file",
            "--rpc-http-tls-keystore-password-file",
            "--rpc-http-tls-client-auth-enabled",
            "--rpc-http-tls-known-clients-file",
            "--rpc-http-tls-ca-clients-enabled",
            "--rpc-http-authentication-jwt-algorithm",
            "--rpc-http-tls-protocols",
            "--rpc-http-tls-cipher-suite",
            "--rpc-http-tls-cipher-suites"));
  }

  private void validateTls(final CommandLine commandLine) {
    if (rpcHttpTlsKeyStoreFile == null) {
      throw new CommandLine.ParameterException(
          commandLine, "Keystore file is required when TLS is enabled for JSON-RPC HTTP endpoint");
    }

    if (rpcHttpTlsKeyStorePasswordFile == null) {
      throw new CommandLine.ParameterException(
          commandLine,
          "File containing password to unlock keystore is required when TLS is enabled for JSON-RPC HTTP endpoint");
    }

    if (isRpcHttpTlsClientAuthEnabled) {
      if (!isRpcHttpTlsCAClientsEnabled
          && rpcHttpTlsKnownClientsFile == null
          && rpcHttpTlsTruststoreFile == null) {
        throw new CommandLine.ParameterException(
            commandLine,
            "Configuration error: TLS client authentication is enabled, but none of the following options are provided: "
                + "1. Specify a known-clients file (--rpc-http-tls-known-clients-file) and/or  Enable CA clients (--rpc-http-tls-ca-clients-enabled). "
                + "2. Specify a truststore file and its password file (--rpc-http-tls-truststore-file and --rpc-http-tls-truststore-password-file). "
                + "Only one of these options must be configured");
      }

      if (rpcHttpTlsTruststoreFile != null && rpcHttpTlsTruststorePasswordFile == null) {
        throw new CommandLine.ParameterException(
            commandLine,
            "Configuration error: A truststore file is specified for JSON RPC HTTP endpoint, but the corresponding truststore password file (--rpc-http-tls-truststore-password-file) is missing");
      }

      if ((isRpcHttpTlsCAClientsEnabled || rpcHttpTlsKnownClientsFile != null)
          && rpcHttpTlsTruststoreFile != null) {
        throw new CommandLine.ParameterException(
            commandLine,
            "Configuration error: Truststore file (--rpc-http-tls-truststore-file) cannot be used together with CA clients (--rpc-http-tls-ca-clients-enabled) or a known-clients (--rpc-http-tls-known-clients-file) option. "
                + "These options are mutually exclusive. Choose either truststore-based authentication or known-clients/CA clients configuration.");
      }
    }

    rpcHttpTlsProtocols.retainAll(getJDKEnabledProtocols());
    if (rpcHttpTlsProtocols.isEmpty()) {
      throw new CommandLine.ParameterException(
          commandLine,
          "No valid TLS protocols specified (the following protocols are enabled: "
              + getJDKEnabledProtocols()
              + ")");
    }

    for (final String cipherSuite : rpcHttpTlsCipherSuites) {
      if (!getJDKEnabledCipherSuites().contains(cipherSuite)) {
        throw new CommandLine.ParameterException(
            commandLine, "Invalid TLS cipher suite specified " + cipherSuite);
      }
    }
  }

  private Optional<TlsConfiguration> rpcHttpTlsConfiguration() {
    if (!isRpcTlsConfigurationRequired()) {
      return Optional.empty();
    }

    rpcHttpTlsCipherSuites.retainAll(getJDKEnabledCipherSuites());

    return Optional.of(
        TlsConfiguration.Builder.aTlsConfiguration()
            .withKeyStorePath(rpcHttpTlsKeyStoreFile)
            .withKeyStorePasswordSupplier(
                new FileBasedPasswordProvider(rpcHttpTlsKeyStorePasswordFile))
            .withClientAuthConfiguration(rpcHttpTlsClientAuthConfiguration())
            .withSecureTransportProtocols(rpcHttpTlsProtocols)
            .withCipherSuites(rpcHttpTlsCipherSuites)
            .build());
  }

  private boolean isRpcTlsConfigurationRequired() {
    return isRpcHttpEnabled && isRpcHttpTlsEnabled;
  }

  private TlsClientAuthConfiguration rpcHttpTlsClientAuthConfiguration() {
    if (isRpcHttpTlsClientAuthEnabled) {
      TlsClientAuthConfiguration.Builder tlsClientAuthConfigurationBuilder =
          TlsClientAuthConfiguration.Builder.aTlsClientAuthConfiguration()
              .withKnownClientsFile(rpcHttpTlsKnownClientsFile)
              .withCaClientsEnabled(isRpcHttpTlsCAClientsEnabled)
              .withTruststorePath(rpcHttpTlsTruststoreFile);

      if (rpcHttpTlsTruststorePasswordFile != null) {
        tlsClientAuthConfigurationBuilder.withTruststorePasswordSupplier(
            new FileBasedPasswordProvider(rpcHttpTlsTruststorePasswordFile));
      }
      return tlsClientAuthConfigurationBuilder.build();
    }

    return null;
  }

  private static List<String> getJDKEnabledCipherSuites() {
    try {
      final SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, null, null);
      final SSLEngine engine = context.createSSLEngine();
      return Arrays.asList(engine.getEnabledCipherSuites());
    } catch (final KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> getJDKEnabledProtocols() {
    try {
      final SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, null, null);
      final SSLEngine engine = context.createSSLEngine();
      return Arrays.asList(engine.getEnabledProtocols());
    } catch (final KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private String rpcHttpAuthenticationCredentialsFile(final CommandLine commandLine) {
    final String filename = rpcHttpAuthenticationCredentialsFile;

    if (filename != null) {
      RpcAuthFileValidator.validate(commandLine, filename, "HTTP");
    }
    return filename;
  }

  /**
   * Returns the list of APIs enabled for RPC over HTTP.
   *
   * @return A list of APIs
   */
  public List<String> getRpcHttpApis() {
    return rpcHttpApis;
  }

  /**
   * Returns the host for RPC over HTTP.
   *
   * @return The port number
   */
  public String getRpcHttpHost() {
    return rpcHttpHost;
  }

  /**
   * Returns the port for RPC over HTTP.
   *
   * @return The port number
   */
  public Integer getRpcHttpPort() {
    return rpcHttpPort;
  }

  /**
   * Checks if RPC over HTTP is enabled.
   *
   * @return true if enabled, false otherwise
   */
  public Boolean isRpcHttpEnabled() {
    return isRpcHttpEnabled;
  }
}
