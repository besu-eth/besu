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
import static org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration.DEFAULT_GRAPHQL_HTTP_PORT;

import org.hyperledger.besu.cli.DefaultCommandValues;
import org.hyperledger.besu.cli.custom.CorsAllowedOriginsProperty;
import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;

import java.util.List;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import picocli.CommandLine;

/** Handles configuration options for the GraphQL HTTP service in Besu. */
// TODO: implement CLIOptions<GraphQLConfiguration>
public class GraphQlOptions {
  @CommandLine.Option(
      names = {"--graphql-http-enabled"},
      description = "Set to start the GraphQL HTTP service (default: ${DEFAULT-VALUE})")
  private final Boolean isGraphQLHttpEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--graphql-http-host"},
      paramLabel = DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP,
      description = "Host for GraphQL HTTP to listen on (default: ${DEFAULT-VALUE})")
  private String graphQLHttpHost;

  @CommandLine.Option(
      names = {"--graphql-http-port"},
      paramLabel = DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP,
      description = "Port for GraphQL HTTP to listen on (default: ${DEFAULT-VALUE})")
  private final Integer graphQLHttpPort = DEFAULT_GRAPHQL_HTTP_PORT;

  @CommandLine.Option(
      names = {"--graphql-http-cors-origins"},
      description = "Comma separated origin domain URLs for CORS validation (default: none)")
  private final CorsAllowedOriginsProperty graphQLHttpCorsAllowedOrigins =
      new CorsAllowedOriginsProperty();

  @CommandLine.Option(
      names = {"--graphql-tls-enabled"},
      description = "Enable TLS for GraphQL HTTP service")
  private Boolean graphqlTlsEnabled = false;

  @CommandLine.Option(
      names = {"--graphql-tls-keystore-file"},
      description = "Path to the TLS keystore file for GraphQL HTTP service")
  private String graphqlTlsKeystoreFile;

  @CommandLine.Option(
      names = {"--graphql-tls-keystore-password-file"},
      description = "Path to the file containing the password for the TLS keystore")
  private String graphqlTlsKeystorePasswordFile;

  @CommandLine.Option(
      names = {"--graphql-mtls-enabled"},
      description = "Enable mTLS for GraphQL HTTP service")
  private Boolean graphqlMtlsEnabled = false;

  @CommandLine.Option(
      names = {"--graphql-tls-truststore-file"},
      description = "Path to the TLS truststore file for GraphQL HTTP service")
  private String graphqlTlsTruststoreFile;

  @CommandLine.Option(
      names = {"--graphql-tls-truststore-password-file"},
      description = "Path to the file containing the password for the TLS truststore")
  private String graphqlTlsTruststorePasswordFile;

  @CommandLine.Option(
      names = {"--graphql-max-blocks-range"},
      description =
          "Specifies the maximum number of blocks that can be retrieved by a single GraphQL "
              + "blocks(from,to) query. Must be >=0. 0 specifies no limit (default: ${DEFAULT-VALUE})")
  private final Long graphQLMaxBlockRange = GraphQLConfiguration.DEFAULT_MAX_BLOCK_RANGE;

  /** Default constructor */
  public GraphQlOptions() {}

  /**
   * Validates the GraphQL HTTP options.
   *
   * @param logger Logger instance
   * @param commandLine CommandLine instance
   */
  public void validate(final Logger logger, final CommandLine commandLine) {
    if (graphQLMaxBlockRange < 0) {
      throw new CommandLine.ParameterException(
          commandLine, "--graphql-max-blocks-range must be >= 0 (0 specifies no limit)");
    }
    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--graphql-http-enabled",
        !isGraphQLHttpEnabled,
        asList(
            "--graphql-http-cors-origins",
            "--graphql-http-host",
            "--graphql-http-port",
            "--graphql-tls-enabled"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--graphql-tls-enabled",
        !graphqlTlsEnabled,
        asList(
            "--graphql-tls-keystore-file",
            "--graphql-tls-keystore-password-file",
            "--graphql-mtls-enabled"));

    CommandLineUtils.checkOptionDependencies(
        logger,
        commandLine,
        "--graphql-mtls-enabled",
        !graphqlMtlsEnabled,
        asList("--graphql-tls-truststore-file", "--graphql-tls-truststore-password-file"));
  }

  /**
   * Creates a GraphQLConfiguration based on the provided options.
   *
   * @param hostsAllowlist List of hosts allowed
   * @param defaultHostAddress Default host address
   * @param timeoutSec Timeout in seconds
   * @return A GraphQLConfiguration instance
   */
  public GraphQLConfiguration graphQLConfiguration(
      final List<String> hostsAllowlist, final String defaultHostAddress, final Long timeoutSec) {
    final GraphQLConfiguration graphQLConfiguration = GraphQLConfiguration.createDefault();
    graphQLConfiguration.setEnabled(isGraphQLHttpEnabled);
    graphQLConfiguration.setHost(
        Strings.isNullOrEmpty(graphQLHttpHost) ? defaultHostAddress : graphQLHttpHost);
    graphQLConfiguration.setPort(graphQLHttpPort);
    graphQLConfiguration.setHostsAllowlist(hostsAllowlist);
    graphQLConfiguration.setCorsAllowedDomains(graphQLHttpCorsAllowedOrigins);
    graphQLConfiguration.setHttpTimeoutSec(timeoutSec);
    graphQLConfiguration.setTlsEnabled(graphqlTlsEnabled);
    graphQLConfiguration.setTlsKeyStorePath(graphqlTlsKeystoreFile);
    graphQLConfiguration.setTlsKeyStorePasswordFile(graphqlTlsKeystorePasswordFile);
    graphQLConfiguration.setMtlsEnabled(graphqlMtlsEnabled);
    graphQLConfiguration.setTlsTrustStorePath(graphqlTlsTruststoreFile);
    graphQLConfiguration.setTlsTrustStorePasswordFile(graphqlTlsTruststorePasswordFile);
    graphQLConfiguration.setMaxBlockRange(graphQLMaxBlockRange);

    return graphQLConfiguration;
  }

  /**
   * Checks if GraphQL over HTTP is enabled.
   *
   * @return true if enabled, false otherwise
   */
  public Boolean isGraphQLHttpEnabled() {
    return isGraphQLHttpEnabled;
  }

  /**
   * Returns the port for GraphQL over HTTP.
   *
   * @return The port number
   */
  public Integer getGraphQLHttpPort() {
    return graphQLHttpPort;
  }
}
