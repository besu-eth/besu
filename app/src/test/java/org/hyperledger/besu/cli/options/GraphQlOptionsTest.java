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
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.api.graphql.GraphQLConfiguration;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GraphQlOptionsTest extends CommandTestAbstract {
  @Test
  public void graphQLHttpEnabledPropertyMustBeUsed() {
    parseCommand("--graphql-http-enabled");

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().isEnabled()).isTrue();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLHttpHostAndPortOptionsMustBeUsed() {

    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand(
        "--graphql-http-enabled",
        "--graphql-http-host",
        host,
        "--graphql-http-port",
        String.valueOf(port));

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);
    assertThat(graphQLConfigArgumentCaptor.getValue().getPort()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLHttpHostMayBeLocalhost() {

    final String host = "localhost";
    parseCommand("--graphql-http-enabled", "--graphql-http-host", host);

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLHttpHostMayBeIPv6() {

    final String host = "2600:DB8::8545";
    parseCommand("--graphql-http-enabled", "--graphql-http-host", host);

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().getHost()).isEqualTo(host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLMaxBlockRangeDefaultsToFiveThousand() {
    parseCommand("--graphql-http-enabled");

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().getMaxBlockRange()).isEqualTo(5000L);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLMaxBlockRangeOptionMustBeUsed() {
    parseCommand("--graphql-http-enabled", "--graphql-max-blocks-range", "42");

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    assertThat(graphQLConfigArgumentCaptor.getValue().getMaxBlockRange()).isEqualTo(42L);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void graphQLMaxBlockRangeRejectsNegativeValue() {
    parseCommand("--graphql-http-enabled", "--graphql-max-blocks-range", "-1");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--graphql-max-blocks-range must be >= 0 (0 specifies no limit)");
  }

  @Test
  public void graphQLTlsPasswordFilesFromTomlMustBeUsed() throws Exception {
    final Path keystorePasswordFile = createTempFile("keystore-password", "keystorePassword");
    final Path truststorePasswordFile = createTempFile("truststore-password", "truststorePassword");
    final Path toml =
        createTempFile(
            "toml",
            """
            graphql-http-enabled = true
            graphql-tls-enabled = true
            graphql-tls-keystore-file = '/tmp/keystore.p12'
            graphql-tls-keystore-password-file = '%s'
            graphql-mtls-enabled = true
            graphql-tls-truststore-file = '/tmp/truststore.p12'
            graphql-tls-truststore-password-file = '%s'
            """
                .formatted(keystorePasswordFile, truststorePasswordFile));

    parseCommand("--config-file", toml.toString());

    verify(mockRunnerBuilder).graphQLConfiguration(graphQLConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    final GraphQLConfiguration graphQLConfiguration = graphQLConfigArgumentCaptor.getValue();
    assertThat(graphQLConfiguration.isTlsEnabled()).isTrue();
    assertThat(graphQLConfiguration.getTlsKeyStorePath()).isEqualTo("/tmp/keystore.p12");
    assertThat(graphQLConfiguration.getTlsKeyStorePassword()).isEqualTo("keystorePassword");
    assertThat(graphQLConfiguration.isMtlsEnabled()).isTrue();
    assertThat(graphQLConfiguration.getTlsTrustStorePath()).isEqualTo("/tmp/truststore.p12");
    assertThat(graphQLConfiguration.getTlsTrustStorePassword()).isEqualTo("truststorePassword");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
