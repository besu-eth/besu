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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.cli.custom.JsonRPCAllowlistHostsProperty;
import org.hyperledger.besu.cli.options.JsonRpcHttpOptions;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.plugin.rpc.RpcConfiguration;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BesuConfigurationImplTest {

  /** An RpcConfiguration that only has the interface defaults. */
  private static final RpcConfiguration INTERFACE_DEFAULTS =
      new RpcConfiguration() {
        @Override
        public String getConfiguredRpcHttpHost() {
          return null;
        }

        @Override
        public Integer getConfiguredRpcHttpPort() {
          return null;
        }

        @Override
        public long getConfiguredRpcHttpTimeoutSec() {
          return 0;
        }
      };

  @Test
  public void defaultsMatchCoreDefaults() {
    final BesuConfigurationImpl configuration = new BesuConfigurationImpl();

    assertThat(configuration.getConfiguredHostAllowlist())
        .containsExactly("localhost", "127.0.0.1");
    assertThat(configuration.getDefaultHostAddress())
        .isEqualTo(JsonRpcConfiguration.DEFAULT_JSON_RPC_HOST);
    assertThat(configuration.getConfiguredMaxLogFilterAddresses())
        .isEqualTo(ApiConfiguration.DEFAULT_MAX_FILTER_ADDRESSES);
    assertThat(configuration.getConfiguredRpcHttpTimeoutSec())
        .isEqualTo(JsonRpcConfiguration.DEFAULT_HTTP_TIMEOUT_SEC);
  }

  @Test
  public void interfaceDefaultsAreUnsupported() {
    assertThatThrownBy(INTERFACE_DEFAULTS::getConfiguredHostAllowlist)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(INTERFACE_DEFAULTS::getDefaultHostAddress)
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(INTERFACE_DEFAULTS::getConfiguredMaxLogFilterAddresses)
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void hostAllowlistIsImmutable() {
    final BesuConfigurationImpl configuration =
        new BesuConfigurationImpl().withHostsAllowlist(List.of("a"));

    assertThatThrownBy(() -> configuration.getConfiguredHostAllowlist().add("b"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void rpcHttpTimeoutIsNotResetByRpcHttpOptions() {
    final BesuConfigurationImpl configuration =
        new BesuConfigurationImpl()
            .withRpcHttpTimeoutSec(513)
            .withJsonRpcHttpOptions(new JsonRpcHttpOptions());

    assertThat(configuration.getConfiguredRpcHttpTimeoutSec()).isEqualTo(513);
  }

  @Test
  public void setsDefaultHostAndMaxLogFilterAddresses() {
    final BesuConfigurationImpl configuration =
        new BesuConfigurationImpl().withDefaultHostAddress("10.1.2.3").withMaxLogFilterAddresses(7);

    assertThat(configuration.getDefaultHostAddress()).isEqualTo("10.1.2.3");
    assertThat(configuration.getConfiguredMaxLogFilterAddresses()).isEqualTo(7);
  }

  static Stream<Arguments> allowlists() {
    return Stream.of(
        Arguments.of("localhost,127.0.0.1", List.of("localhost", "127.0.0.1")),
        Arguments.of("a,b", List.of("a", "b")),
        Arguments.of("none", List.of()),
        Arguments.of("*", List.of("*")),
        Arguments.of("all", List.of("*")));
  }

  @ParameterizedTest
  @MethodSource("allowlists")
  public void hostsAllowlistIsCopiedFromCliProperty(
      final String cliValue, final List<String> expected) {
    final JsonRPCAllowlistHostsProperty property = new JsonRPCAllowlistHostsProperty();
    property.add(cliValue);

    final BesuConfigurationImpl configuration =
        new BesuConfigurationImpl().withHostsAllowlist(property);

    assertThat(configuration.getConfiguredHostAllowlist()).isEqualTo(expected);
    // a host check written like core's JSON-RPC one gives the same answers
    for (final String host : List.of("localhost", "127.0.0.1", "LOCALHOST", "a", "b", "none")) {
      assertThat(isAllowed(configuration.getConfiguredHostAllowlist(), host))
          .as("host %s with --host-allowlist=%s", host, cliValue)
          .isEqualTo(isAllowed(property, host));
    }
  }

  /** A host check as a plugin HTTP service would write it over the configured allowlist. */
  private static boolean isAllowed(final Collection<String> allowlist, final String host) {
    return allowlist.contains("*")
        || allowlist.stream().anyMatch(entry -> entry.equalsIgnoreCase(host));
  }
}
