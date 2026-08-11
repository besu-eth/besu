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
package org.hyperledger.besu.cli.options;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryMode;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class P2PDiscoveryOptionsTest extends CommandTestAbstract {

  @Captor private ArgumentCaptor<Optional<String>> optionalStringArgumentCaptor;

  @Test
  public void p2pHostAndPortOptionsAreRespectedAndNotLeakedWithMetricsEnabled() {

    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand("--p2p-host", host, "--p2p-port", String.valueOf(port), "--metrics-enabled");

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(host);
    assertThat(intArgumentCaptor.getValue()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    // all other port values remain default
    assertThat(metricsConfigArgumentCaptor.getValue().getPort()).isEqualTo(9545);
    assertThat(metricsConfigArgumentCaptor.getValue().getPushPort()).isEqualTo(9001);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(8545);

    // all other host values remain default
    final String defaultHost = "127.0.0.1";
    assertThat(metricsConfigArgumentCaptor.getValue().getHost()).isEqualTo(defaultHost);
    assertThat(metricsConfigArgumentCaptor.getValue().getPushHost()).isEqualTo(defaultHost);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(defaultHost);
  }

  @Test
  public void p2pInterfaceOptionIsRespected() {

    final String ip = "1.2.3.4";
    parseCommand("--p2p-interface", ip);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    verify(mockRunnerBuilder).p2pListenInterface(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ip);
  }

  @ParameterizedTest
  @ValueSource(strings = {"localhost", " localhost.localdomain", "invalid-host"})
  public void p2pHostMustBeAnIPAddress(final String host) {
    parseCommand("--p2p-host", host);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    String errorMessage = "The provided --p2p-host is invalid: " + host;
    assertThat(commandErrorOutput.toString(UTF_8)).contains(errorMessage);
  }

  @Test
  public void p2pHostMayBeIPv6() {
    final String host = "2600:DB8::8545";
    parseCommand("--p2p-host", host);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(host);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pHostAndPortOptionsAreRespectedAndNotLeaked() {

    final String host = "1.2.3.4";
    final int port = 1234;
    parseCommand("--p2p-host", host, "--p2p-port", String.valueOf(port));

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).metricsConfiguration(metricsConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).jsonRpcConfiguration(jsonRpcConfigArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(host);
    assertThat(intArgumentCaptor.getValue()).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();

    // all other port values remain default
    assertThat(metricsConfigArgumentCaptor.getValue().getPort()).isEqualTo(9545);
    assertThat(metricsConfigArgumentCaptor.getValue().getPushPort()).isEqualTo(9001);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getPort()).isEqualTo(8545);

    // all other host values remain default
    final String defaultHost = "127.0.0.1";
    assertThat(metricsConfigArgumentCaptor.getValue().getHost()).isEqualTo(defaultHost);
    assertThat(metricsConfigArgumentCaptor.getValue().getPushHost()).isEqualTo(defaultHost);
    assertThat(jsonRpcConfigArgumentCaptor.getValue().getHost()).isEqualTo(defaultHost);
  }

  @Test
  public void dualStackWithIpv4PrimaryAndIpv6SecondaryIsValid() {
    final String ipv4Host = "192.0.2.1";
    final String ipv6Host = "2001:db8::1";
    parseCommand("--p2p-host", ipv4Host, "--p2p-host-ipv6", ipv6Host);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv4Host);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void dualStackWithIpv6PrimaryAndIpv6SecondaryIsInvalid() {
    final String ipv6Primary = "2001:db8::1";
    final String ipv6Secondary = "2001:db8::2";
    parseCommand("--p2p-host", ipv6Primary, "--p2p-host-ipv6", ipv6Secondary);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--p2p-host must be an IPv4 address")
        .contains(ipv6Primary);
  }

  @Test
  public void ipv6OnlyConfigurationIsValid() {
    final String ipv6Host = "2001:db8::1";
    parseCommand("--p2p-host", ipv6Host);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv6Host);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pHostIpv6MustBeValidIpv6Address() {
    final String ipv4Primary = "192.0.2.1";
    final String invalidIpv6 = "192.0.2.2";
    parseCommand("--p2p-host", ipv4Primary, "--p2p-host-ipv6", invalidIpv6);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--p2p-host-ipv6 must be an IPv6 address")
        .contains(invalidIpv6);
  }

  @Test
  public void dualStackInterfaceWithIpv4PrimaryAndIpv6SecondaryIsValid() {
    final String ipv4Interface = "0.0.0.0";
    final String ipv6Interface = "::";
    parseCommand("--p2p-interface", ipv4Interface, "--p2p-interface-ipv6", ipv6Interface);

    verify(mockRunnerBuilder).p2pListenInterface(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv4Interface);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void dualStackInterfaceWithIpv6PrimaryAndIpv6SecondaryIsInvalid() {
    final String ipv6Primary = "2001:db8::1";
    final String ipv6Secondary = "::";
    parseCommand("--p2p-interface", ipv6Primary, "--p2p-interface-ipv6", ipv6Secondary);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--p2p-interface must be an IPv4 address or 0.0.0.0")
        .contains(ipv6Primary);
  }

  @Test
  public void ipv6OnlyInterfaceConfigurationIsValid() {
    final String ipv6Interface = "::";
    parseCommand("--p2p-interface", ipv6Interface);

    verify(mockRunnerBuilder).p2pListenInterface(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv6Interface);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pInterfaceIpv6MustBeValidIpv6Address() {
    final String ipv4Interface = "0.0.0.0";
    final String invalidIpv6 = "192.0.2.1";
    parseCommand("--p2p-interface", ipv4Interface, "--p2p-interface-ipv6", invalidIpv6);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--p2p-interface-ipv6 must be an IPv6 address")
        .contains(invalidIpv6);
  }

  @Test
  public void smartDefaultAppliesIpv6InterfaceWhenOnlyHostIpv6IsSpecified() {
    final String ipv4Host = "192.0.2.1";
    final String ipv6Host = "2001:db8::1";
    parseCommand("--p2p-host", ipv4Host, "--p2p-host-ipv6", ipv6Host);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pListenInterfaceIpv6(optionalStringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv4Host);
    // Smart default should auto-set interface-ipv6 to :: (0:0:0:0:0:0:0:0)
    assertThat(optionalStringArgumentCaptor.getValue()).isPresent();
    assertThat(optionalStringArgumentCaptor.getValue().get()).isEqualTo("0:0:0:0:0:0:0:0");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void smartDefaultDoesNotOverrideExplicitIpv6Interface() {
    final String ipv4Host = "192.0.2.1";
    final String ipv6Host = "2001:db8::1";
    final String customIpv6Interface = "fe80::1";
    parseCommand(
        "--p2p-host",
        ipv4Host,
        "--p2p-host-ipv6",
        ipv6Host,
        "--p2p-interface-ipv6",
        customIpv6Interface);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pListenInterfaceIpv6(optionalStringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv4Host);
    // Should use the explicitly specified interface, not the smart default
    assertThat(optionalStringArgumentCaptor.getValue()).isPresent();
    assertThat(optionalStringArgumentCaptor.getValue().get()).isEqualTo(customIpv6Interface);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortDefaultsToP2pPort() {
    final int port = 30303;
    parseCommand("--p2p-port", String.valueOf(port));

    verify(mockRunnerBuilder).p2pListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(port);
    assertThat(capturedPorts.get(1)).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortCanDifferFromP2pPort() {
    final int tcpPort = 30303;
    final int udpPort = 30304;
    parseCommand(
        "--p2p-port", String.valueOf(tcpPort), "--p2p-discovery-port", String.valueOf(udpPort));

    verify(mockRunnerBuilder).p2pListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(tcpPort);
    assertThat(capturedPorts.get(1)).isEqualTo(udpPort);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortIpv6DefaultsToP2pPortIpv6() {
    final int port = 30404;
    parseCommand("--p2p-port-ipv6", String.valueOf(port));

    verify(mockRunnerBuilder).p2pListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(port);
    assertThat(capturedPorts.get(1)).isEqualTo(port);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortIpv6CanDifferFromP2pPortIpv6() {
    final int tcpPort = 30404;
    final int udpPort = 30405;
    parseCommand(
        "--p2p-port-ipv6",
        String.valueOf(tcpPort),
        "--p2p-discovery-port-ipv6",
        String.valueOf(udpPort));

    verify(mockRunnerBuilder).p2pListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(tcpPort);
    assertThat(capturedPorts.get(1)).isEqualTo(udpPort);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortZeroRequestsEphemeralPort() {
    final int tcpPort = 30303;
    parseCommand("--p2p-port", String.valueOf(tcpPort), "--p2p-discovery-port", "0");

    verify(mockRunnerBuilder).p2pListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPort(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(tcpPort);
    assertThat(capturedPorts.get(1)).isEqualTo(0);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void p2pDiscoveryPortIpv6ZeroRequestsEphemeralPort() {
    final int tcpPort = 30404;
    parseCommand("--p2p-port-ipv6", String.valueOf(tcpPort), "--p2p-discovery-port-ipv6", "0");

    verify(mockRunnerBuilder).p2pListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pDiscoveryListenPortIpv6(intArgumentCaptor.capture());
    verify(mockRunnerBuilder).build();

    final var capturedPorts = intArgumentCaptor.getAllValues();
    assertThat(capturedPorts.get(0)).isEqualTo(tcpPort);
    assertThat(capturedPorts.get(1)).isEqualTo(0);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void ipv4OnlyConfigurationDoesNotApplySmartDefault() {
    final String ipv4Host = "192.0.2.1";
    parseCommand("--p2p-host", ipv4Host);

    verify(mockRunnerBuilder).p2pAdvertisedHost(stringArgumentCaptor.capture());
    verify(mockRunnerBuilder).p2pListenInterfaceIpv6(optionalStringArgumentCaptor.capture());
    verify(mockRunnerBuilder).preferIpv6Outbound(anyBoolean());
    verify(mockRunnerBuilder).build();

    assertThat(stringArgumentCaptor.getValue()).isEqualTo(ipv4Host);
    // No IPv6 options specified, so interface-ipv6 should be empty
    assertThat(optionalStringArgumentCaptor.getValue()).isEmpty();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void discoveryModeDefaultIsBoth() {
    parseCommand();

    verify(mockRunnerBuilder).discoveryMode(eq(DiscoveryMode.BOTH));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(DiscoveryMode.class)
  public void discoveryModeCanBeSetExplicitly(final DiscoveryMode mode) {
    parseCommand("--discovery-mode", mode.name());

    verify(mockRunnerBuilder).discoveryMode(eq(mode));
    verify(mockRunnerBuilder).build();

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }
}
