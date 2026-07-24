/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.nat.docker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.nat.core.NatManager;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.core.exception.NatInitializationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DockerNatManagerTest {

  private final String advertisedHost = "99.45.69.12";
  private final String detectedAdvertisedHost = "199.45.69.12";

  private final int rpcHttpPort = 2;

  @Mock private HostBasedIpDetector hostBasedIpDetector;

  private DockerNatManager natManager;

  @BeforeEach
  public void initialize() throws NatInitializationException {
    hostBasedIpDetector = mock(HostBasedIpDetector.class);
    when(hostBasedIpDetector.detectAdvertisedIp()).thenReturn(Optional.of(detectedAdvertisedHost));
    natManager =
        new DockerNatManager(
            hostBasedIpDetector, advertisedHost, rpcHttpPort, port -> Optional.empty());
    natManager.start();
  }

  @Test
  public void assertThatExternalIPIsEqualToRemoteHost()
      throws ExecutionException, InterruptedException {
    assertThat(natManager.queryExternalIPAddress().get()).isEqualTo(detectedAdvertisedHost);
  }

  @Test
  public void assertThatExternalIPIsEqualToDefaultHostIfIpDetectorCannotRetrieveIP()
      throws ExecutionException, InterruptedException {
    final NatManager natManager =
        new DockerNatManager(
            hostBasedIpDetector, advertisedHost, rpcHttpPort, port -> Optional.empty());
    when(hostBasedIpDetector.detectAdvertisedIp()).thenReturn(Optional.empty());
    try {
      natManager.start();
    } catch (NatInitializationException e) {
      Assertions.fail(e.getMessage());
    }
    assertThat(natManager.queryExternalIPAddress().get()).isEqualTo(advertisedHost);
  }

  @Test
  public void assertThatLocalIPIsEqualToLocalHost()
      throws ExecutionException, InterruptedException, UnknownHostException {
    final String internalHost = InetAddress.getLocalHost().getHostAddress();
    assertThat(natManager.queryLocalIPAddress().get()).isEqualTo(internalHost);
  }

  @Test
  public void assertThatMappingForJsonRpcWorks() throws UnknownHostException {
    final String internalHost = InetAddress.getLocalHost().getHostAddress();

    final NatPortMapping mapping =
        natManager.getPortMapping(NatServiceType.JSON_RPC, NetworkProtocol.TCP);

    final NatPortMapping expectedMapping =
        new NatPortMapping(
            NatServiceType.JSON_RPC,
            NetworkProtocol.TCP,
            internalHost,
            detectedAdvertisedHost,
            rpcHttpPort,
            rpcHttpPort);

    assertThat(mapping).usingRecursiveComparison().isEqualTo(expectedMapping);
  }

  @Test
  public void assertThatJsonRpcMappingReflectsExternalPortOverride()
      throws NatInitializationException {
    final DockerNatManager overriddenNatManager =
        new DockerNatManager(
            hostBasedIpDetector,
            advertisedHost,
            rpcHttpPort,
            port -> port == rpcHttpPort ? Optional.of(40000) : Optional.empty());
    overriddenNatManager.start();

    final NatPortMapping mapping =
        overriddenNatManager.getPortMapping(NatServiceType.JSON_RPC, NetworkProtocol.TCP);

    // Regression test for a long-standing bug where the external/internal port constructor
    // arguments were swapped, silently breaking the HOST_PORT_<port> override mechanism for
    // admin_nodeInfo: internal port must stay the configured value, external must reflect
    // the override.
    assertThat(mapping.getInternalPort()).isEqualTo(rpcHttpPort);
    assertThat(mapping.getExternalPort()).isEqualTo(40000);
  }

  @Test
  public void assertThatRlpxAndDiscoveryMappingsAreAbsentBeforeUpdatePort() {
    assertThatThrownBy(() -> natManager.getPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> natManager.getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void assertThatUpdatePortRecordsTheActualResolvedPort() throws UnknownHostException {
    final String internalHost = InetAddress.getLocalHost().getHostAddress();
    final int actualRlpxPort = 38217;
    final int actualDiscoveryPort = 45734;

    natManager.updatePort(NatServiceType.RLPX, NetworkProtocol.TCP, actualRlpxPort);
    natManager.updatePort(NatServiceType.DISCOVERY, NetworkProtocol.UDP, actualDiscoveryPort);

    final NatPortMapping rlpxMapping =
        natManager.getPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP);
    final NatPortMapping discoveryMapping =
        natManager.getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP);

    assertThat(rlpxMapping)
        .usingRecursiveComparison()
        .isEqualTo(
            new NatPortMapping(
                NatServiceType.RLPX,
                NetworkProtocol.TCP,
                internalHost,
                detectedAdvertisedHost,
                actualRlpxPort,
                actualRlpxPort));
    assertThat(discoveryMapping)
        .usingRecursiveComparison()
        .isEqualTo(
            new NatPortMapping(
                NatServiceType.DISCOVERY,
                NetworkProtocol.UDP,
                internalHost,
                detectedAdvertisedHost,
                actualDiscoveryPort,
                actualDiscoveryPort));
  }

  @Test
  public void assertThatUpdatePortReflectsExternalPortOverride() throws NatInitializationException {
    final int actualDiscoveryPort = 45734;
    final int overriddenExternalPort = 55734;
    final DockerNatManager overriddenNatManager =
        new DockerNatManager(
            hostBasedIpDetector,
            advertisedHost,
            rpcHttpPort,
            port ->
                port == actualDiscoveryPort
                    ? Optional.of(overriddenExternalPort)
                    : Optional.empty());
    overriddenNatManager.start();

    overriddenNatManager.updatePort(
        NatServiceType.DISCOVERY, NetworkProtocol.UDP, actualDiscoveryPort);

    final NatPortMapping mapping =
        overriddenNatManager.getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP);
    assertThat(mapping.getInternalPort()).isEqualTo(actualDiscoveryPort);
    assertThat(mapping.getExternalPort()).isEqualTo(overriddenExternalPort);
  }

  @Test
  public void assertThatUpdatePortReplacesRatherThanDuplicatesExistingMapping()
      throws ExecutionException, InterruptedException {
    natManager.updatePort(NatServiceType.DISCOVERY, NetworkProtocol.UDP, 1111);
    natManager.updatePort(NatServiceType.DISCOVERY, NetworkProtocol.UDP, 2222);

    final long discoveryMappingCount =
        natManager.getPortMappings().get().stream()
            .filter(m -> m.getNatServiceType() == NatServiceType.DISCOVERY)
            .count();
    assertThat(discoveryMappingCount).isEqualTo(1);
    assertThat(
            natManager
                .getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP)
                .getInternalPort())
        .isEqualTo(2222);
  }

  @Test
  public void assertThatRlpxAndDiscoveryCanResolveToDifferentActualPorts() {
    natManager.updatePort(NatServiceType.RLPX, NetworkProtocol.TCP, 30303);
    natManager.updatePort(NatServiceType.DISCOVERY, NetworkProtocol.UDP, 30304);

    assertThat(
            natManager.getPortMapping(NatServiceType.RLPX, NetworkProtocol.TCP).getInternalPort())
        .isEqualTo(30303);
    assertThat(
            natManager
                .getPortMapping(NatServiceType.DISCOVERY, NetworkProtocol.UDP)
                .getInternalPort())
        .isEqualTo(30304);
  }
}
