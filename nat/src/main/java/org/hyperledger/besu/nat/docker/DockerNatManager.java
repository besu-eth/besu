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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.AbstractNatManager;
import org.hyperledger.besu.nat.core.IpDetector;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.core.exception.NatInitializationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class describes the behaviour of the Docker NAT manager. Docker Nat manager add support for
 * Docker’s NAT implementation when Besu is being run from a Docker container
 */
public class DockerNatManager extends AbstractNatManager {
  private static final Logger LOG = LoggerFactory.getLogger(DockerNatManager.class);

  /**
   * A container's RLPx/discovery ports are only known once the real socket bind completes (see
   * {@link #updatePort}), so setting {@code HOST_PORT_<port>} for those has to name the actual
   * bound port, not necessarily the value passed on the CLI.
   */
  private static final String PORT_MAPPING_TAG = "HOST_PORT_";

  private final IpDetector ipDetector;
  private final int internalRpcHttpPort;
  private final IntFunction<Optional<Integer>> externalPortOverrideLookup;

  private String internalAdvertisedHost;
  private String internalHost = "";
  private final List<NatPortMapping> forwardedPorts = new ArrayList<>();

  /**
   * Instantiates a new Docker nat manager.
   *
   * @param advertisedHost the advertised host
   * @param rpcHttpPort the rpc http port
   */
  public DockerNatManager(final String advertisedHost, final int rpcHttpPort) {
    this(new HostBasedIpDetector(), advertisedHost, rpcHttpPort);
  }

  /**
   * Instantiates a new Docker nat manager.
   *
   * @param ipDetector the ip detector
   * @param advertisedHost the advertised host
   * @param rpcHttpPort the rpc http port
   */
  public DockerNatManager(
      final IpDetector ipDetector, final String advertisedHost, final int rpcHttpPort) {
    this(
        ipDetector,
        advertisedHost,
        rpcHttpPort,
        internalPort ->
            Optional.ofNullable(System.getenv(PORT_MAPPING_TAG + internalPort))
                .map(Integer::valueOf));
  }

  @VisibleForTesting
  DockerNatManager(
      final IpDetector ipDetector,
      final String advertisedHost,
      final int rpcHttpPort,
      final IntFunction<Optional<Integer>> externalPortOverrideLookup) {
    super(NatMethod.DOCKER);
    this.ipDetector = ipDetector;
    this.internalAdvertisedHost = advertisedHost;
    this.internalRpcHttpPort = rpcHttpPort;
    this.externalPortOverrideLookup = externalPortOverrideLookup;
  }

  @Override
  protected void doStart() throws NatInitializationException {
    LOG.info("Starting docker NAT manager.");
    try {
      ipDetector.detectAdvertisedIp().ifPresent(ipFound -> internalAdvertisedHost = ipFound);
      buildForwardedPorts();
    } catch (Exception e) {
      throw new NatInitializationException("Unable to retrieve IP from docker");
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Stopping docker NAT manager.");
  }

  @Override
  protected CompletableFuture<String> retrieveExternalIPAddress() {
    return CompletableFuture.completedFuture(internalAdvertisedHost);
  }

  @Override
  public CompletableFuture<List<NatPortMapping>> getPortMappings() {
    synchronized (forwardedPorts) {
      return CompletableFuture.completedFuture(List.copyOf(forwardedPorts));
    }
  }

  private int getExternalPort(final int internalPort) {
    return externalPortOverrideLookup.apply(internalPort).orElse(internalPort);
  }

  private void buildForwardedPorts() {
    try {
      internalHost = queryLocalIPAddress().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      final String advertisedHost =
          retrieveExternalIPAddress().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      synchronized (forwardedPorts) {
        forwardedPorts.add(
            new NatPortMapping(
                NatServiceType.JSON_RPC,
                NetworkProtocol.TCP,
                internalHost,
                advertisedHost,
                getExternalPort(internalRpcHttpPort),
                internalRpcHttpPort));
      }
    } catch (Exception e) {
      LOG.warn("Failed to create forwarded port list", e);
    }
  }

  /**
   * Records the real, post-bind port Besu resolved for the RLPx or discovery service - not the
   * pre-bind configured value, which may have been {@code 0} (ephemeral port allocation) or may
   * simply differ from the other service's port when discovery is configured on its own port.
   * Unlike the RPC port (known statically at startup), RLPx/discovery ports are only known once the
   * real socket bind completes, so this manager starts without a mapping for them at all - callers
   * must call this once the real port is known, mirroring how {@code UpnpNatManager}'s port
   * mappings are only added once its own {@code requestPortForward} calls succeed.
   *
   * @param serviceType the NAT service type - {@link NatServiceType#RLPX} or {@link
   *     NatServiceType#DISCOVERY}
   * @param protocol the network protocol
   * @param actualPort the real, resolved internal port
   */
  public void updatePort(
      final NatServiceType serviceType, final NetworkProtocol protocol, final int actualPort) {
    checkState(isStarted(), "Cannot call updatePort() when in stopped state");
    synchronized (forwardedPorts) {
      forwardedPorts.removeIf(
          mapping ->
              mapping.getNatServiceType() == serviceType && mapping.getProtocol() == protocol);
      forwardedPorts.add(
          new NatPortMapping(
              serviceType,
              protocol,
              internalHost,
              internalAdvertisedHost,
              getExternalPort(actualPort),
              actualPort));
    }
  }
}
