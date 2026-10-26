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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import org.hyperledger.besu.plugin.services.NetworkEndpointService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe registry of the network listeners owned by plugins. Besu reads it to include these
 * listeners in its port checks, in the {@code net_services} JSON-RPC result and in the ports file.
 *
 * <p>Besu calls {@link #seal()} once plugins have registered, before it runs its port checks. An
 * endpoint registered after that is still recorded, but a warning is logged because it was not
 * included in those checks.
 */
public class NetworkEndpointServiceImpl implements NetworkEndpointService {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkEndpointServiceImpl.class);

  /** The names of Besu's own listeners in the {@code net_services} JSON-RPC result. */
  static final Set<String> RESERVED_SERVICE_NAMES = Set.of("jsonrpc", "ws", "p2p", "metrics");

  /** The keys of Besu's own listeners in the ports file. */
  static final Set<String> RESERVED_PORTS_FILE_KEYS =
      Set.of("discovery", "p2p", "json-rpc", "ws-rpc", "metrics", "engine-json-rpc");

  private final List<NetworkEndpoint> endpoints = new ArrayList<>();
  private final Set<String> serviceNames = new HashSet<>();
  private final Set<String> portsFileKeys = new HashSet<>();
  private volatile boolean sealed = false;

  /** Default constructor. */
  public NetworkEndpointServiceImpl() {}

  @Override
  public synchronized void registerEndpoint(final NetworkEndpoint endpoint) {
    Objects.requireNonNull(endpoint, "endpoint must not be null");
    final String serviceName = endpoint.getServiceName();
    final String portsFileKey = endpoint.getPortsFileKey();
    if (serviceName == null || serviceName.isBlank()) {
      throw new IllegalArgumentException("endpoint service name must not be null or blank");
    }
    if (portsFileKey == null || portsFileKey.isBlank()) {
      throw new IllegalArgumentException(
          "endpoint ports file key must not be null or blank (service " + serviceName + ")");
    }
    if (RESERVED_SERVICE_NAMES.contains(serviceName)) {
      throw new IllegalArgumentException(
          "endpoint service name " + serviceName + " is reserved by a built-in service");
    }
    if (RESERVED_PORTS_FILE_KEYS.contains(portsFileKey)) {
      throw new IllegalArgumentException(
          "endpoint ports file key "
              + portsFileKey
              + " is reserved by a built-in service (service "
              + serviceName
              + ")");
    }
    if (serviceNames.contains(serviceName)) {
      throw new IllegalArgumentException(
          "endpoint service name " + serviceName + " is already registered");
    }
    if (portsFileKeys.contains(portsFileKey)) {
      throw new IllegalArgumentException(
          "endpoint ports file key "
              + portsFileKey
              + " is already registered (service "
              + serviceName
              + ")");
    }
    if (sealed) {
      LOG.warn(
          "Network endpoint {} was registered after plugin registration finished; "
              + "it was not included in the port clash and port availability checks",
          serviceName);
    }
    serviceNames.add(serviceName);
    portsFileKeys.add(portsFileKey);
    endpoints.add(endpoint);
  }

  @Override
  public synchronized List<NetworkEndpoint> getEndpoints() {
    return List.copyOf(endpoints);
  }

  /**
   * Marks the end of plugin registration. Besu calls this after all plugins, including the built-in
   * ones, have registered, and before it checks ports. Endpoints registered afterwards are still
   * accepted but a warning is logged.
   */
  public void seal() {
    sealed = true;
  }

  /**
   * Whether {@link #seal()} has been called.
   *
   * @return true once plugin registration has finished
   */
  public boolean isSealed() {
    return sealed;
  }
}
