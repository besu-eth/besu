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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.Unstable;

import java.util.List;
import java.util.Optional;

/**
 * This service lets a plugin declare a network listener that it owns, so that Besu includes it in
 * its port checks, in the {@code net_services} JSON-RPC result, and in the ports file it writes on
 * startup.
 */
@Unstable
public interface NetworkEndpointService extends BesuService {

  /**
   * Declare a network listener owned by a plugin. This must be called during the registration
   * callback; an endpoint registered after the registration callbacks have finished is not included
   * in Besu's port checks.
   *
   * <p>Service names and ports file keys are unique: each registered endpoint must use a service
   * name and a ports file key that no other endpoint uses, and that Besu does not use for its own
   * services, whether or not the built-in service is enabled. Besu reserves the service names
   * "jsonrpc", "ws", "p2p" and "metrics", and the ports file keys "discovery", "p2p", "json-rpc",
   * "ws-rpc", "metrics" and "engine-json-rpc".
   *
   * @param endpoint the endpoint
   * @throws NullPointerException if {@code endpoint} is null
   * @throws IllegalArgumentException if the service name or ports file key is null or blank, is
   *     already used by a registered endpoint, or is reserved by Besu
   */
  void registerEndpoint(NetworkEndpoint endpoint);

  /**
   * Get the endpoints registered so far, in registration order.
   *
   * @return the registered endpoints, as an immutable list
   */
  List<NetworkEndpoint> getEndpoints();

  /**
   * A network listener owned by a plugin.
   *
   * <p>Besu may call the getters from any thread, at any time after the endpoint is registered,
   * including on every {@code net_services} request, so they must be thread-safe and cheap. The
   * listener is assumed to use TCP.
   */
  interface NetworkEndpoint {

    /**
     * Get the name of the service, used as its key in the {@code net_services} JSON-RPC result.
     * Besu reads it when the endpoint is registered, to check that it is unique, and again whenever
     * it reports the endpoint, so it must return the same value every time.
     *
     * @return the service name, for example "myService"
     */
    String getServiceName();

    /**
     * Get the key of the service in the ports file. Besu reads it when the endpoint is registered,
     * to check that it is unique, and again when it writes the ports file, so it must return the
     * same value every time.
     *
     * @return the ports file key, for example "my-service-http"
     */
    String getPortsFileKey();

    /**
     * Whether the listener is enabled. Besu reads it after the command line has been parsed, for
     * its port checks, and again whenever it reports the endpoint. A disabled endpoint is left out
     * of the port checks, {@code net_services} and the ports file.
     *
     * @return true if the listener is enabled
     */
    boolean isEnabled();

    /**
     * Get the host the listener binds to, as reported by {@code net_services}. Besu reads it on
     * each {@code net_services} request.
     *
     * @return the host
     */
    String getHost();

    /**
     * Get the configured port of the listener, which may be 0 for an ephemeral port. Besu reads it
     * after the command line has been parsed, for its port clash and port availability checks, and
     * reports it in {@code net_services}, as it does for its own listeners.
     *
     * @return the configured port
     */
    int getConfiguredPort();

    /**
     * Get the port the listener is actually listening on. Besu reads it after the start callback,
     * when it writes the ports file; an endpoint without a listening port is left out of the file.
     *
     * @return the listening port, or empty if the listener is not started; never null
     */
    Optional<Integer> getListeningPort();
  }
}
