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
package org.hyperledger.besu.plugin.services;

import java.util.Optional;

/** This service allows plugins to register custom health check endpoints. */
public interface HealthCheckService extends BesuService {

  /**
   * Registers a health check provider for a specific endpoint.
   *
   * @param endpoint the health check endpoint path (e.g., "/readiness")
   * @param provider the health check provider that evaluates health status
   * @throws IllegalArgumentException if an endpoint is already registered
   */
  void registerHealthCheck(String endpoint, HealthCheckProvider provider);

  /**
   * Unregisters a health check provider for a specific endpoint.
   *
   * @param endpoint the health check endpoint path to unregister
   */
  void unregisterHealthCheck(String endpoint);

  /**
   * Gets the health check provider registered for a specific endpoint.
   *
   * @param endpoint the health check endpoint path
   * @return an optional containing the provider if registered, or empty if not
   */
  Optional<HealthCheckProvider> getHealthCheck(String endpoint);

  /**
   * Gets the liveness health check provider if registered.
   *
   * @return an optional containing the liveness provider if registered
   */
  default Optional<HealthCheckProvider> getLivenessCheck() {
    return getHealthCheck("/liveness");
  }

  /**
   * Gets the readiness health check provider if registered.
   *
   * @return an optional containing the readiness provider if registered
   */
  default Optional<HealthCheckProvider> getReadinessCheck() {
    return getHealthCheck("/readiness");
  }

  /** Functional interface for health check providers. */
  @FunctionalInterface
  interface HealthCheckProvider {
    /**
     * Evaluates the health status based on query parameters.
     *
     * @param paramSource the query parameter source from the health check request
     * @return true if healthy, false otherwise
     */
    boolean isHealthy(ParamSource paramSource);
  }

  /** Functional interface for accessing query parameters. */
  @FunctionalInterface
  interface ParamSource {
    /**
     * Gets a parameter value by name.
     *
     * @param name the parameter name
     * @return the parameter value, or null if not present
     */
    String getParam(String name);
  }
}
