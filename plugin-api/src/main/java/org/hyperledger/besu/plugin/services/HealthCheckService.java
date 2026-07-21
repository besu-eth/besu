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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * This service lets plugins provide custom implementations for Besu's built-in health check
 * endpoints. Registering a provider for {@code /liveness} or {@code /readiness} overrides that
 * endpoint's default implementation.
 */
public interface HealthCheckService extends BesuService {

  /**
   * Registers a health check provider for one of the built-in endpoints, overriding its default
   * implementation. The supported endpoints are {@code /liveness} and {@code /readiness}.
   *
   * @param endpoint the built-in endpoint path to override ({@code /liveness} or {@code
   *     /readiness})
   * @param provider the health check provider that evaluates health status
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

  /** Result of a health check evaluation, including optional diagnostic details. */
  final class HealthCheckResult {
    private final boolean healthy;
    private final Map<String, Object> details;

    /**
     * Creates a health check result.
     *
     * @param healthy whether the check passed
     * @param details diagnostic details to include under {@code checks}; may be null or empty
     */
    public HealthCheckResult(final boolean healthy, final Map<String, Object> details) {
      this.healthy = healthy;
      this.details =
          details == null || details.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(details);
    }

    /**
     * Creates a healthy or unhealthy result with no details.
     *
     * @param healthy whether the check passed
     * @return the result
     */
    public static HealthCheckResult of(final boolean healthy) {
      return new HealthCheckResult(healthy, Collections.emptyMap());
    }

    /** Returns true if healthy. */
    public boolean isHealthy() {
      return healthy;
    }

    /** Returns diagnostic details for the response {@code checks} object. */
    public Map<String, Object> getDetails() {
      return details;
    }
  }

  /** Functional interface for health check providers. */
  @FunctionalInterface
  interface HealthCheckProvider {
    /**
     * Evaluates the health status based on query parameters.
     *
     * @param paramSource the query parameter source from the health check request
     * @return the health check result, including optional structured details
     */
    HealthCheckResult check(ParamSource paramSource);
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
