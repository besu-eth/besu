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

import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.HealthCheckService;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/** The health check service implementation. */
public class HealthCheckServiceImpl implements HealthCheckService {

  private final ConcurrentHashMap<String, HealthCheckProvider> healthChecks =
      new ConcurrentHashMap<>();
  private final BooleanSupplier registrationOpen;

  /** Instantiates a health check service that accepts registrations at any time. */
  public HealthCheckServiceImpl() {
    this(() -> true);
  }

  /**
   * Instantiates a new health check service implementation.
   *
   * @param registrationOpen whether plugin registration is currently open; health checks registered
   *     or unregistered while it is closed are rejected
   */
  public HealthCheckServiceImpl(final BooleanSupplier registrationOpen) {
    this.registrationOpen = registrationOpen;
  }

  @Override
  public void registerHealthCheck(final String endpoint, final HealthCheckProvider provider) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("HealthCheckService", "registerHealthCheck");
    }
    healthChecks.put(endpoint, provider);
  }

  @Override
  public void unregisterHealthCheck(final String endpoint) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("HealthCheckService", "unregisterHealthCheck");
    }
    healthChecks.remove(endpoint);
  }

  @Override
  public Optional<HealthCheckProvider> getHealthCheck(final String endpoint) {
    return Optional.ofNullable(healthChecks.get(endpoint));
  }
}
