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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The health check service implementation. Unlike the other named registrations, a health check
 * replaces the one registered before it for the same endpoint: that is how a plugin takes over a
 * default check. The replacement is logged with both registrants so a plugin never loses its check
 * silently.
 */
public class HealthCheckServiceImpl implements HealthCheckService {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServiceImpl.class);

  /** The registrant of an entry Besu registered itself, outside any plugin's register(). */
  private static final String BESU = "Besu";

  /** A registered provider with the id of the plugin that registered it. */
  private record Registered(String registrant, HealthCheckProvider provider) {}

  private final ConcurrentHashMap<String, Registered> healthChecks = new ConcurrentHashMap<>();
  private final BooleanSupplier registrationOpen;
  private final Supplier<Optional<String>> currentRegistrant;

  /** Instantiates a health check service that accepts registrations at any time. */
  public HealthCheckServiceImpl() {
    this(() -> true);
  }

  /**
   * Instantiates a health check service whose registrations all count as Besu's own.
   *
   * @param registrationOpen whether plugin registration is currently open; health checks registered
   *     or unregistered while it is closed are rejected
   */
  public HealthCheckServiceImpl(final BooleanSupplier registrationOpen) {
    this(registrationOpen, Optional::empty);
  }

  /**
   * Instantiates a new health check service implementation.
   *
   * @param registrationOpen whether plugin registration is currently open; health checks registered
   *     or unregistered while it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; named in the log when a check replaces another registrant's
   */
  public HealthCheckServiceImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this.registrationOpen = registrationOpen;
    this.currentRegistrant = currentRegistrant;
  }

  @Override
  public void registerHealthCheck(final String endpoint, final HealthCheckProvider provider) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("HealthCheckService", "registerHealthCheck");
    }
    final String registrant = currentRegistrant.get().orElse(BESU);
    final Registered previous = healthChecks.put(endpoint, new Registered(registrant, provider));
    if (previous != null && !previous.registrant().equals(registrant)) {
      LOG.warn(
          "Health check {} is now served by {}, replacing the check {} registered",
          endpoint,
          owner(registrant),
          owner(previous.registrant()));
    }
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
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
    return Optional.ofNullable(healthChecks.get(endpoint)).map(Registered::provider);
  }
}
