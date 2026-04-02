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
package org.hyperledger.besu.services;

import org.hyperledger.besu.plugin.services.HealthCheckService;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The health check service implementation. */
public class HealthCheckServiceImpl implements HealthCheckService {

  /** Instantiates a new health check service implementation. */
  public HealthCheckServiceImpl() {}

  private final ConcurrentHashMap<String, HealthCheckProvider> healthChecks =
      new ConcurrentHashMap<>();

  @Override
  public void registerHealthCheck(final String endpoint, final HealthCheckProvider provider) {
    final Object existing = healthChecks.putIfAbsent(endpoint, provider);
    if (existing != null) {
      throw new IllegalArgumentException(
          "Health check already registered for endpoint: " + endpoint);
    }
  }

  @Override
  public void unregisterHealthCheck(final String endpoint) {
    healthChecks.remove(endpoint);
  }

  @Override
  public Optional<HealthCheckProvider> getHealthCheck(final String endpoint) {
    return Optional.ofNullable(healthChecks.get(endpoint));
  }
}
