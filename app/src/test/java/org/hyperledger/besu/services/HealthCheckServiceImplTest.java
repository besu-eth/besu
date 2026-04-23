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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.plugin.services.HealthCheckService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HealthCheckServiceImplTest {

  private HealthCheckServiceImpl healthCheckService;

  @BeforeEach
  void setUp() {
    healthCheckService = new HealthCheckServiceImpl();
  }

  @Test
  void shouldRegisterHealthCheck() {
    final HealthCheckService.HealthCheckProvider provider = params -> true;

    healthCheckService.registerHealthCheck("/test", provider);

    assertThat(healthCheckService.getHealthCheck("/test")).isPresent();
    assertThat(healthCheckService.getHealthCheck("/test").get()).isSameAs(provider);
  }

  @Test
  void shouldThrowWhenRegisteringDuplicateEndpoint() {
    final HealthCheckService.HealthCheckProvider provider = params -> true;
    healthCheckService.registerHealthCheck("/test", provider);

    assertThatThrownBy(() -> healthCheckService.registerHealthCheck("/test", provider))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("/test");
  }

  @Test
  void shouldUnregisterHealthCheck() {
    final HealthCheckService.HealthCheckProvider provider = params -> true;
    healthCheckService.registerHealthCheck("/test", provider);

    healthCheckService.unregisterHealthCheck("/test");

    assertThat(healthCheckService.getHealthCheck("/test")).isEmpty();
  }

  @Test
  void shouldReturnEmptyWhenGettingNonexistentEndpoint() {
    assertThat(healthCheckService.getHealthCheck("/nonexistent")).isEmpty();
  }

  @Test
  void shouldGetLivenessCheck() {
    final HealthCheckService.HealthCheckProvider provider = params -> true;
    healthCheckService.registerHealthCheck("/liveness", provider);

    assertThat(healthCheckService.getLivenessCheck()).isPresent();
  }

  @Test
  void shouldGetReadinessCheck() {
    final HealthCheckService.HealthCheckProvider provider = params -> true;
    healthCheckService.registerHealthCheck("/readiness", provider);

    assertThat(healthCheckService.getReadinessCheck()).isPresent();
  }

  @Test
  void shouldReturnEmptyForLivenessWhenNotRegistered() {
    assertThat(healthCheckService.getLivenessCheck()).isEmpty();
  }

  @Test
  void shouldReturnEmptyForReadinessWhenNotRegistered() {
    assertThat(healthCheckService.getReadinessCheck()).isEmpty();
  }
}
