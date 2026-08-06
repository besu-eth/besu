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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.plugin.services.HealthCheckService;

import java.util.HashMap;
import java.util.Map;

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
    final HealthCheckService.HealthCheckProvider provider =
        params -> HealthCheckService.HealthCheckResult.of(true);

    healthCheckService.registerHealthCheck("/test", provider);

    assertThat(healthCheckService.getHealthCheck("/test")).isPresent();
    assertThat(healthCheckService.getHealthCheck("/test").get()).isSameAs(provider);
  }

  @Test
  void shouldAllowOverridingExistingEndpoint() {
    final HealthCheckService.HealthCheckProvider provider1 =
        params -> HealthCheckService.HealthCheckResult.of(true);
    final HealthCheckService.HealthCheckProvider provider2 =
        params -> HealthCheckService.HealthCheckResult.of(false);
    healthCheckService.registerHealthCheck("/test", provider1);

    healthCheckService.registerHealthCheck("/test", provider2);

    assertThat(healthCheckService.getHealthCheck("/test")).isPresent();
    assertThat(healthCheckService.getHealthCheck("/test").get()).isSameAs(provider2);
  }

  @Test
  void shouldUnregisterHealthCheck() {
    final HealthCheckService.HealthCheckProvider provider =
        params -> HealthCheckService.HealthCheckResult.of(true);
    healthCheckService.registerHealthCheck("/test", provider);

    healthCheckService.unregisterHealthCheck("/test");

    assertThat(healthCheckService.getHealthCheck("/test")).isEmpty();
  }

  @Test
  void healthCheckResultOmitsNullKeysAndValues() {
    final Map<String, Object> details = new HashMap<>();
    details.put("peers", Map.of("status", true));
    details.put("bad", null);
    details.put(null, "x");

    final HealthCheckService.HealthCheckResult result =
        new HealthCheckService.HealthCheckResult(true, details);

    assertThat(result.getDetails()).containsOnlyKeys("peers");
    assertThat(result.getDetails().get("peers")).isEqualTo(Map.of("status", true));
  }

  @Test
  void shouldReturnEmptyWhenGettingNonexistentEndpoint() {
    assertThat(healthCheckService.getHealthCheck("/nonexistent")).isEmpty();
  }

  @Test
  void shouldGetLivenessCheck() {
    final HealthCheckService.HealthCheckProvider provider =
        params -> HealthCheckService.HealthCheckResult.of(true);
    healthCheckService.registerHealthCheck("/liveness", provider);

    assertThat(healthCheckService.getLivenessCheck()).isPresent();
  }

  @Test
  void shouldGetReadinessCheck() {
    final HealthCheckService.HealthCheckProvider provider =
        params -> HealthCheckService.HealthCheckResult.of(true);
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

  @Test
  void healthCheckResultOfTrueIsHealthyWithEmptyDetails() {
    final HealthCheckService.HealthCheckResult result =
        HealthCheckService.HealthCheckResult.of(true);

    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getDetails()).isEmpty();
  }

  @Test
  void healthCheckResultOfFalseIsUnhealthyWithEmptyDetails() {
    final HealthCheckService.HealthCheckResult result =
        HealthCheckService.HealthCheckResult.of(false);

    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getDetails()).isEmpty();
  }

  @Test
  void healthCheckResultTwoArgConstructorStoresDetails() {
    final Map<String, Object> details = Map.of("peers", Map.of("status", false));
    final HealthCheckService.HealthCheckResult result =
        new HealthCheckService.HealthCheckResult(false, details);

    assertThat(result.isHealthy()).isFalse();
    assertThat(result.getDetails()).isEqualTo(details);
  }

  @Test
  void healthCheckResultNullDetailsBecomesEmptyMap() {
    final HealthCheckService.HealthCheckResult result =
        new HealthCheckService.HealthCheckResult(true, null);

    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getDetails()).isEmpty();
  }

  @Test
  void healthCheckResultCopiesDetailsDefensively() {
    final Map<String, Object> details = new HashMap<>();
    details.put("peers", Map.of("status", true));
    final HealthCheckService.HealthCheckResult result =
        new HealthCheckService.HealthCheckResult(true, details);

    details.put("sync", Map.of("status", false));

    assertThat(result.getDetails()).containsOnlyKeys("peers");
    assertThat(result.getDetails()).doesNotContainKey("sync");
  }
}
