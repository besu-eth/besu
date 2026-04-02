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
package org.hyperledger.besu.plugin.services.health;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.HealthCheckService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LivenessCheckPluginTest {

  private ServiceManager serviceManager;
  private HealthCheckService healthCheckService;

  @BeforeEach
  void setUp() {
    serviceManager = mock(ServiceManager.class);
    healthCheckService = mock(HealthCheckService.class);
    when(serviceManager.getService(HealthCheckService.class))
        .thenReturn(java.util.Optional.of(healthCheckService));
  }

  @Test
  void shouldRegisterLivenessCheck() {
    final LivenessCheckPlugin plugin = new LivenessCheckPlugin();

    plugin.register(serviceManager);

    verify(healthCheckService)
        .registerHealthCheck(eq("/liveness"), any(HealthCheckService.HealthCheckProvider.class));
  }

  @Test
  void shouldAlwaysReturnTrueForLiveness() {
    final LivenessCheckPlugin plugin = new LivenessCheckPlugin();

    plugin.register(serviceManager);

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/liveness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam(any())).thenReturn(null);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldHaveNoOpStartAndStop() {
    final LivenessCheckPlugin plugin = new LivenessCheckPlugin();

    plugin.start();
    plugin.stop();
  }
}
