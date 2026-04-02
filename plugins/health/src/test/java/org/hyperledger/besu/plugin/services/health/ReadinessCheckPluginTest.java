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
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReadinessCheckPluginTest {

  private ServiceManager serviceManager;
  private HealthCheckService healthCheckService;
  private P2PService p2pService;
  private BesuEvents besuEvents;

  @BeforeEach
  void setUp() {
    serviceManager = mock(ServiceManager.class);
    healthCheckService = mock(HealthCheckService.class);
    p2pService = mock(P2PService.class);
    besuEvents = mock(BesuEvents.class);
    when(serviceManager.getService(HealthCheckService.class))
        .thenReturn(java.util.Optional.of(healthCheckService));
    when(serviceManager.getService(P2PService.class)).thenReturn(java.util.Optional.of(p2pService));
    when(serviceManager.getService(BesuEvents.class)).thenReturn(java.util.Optional.of(besuEvents));
  }

  @Test
  void shouldRegisterReadinessCheck() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);

    verify(healthCheckService)
        .registerHealthCheck(eq("/readiness"), any(HealthCheckService.HealthCheckProvider.class));
  }

  @Test
  void shouldCheckPeerCount() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("5");
    when(p2pService.getPeerCount()).thenReturn(3);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldPassWhenPeerCountMet() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("3");
    when(p2pService.getPeerCount()).thenReturn(5);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldHaveNoOpStart() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.start();
  }

  @Test
  void shouldRemoveSyncStatusListenerOnStop() {
    when(besuEvents.addSyncStatusListener(any())).thenReturn(1L);

    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);

    plugin.stop();

    verify(besuEvents).removeSyncStatusListener(1L);
  }
}
