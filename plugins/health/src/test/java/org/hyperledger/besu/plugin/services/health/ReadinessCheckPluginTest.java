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
package org.hyperledger.besu.plugin.services.health;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;

import java.util.Optional;

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
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("5");
    when(params.getParam("maxBlocksBehind")).thenReturn("2");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(3);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldPassWhenPeerCountMet() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("3");
    when(params.getParam("maxBlocksBehind")).thenReturn("2");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(5);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldInitializeRuntimeServicesOnStart() {
    when(besuEvents.addSyncStatusListener(any())).thenReturn(1L);

    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    verify(besuEvents).addSyncStatusListener(any());
  }

  @Test
  void shouldRemoveSyncStatusListenerOnStop() {
    when(besuEvents.addSyncStatusListener(any())).thenReturn(1L);

    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();

    plugin.register(serviceManager);
    plugin.start();

    plugin.stop();

    verify(besuEvents).removeSyncStatusListener(1L);
  }

  @Test
  void shouldPassWhenSyncStatusWithinThreshold() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(101L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("1");
    when(params.getParam("maxBlocksBehind")).thenReturn("2");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(1);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldFailWhenSyncStatusExceedsThreshold() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(200L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("1");
    when(params.getParam("maxBlocksBehind")).thenReturn("2");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(1);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldPassWhenHighestBlockEqualsCurrentBlock() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(100L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("1");
    when(params.getParam("maxBlocksBehind")).thenReturn("2");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(1);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldTreatMalformedMinPeersAsUnhealthy() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("not-a-number");
    when(p2pService.getPeerCount()).thenReturn(100);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldTreatMalformedMaxBlocksBehindAsUnhealthy() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(200L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("1");
    when(params.getParam("maxBlocksBehind")).thenReturn("invalid");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(100);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldTreatNegativeMinPeersAsUnhealthy() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("-1");
    when(p2pService.getPeerCount()).thenReturn(100);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldTreatNegativeMaxBlocksBehindAsUnhealthy() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(200L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn("1");
    when(params.getParam("maxBlocksBehind")).thenReturn("-5");
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(100);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldFailByDefaultWhenMoreThanTwoBlocksBehind() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(103L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn(null);
    when(params.getParam("maxBlocksBehind")).thenReturn(null);
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(1);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isFalse();
  }

  @Test
  void shouldPassByDefaultWhenWithinTwoBlocksBehind() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    final SyncStatus syncStatus = mock(SyncStatus.class);
    when(syncStatus.getCurrentBlock()).thenReturn(100L);
    when(syncStatus.getHighestBlock()).thenReturn(102L);
    triggerSyncStatusUpdate(syncStatus);

    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn(null);
    when(params.getParam("maxBlocksBehind")).thenReturn(null);
    when(p2pService.isP2pEnabled()).thenReturn(true);
    when(p2pService.getPeerCount()).thenReturn(1);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @Test
  void shouldSkipPeerCheckWhenP2pDisabled() {
    final ReadinessCheckPlugin plugin = new ReadinessCheckPlugin();
    plugin.register(serviceManager);
    plugin.start();

    final var captor =
        org.mockito.ArgumentCaptor.forClass(HealthCheckService.HealthCheckProvider.class);
    verify(healthCheckService).registerHealthCheck(eq("/readiness"), captor.capture());

    final HealthCheckService.HealthCheckProvider provider = captor.getValue();

    // P2P is disabled — peerCount 0 with the default minPeers threshold (1) must NOT fail
    // readiness because the peer check is gated on isP2pEnabled().
    final HealthCheckService.ParamSource params = mock(HealthCheckService.ParamSource.class);
    when(params.getParam("minPeers")).thenReturn(null);
    when(params.getParam("maxBlocksBehind")).thenReturn(null);
    when(p2pService.isP2pEnabled()).thenReturn(false);
    when(p2pService.getPeerCount()).thenReturn(0);

    org.assertj.core.api.Assertions.assertThat(provider.isHealthy(params)).isTrue();
  }

  @SuppressWarnings("unchecked")
  private void triggerSyncStatusUpdate(final SyncStatus syncStatus) {
    final var listenerCaptor =
        org.mockito.ArgumentCaptor.forClass(BesuEvents.SyncStatusListener.class);
    verify(besuEvents).addSyncStatusListener(listenerCaptor.capture());
    listenerCaptor.getValue().onSyncStatusChanged(Optional.of(syncStatus));
  }
}
