/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.DefaultSyncStatus;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.plugin.data.SyncStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

public class ReadinessCheckTest {

  private static final String MIN_PEERS_PARAM = "minPeers";
  private final P2PNetwork p2pNetwork = mock(P2PNetwork.class);
  private final Synchronizer synchronizer = mock(Synchronizer.class);

  private final Map<String, String> params = new HashMap<>();
  private final HealthService.ParamSource paramSource = params::get;

  private final ReadinessCheck readinessCheck = new ReadinessCheck(p2pNetwork, synchronizer);

  @Test
  public void shouldBeReadyWhenDefaultLimitsUsedAndReached() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(1);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    assertThat(readinessCheck.isHealthy(paramSource)).isTrue();
  }

  @Test
  public void shouldBeReadyWithNoPeersWhenP2pIsDisabled() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(false);
    when(p2pNetwork.getPeerCount()).thenReturn(0);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    assertThat(readinessCheck.isHealthy(paramSource)).isTrue();
  }

  @Test
  public void shouldBeReadyWithNoPeersWhenMinimumPeersSetToZero() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(0);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    params.put(MIN_PEERS_PARAM, "0");

    assertThat(readinessCheck.isHealthy(paramSource)).isTrue();
  }

  @Test
  public void shouldNotBeReadyWhenIncreasedMinimumPeersNotReached() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    params.put(MIN_PEERS_PARAM, "10");

    assertThat(readinessCheck.isHealthy(paramSource)).isFalse();
  }

  @Test
  public void shouldNotBeReadyWhenMinimumPeersParamIsInvalid() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(500);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    params.put(MIN_PEERS_PARAM, "abc");

    assertThat(readinessCheck.isHealthy(paramSource)).isFalse();
  }

  @Test
  public void shouldBeReadyWhenLessThanDefaultMaxBlocksBehind() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(1000, 1002));

    assertThat(readinessCheck.isHealthy(paramSource)).isTrue();
  }

  @Test
  public void shouldNotBeReadyWhenLessThanDefaultMaxBlocksBehind() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(1000, 1003));

    assertThat(readinessCheck.isHealthy(paramSource)).isFalse();
  }

  @Test
  public void shouldBeReadyWhenLessThanCustomMaxBlocksBehind() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(500, 600));

    params.put("maxBlocksBehind", "100");

    assertThat(readinessCheck.isHealthy(paramSource)).isTrue();
  }

  @Test
  public void shouldNotBeReadyWhenGreaterThanCustomMaxBlocksBehind() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(500, 601));

    params.put("maxBlocksBehind", "100");

    assertThat(readinessCheck.isHealthy(paramSource)).isFalse();
  }

  @Test
  public void shouldNotBeReadyWhenCustomMaxBlocksBehindIsInvalid() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(500, 500));

    params.put("maxBlocksBehind", "abc");

    assertThat(readinessCheck.isHealthy(paramSource)).isFalse();
  }

  @Test
  public void checkHealthShouldIncludePeerDiagnostics() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    final HealthService.HealthCheckResult result = readinessCheck.checkHealth(paramSource);

    assertThat(result.isHealthy()).isTrue();
    final JsonObject peers = result.getDetails().getJsonObject("peers");
    assertThat(peers.getBoolean("status")).isTrue();
    assertThat(peers.getInteger("currentPeers")).isEqualTo(5);
    assertThat(peers.getInteger("requiredPeers")).isEqualTo(1);
  }

  @Test
  public void checkHealthShouldIncludeSyncDiagnostics() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(true);
    when(p2pNetwork.getPeerCount()).thenReturn(5);
    when(synchronizer.getSyncStatus()).thenReturn(createSyncStatus(1000, 1150));

    final HealthService.HealthCheckResult result = readinessCheck.checkHealth(paramSource);

    assertThat(result.isHealthy()).isFalse();
    final JsonObject sync = result.getDetails().getJsonObject("sync");
    assertThat(sync.getBoolean("status")).isFalse();
    assertThat(sync.getLong("blocksBehind")).isEqualTo(150L);
    assertThat(sync.getLong("maxBlocksBehind")).isEqualTo(2L);
  }

  @Test
  public void checkHealthShouldOmitPeersWhenP2pDisabled() {
    when(p2pNetwork.isP2pEnabled()).thenReturn(false);
    when(synchronizer.getSyncStatus()).thenReturn(Optional.empty());

    final HealthService.HealthCheckResult result = readinessCheck.checkHealth(paramSource);

    assertThat(result.isHealthy()).isTrue();
    assertThat(result.getDetails().containsKey("peers")).isFalse();
  }

  private Optional<SyncStatus> createSyncStatus(final int currentBlock, final int highestBlock) {
    return Optional.of(
        new DefaultSyncStatus(0, currentBlock, highestBlock, Optional.empty(), Optional.empty()));
  }
}
