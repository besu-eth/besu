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

import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.p2p.network.P2PNetwork;
import org.hyperledger.besu.plugin.data.SyncStatus;

import java.util.Optional;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadinessCheck implements HealthService.HealthCheck {
  private static final Logger LOG = LoggerFactory.getLogger(ReadinessCheck.class);
  private static final int DEFAULT_MINIMUM_PEERS = 1;
  private static final int DEFAULT_MAX_BLOCKS_BEHIND = 2;
  private final P2PNetwork p2pNetwork;
  private final Synchronizer synchronizer;

  public ReadinessCheck(final P2PNetwork p2pNetwork, final Synchronizer synchronizer) {
    this.p2pNetwork = p2pNetwork;
    this.synchronizer = synchronizer;
  }

  @Override
  public boolean isHealthy(final HealthService.ParamSource params) {
    return checkHealth(params).isHealthy();
  }

  @Override
  public HealthService.HealthCheckResult checkHealth(final HealthService.ParamSource params) {
    LOG.debug("Invoking readiness check.");
    final JsonObject checks = new JsonObject();
    boolean healthy = true;

    if (p2pNetwork.isP2pEnabled()) {
      final int peerCount = p2pNetwork.getPeerCount();
      final String peersParam = params.getParam("minPeers");
      int requiredPeers = DEFAULT_MINIMUM_PEERS;
      boolean peersOk;
      String peersError = null;
      if (peersParam != null) {
        try {
          requiredPeers = Integer.parseInt(peersParam);
          peersOk = peerCount >= requiredPeers;
        } catch (final NumberFormatException e) {
          LOG.debug("Invalid minPeers: {}. Reporting as not ready.", peersParam);
          peersOk = false;
          peersError = "invalid minPeers parameter: " + peersParam;
        }
      } else {
        peersOk = peerCount >= requiredPeers;
      }
      final JsonObject peersDetail =
          new JsonObject()
              .put("status", peersOk)
              .put("currentPeers", peerCount)
              .put("requiredPeers", requiredPeers);
      if (peersError != null) {
        peersDetail.put("error", peersError);
      }
      checks.put("peers", peersDetail);
      if (!peersOk) {
        healthy = false;
      }
    }

    final Optional<SyncStatus> syncStatusOpt = synchronizer.getSyncStatus();
    if (syncStatusOpt.isPresent()) {
      final SyncStatus syncStatus = syncStatusOpt.get();
      final String maxBlocksBehindParam = params.getParam("maxBlocksBehind");
      long maxBlocksBehind = DEFAULT_MAX_BLOCKS_BEHIND;
      final long blocksBehind = syncStatus.getHighestBlock() - syncStatus.getCurrentBlock();
      boolean syncOk;
      String syncError = null;
      if (maxBlocksBehindParam != null) {
        try {
          maxBlocksBehind = Long.parseLong(maxBlocksBehindParam);
          syncOk = blocksBehind <= maxBlocksBehind;
        } catch (final NumberFormatException e) {
          LOG.debug("Invalid maxBlocksBehind: {}. Reporting as not ready.", maxBlocksBehindParam);
          syncOk = false;
          syncError = "invalid maxBlocksBehind parameter: " + maxBlocksBehindParam;
        }
      } else {
        syncOk = blocksBehind <= maxBlocksBehind;
      }
      final JsonObject syncDetail =
          new JsonObject()
              .put("status", syncOk)
              .put("blocksBehind", blocksBehind)
              .put("maxBlocksBehind", maxBlocksBehind);
      if (syncError != null) {
        syncDetail.put("error", syncError);
      }
      checks.put("sync", syncDetail);
      if (!syncOk) {
        healthy = false;
      }
    }

    return new HealthService.HealthCheckResult(healthy, checks);
  }
}
