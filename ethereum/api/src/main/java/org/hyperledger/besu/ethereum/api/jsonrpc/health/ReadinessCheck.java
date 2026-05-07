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
    LOG.debug("Invoking readiness check.");
    if (p2pNetwork.isP2pEnabled()) {
      final int peerCount = p2pNetwork.getPeerCount();
      if (!hasMinimumPeers(params, peerCount)) {
        return false;
      }
    }
    return synchronizer
        .getSyncStatus()
        .map(syncStatus -> isInSync(syncStatus, params))
        .orElse(true);
  }

  private boolean isInSync(final SyncStatus syncStatus, final HealthService.ParamSource params) {
    final String maxBlocksBehindParam = params.getParam("maxBlocksBehind");
    final long maxBlocksBehind;
    if (maxBlocksBehindParam == null) {
      maxBlocksBehind = DEFAULT_MAX_BLOCKS_BEHIND;
    } else {
      try {
        maxBlocksBehind = Long.parseLong(maxBlocksBehindParam);
      } catch (final NumberFormatException e) {
        LOG.debug("Invalid maxBlocksBehind: {}. Reporting as not ready.", maxBlocksBehindParam);
        return false;
      }
      if (maxBlocksBehind < 0) {
        LOG.debug("Negative maxBlocksBehind: {}. Reporting as not ready.", maxBlocksBehindParam);
        return false;
      }
    }
    return syncStatus.getHighestBlock() - syncStatus.getCurrentBlock() <= maxBlocksBehind;
  }

  private boolean hasMinimumPeers(final HealthService.ParamSource params, final int peerCount) {
    final int minimumPeers;
    final String peersParam = params.getParam("minPeers");
    if (peersParam == null) {
      minimumPeers = DEFAULT_MINIMUM_PEERS;
    } else {
      try {
        minimumPeers = Integer.parseInt(peersParam);
      } catch (final NumberFormatException e) {
        LOG.debug("Invalid minPeers: {}. Reporting as not ready.", peersParam);
        return false;
      }
      if (minimumPeers < 0) {
        LOG.debug("Negative minPeers: {}. Reporting as not ready.", peersParam);
        return false;
      }
    }
    return peerCount >= minimumPeers;
  }
}
