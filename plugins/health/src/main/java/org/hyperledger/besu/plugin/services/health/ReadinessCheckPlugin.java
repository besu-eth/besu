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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;

import java.util.Optional;

/** The readiness check plugin. */
public class ReadinessCheckPlugin implements BesuPlugin {

  private static final String READINESS_ENDPOINT = "/readiness";
  private static final int DEFAULT_MIN_PEERS = 1;
  private static final long DEFAULT_MAX_BLOCKS_BEHIND = 2;

  /** Instantiates a new readiness check plugin. */
  public ReadinessCheckPlugin() {}

  /**
   * Safely parses a string to a non-negative integer.
   *
   * @param value the string to parse
   * @return an Optional containing the parsed value if successful and non-negative, or empty
   *     otherwise
   */
  private static Optional<Integer> parseNonNegativeInt(final String value) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      int parsed = Integer.parseInt(value);
      return (parsed >= 0) ? Optional.of(parsed) : Optional.empty();
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /**
   * Safely parses a string to a non-negative long.
   *
   * @param value the string to parse
   * @return an Optional containing the parsed value if successful and non-negative, or empty
   *     otherwise
   */
  private static Optional<Long> parseNonNegativeLong(final String value) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      long parsed = Long.parseLong(value);
      return (parsed >= 0) ? Optional.of(parsed) : Optional.empty();
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private ServiceManager context;
  private P2PService p2pService;
  private volatile Optional<SyncStatus> cachedSyncStatus = Optional.empty();
  private long syncListenerId;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;

    this.p2pService =
        context
            .getService(P2PService.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: P2PService"));
    final BesuEvents besuEvents =
        context
            .getService(BesuEvents.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: BesuEvents"));
    final HealthCheckService healthCheckService =
        context
            .getService(HealthCheckService.class)
            .orElseThrow(
                () -> new IllegalStateException("Required service missing: HealthCheckService"));

    syncListenerId = besuEvents.addSyncStatusListener(status -> cachedSyncStatus = status);
    healthCheckService.registerHealthCheck(READINESS_ENDPOINT, this::checkReadiness);
  }

  private boolean checkReadiness(final HealthCheckService.ParamSource params) {
    final int minPeers = parseNonNegativeInt(params.getParam("minPeers")).orElse(DEFAULT_MIN_PEERS);
    if (p2pService.getPeerCount() < minPeers) {
      return false;
    }

    final long maxBlocksBehind =
        parseNonNegativeLong(params.getParam("maxBlocksBehind")).orElse(DEFAULT_MAX_BLOCKS_BEHIND);
    return cachedSyncStatus
        .map(
            syncStatus -> {
              long highestBlock = syncStatus.getHighestBlock();
              long currentBlock = syncStatus.getCurrentBlock();
              // Check for overflow: if currentBlock > Long.MAX_VALUE - maxBlocksBehind,
              // then currentBlock + maxBlocksBehind would overflow
              if (currentBlock > Long.MAX_VALUE - maxBlocksBehind) {
                return true; // Treat as healthy if we would overflow (conservative approach)
              }
              // Safe comparison: highestBlock <= currentBlock + maxBlocksBehind
              return highestBlock <= currentBlock + maxBlocksBehind;
            })
        .orElse(true);
  }

  @Override
  public void start() {}

  @Override
  public void stop() {
    if (context != null) {
      context
          .getService(BesuEvents.class)
          .ifPresent(besuEvents -> besuEvents.removeSyncStatusListener(syncListenerId));
    }
  }
}
