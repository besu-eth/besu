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
  private static final long DEFAULT_MAX_BLOCKS_BEHIND = 2L;

  /** Instantiates a new readiness check plugin. */
  public ReadinessCheckPlugin() {}

  /**
   * Safely parses a string to a non-negative integer.
   *
   * @param value the string to parse
   * @param defaultValue the value to return when the input is null or unparseable
   * @return an Optional containing the parsed value if successful and non-negative, or the default
   *     otherwise
   */
  private static Optional<Integer> parseNonNegativeInt(final String value, final int defaultValue) {
    if (value == null) {
      return Optional.of(defaultValue);
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
   * @param defaultValue the value to return when the input is null
   * @return an Optional containing the parsed value if successful and non-negative, the default
   *     when null, or empty for malformed/negative input
   */
  private static Optional<Long> parseNonNegativeLong(final String value, final long defaultValue) {
    if (value == null) {
      return Optional.of(defaultValue);
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
  // Push model: the sync status is fed by a BesuEvents listener registered in start() rather than
  // pulled live from the synchronizer on every health check. Until the first listener callback
  // arrives, cachedSyncStatus is empty, and checkReadiness treats the node as healthy
  // (orElse(true))
  // to avoid a false-negative at startup before any sync event has been delivered.
  private volatile Optional<SyncStatus> cachedSyncStatus = Optional.empty();
  private long syncListenerId = -1;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;

    final HealthCheckService healthCheckService =
        context
            .getService(HealthCheckService.class)
            .orElseThrow(
                () -> new IllegalStateException("Required service missing: HealthCheckService"));

    healthCheckService.registerHealthCheck(READINESS_ENDPOINT, this::checkReadiness);
  }

  @Override
  public void start() {
    this.p2pService =
        context
            .getService(P2PService.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: P2PService"));
    final BesuEvents besuEvents =
        context
            .getService(BesuEvents.class)
            .orElseThrow(() -> new IllegalStateException("Required service missing: BesuEvents"));

    syncListenerId = besuEvents.addSyncStatusListener(status -> cachedSyncStatus = status);
  }

  private boolean checkReadiness(final HealthCheckService.ParamSource params) {
    if (p2pService == null) {
      return false;
    }

    final String minPeersStr = params.getParam("minPeers");
    final Optional<Integer> minPeers = parseNonNegativeInt(minPeersStr, DEFAULT_MIN_PEERS);
    if (minPeers.isEmpty()) {
      return false;
    }
    if (p2pService.isP2pEnabled() && p2pService.getPeerCount() < minPeers.get()) {
      return false;
    }

    final String maxBlocksStr = params.getParam("maxBlocksBehind");
    final Optional<Long> maxBlocksBehind =
        parseNonNegativeLong(maxBlocksStr, DEFAULT_MAX_BLOCKS_BEHIND);
    if (maxBlocksBehind.isEmpty()) {
      return false;
    }
    return cachedSyncStatus
        .map(
            syncStatus -> {
              long highestBlock = syncStatus.getHighestBlock();
              long currentBlock = syncStatus.getCurrentBlock();
              if (currentBlock > Long.MAX_VALUE - maxBlocksBehind.get()) {
                return true;
              }
              return highestBlock <= currentBlock + maxBlocksBehind.get();
            })
        .orElse(true);
  }

  @Override
  public void stop() {
    if (context != null && syncListenerId != -1) {
      context
          .getService(BesuEvents.class)
          .ifPresent(
              besuEvents -> {
                besuEvents.removeSyncStatusListener(syncListenerId);
                syncListenerId = -1;
              });
    }
  }
}
